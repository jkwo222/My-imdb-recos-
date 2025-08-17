# engine/summarize.py
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------- helpers ----------

def _load_json(p: Optional[Path]) -> Any:
    if not p:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

def _as_list(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v) for v in x]
    if isinstance(x, str):
        return [s.strip() for s in x.split(",") if s.strip()]
    return [str(x)]

def _score(it: Dict[str, Any]) -> float:
    try:
        return float(it.get("match", it.get("score", 0.0)) or 0.0)
    except Exception:
        return 0.0

def _aud_0_100(it: Dict[str, Any]) -> float:
    # Prefer normalized audience; else tmdb_vote (0..10) → 0..100
    for k in ("audience", "tmdb_vote"):
        v = it.get(k)
        try:
            f = float(v)
        except Exception:
            continue
        if f <= 10.0:
            f *= 10.0
        return max(0.0, min(100.0, f))
    return 50.0

def _providers(it: Dict[str, Any]) -> List[str]:
    p = it.get("providers") or it.get("providers_slugs") or []
    return [s for s in p if isinstance(s, str)]

# Icon + friendly-name label for providers
def _provider_label(slug: str) -> str:
    if not isinstance(slug, str):
        return str(slug)
    s = slug.strip().lower()
    ICONS = {
        "apple_tv_plus": "🍎 Apple TV+",
        "prime_video":  "🟦 Prime Video",
        "disney_plus":  "✨ Disney+",
        "paramount_plus":"⛰️ Paramount+",
        "max":          "🟪 Max",
        "netflix":      "🟥 Netflix",
        "hulu":         "🟩 Hulu",
        "peacock":      "🦚 Peacock",
        "peacock_premium":"🦚 Peacock Premium",
        "starz":        "⭐ STARZ",
        "showtime":     "🎞️ Showtime",
        "amc_plus":     "🎬 AMC+",
        "criterion_channel": "🎞️ Criterion Channel",
        "mubi":         "🎥 MUBI",
    }
    if s in ICONS:
        return ICONS[s]
    # Fallback: Title Case with a camera emoji
    t = " ".join(w.capitalize() for w in s.split("_"))
    return f"🎬 {t}"

def _fmt_providers(provs: List[str], maxn: int = 3) -> str:
    if not provs:
        return "—"
    friendly = [_provider_label(p) for p in provs]
    short = friendly[:maxn]
    return ", ".join(short) + ("…" if len(friendly) > maxn else "")

def _imdb_link(it: Dict[str, Any]) -> Optional[str]:
    imdb = it.get("imdb_id")
    return f"https://www.imdb.com/title/{imdb}/" if isinstance(imdb, str) and imdb else None

def _tmdb_link(it: Dict[str, Any]) -> Optional[str]:
    tid = it.get("tmdb_id")
    kind = (it.get("media_type") or "").lower()
    if not tid or not kind:
        return None
    try:
        tid = int(tid)
    except Exception:
        return None
    return f"https://www.themoviedb.org/{'movie' if kind=='movie' else 'tv'}/{tid}"

def _emoji_for_kind(kind: str) -> str:
    kind = (kind or "").lower()
    return "🍿" if kind == "movie" else "📺"

def _bullet(it: Dict[str, Any]) -> str:
    kind = it.get("media_type") or ""
    emoji = _emoji_for_kind(kind)
    title = it.get("title") or it.get("name") or "—"
    year = it.get("year") or ""
    sc = _score(it)
    aud = _aud_0_100(it)
    prov = _fmt_providers(_providers(it))
    why = (it.get("why") or "").strip()

    links = []
    imdb = _imdb_link(it)
    tmdb = _tmdb_link(it)
    if imdb: links.append(f"[IMDb]({imdb})")
    if tmdb: links.append(f"[TMDB]({tmdb})")
    link_s = " • ".join(links)

    main = f"{emoji} **{title}** ({year}) — **Match {sc:.0f}** | Audience {aud:.0f} | {prov}"
    if why:
        main += f" — _{why}_"
    if link_s:
        main += f" — {link_s}"
    return f"- {main}"

def _pick_top(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_score, reverse=True)[:n]

def _read_ratings_csv(p: Path) -> Tuple[int, Dict[str, int]]:
    """
    Read ratings.csv (optional). Return (row_count, simple genre counts) if a 'genres' column exists.
    """
    if not p.exists():
        return 0, {}
    import re
    sep = re.compile(r"[|,/;+]")
    n, g = 0, {}
    with p.open("r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            n += 1
            gs = row.get("genres") or row.get("Genres") or ""
            for tok in (t.strip() for t in sep.split(gs)):
                if tok:
                    g[tok] = g.get(tok, 0) + 1
    return n, dict(sorted(g.items(), key=lambda kv: kv[1], reverse=True))

# ---------- digest builder ----------

def build_digest(
    items: List[Dict[str, Any]],
    diag: Dict[str, Any],
    ratings_csv: Optional[Path],
    top_n: int = 12
) -> str:
    """Create a compact markdown digest with ONLY 'Top Picks' and a telemetry block."""
    env = (diag or {}).get("env", {}) if isinstance(diag, dict) else {}
    subs = _as_list(env.get("SUBS_INCLUDE"))
    region = env.get("REGION", "US")
    langs = env.get("ORIGINAL_LANGS", [])
    pages = env.get("DISCOVER_PAGES", 0)
    prov_map = env.get("PROVIDER_MAP", {})
    prov_unmatched = env.get("PROVIDER_UNMATCHED", [])
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    ran_at = (diag or {}).get("ran_at_utc")
    run_sec = (diag or {}).get("run_seconds")

    discovered = env.get("DISCOVERED_COUNT", None)
    eligible = env.get("ELIGIBLE_COUNT", None)

    # selections
    picks = _pick_top(items, top_n)

    # taste profile (optional)
    ratings_rows, genre_counter = (0, {})
    if ratings_csv and ratings_csv.exists():
        try:
            ratings_rows, genre_counter = _read_ratings_csv(ratings_csv)
        except Exception:
            pass

    lines: List[str] = []
    lines.append(f"### 🎬 Top Picks ({region})\n")
    if ratings_rows:
        top_gen = ", ".join([f"{g}×{c}" for g, c in list(genre_counter.items())[:6]])
        lines.append(f"_Taste profile (from your ratings.csv, {ratings_rows} rows):_ {top_gen}\n")

    if picks:
        for it in picks:
            lines.append(_bullet(it))
        lines.append("")
    else:
        lines.append("_No items to show._\n")

    # Telemetry (expanded + pool growth deltas)
    lines.append("### 📊 Telemetry")
    if ran_at is not None:
        lines.append(f"- Ran at (UTC): **{ran_at}**" + (f" — {run_sec:.1f}s" if isinstance(run_sec, (int, float)) else ""))
    lines.append(f"- Region: **{region}**")
    if langs:
        lines.append(f"- Original languages: `{', '.join(langs)}`")
    lines.append(f"- SUBS_INCLUDE: `{', '.join(subs) if subs else '—'}`")
    lines.append(f"- Discover pages: **{pages}**")
    if discovered is not None:
        lines.append(f"- Discovered (raw): **{discovered}**")
    if eligible is not None:
        lines.append(f"- Eligible after exclusions: **{eligible}**")
    lines.append(f"- Provider map: `{json.dumps(prov_map, ensure_ascii=False)}`")
    if prov_unmatched:
        lines.append(f"- Provider slugs not matched: `{prov_unmatched}`")

    if pool_t:
        before = pool_t.get("file_lines_before")
        after = pool_t.get("file_lines_after")
        appended = pool_t.get("appended_this_run")
        delta = (after - before) if isinstance(before, int) and isinstance(after, int) else appended
        lines.append(f"- Pool growth: **{('+'+str(delta)) if isinstance(delta, int) else '—'} this run**")
        lines.append(f"- Pool size (lines): **{before} → {after}**")
        lines.append(f"- Appended records this run: **{appended}**")
        lines.append(f"- Loaded unique from pool: **{pool_t.get('loaded_unique')}** / cap **{pool_t.get('pool_max_items')}**")
        if pool_t.get("unique_keys_est") is not None:
            lines.append(f"- Unique keys (est): **{pool_t.get('unique_keys_est')}**")
        if pool_t.get("prune_at"):
            lines.append(f"- Prune policy: prune_at={pool_t.get('prune_at')}, keep={pool_t.get('prune_keep')}")

    return "\n".join(lines).strip() + "\n"

# ---------- CLI ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="Produce a compact Top Picks digest into summary.md")
    ap.add_argument("--in", dest="inp", required=True, help="items.enriched.json (or assistant_feed.json)")
    ap.add_argument("--diag", dest="diag", required=False, help="diag.json (runner telemetry)")
    ap.add_argument("--ratings", dest="ratings", required=False, help="data/user/ratings.csv (optional)")
    ap.add_argument("--out", dest="out", required=True, help="output markdown (summary.md)")
    ap.add_argument("--top", dest="top", type=int, default=12, help="Top-N picks to list")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)
    diagp = Path(args.diag) if args.diag else None
    ratingsp = Path(args.ratings) if args.ratings else None

    items = _load_json(inp) or []
    if not isinstance(items, list):
        items = []
    diag = _load_json(diagp) if (diagp and diagp.exists()) else {}
    body = build_digest(items, diag, ratingsp if (ratingsp and ratingsp.exists()) else None, top_n=args.top)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(body, encoding="utf-8")

if __name__ == "__main__":
    main()