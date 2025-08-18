# engine/summarize.py
from __future__ import annotations
import argparse, csv, json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

def _load_json(p: Optional[Path]) -> Any:
    if not p: return None
    try: return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception: return None

def _as_list(x) -> List[str]:
    if x is None: return []
    if isinstance(x, list): return [str(v) for v in x]
    if isinstance(x, str): return [s.strip() for s in x.split(",") if s.strip()]
    return [str(x)]

def _score(it: Dict[str, Any]) -> float:
    try: return float(it.get("match", it.get("score", 0.0)) or 0.0)
    except Exception: return 0.0

def _aud_0_100(it: Dict[str, Any]) -> float:
    for k in ("audience","tmdb_vote"):
        v = it.get(k)
        try:
            f = float(v)
            if f <= 10.0: f *= 10.0
            return max(0.0, min(100.0, f))
        except Exception: pass
    return 50.0

def _providers(it: Dict[str, Any]) -> List[str]:
    p = it.get("providers") or it.get("providers_slugs") or []
    return [s for s in p if isinstance(s, str)]

def _provider_label(slug: str) -> str:
    if not isinstance(slug, str): return str(slug)
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
    if s in ICONS: return ICONS[s]
    t = " ".join(w.capitalize() for w in s.split("_"))
    return f"🎬 {t}"

def _fmt_providers(provs: List[str], maxn: int = 3) -> str:
    if not provs: return "—"
    friendly = [_provider_label(p) for p in provs]
    short = friendly[:maxn]
    return ", ".join(short) + ("…" if len(friendly) > maxn else "")

def _imdb_link(it: Dict[str, Any]) -> Optional[str]:
    imdb = it.get("imdb_id")
    return f"https://www.imdb.com/title/{imdb}/" if isinstance(imdb, str) and imdb else None

def _tmdb_link(it: Dict[str, Any]) -> Optional[str]:
    tid = it.get("tmdb_id"); kind = (it.get("media_type") or "").lower()
    if not tid or not kind: return None
    try: tid = int(tid)
    except Exception: return None
    return f"https://www.themoviedb.org/{'movie' if kind=='movie' else 'tv'}/{tid}"

def _emoji_for_kind(kind: str) -> str:
    return "🍿" if (kind or "").lower()=="movie" else "📺"

def _bullet(it: Dict[str, Any]) -> str:
    emoji = _emoji_for_kind(it.get("media_type") or "")
    title = it.get("title") or it.get("name") or "—"
    year = it.get("year") or ""
    sc = _score(it); aud = _aud_0_100(it)
    prov = _fmt_providers(_providers(it))
    why = (it.get("why") or "").strip()
    links = []
    imdb = _imdb_link(it); tmdb = _tmdb_link(it)
    if imdb: links.append(f"[IMDb]({imdb})")
    if tmdb: links.append(f"[TMDB]({tmdb})")
    link_s = " • ".join(links)
    main = f"{emoji} **{title}** ({year}) — **Match {sc:.0f}** | Audience {aud:.0f} | {prov}"
    if why: main += f" — _{why}_"
    if link_s: main += f" — {link_s}"
    return f"- {main}"

def _pick_top(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_score, reverse=True)[:n]

def _read_ratings_csv(p: Path) -> Tuple[int, Dict[str, int]]:
    if not p.exists(): return 0, {}
    import re
    sep = re.compile(r"[|,/;+]"); n, g = 0, {}
    with p.open("r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            n += 1
            gs = row.get("genres") or row.get("Genres") or ""
            for tok in (t.strip() for t in sep.split(gs)):
                if tok: g[tok] = g.get(tok, 0) + 1
    return n, dict(sorted(g.items(), key=lambda kv: kv[1], reverse=True))

def build_digest(items: List[Dict[str, Any]], diag: Dict[str, Any], ratings_csv: Optional[Path], top_n: int = 12) -> str:
    env = (diag or {}).get("env", {}) if isinstance(diag, dict) else {}
    subs = _as_list(env.get("SUBS_INCLUDE"))
    region = env.get("REGION", "US")
    langs = env.get("ORIGINAL_LANGS", [])
    pages = env.get("DISCOVER_PAGES", 0)
    prov_map = env.get("PROVIDER_MAP", {})
    prov_unmatched = env.get("PROVIDER_UNMATCHED", [])
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    ran_at = (diag or {}).get("ran_at_utc"); run_sec = (diag or {}).get("run_seconds")
    discovered = env.get("DISCOVERED_COUNT", None); eligible = env.get("ELIGIBLE_COUNT", None)

    picks = _pick_top(items, top_n)

    ratings_rows, genre_counter = (0, {})
    if ratings_csv and ratings_csv.exists():
        try: ratings_rows, genre_counter = _read_ratings_csv(ratings_csv)
        except Exception: pass

    lines: List[str] = []
    lines.append(f"### 🎬 Top Picks ({region})\n")
    if ratings_rows:
        top_gen = ", ".join([f\"{g}×{c}\" for g, c in list(genre_counter.items())[:6]])
        lines.append(f"_Taste profile (from your ratings.csv, {ratings_rows} rows):_ {top_gen}\n")

    if picks:
        for it in picks: lines.append(_bullet(it))
        lines.append("")
    else:
        lines.append("_No items to show._\n")

    # Telemetry with pool growth deltas
    lines.append("### 📊 Telemetry")
    if ran_at is not None:
        lines.append(f"- Ran at (UTC): **{ran_at}**" + (f" — {run_sec:.1f}s" if isinstance(run_sec, (int, float)) else ""))
    lines.append(f"- Region: **{region}**")
    if langs: lines.append(f"- Original languages: `{', '.join(langs)}`")
    lines.append(f"- SUBS_INCLUDE: `{', '.join(subs) if subs else '—'}`")
    lines.append(f"- Discover pages: **{pages}**")
    if discovered is not None: lines.append(f"- Discovered (raw): **{discovered}**")
    if eligible is not None: lines.append(f"- Eligible after exclusions: **{eligible}**")
    lines.append(f"- Provider map: `{json.dumps(prov_map, ensure_ascii=False)}`")
    if prov_unmatched: lines.append(f"- Provider slugs not matched: `{prov_unmatched}`")

    if pool_t:
        before = pool_t.get("file_lines_before"); after = pool_t.get("file_lines_after"); appended = pool_t.get("appended_this_run")
        delta = (after - before) if isinstance(before, int) and isinstance(after, int) else appended
        lines.append(f"- Pool growth: **{('+'+str(delta)) if isinstance(delta, int) else '—'} this run**")
        lines.append(f"- Pool size (lines): **{before} → {after}**")
        lines.append(f"- Appended records this run: **{appended}**")
        lines.append(f"- Loaded unique from pool: **{pool_t.get('loaded_unique')}** / cap **{pool_t.get('pool_max_items')}**")
        if pool_t.get('unique_keys_est') is not None:
            lines.append(f"- Unique keys (est): **{pool_t.get('unique_keys_est')}**")
        if pool_t.get("prune_at"):
            lines.append(f"- Prune policy: prune_at={pool_t.get('prune_at')}, keep={pool_t.get('prune_keep')}")

    return "\n".join(lines).strip() + "\n"

def main() -> None:
    ap = argparse.ArgumentParser(description="Produce a compact Top Picks digest into summary.md")
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--diag", dest="diag", required=False)
    ap.add_argument("--ratings", dest="ratings", required=False)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--top", dest="top", type=int, default=12)
    args = ap.parse_args()

    inp = Path(args.inp); outp = Path(args.out)
    diagp = Path(args.diag) if args.diag else None
    ratingsp = Path(args.ratings) if args.ratings else None

    items = _load_json(inp) or []; items = items if isinstance(items, list) else []
    diag = _load_json(diagp) if (diagp and diagp.exists()) else {}
    body = build_digest(items, diag, ratingsp if (ratingsp and ratingsp.exists()) else None, top_n=args.top)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(body, encoding="utf-8")

if __name__ == "__main__":
    main()