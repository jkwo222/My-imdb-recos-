# engine/summarize.py
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Strict set of services to display / filter on
ALLOWED_SUBS = {
    "apple_tv_plus", "netflix", "max", "paramount_plus",
    "disney_plus", "peacock", "hulu",
}

FRIENDLY = {
    "apple_tv_plus": "Apple TV+",
    "netflix": "Netflix",
    "max": "Max",
    "paramount_plus": "Paramount+",
    "disney_plus": "Disney+",
    "peacock": "Peacock",
    "hulu": "Hulu",
    # common extras (ignored unless also in ALLOWED_SUBS)
    "prime_video": "Prime Video",
    "peacock_premium": "Peacock Premium",
    "starz": "STARZ",
    "showtime": "Showtime",
    "amc_plus": "AMC+",
    "criterion_channel": "Criterion Channel",
    "mubi": "MUBI",
}

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

def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    # Normalize HBO variants to 'max'
    if s in {"hbo_max", "hbomax", "hbo"}:
        return "max"
    return s

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

def _providers_slugs(it: Dict[str, Any]) -> List[str]:
    p = it.get("providers") or it.get("providers_slugs") or []
    return [s for s in p if isinstance(s, str)]

def _allowed_from_env(diag_env: Dict[str, Any]) -> List[str]:
    # Respect SUBS_INCLUDE but restrict strictly to our ALLOWED_SUBS set
    subs = [_normalize_slug(s) for s in _as_list(diag_env.get("SUBS_INCLUDE"))]
    if not subs:
        subs = list(ALLOWED_SUBS)
    allowed = [s for s in subs if s in ALLOWED_SUBS]
    # Fallback to all seven if nothing intersected
    return allowed or list(ALLOWED_SUBS)

def _eligible_item(it: Dict[str, Any], allowed: List[str]) -> bool:
    provs = [_normalize_slug(p) for p in _providers_slugs(it)]
    if not provs:
        return False
    allowed_set = set(allowed)
    # Keep only titles available on at least one allowed provider
    return any(p in allowed_set for p in provs)

def _fmt_providers_bold(it: Dict[str, Any], allowed: List[str], maxn: int = 3) -> Optional[str]:
    provs = [_normalize_slug(p) for p in _providers_slugs(it)]
    if not provs:
        return None
    allowed_set = set(allowed)
    kept = [p for p in provs if p in allowed_set]
    if not kept:
        return None
    names = [FRIENDLY.get(p, " ".join(w.capitalize() for w in p.split("_"))) for p in kept]
    names = [f"**{n}**" for n in names]
    short = names[:maxn]
    return ", ".join(short) + ("…" if len(names) > maxn else "")

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

def _bullet(it: Dict[str, Any], allowed: List[str]) -> Optional[str]:
    if not _eligible_item(it, allowed):
        return None
    title = it.get("title") or it.get("name") or "—"
    year = it.get("year") or ""
    sc = _score(it)
    aud = _aud_0_100(it)
    kind = it.get("media_type") or ""
    emoji = _emoji_for_kind(kind)

    prov_bold = _fmt_providers_bold(it, allowed, maxn=3)
    if not prov_bold:
        return None

    why = (it.get("why") or "").strip()
    links = []
    imdb = _imdb_link(it)
    tmdb = _tmdb_link(it)
    if imdb: links.append(f"[IMDb]({imdb})")
    if tmdb: links.append(f"[TMDB]({tmdb})")
    link_s = " • ".join(links)

    main = f"{emoji} **{title}** ({year}) — **Match {sc:.0f}** | Audience {aud:.0f} | {prov_bold}"
    if why:
        main += f" — _{why}_"
    if link_s:
        main += f" — {link_s}"
    return f"- {main}"

def _pick_top(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_score, reverse=True)[:n]

def _read_ratings_csv(p: Path) -> Tuple[int, Dict[str, int]]:
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

def build_digest(items: List[Dict[str, Any]], diag: Dict[str, Any], ratings_csv: Optional[Path], top_n: int = 12) -> str:
    env = (diag or {}).get("env", {}) if isinstance(diag, dict) else {}
    allowed = _allowed_from_env(env)

    region = env.get("REGION", "US")
    langs = _as_list(env.get("ORIGINAL_LANGS"))
    pages = env.get("DISCOVER_PAGES", 0)
    prov_map = env.get("PROVIDER_MAP", {})
    prov_unmatched = env.get("PROVIDER_UNMATCHED", [])
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    ran_at = (diag or {}).get("ran_at_utc")
    run_sec = (diag or {}).get("run_seconds")
    discovered = env.get("DISCOVERED_COUNT", None)
    eligible_pre = env.get("ELIGIBLE_COUNT", None)

    # Filter to allowed services only
    filtered = [it for it in items if _eligible_item(it, allowed)]

    # selections
    picks = _pick_top(filtered, top_n)

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
            b = _bullet(it, allowed)
            if b:
                lines.append(b)
        lines.append("")
    else:
        lines.append("_No items to show on your services right now._\n")

    # Telemetry (exclusions + pool growth deltas)
    lines.append("### 📊 Telemetry")
    if ran_at is not None:
        lines.append(f"- Ran at (UTC): **{ran_at}**" + (f" — {run_sec:.1f}s" if isinstance(run_sec, (int, float)) else ""))
    lines.append(f"- Region: **{region}**")
    if langs:
        lines.append(f"- Original languages: `{', '.join(langs)}`")
    lines.append(f"- SUBS_INCLUDE (effective): `{', '.join(allowed)}`")
    lines.append(f"- Discover pages: **{pages}**")
    if discovered is not None:
        lines.append(f"- Discovered (raw): **{discovered}**")
    if eligible_pre is not None:
        lines.append(f"- Eligible before service filter: **{eligible_pre}**")
    lines.append(f"- Eligible after service filter: **{len(filtered)}**")
    lines.append(f"- Provider map: `{json.dumps(prov_map, ensure_ascii=False)}`")
    if prov_unmatched:
        lines.append(f"- Provider slugs not matched: `{prov_unmatched}`")

    # NEW: strict-exclusion counts
    excl = (diag or {}).get("env", {}).get("EXCLUSIONS", {})
    if excl:
        lines.append(f"- Excluded as seen: **{excl.get('excluded_count', 0)}** "
                     f"(ratings_ids~{excl.get('ratings_rows', 0)}, public_ids={excl.get('public_ids', 0)})")

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