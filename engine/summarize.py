# engine/summarize.py
from __future__ import annotations
import argparse
import csv
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

def _load_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

def _coerce_list(x) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        return [s.strip() for s in x.split(",") if s.strip()]
    return [x]

def _audience_0_100(it: Dict[str, Any]) -> float:
    # Prefer normalized audience if present; else coerce tmdb_vote (0..10) -> 0..100
    aud = it.get("audience")
    try:
        a = float(aud)
        return max(0.0, min(100.0, a if a > 10.0 else a * 10.0))
    except Exception:
        pass
    tv = it.get("tmdb_vote")
    try:
        v = float(tv)
        return max(0.0, min(100.0, v * 10.0 if v <= 10.0 else v))
    except Exception:
        pass
    return 50.0

def _score(it: Dict[str, Any]) -> float:
    try:
        return float(it.get("match", it.get("score", 0.0)) or 0.0)
    except Exception:
        return 0.0

def _providers(it: Dict[str, Any]) -> List[str]:
    provs = it.get("providers") or it.get("providers_slugs") or []
    return [p for p in provs if isinstance(p, str)]

def _fmt_providers(provs: List[str], maxn: int = 3) -> str:
    if not provs:
        return "—"
    short = provs[:maxn]
    if len(provs) > maxn:
        return ", ".join(short) + "…"
    return ", ".join(short)

def _imdb_link(it: Dict[str, Any]) -> Optional[str]:
    imdb = it.get("imdb_id")
    if imdb and isinstance(imdb, str):
        return f"https://www.imdb.com/title/{imdb}/"
    return None

def _tmdb_link(it: Dict[str, Any]) -> Optional[str]:
    tid = it.get("tmdb_id")
    mt = (it.get("media_type") or "").lower()
    if not tid or not mt:
        return None
    if mt == "movie":
        return f"https://www.themoviedb.org/movie/{int(tid)}"
    return f"https://www.themoviedb.org/tv/{int(tid)}"

def _bullet_line(it: Dict[str, Any]) -> str:
    title = it.get("title") or it.get("name") or "—"
    year = it.get("year") or ""
    sc = _score(it)
    aud = _audience_0_100(it)
    prov = _fmt_providers(_providers(it))
    why = it.get("why") or ""
    links = []
    imdb = _imdb_link(it)
    if imdb: links.append(f"[IMDb]({imdb})")
    tmdb = _tmdb_link(it)
    if tmdb: links.append(f"[TMDB]({tmdb})")
    links_s = (" • ".join(links)) if links else ""
    base = f"**{title}** ({year}) — **Match {sc:.0f}** | Audience {aud:.0f} | {prov}"
    if why:
        base += f" — _{why}_"
    if links_s:
        base += f" — {links_s}"
    return base

def _pick(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_score, reverse=True)[:n]

def _fresh_and_high(items: List[Dict[str, Any]], recent_year_min: int, min_aud: float, n: int) -> List[Dict[str, Any]]:
    filt = [it for it in items if isinstance(it.get("year"), int) and it["year"] >= recent_year_min and _audience_0_100(it) >= min_aud]
    return _pick(filt, n)

def _on_services(items: List[Dict[str, Any]], subs: List[str], n: int) -> List[Dict[str, Any]]:
    if not subs:
        return []
    s = {x.strip().lower() for x in subs}
    filt = []
    for it in items:
        provs = [p.strip().lower() for p in _providers(it)]
        if any(p in s for p in provs):
            filt.append(it)
    return _pick(filt, n)

def _deep_cuts(items: List[Dict[str, Any]], min_match: float, max_aud: float, n: int) -> List[Dict[str, Any]]:
    # Higher match but not overly popular (audience <= max_aud)
    filt = [it for it in items if _score(it) >= min_match and _audience_0_100(it) <= max_aud]
    return _pick(filt, n)

def _read_ratings_csv(p: Path) -> Tuple[int, Dict[str, int]]:
    """
    Return (rows, simple_genre_counter). The CSV column can be 'genres' (delimited by | or ,).
    """
    if not p.exists():
        return 0, {}
    count = 0
    genres: Dict[str, int] = {}
    with p.open("r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            count += 1
            gs = row.get("genres") or row.get("Genres") or ""
            if gs:
                parts = [g.strip() for g in re_split(gs)]
                for g in parts:
                    if g:
                        genres[g] = genres.get(g, 0) + 1
    return count, dict(sorted(genres.items(), key=lambda kv: kv[1], reverse=True))

def re_split(s: str) -> List[str]:
    # split on common separators
    return [t for t in re_sep_split(s) if t]

def re_sep_split(s: str) -> List[str]:
    import re
    return re.split(r"[|,/;+]", s)

def build_digest(
    items: List[Dict[str, Any]],
    diag_env: Dict[str, Any],
    ratings_csv: Optional[Path],
    top_n: int = 10
) -> str:
    """
    Build a compact, comment-friendly markdown digest with sections:
    - Top Picks (🎯)
    - On Your Services (🎟️)
    - Fresh & Highly Rated (🆕⭐)
    - Deep Cuts / Underrated (🕵️)
    """
    subs = _coerce_list(diag_env.get("SUBS_INCLUDE"))
    region = diag_env.get("REGION", "US")
    now_year = 2025  # conservative fixed; runner doesn't export 'now'; adjust if needed
    recent_year_min = now_year - 3

    # Selections
    top_picks = _pick(items, top_n)
    on_services = _on_services(items, subs, top_n)
    fresh_high = _fresh_and_high(items, recent_year_min=recent_year_min, min_aud=75.0, n=top_n)
    deep = _deep_cuts(items, min_match=60.0, max_aud=70.0, n=top_n)

    # Taste profile from ratings.csv (optional)
    ratings_rows, genre_counter = (0, {})
    if ratings_csv:
        try:
            ratings_rows, genre_counter = _read_ratings_csv(ratings_csv)
        except Exception:
            pass

    lines: List[str] = []
    lines.append(f"### 🎬 Daily Picks ({region})")
    lines.append("")
    if ratings_rows:
        top_gen = ", ".join([f"{g}×{c}" for g, c in list(genre_counter.items())[:6]])
        lines.append(f"_Taste profile (from your ratings.csv, {ratings_rows} rows):_ {top_gen}")
        lines.append("")
    # Top picks
    if top_picks:
        lines.append("#### 🎯 Top Picks")
        for it in top_picks:
            lines.append(f"- {_bullet_line(it)}")
        lines.append("")

    # On your services
    if on_services:
        lines.append("#### 🎟️ On Your Services")
        for it in on_services:
            lines.append(f"- {_bullet_line(it)}")
        lines.append("")

    # Fresh & highly rated
    if fresh_high:
        lines.append("#### 🆕⭐ Fresh & Highly Rated")
        for it in fresh_high:
            lines.append(f"- {_bullet_line(it)}")
        lines.append("")

    # Deep cuts
    if deep:
        lines.append("#### 🕵️ Underrated Deep Cuts")
        for it in deep:
            lines.append(f"- {_bullet_line(it)}")
        lines.append("")

    # Telemetry footer (compact)
    prov_map = diag_env.get("PROVIDER_MAP", {})
    prov_unmatched = diag_env.get("PROVIDER_UNMATCHED", [])
    lines.append("<sub>")
    lines.append(f"Providers: `{json.dumps(prov_map, ensure_ascii=False)}`")
    if prov_unmatched:
        lines.append(f" Unmatched: `{prov_unmatched}`")
    lines.append("</sub>")
    return "\n".join(lines)

def main() -> None:
    ap = argparse.ArgumentParser(description="Build a compact digest-style summary.md from enriched items.")
    ap.add_argument("--in", dest="inp", required=True, help="items.enriched.json (or assistant_feed.json)")
    ap.add_argument("--diag", dest="diag", required=False, help="diag.json for env telemetry")
    ap.add_argument("--ratings", dest="ratings", required=False, help="data/user/ratings.csv (optional)")
    ap.add_argument("--out", dest="out", required=True, help="output markdown path (summary.md)")
    ap.add_argument("--top", dest="top", type=int, default=10, help="Top-N per bucket")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)
    diagp = Path(args.diag) if args.diag else None
    ratingsp = Path(args.ratings) if args.ratings else None

    items = _load_json(inp) or []
    if not isinstance(items, list):
        items = []
    diag = _load_json(diagp) if diagp and diagp.exists() else {}
    env = (diag or {}).get("env", {})

    body = build_digest(items, env, ratingsp if (ratingsp and ratingsp.exists()) else None, top_n=args.top)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(body, encoding="utf-8")

if __name__ == "__main__":
    main()