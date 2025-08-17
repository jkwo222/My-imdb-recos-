# engine/feed.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
from .provider_filter import any_allowed
from .recency import score as recency_score
from .taste import taste_boost_for

def _why(it: Dict[str,Any], taste: Dict[str,float]) -> str:
    bits = []
    if it.get("imdb_rating"):
        bits.append(f"IMDb {it['imdb_rating']:.1f}")
    if it.get("tmdb_vote"):
        bits.append(f"TMDB {it['tmdb_vote']:.1f}/10")
    prov = ", ".join(it.get("providers") or []) or "—"
    bits.append(f"on {prov}")
    tb = taste_boost_for(it.get("genres", []), taste)
    if tb > 0.0:
        bits.append("matches your genre prefs")
    return " | ".join(bits)

def filter_by_providers(pool: List[Dict], subs: List[str]) -> List[Dict]:
    return [x for x in pool if any_allowed(x.get("providers"), subs)]

def score_items(pool: List[Dict], weights: Dict[str,Any]) -> List[Dict]:
    out=[]
    for c in pool:
        s = recency_score(c, weights)
        x = dict(c); x["match"]=s; out.append(x)
    out.sort(key=lambda r: r["match"], reverse=True)
    return out

def top10_by_type(scored: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    movies = [x for x in scored if x.get("type")=="movie"]
    series = [x for x in scored if x.get("type") in ("tvSeries","tvMiniSeries")]
    return movies[:10], series[:10]

def to_markdown(movies: List[Dict], series: List[Dict], taste: Dict[str,float]) -> str:
    def fmt_row(i: int, it: Dict[str,Any]) -> str:
        prov = ", ".join(it.get("providers") or []) or "—"
        why = _why(it, taste)
        return f"{i}. **{it.get('title','')}** ({it.get('year','')}) — {it.get('match',0):.1f}\n   - {why}"
    lines = []
    lines.append("### 🎬 Top 10 Movies")
    if movies:
        for i, it in enumerate(movies, 1):
            lines.append(fmt_row(i, it))
    else:
        lines.append("_No movies met the cut today._")
    lines.append("")
    lines.append("### 📺 Top 10 Series")
    if series:
        for i, it in enumerate(series, 1):
            lines.append(fmt_row(i, it))
    else:
        lines.append("_No series met the cut today._")
    return "\n".join(lines)