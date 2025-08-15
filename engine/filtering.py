# engine/filtering.py
from __future__ import annotations
from typing import Dict, List, Any, Iterable, Optional
from .seen_index import SeenIndex, _norm_title

def _candidates(item: Dict[str, Any]) -> List[str]:
    c = []
    for k in ("title", "name"):
        v = item.get(k)
        if v: c.append(v)
    for v in item.get("alt_titles", []) or []:
        c.append(v)
    # dedupe while preserving order
    out, seen = [], set()
    for t in c:
        nt = _norm_title(t)
        if nt and nt not in seen:
            out.append(t)
            seen.add(nt)
    return out

def _kind(item: Dict[str, Any]) -> str:
    t = (item.get("type") or "").lower()
    if t in ("tv", "tvseries", "tvminiseries"): return "tvSeries"
    return "movie" if t == "movie" else "movie"

def filter_unseen(pool: Iterable[Dict[str, Any]], seen: SeenIndex) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in pool:
        k = _kind(it)
        y = it.get("year")
        titles = _candidates(it)
        keep = True
        for t in titles:
            if seen.has(k, t, y):
                keep = False
                break
        if keep:
            out.append(it)
    return out