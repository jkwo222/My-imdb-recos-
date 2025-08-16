# FILE: engine/filtering.py
from __future__ import annotations
from typing import Dict, List, Any, Iterable
from .seen_index import SeenIndex, _norm_title

def _kind(item: Dict[str, Any]) -> str:
    t = (item.get("type") or item.get("kind") or "").lower()
    if t in ("tv", "tvseries", "tvminiseries"): return "tvSeries"
    return "movie"

def _candidate_titles(item: Dict[str, Any]) -> List[str]:
    c = []
    for k in ("title", "name"):
        v = item.get(k)
        if v: c.append(v)
    for v in item.get("alt_titles", []) or []:
        if v: c.append(v)
    # dedupe by normalized form
    out, seen = [], set()
    for t in c:
        nt = _norm_title(t)
        if nt and nt not in seen:
            out.append(t)
            seen.add(nt)
    return out

def filter_unseen(pool: Iterable[Dict[str, Any]], seen: SeenIndex) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in pool:
        # 1) IMDb ID exact
        iid = (it.get("imdb_id") or "").lower()
        if iid and seen.has_id(iid):
            continue
        # 2) Title+year tolerant
        k = _kind(it)
        y = it.get("year")
        keep = True
        for t in _candidate_titles(it):
            if seen.has_title(k, t, y):
                keep = False
                break
        if keep:
            out.append(it)
    return out