# FILE: engine/filtering.py
from __future__ import annotations
from typing import Dict, List, Any, Iterable, Tuple
from .seen_index import SeenIndex, _norm_title

def _kind(item: Dict[str, Any]) -> str:
    t = (item.get("type") or item.get("kind") or "").lower()
    if t in ("tv", "tvseries", "tvminiseries"): 
        return "tvSeries"
    return "movie"

def _candidate_titles(item: Dict[str, Any]) -> List[str]:
    c = []
    for k in ("title", "name"):
        v = item.get(k)
        if v:
            c.append(v)
    for v in item.get("alt_titles", []) or []:
        if v:
            c.append(v)
    # dedupe by normalized form
    out, seen = [], set()
    for t in c:
        nt = _norm_title(t)
        if nt and nt not in seen:
            out.append(t)
            seen.add(nt)
    return out

def filter_unseen(
    pool: Iterable[Dict[str, Any]], 
    seen: SeenIndex
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (kept_items, dropped_details)

    dropped_details entries look like:
      {
        "title": "...", "year": 2023, "type": "tvSeries",
        "imdb_id": "tt1234567", "reason": "imdb_id" | "title",
        "matched_title": "Planet Earth III"  # only for title reason
      }
    """
    kept: List[Dict[str, Any]] = []
    dropped: List[Dict[str, Any]] = []

    for it in pool:
        title = it.get("title") or it.get("name") or ""
        year  = it.get("year")
        typ   = _kind(it)

        # 1) IMDb ID exact match
        iid = (it.get("imdb_id") or "").lower()
        if iid and seen.has_id(iid):
            dropped.append({
                "title": title, "year": year, "type": typ, 
                "imdb_id": iid, "reason": "imdb_id"
            })
            continue

        # 2) Title+year tolerant
        matched_by_title = None
        for cand in _candidate_titles(it):
            if seen.has_title(typ, cand, year):
                matched_by_title = cand
                break

        if matched_by_title:
            dropped.append({
                "title": title, "year": year, "type": typ,
                "imdb_id": iid or "", "reason": "title",
                "matched_title": matched_by_title
            })
            continue

        kept.append(it)

    return kept, dropped