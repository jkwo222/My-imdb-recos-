# engine/filtering.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple
import re

_norm_re = re.compile(r"[^a-z0-9]+")

def _norm(t: str) -> str:
    return _norm_re.sub("", (t or "").lower())

def _title_year_key(item: Dict[str, Any]) -> Tuple[str, int] | None:
    title = item.get("title") or item.get("name") or ""
    year = item.get("year")
    if not title or not year:
        return None
    return _norm(title), int(year)

def filter_unseen(pool: List[Dict[str, Any]], seen) -> List[Dict[str, Any]]:
    out = []
    for x in pool:
        imdb_id = (x.get("imdb_id") or "").strip()
        if imdb_id and imdb_id in seen.ttids:
            continue
        k = _title_year_key(x)
        if k and k in seen.by_title_year:
            continue
        out.append(x)
    return out