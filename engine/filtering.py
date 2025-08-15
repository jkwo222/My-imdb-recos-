# engine/filtering.py
from __future__ import annotations
from typing import Dict, Iterable, List, Optional

from .seen_index import SeenIndex, title_keys

def _year_from_item(item: Dict) -> Optional[int]:
    y = item.get("year")
    if y:
        try:
            return int(y)
        except Exception:
            pass
    rd = item.get("release_date") or item.get("first_air_date")
    if isinstance(rd, str) and len(rd) >= 4 and rd[:4].isdigit():
        return int(rd[:4])
    return None

def _collect_alt_titles(item: Dict) -> List[str]:
    alts = []
    for k in ("title", "name", "original_title", "original_name"):
        v = item.get(k)
        if isinstance(v, str) and v:
            alts.append(v)
    if isinstance(item.get("alternative_titles"), list):
        for t in item["alternative_titles"]:
            if isinstance(t, dict):
                n = t.get("title") or t.get("name")
                if n:
                    alts.append(n)
            elif isinstance(t, str):
                alts.append(t)
    out, seen = [], set()
    for t in alts:
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out

def _item_keys(item: Dict) -> List[str]:
    keys: List[str] = []
    typ = item.get("type") or ("tvSeries" if item.get("seasons") else "movie")
    imdb = item.get("imdb_id")
    if imdb and str(imdb).startswith("tt"):
        keys.append(f"imdb:{imdb}")
    tmdb_id = item.get("tmdb_id") or item.get("id")
    if tmdb_id:
        keys.append(f"tmdb:{typ}:{tmdb_id}")
    yr = _year_from_item(item)
    for t in _collect_alt_titles(item):
        keys.extend(title_keys(t, yr, typ="tv" if "tv" in typ.lower() else "movie"))
    out, seen = [], set()
    for k in keys:
        if k and k not in seen:
            out.append(k)
            seen.add(k)
    return out

def is_seen(item: Dict, seen: SeenIndex) -> bool:
    for k in _item_keys(item):
        if seen.contains(k):
            return True
    return False

def filter_unseen(items: Iterable[Dict], seen: SeenIndex) -> List[Dict]:
    unseen = []
    for it in items:
        if not is_seen(it, seen):
            unseen.append(it)
    return unseen