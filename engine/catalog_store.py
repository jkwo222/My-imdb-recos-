# engine/catalog_store.py
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Iterable, List, Tuple

_STORE_PATH_DEFAULT = "data/catalog_store.json"


def _now_ts() -> int:
    return int(time.time())


def _ensure_dirs(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def load_store(path: str = _STORE_PATH_DEFAULT) -> Dict[str, Dict[str, Any]]:
    """
    Load the cumulative catalog store from disk.
    Shape:
      {
        "movie": { "<tmdb_id>": {...}}, 
        "tv":    { "<tmdb_id>": {...} }
      }
    """
    if not os.path.isfile(path):
        return {"movie": {}, "tv": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # normalize minimal structure
            if not isinstance(data, dict):
                return {"movie": {}, "tv": {}}
            data.setdefault("movie", {})
            data.setdefault("tv", {})
            return data
    except Exception:
        # If the file is corrupted, start fresh rather than crash the run
        return {"movie": {}, "tv": {}}


def save_store(store: Dict[str, Dict[str, Any]], path: str = _STORE_PATH_DEFAULT) -> None:
    _ensure_dirs(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _coerce_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    # Expecting YYYY-MM-DD or YYYY
    try:
        return int(str(date_str)[:4])
    except Exception:
        return None


def _field(item: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in item and item[k] not in (None, ""):
            return item[k]
    return None


def merge_discover_batch(
    store: Dict[str, Dict[str, Any]],
    batch: Iterable[Dict[str, Any]],
    media_type: str,
    region: str | None = None,
    providers: List[str] | None = None,
) -> Tuple[int, int]:
    """
    Merge a TMDB discover 'results' batch into the cumulative store.
    Returns: (added, updated)
    """
    assert media_type in {"movie", "tv"}, "media_type must be 'movie' or 'tv'"
    added = 0
    updated = 0
    bucket = store.setdefault(media_type, {})

    ts = _now_ts()
    prov_str = ",".join(providers or [])

    for r in batch or []:
        tmdb_id = r.get("id")
        if tmdb_id is None:
            continue
        key = str(tmdb_id)

        title = _field(r, "title", "name", "original_title", "original_name")
        year = _coerce_year(_field(r, "release_date", "first_air_date"))
        orig_lang = r.get("original_language")
        vote_avg = r.get("vote_average")
        vote_cnt = r.get("vote_count")
        popularity = r.get("popularity")

        payload = {
            "id": tmdb_id,
            "type": media_type,
            "title": title,
            "year": year,
            "original_language": orig_lang,
            "vote_average": vote_avg,
            "vote_count": vote_cnt,
            "popularity": popularity,
            "watch_region": region,
            "providers": prov_str,
            "first_seen": ts,
            "last_seen": ts,
        }

        if key in bucket:
            # update a few fields that may change over time
            cur = bucket[key]
            cur.update({
                "title": title or cur.get("title"),
                "year": year or cur.get("year"),
                "original_language": orig_lang or cur.get("original_language"),
                "vote_average": vote_avg,
                "vote_count": vote_cnt,
                "popularity": popularity,
                "watch_region": region or cur.get("watch_region"),
                "providers": prov_str or cur.get("providers"),
                "last_seen": ts,
            })
            updated += 1
        else:
            bucket[key] = payload
            added += 1

    return added, updated


def all_items(store: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten all items in the store (movie + tv) into a single list.
    """
    out: List[Dict[str, Any]] = []
    for t in ("movie", "tv"):
        out.extend((store.get(t) or {}).values())
    return out