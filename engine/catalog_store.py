# engine/catalog_store.py
import json
import os
from typing import Dict, List, Tuple

STORE_PATH = "data/catalog_store.json"

def load_store(path: str = STORE_PATH) -> Dict:
    if not os.path.exists(path):
        return {"movie": {}, "tv": {}, "meta": {"schema": 1}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Ensure expected shape
        data.setdefault("movie", {})
        data.setdefault("tv", {})
        data.setdefault("meta", {"schema": 1})
        return data
    except Exception:
        return {"movie": {}, "tv": {}, "meta": {"schema": 1}}

def save_store(store: Dict, path: str = STORE_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2, sort_keys=True)

def _key(media_type: str, tmdb_id: int) -> str:
    return f"{media_type}:{tmdb_id}"

def merge_discover_batch(store: Dict, batch: Dict, media_type: str) -> Tuple[int, int]:
    """
    Merge a TMDB discover 'page' (results list) into the store.
    Returns (added, total_for_type).
    """
    if not batch or "results" not in batch:
        return 0, len(store.get(media_type, {}))
    bucket = store.setdefault(media_type, {})
    added = 0
    for item in batch["results"]:
        tmdb_id = item.get("id")
        if tmdb_id is None:
            continue
        k = str(tmdb_id)
        if k not in bucket:
            # Keep only light fields needed for filtering/scoring; extend as needed
            bucket[k] = {
                "id": tmdb_id,
                "title": item.get("title") or item.get("name"),
                "original_title": item.get("original_title") or item.get("original_name"),
                "year": (item.get("release_date") or item.get("first_air_date") or "")[:4],
                "popularity": item.get("popularity"),
                "vote_average": item.get("vote_average"),
                "vote_count": item.get("vote_count"),
                "media_type": media_type,
            }
            added += 1
    return added, len(bucket)

def all_items(store: Dict) -> List[Dict]:
    out = []
    for mtype in ("movie", "tv"):
        out.extend(store.get(mtype, {}).values())
    return out