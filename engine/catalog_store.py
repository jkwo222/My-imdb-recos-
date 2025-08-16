# engine/catalog_store.py
from __future__ import annotations
import json
import os
from typing import Dict, Any, List

def _blank_store() -> Dict[str, Any]:
    return {
        "items": [],          # list of dicts: {id, type, title, year, tmdb_id, popularity, ...}
        "seen_ids": set(),    # in-memory only; not serialized
        "added_counts": {"movie": 0, "tv": 0},
    }

def _rehydrate(store: Dict[str, Any]) -> Dict[str, Any]:
    store.setdefault("items", [])
    store.setdefault("added_counts", {"movie": 0, "tv": 0})
    # create working set
    seen = {it.get("id") for it in store["items"] if it.get("id")}
    store["seen_ids"] = seen
    return store

def load_store(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return _blank_store()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return _rehydrate(data)

def save_store(path: str, store: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # serialize a copy without the non-serializable set
    serial = {
        "items": store.get("items", []),
        "added_counts": store.get("added_counts", {"movie": 0, "tv": 0}),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(serial, f, ensure_ascii=False, indent=2)

def merge_discover_batch(store: Dict[str, Any], batch: List[Dict[str, Any]]) -> int:
    """Add new items (by id) into the store; return count added."""
    if not batch:
        return 0
    added = 0
    seen_ids = store["seen_ids"]
    items = store["items"]
    for it in batch:
        _id = it.get("id")
        if not _id or _id in seen_ids:
            continue
        items.append(it)
        seen_ids.add(_id)
        t = it.get("type")
        if t in ("movie", "tv"):
            store["added_counts"][t] = store["added_counts"].get(t, 0) + 1
        added += 1
    # maintain newest-first for convenience (by popularity descending if present)
    items.sort(key=lambda x: (x.get("popularity") or 0.0), reverse=True)
    return added

def all_items(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(store.get("items", []))