# engine/store.py
from __future__ import annotations
import json, os, time, pathlib
from typing import Dict, List, Any, Tuple

DATA_DIR = pathlib.Path("data")
STORE_PATH = DATA_DIR / "catalog_store.json"
RATINGS_SEEN_PATH = DATA_DIR / "ratings_seen_store.json"

def _now() -> float:
    return time.time()

def _key_for(item: Dict[str, Any]) -> Tuple[str, int, str]:
    title = (item.get("title") or item.get("name") or "").strip().lower()
    year = int(item.get("year") or 0)
    typ = (item.get("type") or "").strip()
    return (title, year, typ)

def _load_json(path: pathlib.Path, default):
    if path.exists():
        try:
            return json.load(open(path, "r", encoding="utf-8"))
        except Exception:
            return default
    return default

def _save_json(path: pathlib.Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(obj, open(path, "w", encoding="utf-8"), indent=2)

def load_store() -> Dict[str, Any]:
    return _load_json(STORE_PATH, {"items": [], "updated_at": 0})

def save_store(store: Dict[str, Any]) -> None:
    store["updated_at"] = _now()
    _save_json(STORE_PATH, store)

def merge_catalog_items(existing: List[Dict[str, Any]],
                        new_items: List[Dict[str, Any]],
                        max_size: int = 20000) -> List[Dict[str, Any]]:
    """
    Merge by (tmdb_id) if present, else by (title, year, type).
    Keeps most recent metadata and union of providers/ids.
    """
    by_tmdb: Dict[int, Dict[str, Any]] = {}
    by_fallback: Dict[Tuple[str,int,str], Dict[str, Any]] = {}

    def _index(it: Dict[str, Any]):
        tmdb_id = it.get("tmdb_id")
        if isinstance(tmdb_id, int) and tmdb_id > 0:
            by_tmdb[tmdb_id] = it
        else:
            by_fallback[_key_for(it)] = it

    # seed with existing
    for it in existing:
        it = dict(it)
        it["last_seen_ts"] = it.get("last_seen_ts") or _now()
        _index(it)

    # merge in new
    for it in new_items:
        it = dict(it)
        it["last_seen_ts"] = _now()
        tmdb_id = it.get("tmdb_id")
        if isinstance(tmdb_id, int) and tmdb_id in by_tmdb:
            cur = by_tmdb[tmdb_id]
            # merge providers & ratings
            prov = sorted(set((cur.get("providers") or []) + (it.get("providers") or [])))
            cur.update(it)
            cur["providers"] = prov
            cur["last_seen_ts"] = _now()
        elif isinstance(tmdb_id, int) and tmdb_id:
            by_tmdb[tmdb_id] = it
        else:
            key = _key_for(it)
            if key in by_fallback:
                cur = by_fallback[key]
                prov = sorted(set((cur.get("providers") or []) + (it.get("providers") or [])))
                cur.update(it)
                cur["providers"] = prov
                cur["last_seen_ts"] = _now()
            else:
                by_fallback[key] = it

    merged = list(by_tmdb.values()) + list(by_fallback.values())

    # keep the most recently seen first; trim to max_size
    merged.sort(key=lambda x: float(x.get("last_seen_ts") or 0.0), reverse=True)
    if len(merged) > max_size:
        merged = merged[:max_size]
    return merged

# Optional tiny store for “ratings we’ve already incorporated” telemetry
def load_ratings_seen_store() -> Dict[str, float]:
    return _load_json(RATINGS_SEEN_PATH, {})

def save_ratings_seen_store(d: Dict[str, float]) -> None:
    _save_json(RATINGS_SEEN_PATH, d)