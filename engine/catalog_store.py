from __future__ import annotations
import json, os
from typing import Dict, Any, List, Tuple

_STORE_PATH = "data/catalog_store.json"

def _ensure_dirs():
    os.makedirs("data", exist_ok=True)

def load_store() -> Dict[str, Dict[str, Any]]:
    """
    Returns a dict:
      {
        "movie": { "<tmdb_id>": {kind,title,year,popularity,vote_average,original_language}, ... },
        "tv":    { "<tmdb_id>": { ... } }
      }
    """
    _ensure_dirs()
    if not os.path.exists(_STORE_PATH):
        return {"movie": {}, "tv": {}}
    try:
        with open(_STORE_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        # shape guard
        if not isinstance(d, dict):
            return {"movie": {}, "tv": {}}
        if "movie" not in d or "tv" not in d:
            return {"movie": dict(d.get("movie", {})), "tv": dict(d.get("tv", {}))}
        return d
    except Exception:
        return {"movie": {}, "tv": {}}

def save_store(store: Dict[str, Dict[str, Any]]) -> None:
    _ensure_dirs()
    tmp = _STORE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _STORE_PATH)

def _minimal(item: Dict[str, Any]) -> Dict[str, Any]:
    # keep only discover-level info needed later
    return {
        "kind": item.get("kind"),
        "tmdb_id": int(item.get("tmdb_id")),
        "title": item.get("title") or "",
        "year": item.get("year"),
        "popularity": float(item.get("popularity") or 0.0),
        "vote_average": float(item.get("vote_average") or 0.0),
        "original_language": item.get("original_language") or "",
    }

def merge_discover_batch(kind: str, items: List[Dict[str, Any]], store: Dict[str, Dict[str, Any]]) -> Tuple[int,int]:
    """
    Merge discover items into the store. Returns (added, updated).
    """
    assert kind in ("movie", "tv")
    bucket = store.setdefault(kind, {})
    added = updated = 0
    for it in items:
        tid = str(int(it.get("tmdb_id")))
        minimal = _minimal(it)
        if tid not in bucket:
            bucket[tid] = minimal
            added += 1
        else:
            # update shallow props (popularity/vote can drift)
            old = bucket[tid]
            new = {**old, **{k: minimal[k] for k in ("title","year","popularity","vote_average","original_language")}}
            if new != old:
                bucket[tid] = new
                updated += 1
    return added, updated

def all_items(store: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for kind in ("movie","tv"):
        for _tid, rec in (store.get(kind) or {}).items():
            out.append(rec)
    return out