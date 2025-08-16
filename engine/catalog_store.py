# engine/catalog_store.py
from __future__ import annotations

import json
import os
from typing import Dict, List, Any, Iterable, Optional
from datetime import datetime

STORE_PATH = "data/catalog_store.json"

def _empty_store() -> Dict[str, Dict[str, Any]]:
    # Two top-level buckets so we can report movie/tv growth independently
    return {"movie": {}, "tv": {}}

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _norm_type(t: str) -> str:
    t = (t or "").lower().strip()
    if t in ("movie", "film"): return "movie"
    if t in ("tv", "tvshow", "tvseries", "series"): return "tv"
    # default to tv if unknown? safer to keep as given but bucket unknown under 'tv' makes no sense.
    # We’ll just return 'movie' as fallback to avoid KeyErrors.
    return "movie"

def _key_for(item: Dict[str, Any]) -> Optional[str]:
    """
    Prefer stable external IDs for de-dup:
      1) imdb_id (e.g. 'tt1234567')
      2) tmdb_id (as 'tmdb:12345')
      3) fallback: slug "title|year|type" (lowercased, trimmed)
    """
    imdb_id = (item.get("imdb_id") or "").strip()
    if imdb_id:
        return imdb_id

    tmdb_id = item.get("tmdb_id")
    if isinstance(tmdb_id, int) and tmdb_id > 0:
        return f"tmdb:{tmdb_id}"
    if isinstance(tmdb_id, str) and tmdb_id.strip().isdigit():
        return f"tmdb:{tmdb_id.strip()}"

    title = (item.get("title") or "").strip().lower()
    year = str(item.get("year") or "").strip()
    typ = _norm_type(item.get("type") or "")
    if title:
        return f"{title}|{year}|{typ}"
    return None

def _merge_providers(old: List[str], new: Iterable[str]) -> List[str]:
    seen = set(p.strip().lower() for p in old or [] if p)
    for p in new or []:
        q = (p or "").strip().lower()
        if q:
            seen.add(q)
    # stable order: alphabetical
    return sorted(seen)

def load_store(path: str = STORE_PATH) -> Dict[str, Dict[str, Any]]:
    """
    Load the persistent catalog store. If missing/corrupt, return a fresh structure.
    """
    try:
        if not os.path.exists(path):
            return _empty_store()
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Validate basic shape
        if not isinstance(data, dict):
            return _empty_store()
        data.setdefault("movie", {})
        data.setdefault("tv", {})
        if not isinstance(data["movie"], dict): data["movie"] = {}
        if not isinstance(data["tv"], dict): data["tv"] = {}
        return data
    except Exception:
        # Don’t fail the run if the file is unreadable; start fresh
        return _empty_store()

def save_store(store: Dict[str, Dict[str, Any]], path: str = STORE_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2, sort_keys=False)

def all_items(store: Dict[str, Dict[str, Any]], kind: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    """
    Iterate all stored entries. If kind is 'movie' or 'tv', limit to that bucket.
    """
    if kind is None:
        for k in ("movie", "tv"):
            for _id, rec in store.get(k, {}).items():
                yield rec
    else:
        k = _norm_type(kind)
        for _id, rec in store.get(k, {}).items():
            yield rec

def merge_discover_batch(store: Dict[str, Dict[str, Any]], batch: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """
    Merge a single discovery batch (from TMDB/IMDb/wherever) into the store.

    Expected item shape (best effort; we’re resilient to missing fields):
      {
        "type": "movie"|"tv",
        "tmdb_id": int|str,
        "imdb_id": "tt1234567"|None,
        "title": str,
        "year": int|str|None,
        "language": "en-US"|None,
        "with_original_language": "en"|None,
        "watch_region": "US"|None,
        "providers": [ "netflix", "max", ... ],
        "fetched_at": ISO8601 or omitted
      }
    """
    added = {"movie": 0, "tv": 0}
    now = _now_iso()

    for item in batch or []:
        kind = _norm_type(item.get("type") or "")
        key = _key_for(item)
        if not key:
            # Skip items we can’t key deterministically
            continue

        bucket = store.setdefault(kind, {})
        existing = bucket.get(key)

        base = {
            "id": key,
            "type": kind,
            "tmdb_id": item.get("tmdb_id"),
            "imdb_id": item.get("imdb_id"),
            "title": item.get("title"),
            "year": item.get("year"),
            "language": item.get("language") or item.get("tmdb_language") or item.get("iso_639_1"),
            "with_original_language": item.get("with_original_language"),
            "watch_region": item.get("watch_region"),
            "providers": list(item.get("providers") or []),
            "first_seen": now,
            "last_seen": now,
            "hits": 1,
        }

        if existing is None:
            bucket[key] = base
            added[kind] += 1
        else:
            # update evolving fields
            existing["last_seen"] = now
            existing["hits"] = int(existing.get("hits", 0)) + 1

            # keep canonical ids/titles if they arrive later
            if item.get("imdb_id") and not existing.get("imdb_id"):
                existing["imdb_id"] = item.get("imdb_id")
            if item.get("tmdb_id") and not existing.get("tmdb_id"):
                existing["tmdb_id"] = item.get("tmdb_id")
            if item.get("title") and not existing.get("title"):
                existing["title"] = item.get("title")
            if item.get("year") and not existing.get("year"):
                existing["year"] = item.get("year")

            # language/region
            for fld in ("language", "with_original_language", "watch_region"):
                val = item.get(fld)
                if val and not existing.get(fld):
                    existing[fld] = val

            # providers = union
            existing["providers"] = _merge_providers(existing.get("providers", []), item.get("providers"))

    return added