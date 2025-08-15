# engine/seen_index.py
from __future__ import annotations
import json, os
from pathlib import Path
from typing import Iterable, Set, Dict, Any

_SEEN_DIR = Path("data/seen")
_SEEN_DIR.mkdir(parents=True, exist_ok=True)

# Canonical store (simple and robust)
_SEEN_JSON = _SEEN_DIR / "seen.json"

# In-memory cache
_seen_cache: Set[str] | None = None

def _normalize_imdb_id(v: str | None) -> str | None:
    if not v:
        return None
    v = v.strip()
    if not v:
        return None
    if not v.startswith("tt"):
        # Accept raw numeric ids
        v = "tt" + v
    return v

def _load() -> Set[str]:
    global _seen_cache
    if _seen_cache is not None:
        return _seen_cache
    s: Set[str] = set()
    if _SEEN_JSON.exists():
        try:
            data = json.load(open(_SEEN_JSON, "r", encoding="utf-8"))
            for x in data or []:
                nid = _normalize_imdb_id(x)
                if nid: s.add(nid)
        except Exception:
            # Corrupt? start fresh
            s = set()
    _seen_cache = s
    return s

def _save(seen: Set[str]) -> None:
    arr = sorted(seen)
    json.dump(arr, open(_SEEN_JSON, "w", encoding="utf-8"), indent=2)

def is_seen_imdb(imdb_id: str | None) -> bool:
    nid = _normalize_imdb_id(imdb_id)
    if not nid:
        return False
    return nid in _load()

def update_seen_from_ratings(rows: Iterable[Dict[str, Any]]) -> None:
    """
    Accepts CSV rows (IMDb export). Any row with a valid const/imdb_id is marked seen.
    """
    seen = _load()
    added = 0
    for r in rows:
        imdb_id = r.get("imdb_id") or r.get("const")
        nid = _normalize_imdb_id(imdb_id)
        if nid and nid not in seen:
            seen.add(nid)
            added += 1
    _save(seen)
    print(f"[seen] updated: +{added} (total={len(seen)})")