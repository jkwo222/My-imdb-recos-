# engine/seen_index.py
from __future__ import annotations
import os, json
from typing import Iterable, Set

_BLOOM_PATH = "data/runtime/seen.bloom"
_SET_PATH = "data/runtime/seen.set.json"

try:
    from bloom_filter2 import BloomFilter  # pip install bloom-filter2
    _HAVE_BLOOM = True
except Exception:
    BloomFilter = None  # type: ignore
    _HAVE_BLOOM = False

# In-memory guards
_seen_ids: Set[str] = set()
_bloom = None

def _ensure_dirs():
    os.makedirs(os.path.dirname(_BLOOM_PATH), exist_ok=True)

def _normalize_imdb_id(x: str) -> str:
    x = (x or "").strip()
    if not x:
        return ""
    if x.startswith("tt"):
        return x
    # accept "tt123..." or raw digits -> normalize to tt
    digits = "".join(ch for ch in x if ch.isdigit())
    return f"tt{digits}" if digits else x

def load_seen():
    global _seen_ids, _bloom
    _ensure_dirs()
    # Load set backup (always available)
    if os.path.exists(_SET_PATH):
        try:
            with open(_SET_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            _seen_ids = set(map(_normalize_imdb_id, data or []))
        except Exception:
            _seen_ids = set()
    # Try bloom if present
    if _HAVE_BLOOM and os.path.exists(_BLOOM_PATH):
        try:
            _bloom = BloomFilter.open(_BLOOM_PATH)  # uses library's file loader
        except Exception:
            _bloom = None

def _save_set():
    try:
        with open(_SET_PATH, "w", encoding="utf-8") as f:
            json.dump(sorted(_seen_ids), f)
    except Exception:
        pass

def _save_bloom():
    global _bloom
    if not _HAVE_BLOOM:
        return
    try:
        # Rebuild bloom each save to keep error rate low.
        capacity = max(1000, len(_seen_ids) * 2)
        error_rate = 0.001
        _bloom = BloomFilter(max_elements=capacity, error_rate=error_rate)
        for k in _seen_ids:
            _bloom.add(k)
        _bloom.save(_BLOOM_PATH)
    except Exception:
        _bloom = None

def update_seen_from_ratings(rows: Iterable[dict]):
    """
    Accepts CSV rows dicts and extracts IMDb IDs from common columns.
    """
    global _seen_ids
    load_seen()
    def extract(row: dict) -> str:
        for key in ("const", "tconst", "imdb_id", "IMDb ID", "url"):
            if key in row and row[key]:
                v = str(row[key])
                if key == "url" and "/title/" in v:
                    # .../title/tt1234567/...
                    i = v.find("/title/")
                    frag = v[i+7:].split("/")[0]
                    return _normalize_imdb_id(frag)
                return _normalize_imdb_id(v)
        # Fallback: any tt-like token in row values
        for v in row.values():
            s = str(v)
            if "tt" in s:
                i = s.find("tt")
                frag = "".join(ch for ch in s[i:i+12] if (ch.isalnum()))
                return _normalize_imdb_id(frag)
        return ""
    added = 0
    for r in rows:
        imdb = extract(r)
        if imdb:
            if imdb not in _seen_ids:
                added += 1
            _seen_ids.add(imdb)
    _save_set()
    _save_bloom()
    return {"total_seen": len(_seen_ids), "added": added}

def is_seen_imdb(imdb_id: str) -> bool:
    imdb_id = _normalize_imdb_id(imdb_id)
    if not imdb_id:
        return False
    if imdb_id in _seen_ids:
        return True
    if _bloom is not None:
        try:
            return imdb_id in _bloom
        except Exception:
            return False
    return False