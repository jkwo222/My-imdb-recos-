# engine/catalog_store.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Tuple

# Default location on disk
DEFAULT_PATH = Path("data/catalog_store.json")


def load_store(path: Path = DEFAULT_PATH) -> Dict[str, Dict[str, Any]]:
    """
    Load the cumulative catalog store from disk.
    Structure:
    {
      "movie": { "<tmdb_or_imdb_id>": {...arbitrary payload...}, ... },
      "tv":    { "<tmdb_or_imdb_id>": {...arbitrary payload...}, ... }
    }
    """
    if path.is_file():
        with path.open("r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                # harden structure
                return {
                    "movie": dict(data.get("movie", {})),
                    "tv": dict(data.get("tv", {})),
                }
            except Exception:
                # Corrupt or unexpected -> start fresh
                return {"movie": {}, "tv": {}}
    return {"movie": {}, "tv": {}}


def merge_store(
    dest: Dict[str, Dict[str, Any]],
    src: Dict[str, Dict[str, Any]],
) -> Tuple[int, int]:
    """
    Merge src into dest. Returns (added_movie, added_tv) counts.
    Existing keys are left untouched.
    """
    added_movie = 0
    added_tv = 0

    for kind in ("movie", "tv"):
        dest.setdefault(kind, {})
        for k, v in src.get(kind, {}).items():
            if k not in dest[kind]:
                dest[kind][k] = v
                if kind == "movie":
                    added_movie += 1
                else:
                    added_tv += 1

    return added_movie, added_tv


def save_store(store: Dict[str, Dict[str, Any]], path: Path = DEFAULT_PATH) -> None:
    """Persist the cumulative store to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2, sort_keys=True)