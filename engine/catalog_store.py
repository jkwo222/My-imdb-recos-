# engine/catalog_store.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, Tuple

DEFAULT_PATH = Path("data/catalog_store.json")

def load_store(path: Path = DEFAULT_PATH) -> Dict[str, Dict[str, Any]]:
    if path.is_file():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"movie": {}, "tv": {}}

def merge_store(dest: Dict[str, Dict[str, Any]],
                src: Dict[str, Dict[str, Any]]) -> Tuple[int, int]:
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

def save_store(store: Dict[str, Dict[str, Any]],
               path: Path = DEFAULT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2, sort_keys=True)