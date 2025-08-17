# engine/pool.py
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

POOL_DIR = Path("data/cache/pool")
POOL_DIR.mkdir(parents=True, exist_ok=True)
POOL_FILE = POOL_DIR / "pool.jsonl"

KEYS = ("media_type", "tmdb_id")  # uniqueness


def _key(it: dict) -> tuple:
    return (it.get("media_type"), int(it.get("tmdb_id") or 0))


def append_candidates(items: List[dict]) -> int:
    """
    Append new candidates to the pool (de-dupe against existing by (media_type, tmdb_id)).
    Returns number of new records appended.
    """
    existing: Dict[tuple, bool] = {}
    if POOL_FILE.exists():
        with POOL_FILE.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                existing[_key(obj)] = True

    new_count = 0
    with POOL_FILE.open("a", encoding="utf-8") as fh:
        ts = time.time()
        for it in items:
            k = _key(it)
            if not k[1]:  # missing tmdb_id
                continue
            if k in existing:
                continue
            obj = dict(it)
            obj.setdefault("added_at", ts)
            fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            existing[k] = True
            new_count += 1
    return new_count


def load_pool(max_items: int = 5000) -> List[dict]:
    """
    Load the pool in reverse (newest-first) up to max_items.
    """
    if not POOL_FILE.exists():
        return []
    lines = POOL_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    out: List[dict] = []
    for line in reversed(lines):
        try:
            obj = json.loads(line)
        except Exception:
            continue
        out.append(obj)
        if len(out) >= max_items:
            break
    # keep most recent unique by key
    seen = set()
    uniq: List[dict] = []
    for it in out:
        k = _key(it)
        if k in seen:
            continue
        uniq.append(it)
        seen.add(k)
    return uniq