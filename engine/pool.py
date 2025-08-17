# engine/pool.py
from __future__ import annotations
import io
import json
import os
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

POOL_DIR = Path("data/cache/pool")
POOL_DIR.mkdir(parents=True, exist_ok=True)
POOL_FILE = POOL_DIR / "pool.jsonl"

# Uniqueness per item: (media_type, tmdb_id)
UNIQ_KEYS = ("media_type", "tmdb_id")


def _key(it: dict) -> Tuple[str, int]:
    return (str(it.get("media_type") or ""), int(it.get("tmdb_id") or 0))


def _now_ts() -> float:
    return time.time()


def append_candidates(items: Iterable[dict], default_ts: Optional[float] = None, max_append: Optional[int] = None) -> int:
    """
    Append candidate items to the pool file as JSONL.
    We *do not* try to dedupe while writing (fast append); de-dupe happens on load.
    """
    POOL_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    ts = _now_ts() if default_ts is None else float(default_ts)
    mode = "a"
    with POOL_FILE.open(mode, encoding="utf-8") as fh:
        for it in items:
            if not isinstance(it, dict):
                continue
            obj = dict(it)
            obj.setdefault("added_at", ts)
            # Ensure minimal fields exist
            obj.setdefault("media_type", obj.get("media_type"))
            obj.setdefault("tmdb_id", obj.get("tmdb_id"))
            try:
                fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
                count += 1
                if max_append and count >= max_append:
                    break
            except Exception:
                # skip bad record
                continue
    return count


def _iter_lines(reverse: bool = False) -> Iterator[str]:
    """
    Iterate lines of the pool file.
    For reverse=True we load to memory once and iterate from the end (fast enough for ~100k lines).
    """
    if not POOL_FILE.exists():
        return iter(())
    if not reverse:
        with POOL_FILE.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                yield line
        return
    # reverse
    with POOL_FILE.open("r", encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()
    for line in reversed(lines):
        yield line


def count_lines() -> int:
    if not POOL_FILE.exists():
        return 0
    # Fast count
    with POOL_FILE.open("rb") as fh:
        return sum(1 for _ in fh)


def load_pool(max_items: Optional[int] = None, unique_only: bool = True, prefer_recent: bool = True) -> List[dict]:
    """
    Load items from the pool, preferring the most recent entry per unique key.
    - unique_only=True: return at most one record per (media_type, tmdb_id)
    - prefer_recent=True: keep the newest record when duplicates exist
    - max_items: optional limit of unique items returned
    """
    if not POOL_FILE.exists():
        return []

    uniq: set = set()
    out: List[dict] = []

    # Iterate newest-to-oldest for prefer_recent semantics
    it = _iter_lines(reverse=True) if prefer_recent else _iter_lines(reverse=False)

    for line in it:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        k = _key(obj)
        if unique_only:
            if k in uniq:
                continue
            uniq.add(k)
        out.append(obj)
        if max_items and unique_only and len(out) >= max_items:
            break

    # We walked newest-first — keep output order as newest-first
    return out


def prune_pool(keep_last_lines: int) -> Tuple[int, int]:
    """
    Prune the pool file to the last N lines (by file order, i.e., newest at end).
    Returns (before_lines, after_lines).
    """
    before = count_lines()
    if before <= keep_last_lines or not POOL_FILE.exists():
        return before, before

    with POOL_FILE.open("r", encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()
    tail = lines[-keep_last_lines:]

    tmp = POOL_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as out:
        out.writelines(tail)
    tmp.replace(POOL_FILE)

    after = count_lines()
    return before, after


def pool_stats(sample_unique: bool = True, sample_limit: Optional[int] = None) -> Dict[str, int]:
    """
    Return basic stats for telemetry:
      - file_lines: total line count
      - unique_keys_est: unique count based on a scan (up to sample_limit if provided)
    """
    total = count_lines()
    uniq_est = 0
    if sample_unique:
        seen = set()
        n = 0
        for line in _iter_lines(reverse=True):
            n += 1
            try:
                obj = json.loads(line)
                seen.add(_key(obj))
            except Exception:
                pass
            if sample_limit and n >= sample_limit:
                break
        uniq_est = len(seen)
    return {"file_lines": total, "unique_keys_est": uniq_est}