# engine/seen_index.py
# Bloom filter-backed "seen" checks (rebuilt each run from JSON; no disk serialization).

import json
import os
from typing import Dict, List, Any

from bloom_filter2 import BloomFilter
from .utils import normalize_title, fuzzy_match

SEEN_PATH = "data/seen_index_v3.json"

# Module-level cache (rebuilt on demand)
_BF = None  # type: BloomFilter | None


def _empty_seen() -> Dict[str, Any]:
    return {"by_imdb": {}, "by_key": {}, "meta": {"count": 0}}


def _load_seen() -> Dict[str, Any]:
    if os.path.exists(SEEN_PATH):
        with open(SEEN_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                # Corrupt file fallback
                return _empty_seen()
    return _empty_seen()


def _save_seen(seen: Dict[str, Any]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(SEEN_PATH, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)


def _build_bf_from_seen(seen: Dict[str, Any]) -> BloomFilter:
    # size with headroom; low false-positive rate
    n = max(1, len(seen["by_imdb"]) + len(seen["by_key"]))
    capacity = max(10000, n * 2)
    bf = BloomFilter(max_elements=capacity, error_rate=0.001)
    for iid in seen["by_imdb"]:
        bf.add(f"imdb:{iid}")
    for k in seen["by_key"]:
        bf.add(f"key:{k}")
    return bf


def _get_bf() -> BloomFilter:
    global _BF
    if _BF is None:
        _BF = _build_bf_from_seen(_load_seen())
    return _BF


def update_seen_from_ratings(rows: List[Dict[str, Any]]) -> None:
    """
    rows: list of dicts with at least {imdb_id, title, year}
    """
    seen = _load_seen()
    by_imdb = seen["by_imdb"]
    by_key = seen["by_key"]

    for r in rows:
        iid = (r.get("imdb_id") or "").strip()
        title = (r.get("title") or "").strip()
        year = int(r.get("year") or 0)
        if iid:
            by_imdb[iid] = {"year": year, "title": title}
        key = normalize_title(title)
        if key:
            by_key[key] = {"year": year, "title": title}

    seen["meta"]["count"] = max(len(by_imdb), len(by_key))
    _save_seen(seen)

    # Rebuild in-process Bloom filter so subsequent checks in this run benefit.
    global _BF
    _BF = _build_bf_from_seen(seen)


def is_seen(
    title: str,
    imdb_id: str = "",
    year: int = 0,
    thr: float = 0.92,
    tol: int = 1,
) -> bool:
    """
    Fast path via Bloom filter; precise fallback via dict + fuzzy.
    - thr: fuzzy threshold (0..1)
    - tol: allowed year delta when fuzzy-matching titles
    """
    bf = _get_bf()

    if imdb_id:
        if f"imdb:{imdb_id}" in bf:
            return True

    key = normalize_title(title)
    if key:
        if f"key:{key}" in bf:
            return True

    # Precise fallback when Bloom says "maybe"
    seen = _load_seen()

    # Exact by imdb_id (in case bf capacity changed mid-run, etc.)
    if imdb_id and imdb_id in seen["by_imdb"]:
        return True

    # Exact by normalized key
    if key in seen["by_key"]:
        y = int((seen["by_key"][key] or {}).get("year") or 0)
        if (year == 0) or (abs(y - year) <= tol):
            return True

    # Fuzzy_by_key with year tolerance
    for k, meta in seen["by_key"].items():
        if fuzzy_match(key, k, thr):
            y = int(meta.get("year") or 0)
            if (year == 0) or (abs(y - year) <= tol):
                return True

    return False