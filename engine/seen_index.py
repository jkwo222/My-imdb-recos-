# engine/seen_index.py
import json, os
from bloom_filter2 import BloomFilter
from .utils import normalize_title, fuzzy_match

SEEN_PATH = "data/seen_index_v3.json"

def _load_seen():
    if os.path.exists(SEEN_PATH):
        return json.load(open(SEEN_PATH, "r", encoding="utf-8"))
    return {"by_imdb": {}, "by_key": {}, "meta": {"count": 0}}

def _save_seen(seen):
    os.makedirs("data", exist_ok=True)
    json.dump(seen, open(SEEN_PATH,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

def _build_bloom_from_seen(seen):
    """Build an in-memory Bloom filter from the persisted JSON."""
    n = max(10000, seen["meta"]["count"]*2)
    bf = BloomFilter(max_elements=n, error_rate=0.001)
    for iid in seen["by_imdb"]:
        bf.add(f"imdb:{iid}")
    for k in seen["by_key"]:
        bf.add(f"key:{k}")
    return bf

def update_seen_from_ratings(rows):
    """Persist ratings into seen_index_v3.json (title keys + imdb ids)."""
    seen = _load_seen()
    for r in rows:
        iid = (r.get("imdb_id") or "").strip()
        title = r.get("title",""); year = int(r.get("year") or 0)
        key = normalize_title(title)
        if iid:
            seen["by_imdb"][iid] = {"year": year, "title": title}
        if key:
            seen["by_key"][key] = {"year": year, "title": title}
    seen["meta"]["count"] = max(len(seen["by_imdb"]), len(seen["by_key"]))
    _save_seen(seen)  # Bloom is rebuilt on demand; no disk persistence needed.

def is_seen(title: str, imdb_id: str = "", year: int = 0, thr: float = 0.92, tol: int = 1) -> bool:
    """Fast membership check with Bloom first, then fuzzy/Year tolerance fallback."""
    seen = _load_seen()
    bf = _build_bloom_from_seen(seen)  # in-memory bloom

    if imdb_id and f"imdb:{imdb_id}" in bf:
        return True
    key = normalize_title(title)
    if key and f"key:{key}" in bf:
        return True

    # Bloom says "maybe" or miss; do precise fuzzy fallback with year tolerance
    for k, meta in seen["by_key"].items():
        if fuzzy_match(key, k, thr):
            y = int(meta.get("year") or 0)
            if (year == 0) or (abs(y - year) <= tol):
                return True
    return False