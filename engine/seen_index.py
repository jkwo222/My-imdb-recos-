# engine/seen_index.py
import json, os
from bloom_filter2 import BloomFilter
from .utils import normalize_title, fuzzy_match

SEEN_PATH = "data/seen_index_v3.json"
BLOOM_PATH = "data/seen_index_v3.bf"

def _load_seen():
    if os.path.exists(SEEN_PATH):
        return json.load(open(SEEN_PATH, "r", encoding="utf-8"))
    return {"by_imdb": {}, "by_key": {}, "meta": {"count": 0}}

def _save_seen(seen):
    os.makedirs("data", exist_ok=True)
    json.dump(seen, open(SEEN_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def _build_bloom(seen):
    n = max(10000, seen["meta"]["count"] * 2)
    bf = BloomFilter(max_elements=n, error_rate=0.001)
    for iid in seen["by_imdb"]:
        bf.add(f"imdb:{iid}")
    for k in seen["by_key"]:
        bf.add(f"key:{k}")
    # NOTE: bloom_filter2 has no .tofile on GH Actions runner – keep it purely in-memory.
    # We skip persisting the bloom and rely on the json index across runs.

def update_seen_from_ratings(rows):
    seen = _load_seen()
    for r in rows:
        iid = (r.get("imdb_id") or "").strip()
        title = r.get("title", "")
        year = int(r.get("year") or 0)
        key = normalize_title(title)
        if iid:
            seen["by_imdb"][iid] = {"year": year, "title": title}
        if key:
            seen["by_key"][key] = {"year": year, "title": title}
    seen["meta"]["count"] = max(len(seen["by_imdb"]), len(seen["by_key"]))
    _save_seen(seen)
    _build_bloom(seen)

def is_seen(title: str, imdb_id: str = "", year: int = 0, thr: float = 0.92, tol: int = 1) -> bool:
    """
    Conservative 'seen' check to avoid false positives:
    1) If IMDB ID matches, it's seen.
    2) If normalized title matches EXACT KEY and year within tolerance, it's seen.
    3) Fuzzy fallback ONLY if BOTH sides have a non-zero year.
    """
    seen = _load_seen()

    if imdb_id and imdb_id in seen["by_imdb"]:
        return True

    key = normalize_title(title)
    meta = seen["by_key"].get(key)
    if meta:
        y = int(meta.get("year") or 0)
        if year == 0 or y == 0:
            # Without years, don't risk a false positive – require ID match.
            return False
        return abs(y - year) <= tol

    # Fuzzy fallback – require both years present to reduce false positives.
    if year == 0:
        return False
    for k, m in seen["by_key"].items():
        y = int(m.get("year") or 0)
        if y == 0:
            continue
        if abs(y - year) <= tol and fuzzy_match(key, k, thr):
            return True

    return False