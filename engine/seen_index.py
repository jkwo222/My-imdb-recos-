import json, os
from typing import Dict, List
from .utils import normalize_title, fuzzy_match

SEEN_PATH = "data/seen_index_v3.json"

def _load_seen():
    if os.path.exists(SEEN_PATH):
        return json.load(open(SEEN_PATH, "r", encoding="utf-8"))
    return {"by_imdb": {}, "by_key": {}, "meta": {"count": 0}}

def _save_seen(seen):
    os.makedirs("data", exist_ok=True)
    json.dump(seen, open(SEEN_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def update_seen_from_ratings(rows: List[Dict]):
    seen = _load_seen()
    for r in rows:
        iid = (r.get("imdb_id") or r.get("const") or "").strip()
        title = r.get("title") or r.get("Title") or ""
        year = int(r.get("year") or r.get("Year") or 0)
        key = normalize_title(title)
        if iid: seen["by_imdb"][iid] = {"year": year, "title": title}
        if key: seen["by_key"][key] = {"year": year, "title": title}
    seen["meta"]["count"] = max(len(seen["by_imdb"]), len(seen["by_key"]))
    _save_seen(seen)

def is_seen(title: str, imdb_id: str = "", year: int = 0, thr: float = 0.92, tol: int = 1) -> bool:
    seen = _load_seen()
    if imdb_id and imdb_id in seen["by_imdb"]: 
        return True
    key = normalize_title(title)
    if key in seen["by_key"]:
        return True
    # fuzzy fallback
    for k, meta in seen["by_key"].items():
        if fuzzy_match(key, k, thr):
            y = int(meta.get("year") or 0)
            if (year==0) or (abs(y - year) <= tol): 
                return True
    return False