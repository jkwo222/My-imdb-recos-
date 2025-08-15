import os, json, time, hashlib
from typing import Any, Optional

BASE = "data/cache"

def _path_for(key: str) -> str:
    os.makedirs(BASE, exist_ok=True)
    safe = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return os.path.join(BASE, f"{safe}.json")

def set(key: str, obj: Any) -> None:
    p = _path_for(key)
    with open(p, "w", encoding="utf-8") as f:
        json.dump({"_ts": time.time(), "data": obj}, f, ensure_ascii=False, indent=2)

def get_raw(key: str) -> Optional[dict]:
    p = _path_for(key)
    if not os.path.exists(p): return None
    try:
        return json.load(open(p, "r", encoding="utf-8"))
    except Exception:
        return None

def get_fresh(key: str, ttl_hours: int = 0, ttl_days: int = 0) -> Optional[Any]:
    blob = get_raw(key)
    if not blob: return None
    ts = float(blob.get("_ts", 0.0))
    age = (time.time() - ts)
    ttl = (ttl_hours * 3600) + (ttl_days * 86400)
    if ttl and age > ttl: return None
    return blob.get("data")