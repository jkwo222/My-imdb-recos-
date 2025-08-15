# engine/cache.py
import os, json, time, hashlib
from typing import Any, Optional

ROOT = "data/cache"

def _p(key: str) -> str:
    os.makedirs(ROOT, exist_ok=True)
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return os.path.join(ROOT, f"{h}.json")

def get_fresh(key: str, ttl_days: float = 1.0) -> Optional[Any]:
    path = _p(key)
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    age_days = (time.time() - st.st_mtime) / 86400.0
    if age_days > ttl_days:
        return None
    try:
        return json.load(open(path, "r", encoding="utf-8"))
    except Exception:
        return None

def set(key: str, value: Any) -> None:
    path = _p(key)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)