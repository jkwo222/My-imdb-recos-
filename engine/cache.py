# File: engine/cache.py
from __future__ import annotations
import json, os, time
from typing import Any, Optional

_ROOT = "data/cache/app"
os.makedirs(_ROOT, exist_ok=True)

def _path(key: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in key)
    return os.path.join(_ROOT, f"{safe}.json")

def get_fresh(key: str, ttl_days: int = 1) -> Optional[Any]:
    p = _path(key)
    if not os.path.exists(p):
        return None
    try:
        age = time.time() - os.path.getmtime(p)
        if age > ttl_days * 86400:
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def set(key: str, value: Any) -> None:
    p = _path(key)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False)
    os.replace(tmp, p)