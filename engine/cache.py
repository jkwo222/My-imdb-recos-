from __future__ import annotations
import json
import os
import time
import hashlib
from typing import Any, Optional

class JsonCache:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _path_for(self, key: str) -> str:
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()
        sub = os.path.join(self.base_dir, h[:2])
        os.makedirs(sub, exist_ok=True)
        return os.path.join(sub, f"{h}.json")

    def get(self, key: str, ttl_seconds: int | None = None) -> Optional[Any]:
        p = self._path_for(key)
        if not os.path.exists(p):
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if ttl_seconds is not None:
                if float(obj.get("_saved_at", 0.0)) + ttl_seconds < time.time():
                    return None
            return obj.get("value", None)
        except Exception:
            return None

    def set(self, key: str, value: Any) -> None:
        p = self._path_for(key)
        tmp = p + ".tmp"
        data = {"_saved_at": time.time(), "value": value}
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, p)

    def delete(self, key: str) -> None:
        p = self._path_for(key)
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass