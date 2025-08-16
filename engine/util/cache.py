# FILE: engine/util/cache.py
from __future__ import annotations
import hashlib
import json
import os
import time
from typing import Any, Optional

class DiskCache:
    """
    Tiny JSON disk cache for API responses (URL+params key).
    Files live under {root}/{prefix}/{sha}.json with mtime-based TTL.
    """

    def __init__(self, root: str, ttl_secs: int):
        self.root = root
        self.ttl = ttl_secs
        os.makedirs(root, exist_ok=True)

    def _key(self, prefix: str, url: str, params: dict | None) -> str:
        base = f"{url}|{json.dumps(params or {}, sort_keys=True)}".encode("utf-8")
        sha = hashlib.sha256(base).hexdigest()
        return os.path.join(self.root, prefix, f"{sha}.json")

    def get(self, prefix: str, url: str, params: dict | None) -> Optional[Any]:
        path = self._key(prefix, url, params)
        if not os.path.exists(path):
            return None
        age = time.time() - os.path.getmtime(path)
        if self.ttl > 0 and age > self.ttl:
            # stale; best-effort delete
            try:
                os.remove(path)
            except Exception:
                pass
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def set(self, prefix: str, url: str, params: dict | None, value: Any) -> None:
        path = self._key(prefix, url, params)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False)
        os.replace(tmp, path)