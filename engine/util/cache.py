# FILE: engine/util/cache.py
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Optional

try:
    # lightweight, already in your requirements
    from bloom_filter2 import BloomFilter
except Exception:  # pragma: no cover
    BloomFilter = None  # graceful fallback

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
        return os.path.join(self.root, prefix, hashlib.sha256(base).hexdigest() + ".json")

    def get(self, prefix: str, url: str, params: dict | None) -> Optional[Any]:
        path = self._key(prefix, url, params)
        if not os.path.exists(path):
            return None
        age = time.time() - os.path.getmtime(path)
        if self.ttl > 0 and age > self.ttl:
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


class BloomSeen:
    """
    Rolling Bloom filter persisted on disk.
    Used to short-circuit expensive provider lookups across runs.

    Key format recommendation: f"prov:{kind}:{tmdb_id}:{region}"
    """
    def __init__(self, path: str, capacity: int = 500_000, error_rate: float = 0.001):
        self.path = path
        self.capacity = capacity
        self.error_rate = error_rate
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._bf = None
        self._load()

    def _load(self):
        if BloomFilter is None:
            self._bf = set()  # fallback to in-memory set
            return
        if os.path.exists(self.path):
            try:
                with open(self.path, "rb") as f:
                    self._bf = BloomFilter.fromfile(f)
                    return
            except Exception:
                pass
        # fresh one
        self._bf = BloomFilter(max_elements=self.capacity, error_rate=self.error_rate)

    def save(self):
        if BloomFilter is None:
            return
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "wb") as f:
                self._bf.tofile(f)
            os.replace(tmp, self.path)
        except Exception:
            pass

    def __contains__(self, key: str) -> bool:
        return key in self._bf

    def add(self, key: str) -> None:
        self._bf.add(key)


class ProviderSlugStore:
    """
    Simple JSON map for normalized provider slugs per title.
    { "<kind>:<tmdb_id>:<region>": ["netflix","max", ...] }
    """
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._data: dict[str, list[str]] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    def save(self):
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False)
            os.replace(tmp, self.path)
        except Exception:
            pass

    def get(self, kind: str, tmdb_id: int, region: str) -> Optional[list[str]]:
        return self._data.get(f"{kind}:{tmdb_id}:{region}")

    def put(self, kind: str, tmdb_id: int, region: str, slugs: list[str]) -> None:
        self._data[f"{kind}:{tmdb_id}:{region}"] = sorted(set(slugs))