import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

JSON = Dict[str, Any]

class DiskCache:
    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, group: str, key: str) -> Path:
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
        p = self.root / group
        p.mkdir(parents=True, exist_ok=True)
        return p / f"{h}.json"

    def get(self, group: str, key: str, ttl_min: int) -> Optional[JSON]:
        path = self._path_for(group, key)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            fetched_at = payload.get("_fetched_ts", 0)
            age_min = (time.time() - fetched_at) / 60.0
            if age_min <= ttl_min:
                return payload.get("data")
            return None
        except Exception:
            return None

    def put(self, group: str, key: str, data: JSON) -> None:
        path = self._path_for(group, key)
        payload = {"_fetched_ts": time.time(), "data": data}
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)

class TMDB:
    def __init__(self, api_key: str, region: str, language: str, cache: Optional[DiskCache]):
        self.api_key = api_key
        self.region = region
        self.language = language
        self.cache = cache
        self.base = "https://api.themoviedb.org/3"

    def _mk_key(self, path: str, params: Dict[str, Any]) -> str:
        items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
        return f"{path}?{items}"

    def _get(self, path: str, params: Dict[str, Any],
             cache_group: Optional[str] = None,
             ttl_min: int = 0) -> JSON:
        params = {**params, "api_key": self.api_key}
        key = self._mk_key(path, params)

        if cache_group and self.cache:
            cached = self.cache.get(cache_group, key, ttl_min)
            if cached is not None:
                return cached

        url = f"{self.base}{path}"
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        if cache_group and self.cache:
            self.cache.put(cache_group, key, data)
        return data

    def providers_map(self, country: str, cache_ttl_min: int = 10080) -> Dict[str, int]:
        """
        Build a lowercased provider-name -> id map using both movie and TV provider lists.
        Cached for 7 days by default.
        """
        mp = self._get("/watch/providers/movie", {"language": self.language})
        tp = self._get("/watch/providers/tv", {"language": self.language})
        out: Dict[str, int] = {}

        for blob in (mp.get("results", []), tp.get("results", [])):
            for p in blob:
                name = (p.get("provider_name") or "").strip().lower().replace("&", "and")
                pid = int(p.get("provider_id"))
                # TMDB returns a list for all countries; we don’t filter by country here because
                # the watch_region filter is applied in /discover anyway.
                if name and pid:
                    out[name] = pid

        return out

    def discover(self, kind: str, page: int, with_provider_ids: str,
                 with_original_language: Optional[str],
                 slot: int,
                 cache_ttl_min: int,
                 cache_enabled: bool) -> JSON:
        """
        kind: 'movie' or 'tv'
        """
        path = f"/discover/{kind}"
        # Cache-buster only affects CDN; our disk cache key includes params anyway.
        params = {
            "page": page,
            "language": self.language,
            "region": self.region,
            "watch_region": self.region,
            "with_watch_providers": with_provider_ids,
            "include_adult": "false",
            "sort_by": "popularity.desc",
            "cb": slot,  # vary every 15 min slot
        }
        if with_original_language:
            params["with_original_language"] = with_original_language

        group = f"discover_{kind}"
        return self._get(
            path, params,
            cache_group=group if cache_enabled else None,
            ttl_min=cache_ttl_min
        )

    def total_pages(self, kind: str, with_provider_ids: str,
                    with_original_language: Optional[str],
                    slot: int,
                    cache_ttl_min: int,
                    cache_enabled: bool) -> int:
        d = self.discover(kind, 1, with_provider_ids, with_original_language,
                          slot, cache_ttl_min, cache_enabled)
        return int(d.get("total_pages") or 1)