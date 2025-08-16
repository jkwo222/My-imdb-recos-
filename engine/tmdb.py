from __future__ import annotations
import requests
from typing import Dict, Any, List, Tuple
from .cache import JsonCache

API_BASE = "https://api.themoviedb.org/3"

class TMDB:
    def __init__(self, api_key: str, cache: JsonCache):
        self.api_key = api_key
        self.cache = cache
        self._session = requests.Session()

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params = {"api_key": self.api_key, **params}
        url = f"{API_BASE}{path}"
        ck = f"GET:{path}:{sorted(params.items())}"
        hit = self.cache.get(ck, ttl_seconds=60*60)  # 1h default
        if hit is not None:
            return hit
        resp = self._session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        self.cache.set(ck, data)
        return data

    # Discover endpoints (no provider filter here; we filter post-hoc via watch/providers)
    def discover(self, kind: str, page: int, language: str, with_original_language: List[str], region: str) -> Dict[str, Any]:
        if kind not in ("movie", "tv"):
            raise ValueError("kind must be 'movie' or 'tv'")
        path = f"/discover/{kind}"
        params = {
            "page": page,
            "language": language,
            "watch_region": region,
            "sort_by": "popularity.desc",
        }
        if with_original_language:
            params["with_original_language"] = ",".join(with_original_language)
        return self._get(path, params)

    def total_pages(self, kind: str, language: str, with_original_language: List[str], region: str) -> int:
        data = self.discover(kind, page=1, language=language, with_original_language=with_original_language, region=region)
        return int(data.get("total_pages", 1) or 1)

    def watch_providers_for(self, kind: str, tmdb_id: int, region: str) -> List[str]:
        path = f"/{kind}/{tmdb_id}/watch/providers"
        data = self._get(path, {})
        # Structure: results: { "US": { "flatrate":[{"provider_name":...}, ...], "ads": [...], "free":[...], ... } }
        res = data.get("results", {})
        reg = res.get(region.upper()) or res.get(region) or {}
        providers = []
        for bucket in ("flatrate", "ads", "free"):
            for ent in reg.get(bucket, []) or []:
                name = ent.get("provider_name")
                if name:
                    providers.append(str(name))
        return sorted(set(providers))

    def external_ids_for(self, kind: str, tmdb_id: int) -> Dict[str, Any]:
        path = f"/{kind}/{tmdb_id}/external_ids"
        return self._get(path, {})