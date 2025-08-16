from __future__ import annotations
import requests
from typing import Dict, Any, List
from .cache import JsonCache

API_BASE = "https://api.themoviedb.org/3"
MAX_DISCOVER_PAGES = 500  # TMDB discover hard cap

def _cap_page(n: int) -> int:
    try:
        n = int(n)
    except Exception:
        n = 1
    if n < 1:
        return 1
    if n > MAX_DISCOVER_PAGES:
        return MAX_DISCOVER_PAGES
    return n

class TMDB:
    def __init__(self, api_key: str, cache: JsonCache):
        self.api_key = api_key
        self.cache = cache
        self._session = requests.Session()

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params = {"api_key": self.api_key, **params}
        url = f"{API_BASE}{path}"
        # Cache key without the api_key to avoid cache busting noise
        ck = f"GET:{path}:{sorted((k, v) for k, v in params.items() if k != 'api_key')}"
        hit = self.cache.get(ck, ttl_seconds=60 * 60)  # 1h
        if hit is not None:
            return hit
        resp = self._session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        self.cache.set(ck, data)
        return data

    def discover(
        self,
        kind: str,
        page: int,
        language: str,
        with_original_language: List[str],
        region: str
    ) -> Dict[str, Any]:
        if kind not in ("movie", "tv"):
            raise ValueError("kind must be 'movie' or 'tv'")
        path = f"/discover/{kind}"
        params = {
            "page": _cap_page(page),  # <= 500 always
            "language": language,
            "watch_region": region,   # ok to include without with_watch_providers
            "sort_by": "popularity.desc",
        }
        if with_original_language:
            params["with_original_language"] = ",".join(with_original_language)
        return self._get(path, params)

    def total_pages(
        self,
        kind: str,
        language: str,
        with_original_language: List[str],
        region: str
    ) -> int:
        """
        TMDB sometimes reports extremely large total_pages,
        but the API only allows fetching pages 1..500 for discover.
        Always return a value capped to 500 (and at least 1).
        """
        data = self.discover(
            kind=kind,
            page=1,
            language=language,
            with_original_language=with_original_language,
            region=region,
        )
        raw_total = int(data.get("total_pages", 1) or 1)
        if raw_total < 1:
            raw_total = 1
        if raw_total > MAX_DISCOVER_PAGES:
            return MAX_DISCOVER_PAGES
        return raw_total

    def watch_providers_for(self, kind: str, tmdb_id: int, region: str) -> List[str]:
        path = f"/{kind}/{tmdb_id}/watch/providers"
        data = self._get(path, {})
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