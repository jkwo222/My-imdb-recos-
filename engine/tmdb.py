# File: engine/tmdb.py
from __future__ import annotations
import requests
from typing import Any, Dict, List
from .util import DiskCache

_TMDB_BASE = "https://api.themoviedb.org/3"

class TMDB:
    def __init__(self, api_key: str, cache: DiskCache):
        self.api_key = api_key
        self.cache = cache

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{_TMDB_BASE}{path}"
        full_params = {"api_key": self.api_key, **params}
        cached = self.cache.get("http", url, full_params)
        if cached is not None:
            return cached
        resp = requests.get(url, params=full_params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        self.cache.set("http", url, full_params, data)
        return data

    def discover(self, kind: str, page: int, language: str,
                 with_original_language: List[str], watch_region: str) -> Dict[str, Any]:
        assert kind in ("movie", "tv")
        params = {
            "page": page,
            "language": language,
            "watch_region": watch_region,
            "sort_by": "popularity.desc",
            "include_adult": "false",
        }
        if with_original_language:
            params["with_original_language"] = ",".join(with_original_language)
        return self._get(f"/discover/{kind}", params)

    def total_pages(self, kind: str, language: str,
                    with_original_language: List[str], watch_region: str) -> int:
        first = self.discover(kind, 1, language, with_original_language, watch_region)
        total = int(first.get("total_pages", 1)) if isinstance(first, dict) else 1
        return max(1, min(total, 500))

    def providers_for_title(self, kind: str, tmdb_id: int, region: str) -> List[str]:
        data = self._get(f"/{kind}/{tmdb_id}/watch/providers", params={})
        res = data.get("results", {})
        region_block = res.get(region.upper(), {})
        names = []
        for key in ("flatrate", "ads", "free", "rent", "buy"):
            for item in region_block.get(key, []) or []:
                n = item.get("provider_name")
                if n:
                    names.append(n)
        return sorted(set(names))

_PROVIDER_NAME_TO_SLUG = {
    "Netflix": "netflix",
    "Amazon Prime Video": "prime_video",
    "Hulu": "hulu",
    "Max": "max",
    "HBO Max": "max",
    "Disney Plus": "disney_plus",
    "Disney+": "disney_plus",
    "Apple TV Plus": "apple_tv_plus",
    "Apple TV+": "apple_tv_plus",
    "Peacock": "peacock",
    "Paramount Plus": "paramount_plus",
    "Paramount+": "paramount_plus",
}

def normalize_provider_names(provider_names: List[str]) -> List[str]:
    out = []
    for n in provider_names:
        out.append(_PROVIDER_NAME_TO_SLUG.get(n, n.strip().lower().replace(" ", "_").replace("+", "plus")))
    return sorted(set(out))