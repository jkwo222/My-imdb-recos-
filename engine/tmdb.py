# FILE: engine/tmdb.py
from __future__ import annotations

import requests
from typing import Any, Dict, List

from .util.cache import DiskCache, BloomSeen, ProviderSlugStore

_TMDB_BASE = "https://api.themoviedb.org/3"

# Minimal normalization map: TMDB display name -> canonical slug
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


class TMDB:
    def __init__(self, api_key: str, cache: DiskCache):
        self.api_key = api_key
        self.cache = cache
        # local provider slug store + bloom (persisted under cache root)
        root = getattr(cache, "root", "data/cache")
        self._prov_store = ProviderSlugStore(f"{root}/prov_slugs.json")
        self._bloom = BloomSeen(f"{root}/seen.bloom")

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

    def discover(
        self,
        kind: str,                 # "movie" or "tv"
        page: int,
        language: str,
        with_original_language: List[str],
        watch_region: str,
    ) -> Dict[str, Any]:
        assert kind in ("movie", "tv")
        params = {
            "page": page,
            "language": language,
            "watch_region": watch_region,
            "sort_by": "popularity.desc",
        }
        if with_original_language:
            params["with_original_language"] = ",".join(with_original_language)
        return self._get(f"/discover/{kind}", params)

    def total_pages(
        self,
        kind: str,
        language: str,
        with_original_language: List[str],
        watch_region: str,
    ) -> int:
        first = self.discover(kind, 1, language, with_original_language, watch_region)
        total = int(first.get("total_pages", 1)) if isinstance(first, dict) else 1
        return max(1, min(total, 500))

    def providers_for_title(self, kind: str, tmdb_id: int, region: str) -> List[str]:
        """
        Returns normalized provider slugs for the given title in region.
        Fast path: ProviderSlugStore + BloomSeen.
        Fallback: GET /{kind}/{id}/watch/providers and cache.
        """
        key = f"prov:{kind}:{tmdb_id}:{region}"

        # If we have it in the JSON store, return immediately (also satisfies bloom intent).
        cached_slugs = self._prov_store.get(kind, tmdb_id, region)
        if cached_slugs:
            return cached_slugs

        # If bloom says "likely seen" but JSON missing, we still try network once.
        data = self._get(f"/{kind}/{tmdb_id}/watch/providers", params={})
        res = data.get("results", {})
        region_block = res.get(region.upper(), {})
        names: List[str] = []
        for key_name in ("flatrate", "ads", "free", "rent", "buy"):
            for item in region_block.get(key_name, []) or []:
                n = item.get("provider_name")
                if n:
                    names.append(n)
        slugs = normalize_provider_names(names)

        # Persist slugs + mark bloom
        if slugs:
            self._prov_store.put(kind, tmdb_id, region, slugs)
            self._prov_store.save()
        self._bloom.add(key)
        self._bloom.save()

        return slugs