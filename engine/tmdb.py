# engine/tmdb.py
import os
import time
from typing import Dict, Iterator, Optional
import requests

TMDB_BASE = "https://api.themoviedb.org/3"

# Minimal provider → TMDB ID map (US region). Add more as you like.
_PROVIDER_IDS = {
    "netflix": 8, "prime_video": 9, "hulu": 15, "max": 384, "disney_plus": 337,
    "apple_tv_plus": 350, "peacock": 386, "paramount_plus": 531
}

def _get(api_key: str, path: str, params: Dict) -> Dict:
    headers = {"Authorization": f"Bearer {api_key}"} if api_key and api_key.count(".") >= 2 else {}
    # Support plain API key too
    if not headers:
        params = dict(params)
        params["api_key"] = api_key
    r = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def _discover(
    api_key: str,
    media_type: str,
    page: int,
    region: str,
    with_original_language: str,
    provider_names,
) -> Dict:
    if media_type not in ("movie", "tv"):
        raise ValueError("media_type must be 'movie' or 'tv'")
    params = {
        "include_adult": "false",
        "language": "en-US",
        "page": page,
        "sort_by": "popularity.desc",
        "watch_region": region,
        "with_original_language": with_original_language,
    }
    # Provider filter
    ids = [_PROVIDER_IDS[p] for p in provider_names if p in _PROVIDER_IDS]
    if ids:
        params["with_watch_providers"] = "|".join(str(i) for i in ids)
        params["watch_region"] = region

    path = f"/discover/{media_type}"
    return _get(api_key, path, params)

def discover_movie_page(
    api_key: str,
    page: int,
    region: str,
    with_original_language: str,
    provider_names,
) -> Dict:
    """Exported for engine.catalog._resolve_callable"""
    return _discover(api_key, "movie", page, region, with_original_language, provider_names)

def discover_tv_page(
    api_key: str,
    page: int,
    region: str,
    with_original_language: str,
    provider_names,
) -> Dict:
    """Exported for engine.catalog._resolve_callable"""
    return _discover(api_key, "tv", page, region, with_original_language, provider_names)

def iter_discover(
    api_key: str,
    media_type: str,
    pages: int,
    region: str,
    with_original_language: str,
    provider_names,
) -> Iterator[Dict]:
    for page in range(1, max(1, pages) + 1):
        yield _discover(api_key, media_type, page, region, with_original_language, provider_names)
        time.sleep(0.15)  # be polite