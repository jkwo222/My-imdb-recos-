# engine/tmdb.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

_TMDB_BASE = "https://api.themoviedb.org/3"
_TMDB_TIMEOUT = 12  # seconds


class TmdbError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise TmdbError("TMDB_API_KEY is not configured.")
    return key


def _get(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{_TMDB_BASE}/{endpoint.lstrip('/')}"
    p = {"api_key": _api_key(), **params}
    r = requests.get(url, params=p, timeout=_TMDB_TIMEOUT)
    if r.status_code != 200:
        raise TmdbError(f"TMDB {endpoint} -> {r.status_code}: {r.text[:200]}")
    return r.json()


def discover_movie_page(
    *,
    page: int,
    watch_region: Optional[str] = None,
    with_watch_providers: Optional[str] = None,
    with_original_language: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Return (results, total_pages) for a single TMDB discover/movie page.
    Filters are optional and mirror TMDB API parameters.
    """
    params: Dict[str, Any] = {
        "page": page,
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "include_video": "false",
    }
    if watch_region:
        params["watch_region"] = watch_region
    if with_watch_providers:
        params["with_watch_providers"] = with_watch_providers
    if with_original_language:
        params["with_original_language"] = with_original_language

    data = _get("discover/movie", params)
    return data.get("results", []), int(data.get("total_pages", 1) or 1)


def discover_tv_page(
    *,
    page: int,
    watch_region: Optional[str] = None,
    with_watch_providers: Optional[str] = None,
    with_original_language: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Return (results, total_pages) for a single TMDB discover/tv page.
    """
    params: Dict[str, Any] = {
        "page": page,
        "sort_by": "popularity.desc",
        "include_null_first_air_dates": "false",
    }
    if watch_region:
        params["watch_region"] = watch_region
    if with_watch_providers:
        params["with_watch_providers"] = with_watch_providers
    if with_original_language:
        params["with_original_language"] = with_original_language

    data = _get("discover/tv", params)
    return data.get("results", []), int(data.get("total_pages", 1) or 1)