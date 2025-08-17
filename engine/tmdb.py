# engine/tmdb.py
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Tuple

import requests

# ---- TMDB settings ----
_TMDB_BASE = "https://api.themoviedb.org/3"
_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

# Cache directory
_CACHE_DIR = os.path.join("data", "cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

# Optional: simple backoff for transient 5xx
_RETRIES = 2
_TIMEOUT = 20


def _hash_cache_key(endpoint: str, params: Dict[str, Any]) -> str:
    """
    Create a stable, short cache key for endpoint+params.
    """
    # Sort params to ensure deterministic hash
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    payload = json.dumps({"endpoint": endpoint, "params": items}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()  # 40 chars


def _cache_path(prefix: str, endpoint: str, params: Dict[str, Any]) -> str:
    """
    Build a safe path under data/cache using a short hashed filename.
    """
    h = _hash_cache_key(endpoint, params)
    # Keep it legible but short
    base = f"{prefix}_{h}.json"
    return os.path.join(_CACHE_DIR, base)


def _get_json(prefix: str, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    GET JSON with local caching. Cache key is a hash of endpoint+params.
    """
    if not _API_KEY:
        raise RuntimeError("TMDB_API_KEY is not set")

    # Attach API key
    q = dict(params or {})
    q["api_key"] = _API_KEY

    path = _cache_path(prefix, endpoint, q)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{_TMDB_BASE}/{endpoint.lstrip('/')}"
    last_err = None
    for attempt in range(_RETRIES + 1):
        try:
            resp = requests.get(url, params=q, timeout=_TIMEOUT)
            if 500 <= resp.status_code < 600:
                # retryable
                last_err = RuntimeError(f"TMDB 5xx on {endpoint}: {resp.status_code}")
                time.sleep(1.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            # Write-through cache
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < _RETRIES:
                time.sleep(1.0 * (attempt + 1))
            else:
                break
    # Surface the last error
    raise last_err  # type: ignore[misc]


def _discover(
    kind: str,
    page: int,
    *,
    region: str,
    provider_ids: List[int] | None,
    original_langs: str | None,
    language: str | None = "en-US",
    include_adult: bool = False,
) -> Dict[str, Any]:
    """
    Call TMDB /discover/{movie|tv} with consistent filters.
    """
    if kind not in ("movie", "tv"):
        raise ValueError("kind must be 'movie' or 'tv'")

    endpoint = f"discover/{kind}"
    params: Dict[str, Any] = {
        "page": page,
        "sort_by": "popularity.desc",
        "include_adult": str(bool(include_adult)).lower(),
        "watch_region": region,
        "language": language or "en-US",
        # Limit to subscription/free/ad-supported surfaces
        "with_watch_monetization_types": "flatrate|free|ads",
    }

    if provider_ids:
        # Pipe-separated numeric IDs
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)

    if original_langs:
        # Comma-separated list (TMDB accepts comma or pipe; comma is fine)
        params["with_original_language"] = original_langs

    prefix = f"tmdb_discover_{kind}"
    return _get_json(prefix, endpoint, params)


def discover_movie_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int] | None,
    original_langs: str | None,
    language: str | None = "en-US",
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Returns (results, page_number). Results are TMDB 'results' list for movies.
    """
    data = _discover(
        "movie",
        page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        language=language,
    )
    return data.get("results", []), int(data.get("page", page) or page)


def discover_tv_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int] | None,
    original_langs: str | None,
    language: str | None = "en-US",
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Returns (results, page_number). Results are TMDB 'results' list for TV.
    """
    data = _discover(
        "tv",
        page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        language=language,
    )
    return data.get("results", []), int(data.get("page", page) or page)