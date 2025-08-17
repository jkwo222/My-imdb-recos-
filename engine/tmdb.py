# engine/tmdb.py
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Tuple

import requests

# ---- TMDB core ----
_TMDB_BASE = "https://api.themoviedb.org/3"
_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

# Cache directory
_CACHE_DIR = os.path.join("data", "cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

# Basic retry/backoff
_RETRIES = 2
_TIMEOUT = 20


# --- Provider helpers ---------------------------------------------------------

# Stable mapping of provider slugs -> TMDB numeric IDs (global ones TMDB uses)
# Add more if you subscribe to other services.
_PROVIDER_ID_MAP = {
    "netflix": 8,
    "prime_video": 9,      # Amazon Prime Video
    "hulu": 15,
    "max": 384,            # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

def providers_from_env(subs_csv: str, region: str) -> List[int]:
    """
    Convert a comma-separated list of provider slugs into TMDB provider IDs.
    Region is accepted for future per-region mappings; for now IDs are global.
    """
    if not subs_csv:
        return []
    slugs = [s.strip().lower() for s in subs_csv.split(",") if s.strip()]
    ids: List[int] = []
    for s in slugs:
        pid = _PROVIDER_ID_MAP.get(s)
        if pid is not None:
            ids.append(pid)
    # De-duplicate while preserving order
    seen = set()
    out: List[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


# --- Caching primitives -------------------------------------------------------

def _hash_cache_key(endpoint: str, params: Dict[str, Any]) -> str:
    # Sort params for stability
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    payload = json.dumps({"endpoint": endpoint, "params": items}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()  # 40 chars


def _cache_path(prefix: str, endpoint: str, params: Dict[str, Any]) -> str:
    h = _hash_cache_key(endpoint, params)
    return os.path.join(_CACHE_DIR, f"{prefix}_{h}.json")


def _get_json(prefix: str, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    GET JSON with local caching. Cache file name is a short hash to avoid
    OS path length limits.
    """
    if not _API_KEY:
        raise RuntimeError("TMDB_API_KEY is not set")

    query = dict(params or {})
    query["api_key"] = _API_KEY

    cache_file = _cache_path(prefix, endpoint, query)
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{_TMDB_BASE}/{endpoint.lstrip('/')}"
    last_err: Exception | None = None
    for attempt in range(_RETRIES + 1):
        try:
            resp = requests.get(url, params=query, timeout=_TIMEOUT)
            if 500 <= resp.status_code < 600:
                last_err = RuntimeError(f"TMDB 5xx on {endpoint}: {resp.status_code}")
                time.sleep(1.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return data
        except Exception as e:
            last_err = e
            if attempt < _RETRIES:
                time.sleep(1.0 * (attempt + 1))
            else:
                break
    assert last_err is not None
    raise last_err


# --- Public discovery functions used by catalog.py ----------------------------

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
    if kind not in ("movie", "tv"):
        raise ValueError("kind must be 'movie' or 'tv'")

    endpoint = f"discover/{kind}"
    params: Dict[str, Any] = {
        "page": page,
        "sort_by": "popularity.desc",
        "include_adult": str(bool(include_adult)).lower(),
        "watch_region": region,
        "language": language or "en-US",
        "with_watch_monetization_types": "flatrate|free|ads",
    }

    if provider_ids:
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)

    if original_langs:
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
    data = _discover(
        "tv",
        page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        language=language,
    )
    return data.get("results", []), int(data.get("page", page) or page)