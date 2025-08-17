# engine/tmdb.py
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Dict, List, Tuple, Any
from urllib.parse import urlencode

import requests

# ---- basic env helpers ----

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

TMDB_API_KEY = _env("TMDB_API_KEY")

# ---- caching with short hashed filenames to avoid OSError(36) ----

def _cache_dir() -> str:
    path = os.path.join("data", "cache")
    os.makedirs(path, exist_ok=True)
    return path

def _cache_key(endpoint: str, url: str, params: Dict[str, Any]) -> str:
    # Deterministic string that captures request uniqueness
    raw = f"{endpoint}|{url}|{json.dumps(params, sort_keys=True, separators=(',',':'))}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def _cache_path(prefix: str, key_hex: str) -> str:
    # Short, safe filename
    return os.path.join(_cache_dir(), f"{prefix}_{key_hex}.json")


# ---- provider mapping & parsing ----

_PROVIDER_NAME_TO_ID = {
    # common US/global streaming ids (TMDB watch providers)
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "max": 384,            # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
    # extend here if you add more services
}

def providers_from_env(subs_csv: str, region: str) -> str:
    """
    Convert a comma-separated list of service slugs into TMDB watch provider IDs
    joined by '|', which TMDB accepts for with_watch_providers.
    We keep the interface (subs_csv, region) to match catalog usage; 'region'
    is not used to change IDs, but it's required by the caller.
    """
    if not subs_csv:
        return ""
    ids: List[str] = []
    for raw in subs_csv.split(","):
        name = raw.strip().lower()
        if not name:
            continue
        # allow both "disney_plus" and "disney-plus" and "disney+"
        name = name.replace("+", "_").replace("-", "_")
        pid = _PROVIDER_NAME_TO_ID.get(name)
        if pid is not None:
            ids.append(str(pid))
    return "|".join(ids)


# ---- HTTP + TMDB helpers ----

_BASE = "https://api.themoviedb.org/3"

def _get_json(prefix: str, url: str, params: Dict[str, Any], ttl_seconds: int = 3600) -> Dict:
    """
    Get JSON from TMDB with a tiny cache on disk using hashed filenames.
    """
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY is not set")

    # attach API key
    params = dict(params)
    params["api_key"] = TMDB_API_KEY

    # short hashed cache key
    key = _cache_key(prefix, url, params)
    path = _cache_path(prefix, key)

    # fetch from cache if fresh
    if os.path.exists(path):
        try:
            mtime = os.path.getmtime(path)
            if time.time() - mtime < ttl_seconds:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass

    # live request
    full_url = f"{url}?{urlencode(params)}"
    resp = requests.get(full_url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # write cache
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        # cache write failures are non-fatal
        pass

    return data


def _simplify_result(kind: str, r: Dict) -> Dict:
    """
    Normalize TMDB discover item into a compact dict the engine expects.
    """
    if kind == "movie":
        title = r.get("title") or r.get("original_title") or ""
        date = r.get("release_date") or ""
        year = int(date[:4]) if date[:4].isdigit() else None
    else:
        title = r.get("name") or r.get("original_name") or ""
        date = r.get("first_air_date") or ""
        year = int(date[:4]) if date[:4].isdigit() else None

    return {
        "type": "movie" if kind == "movie" else "tvSeries",
        "tmdb_id": r.get("id"),
        "title": title,
        "year": year,
        "popularity": r.get("popularity", 0.0),
        "vote_average": r.get("vote_average", 0.0),
    }


def _discover(kind: str, page: int, *, region: str, provider_ids: str, original_langs: str) -> Dict:
    """
    Call TMDB discover endpoint for movies or TV.
    """
    url = f"{_BASE}/discover/{kind}"

    params: Dict[str, Any] = {
        "include_adult": "false",
        "language": "en-US",  # response language
        "page": page,
        "sort_by": "popularity.desc",
        "watch_region": region or "US",
    }

    if provider_ids:
        params["with_watch_providers"] = provider_ids
        params["with_watch_monetization_types"] = "flatrate|free|ads"

    if original_langs:
        # comma-separated list like "en,es" becomes filter for original language
        params["with_original_language"] = original_langs

    return _get_json(f"discover_{kind}", url, params)


def discover_movie_page(
    page: int,
    *,
    region: str,
    provider_ids: str,
    original_langs: str,
) -> Tuple[List[Dict], Dict]:
    data = _discover("movie", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", [])
    items = [_simplify_result("movie", r) for r in results]
    return items, {"page": data.get("page"), "total_pages": data.get("total_pages")}


def discover_tv_page(
    page: int,
    *,
    region: str,
    provider_ids: str,
    original_langs: str,
    include_seasons: bool = True,  # placeholder; not used in discover
) -> Tuple[List[Dict], Dict]:
    data = _discover("tv", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", [])
    items = [_simplify_result("tv", r) for r in results]
    return items, {"page": data.get("page"), "total_pages": data.get("total_pages")}