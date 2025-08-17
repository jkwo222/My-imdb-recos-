# engine/tmdb.py
from __future__ import annotations
import os
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests

# ----------------------------
# Config / cache directories
# ----------------------------
ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache" / "tmdb"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TMDB_BASE = "https://api.themoviedb.org/3"

# Common watch provider ids (US) to avoid an API call just to resolve names.
# (This matches TMDB as of 2025; adjust if you add more.)
_PROVIDER_MAP = {
    "netflix": 8,
    "prime_video": 9,          # Amazon Prime Video
    "hulu": 15,
    "max": 384,                # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

# ----------------------------
# HTTP helpers with caching
# ----------------------------

def _headers() -> Dict[str, str]:
    """
    Use Bearer when TMDB_ACCESS_TOKEN (Read Access Token v4) is provided.
    Otherwise fall back to API key via query param (handled in _get_json).
    """
    h = {
        "Accept": "application/json",
        "User-Agent": "jkwo222-imdb-recos/1.0",
    }
    tok = os.getenv("TMDB_ACCESS_TOKEN", "").strip()
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def _hash_for(url: str, params: Dict[str, Any]) -> str:
    # Stable hashing for long param sets
    key = url + "?" + "&".join(
        f"{k}={params[k]}" for k in sorted(params.keys())
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _cache_path(prefix: str, url: str, params: Dict[str, Any]) -> Path:
    h = _hash_for(url, params)
    return CACHE_DIR / f"{prefix}_{h}.json"


def _cache_get(prefix: str, url: str, params: Dict[str, Any], ttl_seconds: int) -> Optional[Dict[str, Any]]:
    p = _cache_path(prefix, url, params)
    try:
        if p.exists():
            age = time.time() - p.stat().st_mtime
            if age <= ttl_seconds:
                with p.open("r", encoding="utf-8") as f:
                    return json.load(f)
    except Exception:
        pass
    return None


def _cache_put(prefix: str, url: str, params: Dict[str, Any], data: Dict[str, Any]) -> None:
    p = _cache_path(prefix, url, params)
    try:
        with p.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _get_json(prefix: str, url: str, params: Dict[str, Any], ttl_seconds: int = 6 * 3600) -> Dict[str, Any]:
    """
    GET with caching. TTL default 6h for discover pages.
    Uses Bearer header if TMDB_ACCESS_TOKEN is set; otherwise adds api_key param.
    Raises for non-200.
    """
    # Ensure language default (avoid None in query)
    params = {k: v for k, v in params.items() if v is not None}

    cached = _cache_get(prefix, url, params, ttl_seconds)
    if cached is not None:
        return cached

    # If no bearer, add api_key
    if "Authorization" not in _headers():
        api_key = os.getenv("TMDB_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("TMDB_API_KEY or TMDB_ACCESS_TOKEN required.")
        params = dict(params)  # copy
        params["api_key"] = api_key

    r = requests.get(url, headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    _cache_put(prefix, url, params, data)
    return data


# ----------------------------
# Provider helpers
# ----------------------------

def providers_from_env(subs_csv: str, region: Optional[str] = None) -> List[int]:
    """
    Convert CSV like 'netflix,prime_video,disney_plus' -> TMDB provider IDs.
    Region is accepted for signature compatibility but not currently used,
    because we rely on a static mapping for the common US providers.
    """
    if not subs_csv:
        return []
    ids: List[int] = []
    for raw in subs_csv.split(","):
        key = raw.strip().lower()
        if not key:
            continue
        if key in _PROVIDER_MAP:
            ids.append(_PROVIDER_MAP[key])
    # Dedup while preserving order
    seen = set()
    out = []
    for i in ids:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


# ----------------------------
# Discover API
# ----------------------------

def _discover(kind: str, page: int, *, region: str, provider_ids: List[int], original_langs: str) -> Dict[str, Any]:
    assert kind in ("movie", "tv")
    url = f"{TMDB_BASE}/discover/{kind}"
    params = {
        "language": "en-US",
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "page": page,
        "watch_region": region,
        "with_watch_monetization_types": "flatrate|free|ads",
        "with_watch_providers": "|".join(str(x) for x in provider_ids) if provider_ids else None,
        "with_original_language": original_langs or None,
    }
    return _get_json(f"discover_{kind}", url, params)


def discover_movie_page(page: int, *, region: str, provider_ids: List[int], original_langs: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data = _discover("movie", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", [])
    items: List[Dict[str, Any]] = []
    for r in results:
        items.append({
            "media_type": "movie",
            "tmdb_id": r.get("id"),
            "title": r.get("title") or r.get("original_title"),
            "original_title": r.get("original_title"),
            "release_date": r.get("release_date") or "",
            "original_language": r.get("original_language"),
            "popularity": r.get("popularity"),
            "vote_average": r.get("vote_average"),
            "genre_ids": r.get("genre_ids", []),
        })
    return items, data


def discover_tv_page(page: int, *, region: str, provider_ids: List[int], original_langs: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data = _discover("tv", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", [])
    items: List[Dict[str, Any]] = []
    for r in results:
        items.append({
            "media_type": "tv",
            "tmdb_id": r.get("id"),
            "title": r.get("name") or r.get("original_name"),
            "original_title": r.get("original_name"),
            "first_air_date": r.get("first_air_date") or "",
            "original_language": r.get("original_language"),
            "popularity": r.get("popularity"),
            "vote_average": r.get("vote_average"),
            "genre_ids": r.get("genre_ids", []),
        })
    return items, data