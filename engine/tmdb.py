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

# Static fallback for common US providers if live fetch fails.
_FALLBACK_PROVIDER_MAP = {
    "netflix": 8,
    "prime_video": 9,          # Amazon Prime Video
    "hulu": 15,
    "max": 384,                # HBO Max / Max (older id)
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

# Normalize common aliases -> canonical keys used above
_ALIAS_TO_KEY = {
    "amazon prime video": "prime_video",
    "amazon prime": "prime_video",
    "prime": "prime_video",
    "hbo max": "max",
    "max": "max",
    "disney+": "disney_plus",
    "apple tv+": "apple_tv_plus",
    "apple tv plus": "apple_tv_plus",
    "paramount+": "paramount_plus",
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
    key = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
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
    params = {k: v for k, v in params.items() if v is not None}

    cached = _cache_get(prefix, url, params, ttl_seconds)
    if cached is not None:
        return cached

    headers = _headers()
    if "Authorization" not in headers:
        api_key = os.getenv("TMDB_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("TMDB_API_KEY or TMDB_ACCESS_TOKEN required.")
        params = dict(params)
        params["api_key"] = api_key

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    _cache_put(prefix, url, params, data)
    return data


# ----------------------------
# Provider helpers
# ----------------------------

def _normalize_sub_key(raw: str) -> str:
    k = (raw or "").strip().lower()
    if not k:
        return ""
    if k in _ALIAS_TO_KEY:
        return _ALIAS_TO_KEY[k]
    return k.replace(" ", "_").replace("-", "_")


def _fetch_region_provider_catalog(region: str) -> Dict[str, int]:
    """
    Fetch the region's provider catalog (id -> name), then build name->id map (normalized).
    Cached for ~1 day.
    """
    url = f"{TMDB_BASE}/watch/providers/movie"
    params = {"watch_region": region, "language": "en-US"}
    try:
        data = _get_json("providers_catalog", url, params, ttl_seconds=24 * 3600)
        results = data.get("results", []) or []
        name_to_id: Dict[str, int] = {}
        for r in results:
            pid = r.get("provider_id")
            name = (r.get("provider_name") or "").strip()
            if not pid or not name:
                continue
            norm = _normalize_sub_key(name)
            if norm:
                name_to_id[norm] = int(pid)
        return name_to_id
    except Exception:
        return {}


def providers_from_env(subs: Any, region: Optional[str] = None) -> List[int]:
    """
    Accepts CSV string or list of provider keys. Returns TMDB provider IDs in order.
    Prefers live region catalog; falls back to static mapping.
    """
    # Normalize input to list of keys
    keys: List[str] = []
    if isinstance(subs, str):
        parts = [x.strip() for x in subs.split(",")]
        keys = [_normalize_sub_key(x) for x in parts if x.strip()]
    elif isinstance(subs, list):
        keys = [_normalize_sub_key(str(x)) for x in subs if str(x).strip()]

    if not keys:
        return []

    region = (region or "US").strip() or "US"
    live_map = _fetch_region_provider_catalog(region)
    resolved: List[int] = []
    for k in keys:
        pid = live_map.get(k)
        if pid is None:
            pid = _FALLBACK_PROVIDER_MAP.get(k)
        if pid is not None:
            resolved.append(int(pid))

    # Dedup preserve order
    out: List[int] = []
    seen: set[int] = set()
    for pid in resolved:
        if pid not in seen:
            out.append(pid)
            seen.add(pid)
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


# ----------------------------
# Per-title watch provider enrichment
# ----------------------------

def _watch_providers_for(kind: str, tmdb_id: int, region: str) -> List[str]:
    """
    Returns list of provider_name strings for the given title in the region,
    combining flatrate + free + ads buckets. Cached for 3 days.
    """
    url = f"{TMDB_BASE}/{kind}/{tmdb_id}/watch/providers"
    params: Dict[str, Any] = {}
    data = _get_json("title_watch_providers", url, params, ttl_seconds=3 * 24 * 3600)
    results = (data or {}).get("results", {}) or {}
    entry = results.get(region.upper()) or {}
    out: List[str] = []
    for bucket in ("flatrate", "ads", "free"):
        arr = entry.get(bucket) or []
        for it in arr:
            name = (it.get("provider_name") or "").strip()
            if name:
                out.append(name)
    # dedup preserve order
    seen: set[str] = set()
    uniq: List[str] = []
    for n in out:
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq


def get_title_watch_providers(kind: str, tmdb_id: int, region: str) -> List[str]:
    kind_norm = "movie" if kind == "movie" else "tv"
    try:
        return _watch_providers_for(kind_norm, int(tmdb_id), region)
    except Exception:
        return []