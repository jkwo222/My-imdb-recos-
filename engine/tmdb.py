# engine/tmdb.py
from __future__ import annotations

import json
import os
import time
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlencode

import requests

# -----------------------------
# Basic HTTP + on-disk caching
# -----------------------------

_TM = "https://api.themoviedb.org/3"
_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
if not _API_KEY:
    raise RuntimeError("TMDB_API_KEY not set")

_CACHE_DIR = os.path.join("data", "cache")
os.makedirs(_CACHE_DIR, exist_ok=True)


def _cache_path(kind: str, key: str) -> str:
    safe = key.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")
    return os.path.join(_CACHE_DIR, f"tmdb_{kind}_{safe}.json")


def _get_json(kind: str, url: str, params: Dict[str, str]) -> Dict:
    # Build full URL w/ params for a stable cache key (excluding api_key and timestamp)
    q = {k: v for k, v in params.items() if k != "api_key"}
    key = f"{url}?{urlencode(sorted(q.items()))}"
    path = _cache_path(kind, key)

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # polite retry
    for attempt in range(3):
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            data = r.json()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f)
            return data
        # backoff on errors
        time.sleep(0.8 * (attempt + 1))

    r.raise_for_status()  # if we fell through retries
    return {}  # placate type checkers


# -----------------------------
# Provider mapping
# -----------------------------
# Map your human-friendly names to TMDB "watch provider" IDs.
# You can add/remove here freely. Unknown names are ignored.
_PROVIDER_ID_MAP: Dict[str, int] = {
    # Major US SVOD
    "netflix": 8,
    "prime_video": 9,           # Amazon Prime Video
    "hulu": 15,
    "disney_plus": 337,
    "max": 384,                  # HBO Max (Max)
    "apple_tv_plus": 350,
    "paramount_plus": 531,
    "peacock": 386,
    # Common FAST/free (optional)
    "pluto_tv": 307,
    "tubi": 73,
    "roku_channel": 207,
    "freevee": 238,
}

def providers_from_env(subs_csv: str) -> List[int]:
    ids: List[int] = []
    for raw in (subs_csv or "").split(","):
        name = raw.strip().lower()
        if not name:
            continue
        pid = _PROVIDER_ID_MAP.get(name)
        if pid and pid not in ids:
            ids.append(pid)
    return ids


# -----------------------------
# Discover helpers (Movies/TV)
# -----------------------------

def _discover(kind: str, page: int, *, region: str, provider_ids: Iterable[int], original_langs: Iterable[str]) -> Dict:
    """
    Call TMDB Discover with an OR-union of all provider IDs.
    kind: "movie" or "tv"
    """
    assert kind in ("movie", "tv")
    url = f"{_TM}/discover/{kind}"
    pid_str = "|".join(str(p) for p in provider_ids if p)
    langs = ",".join([lang.strip() for lang in original_langs if lang.strip()])

    params = {
        "api_key": _API_KEY,
        "language": "en-US",  # API response language; does not filter originals
        "page": str(page),
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "include_null_first_air_dates": "false" if kind == "tv" else None,
        # Provider filters — OR union across your services:
        "with_watch_providers": pid_str if pid_str else None,
        "watch_region": region,
        # Allow flatrate + free + ads (broad coverage)
        "with_watch_monetization_types": "flatrate|free|ads",
        # Language constraints for original-language (optional; empty means no filter)
        "with_original_language": langs if langs else None,
        # Recency bias a bit (keep broad)
        # You can add release_date.gte / first_air_date.gte here if you want to limit age.
    }
    # Remove None
    params = {k: v for k, v in params.items() if v is not None}
    return _get_json(f"discover_{kind}", url, params)


def discover_movie_page(page: int, *, region: str, provider_ids: List[int], original_langs: List[str]) -> Tuple[List[Dict], Dict]:
    """
    Returns (items, page_meta)
    items contain: id, media_type='movie', title, release_date, vote_average, vote_count
    """
    data = _discover("movie", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", []) or []
    items: List[Dict] = []
    for r in results:
        items.append({
            "id": int(r.get("id")),
            "media_type": "movie",
            "title": r.get("title") or r.get("original_title") or "",
            "original_language": r.get("original_language"),
            "release_date": r.get("release_date"),
            "popularity": r.get("popularity"),
            "vote_average": r.get("vote_average"),
            "vote_count": r.get("vote_count"),
        })
    meta = {"page": data.get("page", page), "total_pages": data.get("total_pages", page), "total_results": data.get("total_results", len(items))}
    return items, meta


def discover_tv_page(page: int, *, region: str, provider_ids: List[int], original_langs: List[str]) -> Tuple[List[Dict], Dict]:
    """
    Returns (items, page_meta)
    items contain: id, media_type='tv', name, first_air_date, vote_average, vote_count
    """
    data = _discover("tv", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", []) or []
    items: List[Dict] = []
    for r in results:
        items.append({
            "id": int(r.get("id")),
            "media_type": "tv",
            "name": r.get("name") or r.get("original_name") or "",
            "original_language": r.get("original_language"),
            "first_air_date": r.get("first_air_date"),
            "popularity": r.get("popularity"),
            "vote_average": r.get("vote_average"),
            "vote_count": r.get("vote_count"),
        })
    meta = {"page": data.get("page", page), "total_pages": data.get("total_pages", page), "total_results": data.get("total_results", len(items))}
    return items, meta