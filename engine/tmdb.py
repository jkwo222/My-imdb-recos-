# engine/tmdb.py
# Full replacement
from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# ------------------------
# Basic config / constants
# ------------------------

_TMDB_API_BASE = "https://api.themoviedb.org/3"
_CACHE_DIR = os.path.join("data", "cache")
os.makedirs(_CACHE_DIR, exist_ok=True)

_DEFAULT_TIMEOUT = (5, 20)  # (connect, read) seconds
_DEFAULT_UA = "my-imdb-recos/1.0 (+github actions)"


# --------------------------------
# Helpers: API key, hashing, cache
# --------------------------------

def _api_key() -> str:
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise RuntimeError("TMDB_API_KEY not set")
    return key


def _hash_key(kind: str, url: str, params: Dict) -> str:
    # stable string from URL + sorted params
    blob = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    h = hashlib.sha1(blob.encode("utf-8")).hexdigest()
    return f"{kind}_{h}.json"


def _cache_path(kind: str, url: str, params: Dict) -> str:
    return os.path.join(_CACHE_DIR, _hash_key(kind, url, params))


def _load_cache(path: str) -> Optional[Dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # If cache is corrupted, ignore it
        return None


def _save_cache(path: str, data: Dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _get_json(kind: str, url: str, params: Dict) -> Dict:
    """
    GET JSON with simple on-disk cache keyed by a short hash (prevents long filenames).
    """
    path = _cache_path(kind, url, params)
    cached = _load_cache(path)
    if cached is not None:
        return cached

    headers = {
        "Authorization": f"Bearer {_api_key()}",
        "Accept": "application/json",
        "User-Agent": _DEFAULT_UA,
    }

    # Basic retry/backoff
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=_DEFAULT_TIMEOUT)
            if r.status_code == 429:
                # Respect backoff if provided, else small sleep
                sleep_s = float(r.headers.get("Retry-After", "1"))
                time.sleep(min(max(sleep_s, 1.0), 5.0))
                continue
            r.raise_for_status()
            data = r.json()
            _save_cache(path, data)
            return data
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)

    # Should never reach here
    raise RuntimeError("TMDB request failed and retries exhausted")


# --------------------------
# Provider mapping / parsing
# --------------------------

def providers_from_env(subs_csv_or_list, region: str = "US") -> List[int]:
    """
    Accepts either:
      - a comma-separated string: "netflix,prime_video,hulu"
      - an iterable/list: ["netflix", "prime_video", "hulu"]

    Returns TMDB watch provider IDs (for the region if needed).
    Unknown slugs are ignored.
    """
    # Normalize to lower-cased provider slugs
    if isinstance(subs_csv_or_list, str):
        wanted = [s.strip().lower() for s in subs_csv_or_list.split(",") if s.strip()]
    elif isinstance(subs_csv_or_list, Iterable):
        wanted = [str(s).strip().lower() for s in subs_csv_or_list if str(s).strip()]
    else:
        wanted = []

    # Minimal, common US mapping; extend as needed.
    PROVIDERS_BY_REGION = {
        "US": {
            "netflix": 8,
            "prime_video": 9,
            "hulu": 15,
            "max": 384,             # formerly HBO Max
            "disney_plus": 337,
            "apple_tv_plus": 350,
            "peacock": 386,
            "paramount_plus": 531,
        },
        # Add other regions if you need specific differences
        # "GB": {...}, "CA": {...}
    }

    mapping = PROVIDERS_BY_REGION.get(region.upper(), PROVIDERS_BY_REGION["US"])
    ids: List[int] = []
    seen = set()
    for slug in wanted:
        pid = mapping.get(slug)
        if pid is not None and pid not in seen:
            ids.append(pid)
            seen.add(pid)
    return ids


# -------------------------
# Discover helpers (common)
# -------------------------

def _normalize_langs(original_langs: Optional[Iterable[str] | str]) -> Optional[str]:
    """
    TMDB discover supports `with_original_language=xx` (single value).
    If multiple langs provided, we can't pass a list directly; we return None
    and let caller do multiple queries if needed. For now we accept one or comma
    and take the first.
    """
    if not original_langs:
        return None
    if isinstance(original_langs, str):
        # tolerate "en,fr" by taking first
        return original_langs.split(",")[0].strip() or None
    try:
        it = list(original_langs)
        return (str(it[0]).strip() or None) if it else None
    except Exception:
        return None


def _discover(kind: str, page: int, *, region: str, provider_ids: List[int], original_langs: Optional[Iterable[str] | str]) -> Dict:
    """
    kind: "movie" or "tv"
    """
    assert kind in ("movie", "tv"), "kind must be 'movie' or 'tv'"

    url = f"{_TMDB_API_BASE}/discover/{kind}"
    with_lang = _normalize_langs(original_langs)

    params = {
        "include_adult": "false",
        "language": "en-US",
        "page": str(page),
        "sort_by": "popularity.desc",
    }

    # Providers
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)
        params["watch_region"] = region.upper()
        params["with_watch_monetization_types"] = "flatrate|free|ads"

    # Original language (single)
    if with_lang:
        params["with_original_language"] = with_lang

    return _get_json(f"discover_{kind}", url, params)


# ---------------------------------
# Public: discover_movie_page / tv
# ---------------------------------

def discover_movie_page(page: int, *, region: str = "US", provider_ids: Optional[List[int]] = None, original_langs: Optional[Iterable[str] | str] = None) -> Tuple[List[Dict], int]:
    """
    Returns: (items, page_number)
    """
    provider_ids = provider_ids or []
    data = _discover("movie", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    items = data.get("results", []) or []
    return items, int(data.get("page", page) or page)


def discover_tv_page(page: int, *, region: str = "US", provider_ids: Optional[List[int]] = None, original_langs: Optional[Iterable[str] | str] = None) -> Tuple[List[Dict], int]:
    """
    Returns: (items, page_number)
    """
    provider_ids = provider_ids or []
    data = _discover("tv", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    items = data.get("results", []) or []
    return items, int(data.get("page", page) or page)
    
    # --- NEW: ID resolution & providers ------------------------------------------

def find_by_imdb_id(tconst: str) -> Dict:
    """
    Map an IMDb tconst -> TMDB id(s) via /find.
    """
    url = f"{_TMDB_API_BASE}/find/{tconst}"
    params = {"external_source": "imdb_id", "language": "en-US"}
    return _get_json("find_imdb", url, params)

def search_title_year(title: str, year: int | None, kind: str) -> Dict:
    """
    Fallback if /find misses. kind: 'movie'|'tv'
    """
    kind = "movie" if kind == "movie" else "tv"
    url = f"{_TMDB_API_BASE}/search/{kind}"
    params = {"query": title, "language": "en-US", "include_adult": "false", "page": "1"}
    if year and kind == "movie":
        params["year"] = str(year)
    if year and kind == "tv":
        params["first_air_date_year"] = str(year)
    return _get_json(f"search_{kind}", url, params)

def watch_providers(media_type: str, tmdb_id: int) -> Dict:
    """
    Fetch watch providers for a given TMDB id.
    media_type: 'movie' or 'tv'
    """
    media_type = "movie" if media_type == "movie" else "tv"
    url = f"{_TMDB_API_BASE}/{media_type}/{tmdb_id}/watch/providers"
    params = {"language": "en-US"}
    return _get_json(f"providers_{media_type}", url, params)