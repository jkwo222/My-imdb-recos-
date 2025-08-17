from __future__ import annotations

import hashlib
import json
import os
from typing import Dict, List, Tuple, Any, Optional

import requests

# ------------ low-level http + cache ------------

_TMBASE = "https://api.themoviedb.org/3"


def _cache_dir() -> str:
    d = os.path.join("data", "cache")
    os.makedirs(d, exist_ok=True)
    return d


def _cache_path(prefix: str, url: str, params: Dict[str, Any]) -> str:
    # Stable short file name to avoid OSError: filename too long
    key = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return os.path.join(_cache_dir(), f"{prefix}_{h}.json")


def _tmdb_headers(use_bearer: bool) -> Dict[str, str]:
    if use_bearer:
        bearer = os.getenv("TMDB_BEARER") or os.getenv("TMDB_TOKEN") or ""
        if bearer:
            return {"Authorization": f"Bearer {bearer}"}
    return {}  # we'll fall back to api_key query param if set


def _get_json(prefix: str, url: str, params: Dict[str, Any], timeout: int, use_bearer: bool) -> Dict[str, Any]:
    path = _cache_path(prefix, url, params)
    # Cache hit
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    headers = _tmdb_headers(use_bearer)
    apikey = os.getenv("TMDB_API_KEY")
    if not headers and apikey:
        params = dict(params)
        params["api_key"] = apikey

    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        # cache write failure is non-fatal
        pass

    return data


# ------------ provider helpers ------------

# Common US provider IDs on TMDB (stable as of long time)
# Fallback: we reuse these for other regions if TMDB mapping fetch is not used.
_PROVIDER_IDS_US: Dict[str, int] = {
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "max": 384,            # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

# Allow a couple aliases people commonly type
_ALIASES = {
    "amazon": "prime_video",
    "amazon_prime": "prime_video",
    "disney": "disney_plus",
    "appletv+": "apple_tv_plus",
    "appletv": "apple_tv_plus",
    "hbo_max": "max",
}


def providers_from_env(subs_csv: str, region: str) -> List[int]:
    """
    Parse a comma-separated list of provider slugs into TMDB provider IDs.
    We currently use a baked US map (works in practice for US region).
    If you need true per-region lookup later, we can add a /watch/providers call + cache.
    """
    slugs = [s.strip().lower() for s in subs_csv.split(",") if s.strip()]
    ids: List[int] = []
    for slug in slugs:
        slug = _ALIASES.get(slug, slug)
        pid = _PROVIDER_IDS_US.get(slug)
        if pid:
            ids.append(pid)
    # Deduplicate but preserve order
    out: List[int] = []
    seen = set()
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ------------ discover helpers ------------

def _normalize_movie(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "movie",
        "tmdb_id": it.get("id"),
        "title": it.get("title") or it.get("original_title"),
        "year": (it.get("release_date") or "")[:4] or None,
        "original_language": it.get("original_language"),
        "popularity": it.get("popularity", 0.0),
        "vote_average": it.get("vote_average", 0.0),
        "vote_count": it.get("vote_count", 0),
        "genre_ids": it.get("genre_ids") or [],
        # imdb_id not present in discover results (would require extra call)
        "imdb_id": None,
    }


def _normalize_tv(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "tvSeries",
        "tmdb_id": it.get("id"),
        "title": it.get("name") or it.get("original_name"),
        "year": (it.get("first_air_date") or "")[:4] or None,
        "original_language": it.get("original_language"),
        "popularity": it.get("popularity", 0.0),
        "vote_average": it.get("vote_average", 0.0),
        "vote_count": it.get("vote_count", 0),
        "genre_ids": it.get("genre_ids") or [],
        "imdb_id": None,
    }


def _discover(
    kind: str,
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: str,
    language: str,
    timeout: int,
    use_bearer: bool,
) -> Dict[str, Any]:
    url = f"{_TMBASE}/discover/{kind}"
    params: Dict[str, Any] = {
        "include_adult": "false",
        "sort_by": "popularity.desc",
        "page": str(page),
        "watch_region": region,
        "with_watch_monetization_types": "flatrate|free|ads",
        "with_original_language": original_langs,  # comma-separated list OK
        "language": language,
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(x) for x in provider_ids)

    return _get_json(f"discover_{kind}", url, params, timeout, use_bearer)


def discover_movie_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: str,
    language: str = "en-US",
    timeout: int = 20,
    use_bearer: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data = _discover(
        "movie",
        page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        language=language,
        timeout=timeout,
        use_bearer=use_bearer,
    )
    items = [_normalize_movie(x) for x in data.get("results", [])]
    return items, {"page": data.get("page"), "total_pages": data.get("total_pages")}


def discover_tv_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: str,
    language: str = "en-US",
    timeout: int = 20,
    use_bearer: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data = _discover(
        "tv",
        page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        language=language,
        timeout=timeout,
        use_bearer=use_bearer,
    )
    items = [_normalize_tv(x) for x in data.get("results", [])]
    return items, {"page": data.get("page"), "total_pages": data.get("total_pages")}


# ---------- optional: TMDB search for profile enrichment ----------

def search_title_once(title: str, year: Optional[int], language: str, timeout: int, use_bearer: bool) -> Dict[str, Any]:
    """Search TMDB (multi) and return first result JSON (cached)."""
    url = f"{_TMBASE}/search/multi"
    params = {"query": title, "language": language, "include_adult": "false", "page": "1"}
    if year:
        params["year"] = str(year)
        params["first_air_date_year"] = str(year)

    return _get_json("search_multi", url, params, timeout, use_bearer)