# engine/tmdb.py
from __future__ import annotations
import os
import time
from typing import Any, Dict, List, Optional
import requests

_API = "https://api.themoviedb.org/3"
_TIMEOUT = (6.0, 20.0)  # (connect, read)
_RETRIES = 2

def _key() -> str:
    k = os.getenv("TMDB_API_KEY", "")
    if not k:
        raise RuntimeError("TMDB_API_KEY is not set")
    return k

def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = {**params, "api_key": _key()}
    last_err = None
    for i in range(_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            # transient-ish: backoff on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0 + i)
                continue
            r.raise_for_status()
        except Exception as e:
            last_err = e
            time.sleep(0.5 + i * 0.5)
    if last_err:
        raise last_err
    return {"results": []}

# Minimal, static mapping for popular US streaming providers (add more as needed)
_PROVIDER_ALIAS_TO_ID = {
    "netflix": 8,
    "prime_video": 119,  # “Amazon Prime Video”
    "hulu": 15,
    "max": 384,          # “Max”
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 387,
    "paramount_plus": 531,
}

def _provider_chain(provider_names: List[str]) -> Optional[str]:
    ids = []
    for name in provider_names:
        pid = _PROVIDER_ALIAS_TO_ID.get(name.lower().strip())
        if pid:
            ids.append(str(pid))
    if not ids:
        return None
    # OR-join (TMDB combines with comma as OR)
    return ",".join(ids)

def _shape_movie(x: Dict[str, Any]) -> Dict[str, Any]:
    # Normalize into our internal item shape
    title = x.get("title") or x.get("original_title") or ""
    year = (x.get("release_date") or "")[:4] if x.get("release_date") else None
    return {
        "id": f"tmdb:m:{x.get('id')}",
        "tmdb_id": int(x.get("id")),
        "type": "movie",
        "title": title,
        "year": int(year) if (year and year.isdigit()) else None,
        "popularity": x.get("popularity"),
    }

def _shape_tv(x: Dict[str, Any]) -> Dict[str, Any]:
    name = x.get("name") or x.get("original_name") or ""
    year = (x.get("first_air_date") or "")[:4] if x.get("first_air_date") else None
    return {
        "id": f"tmdb:t:{x.get('id')}",
        "tmdb_id": int(x.get("id")),
        "type": "tv",
        "title": name,
        "year": int(year) if (year and year.isdigit()) else None,
        "popularity": x.get("popularity"),
    }

def _discover(
    kind: str,
    page: int,
    provider_names: List[str],
    watch_region: str,
    with_original_language: str,
    extra: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Core discover with fallback (first try with providers, then without)."""
    base = f"{_API}/discover/{'movie' if kind=='movie' else 'tv'}"
    params = {
        "page": int(page),
        "sort_by": "popularity.desc",
        "watch_region": watch_region or "US",
        "with_original_language": with_original_language or "en",
        # NOTE: for best coverage, we do not hard filter by availability on first pass.
        # We *do* attempt with providers, but fallback without if it returns 0.
    }
    if extra:
        params.update(extra)

    shaped: List[Dict[str, Any]] = []

    # 1) try with providers if we have any
    chain = _provider_chain(provider_names)
    tried_with_providers = False
    if chain:
        tried_with_providers = True
        params_with = {**params, "with_watch_providers": chain, "include_null_first_air_dates": False}
        data = _get(base, params_with)
        results = data.get("results", []) or []
        if results:
            for r in results:
                shaped.append(_shape_movie(r) if kind == "movie" else _shape_tv(r))

    # 2) fallback: if none found, try without provider filter (ensures pool grows)
    if not shaped:
        data = _get(base, params)
        results = data.get("results", []) or []
        for r in results:
            shaped.append(_shape_movie(r) if kind == "movie" else _shape_tv(r))

    return shaped

def discover_movie_page(
    page: int,
    provider_names: List[str],
    watch_region: str,
    with_original_language: str,
) -> List[Dict[str, Any]]:
    return _discover(
        kind="movie",
        page=page,
        provider_names=provider_names,
        watch_region=watch_region,
        with_original_language=with_original_language,
        extra={"include_adult": False},
    )

def discover_tv_page(
    page: int,
    provider_names: List[str],
    watch_region: str,
    with_original_language: str,
    include_tv_seasons: bool = True,
) -> List[Dict[str, Any]]:
    # include_tv_seasons currently unused by TMDB discover endpoint (placeholder for future season expansion)
    return _discover(
        kind="tv",
        page=page,
        provider_names=provider_names,
        watch_region=watch_region,
        with_original_language=with_original_language,
        extra={"include_adult": False},
    )