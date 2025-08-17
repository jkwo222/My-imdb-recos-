from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Dict, List, Tuple, Any, Optional
from urllib.parse import urlencode

import requests

# ============================================================
# Config helpers
# ============================================================

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default

# ============================================================
# Cache utilities — read-first
# ============================================================

def _cache_dir() -> str:
    d = os.path.join("data", "cache")
    os.makedirs(d, exist_ok=True)
    return d

def _cache_key(prefix: str, url: str, params: Dict[str, Any]) -> str:
    # stable encoding: sorted params
    qp = urlencode(sorted((k, str(v)) for k, v in params.items()))
    raw = f"{prefix}|{url}|{qp}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()

def _cache_path(prefix: str, url: str, params: Dict[str, Any]) -> str:
    key = _cache_key(prefix, url, params)
    return os.path.join(_cache_dir(), f"{prefix}_{key}.json")

def _cache_read(path: str, max_age_s: Optional[int]) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    if isinstance(max_age_s, int):
        try:
            mtime = os.path.getmtime(path)
            if time.time() - mtime > max_age_s:
                return None
        except Exception:
            return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _cache_write(path: str, payload: Dict[str, Any]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass

# ============================================================
# HTTP with retry/backoff + throttle
# ============================================================

_TMBD_BASE = "https://api.themoviedb.org/3"
# small throttle between network calls to smooth bursts
_THROTTLE_SECONDS = float(_env_str("TMDB_THROTTLE_SECONDS", "0.15"))  # 150 ms default
# cache TTL (seconds) for discover queries (tunable)
_DISCOVER_TTL = _env_int("TMDB_DISCOVER_TTL", 6 * 60 * 60)  # 6 hours

def _http_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    api_key = _env_str("TMDB_API_KEY", "")
    if not api_key:
        raise RuntimeError("Missing TMDB_API_KEY env var")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}" if api_key.startswith("eyJ") else None,
    }
    # If provided API key is a classic v3 key (non-Bearer), pass as query param
    req_params = dict(params)
    if not (api_key.startswith("eyJ")):
        req_params["api_key"] = api_key

    session = requests.Session()
    max_retries = 5
    backoff = 0.5  # seconds

    for attempt in range(1, max_retries + 1):
        # throttle before the call
        if _THROTTLE_SECONDS > 0:
            time.sleep(_THROTTLE_SECONDS)

        resp = session.get(url, headers={k: v for k, v in headers.items() if v}, params=req_params, timeout=20)
        status = resp.status_code

        if status == 200:
            return resp.json()

        # Handle 429 with Retry-After
        if status == 429:
            ra = resp.headers.get("Retry-After")
            try:
                sleep_for = float(ra) if ra else backoff
            except Exception:
                sleep_for = backoff
            time.sleep(sleep_for)
            backoff *= 2
            continue

        # Retry on 5xx
        if 500 <= status < 600:
            time.sleep(backoff)
            backoff *= 2
            continue

        # other errors: raise
        try:
            payload = resp.json()
        except Exception:
            payload = {"error": resp.text}
        raise RuntimeError(f"TMDB GET {url} failed [{status}]: {payload}")

    raise RuntimeError(f"TMDB GET {url} exceeded retries")

def _get_json_cached(prefix: str, url: str, params: Dict[str, Any], ttl_s: Optional[int]) -> Dict[str, Any]:
    """
    Read-first cache: return cached payload when fresh; otherwise fetch, cache, return.
    """
    path = _cache_path(prefix, url, params)
    cached = _cache_read(path, max_age_s=ttl_s)
    if cached is not None:
        return cached

    data = _http_get(url, params)
    _cache_write(path, data)
    return data

# ============================================================
# Provider helpers
# ============================================================

# Common US provider IDs used in your logs:
# netflix=8, prime_video=9, hulu=15, max=384, disney_plus=337,
# apple_tv_plus=350, peacock=386, paramount_plus=531
_DEFAULT_PROVIDER_SLUG_TO_ID = {
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "max": 384,  # formerly HBO Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

def providers_from_env(subs_csv: str, region: str) -> List[int]:
    """
    Convert a CSV of provider *slugs* (case-insensitive) to TMDB provider IDs.
    If an entry parses as int, accept it directly.
    """
    out: List[int] = []
    if not subs_csv:
        return out
    for raw in subs_csv.split(","):
        slug = raw.strip().lower()
        if not slug:
            continue
        try:
            out.append(int(slug))
            continue
        except Exception:
            pass
        mapped = _DEFAULT_PROVIDER_SLUG_TO_ID.get(slug)
        if mapped:
            out.append(mapped)
    # de-dupe and keep stable order
    seen = set()
    uniq: List[int] = []
    for pid in out:
        if pid in seen:
            continue
        seen.add(pid)
        uniq.append(pid)
    return uniq

# ============================================================
# Discover API wrappers
# ============================================================

def _discover(kind: str, page: int, *, region: str, provider_ids: List[int], original_langs: str) -> Dict[str, Any]:
    """
    kind: "movie" or "tv"
    """
    assert kind in ("movie", "tv")
    url = f"{_TMBD_BASE}/discover/{kind}"

    # Build params
    params: Dict[str, Any] = {
        "include_adult": "false",
        "language": "en-US",
        "page": page,
        "sort_by": "popularity.desc",
        "watch_region": region,
        "with_original_language": original_langs,
        # Prefer subscription/free/ad-supported
        "with_watch_monetization_types": "flatrate|free|ads",
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(x) for x in provider_ids)

    return _get_json_cached(f"discover_{kind}", url, params, ttl_s=_DISCOVER_TTL)

def discover_movie_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: str,
    **_ignored,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (items, meta_page)
    """
    data = _discover("movie", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", []) or []
    items: List[Dict[str, Any]] = []
    for r in results:
        items.append({
            "type": "movie",
            "tmdb_id": r.get("id"),
            "title": r.get("title") or r.get("original_title"),
            "original_language": r.get("original_language"),
            "release_date": r.get("release_date"),
            "popularity": r.get("popularity", 0.0),
            "vote_average": r.get("vote_average", 0.0),
            "vote_count": r.get("vote_count", 0),
            "overview": r.get("overview"),
        })
    meta = {
        "page": data.get("page", page),
        "total_pages": data.get("total_pages"),
        "total_results": data.get("total_results"),
        "count": len(items),
    }
    return items, meta

def discover_tv_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: str,
    **_ignored,  # tolerate unknown kwargs (e.g., include_seasons) without crashing
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (items, meta_page)
    """
    data = _discover("tv", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    results = data.get("results", []) or []
    items: List[Dict[str, Any]] = []
    for r in results:
        items.append({
            "type": "tvSeries",
            "tmdb_id": r.get("id"),
            "title": r.get("name") or r.get("original_name"),
            "original_language": r.get("original_language"),
            "first_air_date": r.get("first_air_date"),
            "popularity": r.get("popularity", 0.0),
            "vote_average": r.get("vote_average", 0.0),
            "vote_count": r.get("vote_count", 0),
            "overview": r.get("overview"),
        })
    meta = {
        "page": data.get("page", page),
        "total_pages": data.get("total_pages"),
        "total_results": data.get("total_results"),
        "count": len(items),
    }
    return items, meta