from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import requests

# -----------------------------
# Configuration / constants
# -----------------------------

TMDB_BASE = "https://api.themoviedb.org/3"
CACHE_ROOT = Path("data/cache/tmdb")
CACHE_ROOT.mkdir(parents=True, exist_ok=True)

# Toggle local HTTP caching
ENABLE_CACHE = True
CACHE_TTL_SECS = 60 * 60 * 12  # 12 hours

# Provider aliases → TMDB watch provider IDs (US)
PROVIDER_ALIASES_US: Dict[str, int] = {
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "max": 384,
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
    # optionally extend:
    "tubi": 73,
    "plutotv": 120,
    "starz": 43,
    "showtime": 37,
}

# -----------------------------
# Auth/session
# -----------------------------

_V4_TOKEN = os.getenv("TMDB_ACCESS_TOKEN", "").strip()
_V3_KEY = os.getenv("TMDB_API_KEY", "").strip()

def _build_session() -> requests.Session:
    s = requests.Session()
    if _V4_TOKEN:
        s.headers.update({
            "Authorization": f"Bearer {_V4_TOKEN}",
            "Accept": "application/json",
        })
        s._tmdb_auth_mode = "v4"  # type: ignore[attr-defined]
    elif _V3_KEY:
        s.headers.update({"Accept": "application/json"})
        s._tmdb_auth_mode = "v3"  # type: ignore[attr-defined]
    else:
        s.headers.update({"Accept": "application/json"})
        s._tmdb_auth_mode = "none"  # type: ignore[attr-defined]
    return s

_SESSION = _build_session()

# -----------------------------
# Small disk cache
# -----------------------------

def _cache_path(key: str) -> Path:
    safe = (
        key.replace("/", "_")
           .replace("?", "_")
           .replace("&", "_")
           .replace("=", "_")
           .replace("|", "_")
           .replace(":", "_")
    )
    return CACHE_ROOT / f"{safe}.json"

def _cache_get(key: str) -> Optional[dict]:
    if not ENABLE_CACHE:
        return None
    p = _cache_path(key)
    if not p.exists():
        return None
    if (time.time() - p.stat().st_mtime) > CACHE_TTL_SECS:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _cache_set(key: str, obj: dict) -> None:
    if not ENABLE_CACHE:
        return
    p = _cache_path(key)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    except Exception:
        pass

# -----------------------------
# HTTP helpers
# -----------------------------

def _get_json(cache_key: str, url: str, params: Dict[str, Any]) -> dict:
    """
    GET JSON using v4 bearer if available; fallback to v3 api_key.
    Raises HTTPError on 4xx/5xx.
    """
    # include params in cache key (order-independent)
    if params:
        cache_key = f"{cache_key}__{json.dumps(params, sort_keys=True, separators=(',', ':'))}"

    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    req_params = dict(params or {})
    if getattr(_SESSION, "_tmdb_auth_mode", "") == "v3" and _V3_KEY:
        req_params["api_key"] = _V3_KEY

    r = _SESSION.get(url, params=req_params, timeout=20)
    r.raise_for_status()
    data = r.json()
    _cache_set(cache_key, data)
    return data

# -----------------------------
# Public helpers
# -----------------------------

def providers_from_env(subs_env: Optional[str], region: Optional[str] = None) -> List[int]:
    """
    Parse SUBS_INCLUDE into TMDB provider IDs.
    Currently uses US mappings regardless of `region`. The parameter
    is accepted to keep compatibility with callers and to allow
    future region-specific maps.
    Example: "netflix,prime_video,hulu" -> [8, 9, 15]
    Unknown tokens are ignored.
    """
    if not subs_env:
        return []
    # Choose mapping (future: per-region switch)
    mapping = PROVIDER_ALIASES_US

    raw_tokens = [t.strip().lower() for t in subs_env.split(",") if t.strip()]
    ids: List[int] = []
    for tok in raw_tokens:
        pid = mapping.get(tok)
        if pid:
            ids.append(pid)

    # dedupe, keep order
    seen = set()
    out: List[int] = []
    for pid in ids:
        if pid not in seen:
            seen.add(pid)
            out.append(pid)
    return out

# -----------------------------
# Discover endpoints
# -----------------------------

def _discover(
    kind: str,
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: Optional[str] = None,
) -> dict:
    assert kind in ("movie", "tv"), f"discover kind must be 'movie' or 'tv', got {kind!r}"
    url = f"{TMDB_BASE}/discover/{kind}"

    params: Dict[str, Any] = {
        "include_adult": "false",
        "sort_by": "popularity.desc",
        "language": "en-US",
        "page": page,
    }

    if provider_ids:
        params["with_watch_providers"] = "|".join(str(x) for x in provider_ids)
        params["watch_region"] = region
        params["with_watch_monetization_types"] = "flatrate|free|ads"

    if original_langs:
        params["with_original_language"] = original_langs  # comma-separated OK

    return _get_json(f"discover_{kind}", url, params)

def discover_movie_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: Optional[str] = None,
) -> Tuple[List[dict], dict]:
    data = _discover("movie", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    return data.get("results", []) or [], data

def discover_tv_page(
    page: int,
    *,
    region: str,
    provider_ids: List[int],
    original_langs: Optional[str] = None,
) -> Tuple[List[dict], dict]:
    data = _discover("tv", page, region=region, provider_ids=provider_ids, original_langs=original_langs)
    return data.get("results", []) or [], data

# -----------------------------
# Optional: expose auth mode for logging
# -----------------------------

def tmdb_auth_mode() -> str:
    return getattr(_SESSION, "_tmdb_auth_mode", "none")