from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ----------------------------------------------------------------------
# Paths / constants
# ----------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache" / "tmdb" / "discover"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TMDB_V3 = "https://api.themoviedb.org/3"
LANG_DEFAULT = "en-US"

# Popular US flatrate providers id map (TMDB watch providers)
PROVIDER_MAP = {
    # flatrate SVODs
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "max": 384,  # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,

    # allow a few synonyms users sometimes use
    "amazon_prime": 9,
    "hbo_max": 384,
    "disney+": 337,
    "atv+": 350,
}

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _cache_path(kind: str, page: int, region: str, providers_key: str, langs_key: str) -> Path:
    key = f"{kind}_p{page}_{region}_{providers_key}_{langs_key}"
    return CACHE_DIR / f"{key}.json"

def _is_fresh(path: Path, ttl_hours: int) -> bool:
    if not path.exists():
        return False
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return (_utcnow() - mtime) <= timedelta(hours=ttl_hours)
    except Exception:
        return False

def _write_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _read_cache(path: Path) -> Optional[dict]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None

def _norm_langs(original_langs: Optional[List[str] | str]) -> str:
    if not original_langs:
        return ""
    if isinstance(original_langs, str):
        # support comma/pipe/space separated
        toks = [t.strip() for t in original_langs.replace("|", ",").replace(" ", ",").split(",") if t.strip()]
    else:
        toks = [t.strip() for t in original_langs if t and t.strip()]
    # TMDB expects a single value; many instances accept pipe-separated.
    # We'll join with "|" (works in practice), but if single, it's fine.
    return "|".join(dict.fromkeys(toks))  # dedupe, preserve order

def _provider_ids_from_slugs(slugs: Optional[str | List[str]]) -> List[int]:
    if not slugs:
        return []
    if isinstance(slugs, str):
        toks = [t.strip() for t in slugs.split(",") if t.strip()]
    else:
        toks = [t.strip() for t in slugs if t and t.strip()]
    ids: List[int] = []
    seen = set()
    for s in toks:
        pid = PROVIDER_MAP.get(s.lower())
        if pid and pid not in seen:
            seen.add(pid)
            ids.append(pid)
    return ids

# ----------------------------------------------------------------------
# Authentication strategy
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class _AuthVariant:
    name: str                 # "v4" or "v3"
    headers: Dict[str, str]
    query: Dict[str, str]

def _auth_variants() -> List[_AuthVariant]:
    """
    Build possible auth variants from environment.
    Preference: v4 bearer (TMDB_ACCESS_TOKEN), then v3 (TMDB_API_KEY).
    """
    v: List[_AuthVariant] = []
    v4 = os.getenv("TMDB_ACCESS_TOKEN") or os.getenv("TMDB_BEARER") or ""
    if v4.strip():
        v.append(_AuthVariant("v4", headers={"Authorization": f"Bearer {v4.strip()}", "Accept": "application/json"}, query={}))
    v3 = os.getenv("TMDB_API_KEY") or ""
    if v3.strip():
        v.append(_AuthVariant("v3", headers={"Accept": "application/json"}, query={"api_key": v3.strip()}))
    return v

class _AuthError(RuntimeError):
    pass

def _request_json_with_fallback(url: str, params: Dict[str, Any], timeout: Tuple[float, float]=(5, 25)) -> dict:
    """
    Try each available auth method until one succeeds.
    If a 401 happens, we try the next variant. Other errors bubble up.
    """
    variants = _auth_variants()
    if not variants:
        raise _AuthError("TMDB auth missing: set TMDB_ACCESS_TOKEN (v4) or TMDB_API_KEY (v3).")

    last_exc: Optional[Exception] = None
    for idx, variant in enumerate(variants):
        q = dict(params or {})
        q.update(variant.query)
        try:
            r = requests.get(url, params=q, headers=variant.headers, timeout=timeout)
            if r.status_code == 401:
                # Try next variant
                print(f"tmdb: 401 with auth={variant.name}; trying next if available…")
                last_exc = requests.HTTPError("401 Unauthorized")
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            # Non-401 — for the first variant, try the next; otherwise bubble up the last
            last_exc = e
            if idx < len(variants) - 1:
                print(f"tmdb: error with auth={variant.name}: {type(e).__name__}; trying next…")
                continue
            break
    # If we got here, nothing worked
    # Provide a concise diagnostic without leaking keys
    raise _AuthError(f"TMDB request failed for {url.split('?')[0]} with all auth variants. "
                     f"Check TMDB_ACCESS_TOKEN / TMDB_API_KEY are valid and not rate-limited.") from last_exc

# ----------------------------------------------------------------------
# Discover
# ----------------------------------------------------------------------

def _discover(kind: str, page: int, *, region: str, provider_ids: List[int], original_langs: Optional[str | List[str]],
              ttl_hours: int = 6) -> dict:
    """
    Raw discover call with caching. kind: "movie" or "tv".
    """
    if kind not in ("movie", "tv"):
        raise ValueError("kind must be 'movie' or 'tv'")

    langs_key = _norm_langs(original_langs)
    providers_key = "_".join(str(x) for x in provider_ids) if provider_ids else "all"

    # Cache
    cache_file = _cache_path(kind, page, region.upper(), providers_key, langs_key or "any")
    if _is_fresh(cache_file, ttl_hours):
        cached = _read_cache(cache_file)
        if cached:
            return cached

    params: Dict[str, Any] = {
        "include_adult": "false",
        "language": LANG_DEFAULT,
        "page": page,
        "sort_by": "popularity.desc",
        "watch_region": region.upper(),
        "with_watch_monetization_types": "flatrate|free|ads",
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(pid) for pid in provider_ids)
    if langs_key:
        params["with_original_language"] = langs_key

    url = f"{TMDB_V3}/discover/{kind}"
    data = _request_json_with_fallback(url, params)

    _write_cache(cache_file, data)
    return data

def _item_from_result(kind: str, r: dict) -> Dict[str, Any]:
    title = r.get("title") or r.get("name") or ""
    release_date = r.get("release_date") or r.get("first_air_date") or ""
    year = None
    if release_date:
        try:
            year = int(release_date[:4])
        except Exception:
            year = None
    return {
        "tmdb_id": r.get("id"),
        "tmdb_media_type": kind,
        "type": "movie" if kind == "movie" else "show",
        "title": title,
        "year": year,
        "overview": r.get("overview") or "",
        "poster_path": r.get("poster_path"),
        "backdrop_path": r.get("backdrop_path"),
        # Keep TMDB rating separate; imdb rating can be merged elsewhere
        "tmdb_vote_average": round(float(r.get("vote_average") or 0.0), 1) if r.get("vote_average") is not None else None,
        # placeholders (filled later by tmdb_detail.enrich_items_with_tmdb)
        "genres": [],
        "providers": [],
    }

def discover_movie_page(page: int, *, region: str, provider_ids: Optional[List[int]] = None,
                        original_langs: Optional[str | List[str]] = None) -> Tuple[List[Dict[str, Any]], dict]:
    data = _discover(
        "movie",
        page,
        region=region,
        provider_ids=provider_ids or _provider_ids_from_slugs(os.getenv("SUBS_INCLUDE", "")),
        original_langs=original_langs or os.getenv("ORIGINAL_LANGS", "en"),
    )
    results = data.get("results") or []
    items = [_item_from_result("movie", r) for r in results]
    return items, data

def discover_tv_page(page: int, *, region: str, provider_ids: Optional[List[int]] = None,
                     original_langs: Optional[str | List[str]] = None) -> Tuple[List[Dict[str, Any]], dict]:
    data = _discover(
        "tv",
        page,
        region=region,
        provider_ids=provider_ids or _provider_ids_from_slugs(os.getenv("SUBS_INCLUDE", "")),
        original_langs=original_langs or os.getenv("ORIGINAL_LANGS", "en"),
    )
    results = data.get("results") or []
    items = [_item_from_result("tv", r) for r in results]
    return items, data