# engine/tmdb.py
from __future__ import annotations
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

TMDB_BASE = "https://api.themoviedb.org/3"
CACHE_DIR = Path("data/cache/tmdb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

UA = {"User-Agent": "RecoEngine/3.0 (+github actions)"}


# ---------- auth / http helpers ----------

def _auth_headers_and_params() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (headers, params) for TMDB v3 endpoints.
    Preference:
      1) TMDB_API_KEY  -> ?api_key=...
      2) TMDB_BEARER   -> Authorization: Bearer <v4 token>
    """
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if api_key:
        return dict(UA), {"api_key": api_key}

    bearer = os.getenv("TMDB_BEARER", "").strip()
    if bearer:
        return {"Authorization": f"Bearer {bearer}", **UA}, {}

    raise RuntimeError("TMDB_API_KEY or TMDB_BEARER is required for TMDB v3 API calls")


def _cache_key(path: str, params: Dict[str, Any]) -> str:
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    raw = f"{path}?{items}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _cache_path(group: str, key: str) -> Path:
    g = CACHE_DIR / group
    g.mkdir(parents=True, exist_ok=True)
    return g / f"{key}.json"


def _http_get_json(path: str, params: Dict[str, Any],
                   group: Optional[str] = None, ttl_min: int = 60) -> Dict[str, Any]:
    headers, base_params = _auth_headers_and_params()
    full_params = {**base_params, **params}
    key = _cache_key(path, full_params)

    if group:
        cp = _cache_path(group, key)
        if cp.exists():
            try:
                st = cp.stat()
                age_min = (time.time() - st.st_mtime) / 60.0
                if age_min <= ttl_min:
                    with cp.open("r", encoding="utf-8") as f:
                        return json.load(f)
            except Exception:
                pass

    url = f"{TMDB_BASE}{path}"
    backoff = 0.7
    last_err: Optional[Dict[str, Any]] = None
    for _ in range(5):
        try:
            r = requests.get(url, params=full_params, headers=headers, timeout=25)
            if r.status_code == 200:
                data = r.json()
                if group:
                    try:
                        with _cache_path(group, key).open("w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False)
                    except Exception:
                        pass
                return data
            else:
                last_err = {"status_code": r.status_code, "text": r.text[:300]}
        except Exception as e:
            last_err = {"exception": repr(e)}
        time.sleep(backoff)
        backoff *= 1.8
    return {"__error__": last_err or {"error": "unknown"}}


# Exposed for detail helpers
def _get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    return _http_get_json(path, params, group="raw", ttl_min=90)


# ---------- provider helpers ----------

def _slugify_provider_name(name: str) -> str:
    """
    Convert TMDB provider display name to a stable slug.
    Examples:
      "Apple TV+" -> "apple_tv_plus"
      "Peacock Premium" -> "peacock_premium"
      "Max" -> "max"
    """
    s = (name or "").strip().lower()
    s = s.replace("&", "and")
    s = s.replace("+", "_plus")
    s = re.sub(r"[^\w\s\-]", "", s)     # drop punctuation except hyphen/underscore
    s = s.replace("-", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s


# Aliases from ENV slugs to one or more candidate TMDB slugs.
# We try candidates in order, then fuzzy fallback.
PROVIDER_ALIASES: Dict[str, List[str]] = {
    # streaming staples
    "netflix": ["netflix"],
    "hulu": ["hulu"],
    "disney_plus": ["disney_plus"],
    "disneyplus": ["disney_plus"],
    "paramount_plus": ["paramount_plus"],
    "paramountplus": ["paramount_plus"],
    "prime_video": ["amazon_prime_video", "amazon_prime"],
    "amazon_prime": ["amazon_prime_video", "amazon_prime"],
    "amazon_prime_video": ["amazon_prime_video"],
    "apple_tv": ["apple_tv_plus"],
    "apple_tv_plus": ["apple_tv_plus"],

    # Max / HBO variants
    "hbo_max": ["max"],
    "hbomax": ["max"],
    "max": ["max"],

    # Peacock variants (region may have Peacock / Peacock Premium)
    "peacock": ["peacock", "peacock_premium"],
    "peacock_tv": ["peacock", "peacock_premium"],
}

def _providers_catalog(kind: str, region: str, ttl_min: int = 8 * 60) -> List[Dict[str, Any]]:
    path = f"/watch/providers/{'movie' if kind=='movie' else 'tv'}"
    data = _http_get_json(path, {"watch_region": region}, group=f"providers_{region}", ttl_min=ttl_min)
    return data.get("results") or []


def providers_from_env(subs: List[str], region: str) -> Tuple[List[int], Dict[str, int]]:
    """
    Map env SUBS_INCLUDE slugs (e.g., 'netflix', 'prime_video', 'max', 'peacock')
    to TMDB provider IDs valid in the region.

    Returns:
      provider_ids: List[int]          # unique TMDB provider IDs
      used_map:    Dict[str, int]      # env slug -> id  (0 if not matched; included for telemetry)
    """
    # Normalize incoming env slugs
    subs_in = [s for s in (subs or []) if s]
    subs_norm: List[str] = []
    for s in subs_in:
        key = s.strip().lower().replace("-", "_")
        key = re.sub(r"_+", "_", key)
        subs_norm.append(key)

    # Build a map of TMDB slugs -> provider_id from both movie & tv lists
    movie_provs = _providers_catalog("movie", region)
    tv_provs = _providers_catalog("tv", region)
    id_by_slug: Dict[str, int] = {}
    for entry in movie_provs + tv_provs:
        nm = str(entry.get("provider_name") or "")
        slug = _slugify_provider_name(nm)  # e.g. "Apple TV+" -> "apple_tv_plus"
        pid = int(entry.get("provider_id") or 0)
        if slug and pid:
            id_by_slug[slug] = pid

    out_ids: List[int] = []
    used_map: Dict[str, int] = {}
    seen_ids = set()

    # Helper: try exact slug, then fuzzy startswith/contains
    def resolve_candidate(cand: str) -> Optional[int]:
        if cand in id_by_slug:
            return id_by_slug[cand]
        # fuzzy: peacock -> peacock_premium (or vice versa)
        for k, v in id_by_slug.items():
            if k.startswith(cand) or cand.startswith(k):
                return v
        # last resort: substring containment (very loose)
        for k, v in id_by_slug.items():
            if cand in k or k in cand:
                return v
        return None

    for env_slug in subs_norm:
        candidates = PROVIDER_ALIASES.get(env_slug, [env_slug])
        chosen_id: Optional[int] = None
        for cand in candidates:
            chosen_id = resolve_candidate(cand)
            if chosen_id:
                break

        if chosen_id and chosen_id not in seen_ids:
            out_ids.append(chosen_id)
            seen_ids.add(chosen_id)
            used_map[env_slug] = chosen_id
        else:
            # record unmatched for telemetry with id=0
            used_map[env_slug] = 0

    return out_ids, used_map


# ---------- discovery ----------

def _normalize_items(kind: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for r in results or []:
        tmdb_id = int(r.get("id") or 0)
        if not tmdb_id:
            continue
        title = r.get("title") if kind == "movie" else r.get("name")
        date = r.get("release_date") if kind == "movie" else r.get("first_air_date")
        year = int((date or "0000")[:4]) if date else None
        genres = r.get("genre_ids") or []
        vote = r.get("vote_average") or 0.0
        items.append({
            "media_type": kind,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "genres": genres,
            "tmdb_vote": vote,
        })
    return items


def _discover(kind: str, page: int, region: str, langs: List[str],
              provider_ids: List[int], slot: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    with_providers = "|".join(str(x) for x in provider_ids) if provider_ids else None
    with_langs = "|".join(langs) if langs else None
    params: Dict[str, Any] = {
        "page": page,
        "include_adult": "false",
        "sort_by": "popularity.desc",
        "watch_region": region,
    }
    if with_providers:
        params["with_watch_providers"] = with_providers
        params["with_monetization_types"] = "flatrate|free|ads|rent|buy"
        params["with_watch_monetization_types"] = "flatrate|free|ads|rent|buy"
    if with_langs:
        params["with_original_language"] = with_langs
    params["cb"] = slot  # tiny cache shard

    data = _http_get_json(f"/discover/{'movie' if kind=='movie' else 'tv'}",
                          params, group=f"discover_{kind}", ttl_min=30)
    results = data.get("results") or []
    items = _normalize_items(kind, results)
    diag = {
        "page": int(page),
        "total_pages": int(data.get("total_pages") or 1),
        "total_results": int(data.get("total_results") or 0),
        "returned": len(items),
        "error": data.get("__error__"),
    }
    return items, diag


def discover_movie_page(page: int, region: str, langs: List[str],
                        provider_ids: List[int], slot: int = 0
                        ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _discover("movie", page, region, langs, provider_ids, slot)


def discover_tv_page(page: int, region: str, langs: List[str],
                     provider_ids: List[int], slot: int = 0
                     ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _discover("tv", page, region, langs, provider_ids, slot)


# ---------- trending ----------

def trending(kind: str, period: str = "day") -> List[Dict[str, Any]]:
    """
    period: 'day' or 'week'
    """
    data = _http_get_json(f"/trending/{'movie' if kind=='movie' else 'tv'}/{period}",
                          {}, group=f"trending_{kind}_{period}", ttl_min=30)
    return _normalize_items(kind, data.get("results") or [])


# ---------- detail lookups ----------

def get_external_ids(kind: str, tmdb_id: int) -> Dict[str, Any]:
    """
    Returns at least {'imdb_id': 'tt...'} when available.
    """
    k = "movie" if kind == "movie" else "tv"
    data = _http_get_json(f"/{k}/{int(tmdb_id)}/external_ids", {},
                          group="external_ids", ttl_min=24*60)
    return {
        "imdb_id": data.get("imdb_id"),
        "__error__": data.get("__error__"),
    }


def get_title_watch_providers(kind: str, tmdb_id: int, region: str) -> List[str]:
    k = "movie" if kind == "movie" else "tv"
    data = _http_get_json(f"/{k}/{int(tmdb_id)}/watch/providers", {},
                          group="title_providers", ttl_min=180)
    results = (data.get("results") or {}).get(region, {})
    out = set()
    for bucket in ("flatrate", "ads", "free", "rent", "buy"):
        for p in results.get(bucket, []) or []:
            nm = (p.get("provider_name") or "")
            slug = _slugify_provider_name(nm)
            if slug:
                out.add(slug)
    return sorted(out)