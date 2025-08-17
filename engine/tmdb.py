# engine/tmdb.py
from __future__ import annotations
import os
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, Iterable, Union

import requests

# ----------------------------
# Config / cache directories
# ----------------------------
ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache" / "tmdb"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TMDB_BASE = "https://api.themoviedb.org/3"

# Safe static fallback for US region (keeps runs green if live lookup fails)
_STATIC_PROVIDER_US: Dict[str, int] = {
    "netflix": 8,
    "prime_video": 9,          # Amazon Prime Video
    "hulu": 15,
    "max": 384,                # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "peacock_premium": 386,    # treat same for filtering
    "paramount_plus": 531,
}

# In-memory provider cache by region
_PROVIDER_CACHE: Dict[str, Dict[str, int]] = {}

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
    # Ensure language default (avoid None in query)
    params = {k: v for k, v in params.items() if v is not None}

    cached = _cache_get(prefix, url, params, ttl_seconds)
    if cached is not None:
        return cached

    # If no bearer, add api_key
    hdrs = _headers()
    if "Authorization" not in hdrs:
        api_key = os.getenv("TMDB_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("TMDB_API_KEY or TMDB_ACCESS_TOKEN required.")
        params = dict(params)  # copy
        params["api_key"] = api_key

    r = requests.get(url, headers=hdrs, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    _cache_put(prefix, url, params, data)
    return data

# ----------------------------
# Provider helpers
# ----------------------------

def _listish(x: Optional[Union[str, Iterable[str]]]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, (list, tuple)):
        raw = [str(v) for v in x]
    else:
        raw = [p for p in str(x).split(",")]
    out: List[str] = []
    for v in raw:
        s = v.strip().lower()
        if not s:
            continue
        s = s.replace(" ", "_")
        s = s.replace("+", "_plus")
        out.append(s)
    return out

def _normalize_provider_name(s: str) -> str:
    s = s.strip().lower().replace(" ", "_").replace("+", "_plus")
    aliases = {
        "amazon": "prime_video",
        "prime": "prime_video",
        "amazon_prime_video": "prime_video",
        "amazon_prime": "prime_video",
        "disney": "disney_plus",
        "disneyplus": "disney_plus",
        "hbo_max": "max",
        "hbomax": "max",
        "appletvplus": "apple_tv_plus",
        "apple_tv": "apple_tv_plus",
        "apple_tv+": "apple_tv_plus",
        "apple+": "apple_tv_plus",
        "paramountplus": "paramount_plus",
        "paramount+": "paramount_plus",
    }
    return aliases.get(s, s)

def _provider_cache_file(region: str) -> Path:
    return CACHE_DIR / f"watch_providers_{region.upper()}.json"

def _load_provider_map_from_disk(region: str) -> Optional[Dict[str, int]]:
    p = _provider_cache_file(region)
    try:
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and all(isinstance(v, int) for v in data.values()):
                return {str(k): int(v) for k, v in data.items()}
    except Exception:
        pass
    return None

def _save_provider_map_to_disk(region: str, mapping: Dict[str, int]) -> None:
    p = _provider_cache_file(region)
    try:
        with p.open("w", encoding="utf-8") as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass

def _resolve_provider_map_from_tmdb(region: str) -> Dict[str, int]:
    mapping: Dict[str, int] = {}

    def ingest(kind: str) -> None:
        url = f"{TMDB_BASE}/watch/providers/{kind}"
        data = _get_json(f"watch_providers_{kind}_{region.upper()}",
                         url,
                         {"watch_region": region.upper()},
                         ttl_seconds=7 * 24 * 3600)
        for prov in data.get("results", []):
            try:
                pid = int(prov.get("provider_id"))
            except Exception:
                continue
            raw_name = str(prov.get("provider_name") or "").strip()
            if not raw_name:
                continue
            key = _normalize_provider_name(raw_name)
            mapping.setdefault(key, pid)
            if "peacock" in key and "premium" not in key:
                mapping.setdefault("peacock_premium", pid)
            if key in ("amazon", "amazon_prime_video"):
                mapping.setdefault("prime_video", pid)

    ingest("movie")
    ingest("tv")
    return mapping

def _provider_map(region: str) -> Dict[str, int]:
    r = (region or "US").upper()

    if r in _PROVIDER_CACHE:
        return _PROVIDER_CACHE[r]

    disk = _load_provider_map_from_disk(r)
    if disk:
        _PROVIDER_CACHE[r] = disk
        return disk

    try:
        live = _resolve_provider_map_from_tmdb(r)
        if live:
            _PROVIDER_CACHE[r] = live
            _save_provider_map_to_disk(r, live)
            print(f"[catalog] Resolved TMDB providers for region={r}: {len(live)} entries", flush=True)
            return live
    except Exception as ex:
        print(f"[catalog] Provider live resolution failed for region={r}: {ex!r}", flush=True)

    if r == "US":
        _PROVIDER_CACHE[r] = dict(_STATIC_PROVIDER_US)
        print(f"[catalog] Using static US provider map ({len(_STATIC_PROVIDER_US)} entries).", flush=True)
        return _PROVIDER_CACHE[r]

    _PROVIDER_CACHE[r] = {}
    print(f"[catalog] No provider map available for region={r}; discovery will be unfiltered.", flush=True)
    return {}

def providers_from_env(subs_include: Optional[Union[str, Iterable[str]]],
                       region: Optional[str] = None) -> List[int]:
    wanted = [_normalize_provider_name(s) for s in _listish(subs_include)]
    if not wanted:
        return []

    r = (region or "US").upper()
    mapping = _provider_map(r)

    ids: List[int] = []
    missing: List[str] = []
    for name in wanted:
        pid = mapping.get(name)
        if pid is None:
            if name == "peacock" and "peacock_premium" in mapping:
                pid = mapping["peacock_premium"]
            elif name == "peacock_premium" and "peacock" in mapping:
                pid = mapping["peacock"]
            elif name in ("amazon", "amazon_prime_video", "prime"):
                pid = mapping.get("prime_video")
        if pid is None:
            missing.append(name)
            continue
        if pid not in ids:
            ids.append(pid)

    if missing:
        print(f"[catalog] Note: could not map some providers in region={r}: {missing}", flush=True)

    if ids:
        print(f"[catalog] Using provider IDs for region={r}: {ids}", flush=True)
    else:
        print(f"[catalog] No provider IDs mapped from SUBS_INCLUDE={wanted} for region={r}; running unfiltered discovery.", flush=True)

    return ids

# ----------------------------
# Discover API
# ----------------------------

def _coerce_langs(original_langs: Union[str, Iterable[str], None]) -> Optional[str]:
    if original_langs is None:
        return None
    if isinstance(original_langs, str):
        s = original_langs.strip()
        if not s:
            return None
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    parts = [str(x).strip() for x in arr if str(x).strip()]
                    return "|".join(parts) if parts else None
            except Exception:
                pass
        if "," in s:
            parts = [t.strip() for t in s.split(",") if t.strip()]
            return "|".join(parts) if parts else None
        return s
    parts = [str(x).strip() for x in original_langs if str(x).strip()]
    return "|".join(parts) if parts else None


def _discover(kind: str, page: int, *, region: str,
              provider_ids: List[int],
              original_langs: Union[str, Iterable[str], None]) -> Dict[str, Any]:
    assert kind in ("movie", "tv")
    url = f"{TMDB_BASE}/discover/{kind}"
    with_langs = _coerce_langs(original_langs)
    params = {
        "language": "en-US",
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "page": page,
        "watch_region": (region or "US").upper(),
        "with_watch_monetization_types": "flatrate|free|ads",
        "with_watch_providers": "|".join(str(x) for x in provider_ids) if provider_ids else None,
        "with_original_language": with_langs,
    }
    return _get_json(f"discover_{kind}", url, params)


def discover_movie_page(page: int, *, region: str,
                        provider_ids: List[int],
                        original_langs: Union[str, Iterable[str], None]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
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


def discover_tv_page(page: int, *, region: str,
                     provider_ids: List[int],
                     original_langs: Union[str, Iterable[str], None]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
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