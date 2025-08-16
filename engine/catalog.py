# engine/catalog.py
from __future__ import annotations

import os
import time
import math
import hashlib
import random
from typing import Dict, Any, Iterable, List, Tuple
import json
import pathlib

import requests

from .catalog_store import load_store, save_store, merge_discover_batch, all_items


# ----------------------------
# Helpers
# ----------------------------

def _int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _split_csv(s: str) -> List[str]:
    out: List[str] = []
    if not s:
        return out
    for p in s.split(","):
        p = p.strip()
        if p:
            out.append(p)
    return out


def _deterministic_pages(total_pages: int, how_many: int, seed: int, salt: str) -> List[int]:
    """Deterministically sample `how_many` distinct pages in [1..total_pages]."""
    how_many = max(0, min(how_many, total_pages))
    rng = random.Random()
    h = hashlib.sha256(f"{seed}:{salt}".encode("utf-8")).digest()
    rng.seed(int.from_bytes(h[:8], "big", signed=False))
    population = list(range(1, total_pages + 1))
    return rng.sample(population, how_many)


# ----------------------------
# TMDB provider ID mapping
# ----------------------------

# Reasonable US defaults (TMDB watch provider IDs)
# These are used as a fallback if live fetch fails.
_FALLBACK_PROVIDER_IDS = {
    "netflix": 8,
    "prime_video": 9,          # Amazon Prime Video
    "hulu": 15,
    "max": 384,                # formerly hbo_max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 387,
    "paramount_plus": 531,
}

_CACHE_DIR = pathlib.Path("data/cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_PROVIDERS_CACHE = _CACHE_DIR / "tmdb_providers_US.json"


def _fetch_provider_ids(api_key: str, region: str) -> Dict[str, int]:
    """
    Try to fetch TMDB watch providers for the region and map some friendly slugs
    to IDs. We keep a small fallback map so runs don't fail if the network
    flakes.
    """
    try:
        if _PROVIDERS_CACHE.exists():
            with open(_PROVIDERS_CACHE, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if isinstance(cached, dict) and cached.get("_region") == region:
                return cached.get("ids", _FALLBACK_PROVIDER_IDS.copy())
    except Exception:
        pass

    try:
        # Movies
        r_m = requests.get(
            "https://api.themoviedb.org/3/watch/providers/movie",
            params={"api_key": api_key, "watch_region": region},
            timeout=15,
        )
        r_m.raise_for_status()
        results_m = r_m.json().get("results", []) or []

        # TV
        r_t = requests.get(
            "https://api.themoviedb.org/3/watch/providers/tv",
            params={"api_key": api_key, "watch_region": region},
            timeout=15,
        )
        r_t.raise_for_status()
        results_t = r_t.json().get("results", []) or []

        # Build lookup by lowercase display name and by TMDB's own lowercase "provider_name"
        look: Dict[str, int] = {}
        for row in results_m + results_t:
            pid = row.get("provider_id")
            name = (row.get("provider_name") or "").strip().lower().replace(" ", "_")
            if isinstance(pid, int) and pid > 0 and name:
                look[name] = pid

        # Normalize our expected slugs to IDs if present
        resolved = _FALLBACK_PROVIDER_IDS.copy()
        # Map a few common variants found in TMDB lists
        aliases = {
            "prime_video": ["amazon_prime_video", "prime_video"],
            "max": ["max", "hbo_max"],
            "apple_tv_plus": ["apple_tv_plus", "apple_tv+"],
            "disney_plus": ["disney+", "disney_plus"],
            "paramount_plus": ["paramount+", "paramount_plus"],
        }

        for slug, pid in list(resolved.items()):
            # If live list has a better match, take it.
            if slug in look:
                resolved[slug] = look[slug]
                continue
            for alias in aliases.get(slug, []):
                if alias in look:
                    resolved[slug] = look[alias]
                    break

        try:
            with open(_PROVIDERS_CACHE, "w", encoding="utf-8") as f:
                json.dump({"_region": region, "ids": resolved}, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return resolved
    except Exception:
        # Fallback if anything fails
        return _FALLBACK_PROVIDER_IDS.copy()


# ----------------------------
# TMDB Discover (direct)
# ----------------------------

def _tmdb_discover(
    kind: str,
    page: int,
    provider_names: List[str],
    language: str,
    with_original_language: str,
    watch_region: str,
    api_key: str,
) -> List[Dict[str, Any]]:
    """
    Minimal TMDB Discover wrapper that returns a normalized list of items.
    """
    base = "https://api.themoviedb.org/3/discover"
    endpoint = f"{base}/movie" if kind == "movie" else f"{base}/tv"

    provider_ids_map = _fetch_provider_ids(api_key, watch_region)
    provider_ids: List[int] = []
    for name in provider_names:
        slug = name.strip().lower()
        if slug in provider_ids_map:
            provider_ids.append(provider_ids_map[slug])

    params = {
        "api_key": api_key,
        "page": page,
        "include_adult": "false",
        "language": language,
        "watch_region": watch_region,
        # provider filters
        "with_watch_providers": ",".join(str(i) for i in sorted(set(provider_ids))) if provider_ids else None,
        "with_original_language": with_original_language or None,
        # light quality sort to surface better things first within a page
        "sort_by": "vote_count.desc",
    }
    # strip Nones
    params = {k: v for k, v in params.items() if v not in (None, "", [])}

    r = requests.get(endpoint, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", []) or []

    out: List[Dict[str, Any]] = []
    for row in results:
        if kind == "movie":
            tmdb_id = row.get("id")
            title = row.get("title") or row.get("original_title") or ""
            year = (row.get("release_date") or "")[:4] or None
        else:
            tmdb_id = row.get("id")
            title = row.get("name") or row.get("original_name") or ""
            year = (row.get("first_air_date") or "")[:4] or None

        if not tmdb_id or not title:
            continue

        out.append(
            {
                "type": "movie" if kind == "movie" else "tvSeries",
                "tmdb_id": tmdb_id,
                "title": title,
                "year": int(year) if year and year.isdigit() else None,
                "original_language": row.get("original_language"),
                "vote_average": row.get("vote_average"),
                "vote_count": row.get("vote_count"),
                "popularity": row.get("popularity"),
                "poster_path": row.get("poster_path"),
                "backdrop_path": row.get("backdrop_path"),
                "providers_filter": provider_names,
                "region": watch_region,
            }
        )

    return out


# ----------------------------
# Planning & Orchestration
# ----------------------------

def _make_page_plan(cfg: Dict[str, Any]) -> Dict[str, Any]:
    movie_pages = _int(cfg.get("TMDB_PAGES_MOVIE", os.getenv("TMDB_PAGES_MOVIE", 24)), 24)
    tv_pages = _int(cfg.get("TMDB_PAGES_TV", os.getenv("TMDB_PAGES_TV", 24)), 24)
    rotate_minutes = _int(cfg.get("ROTATE_MINUTES", 15), 15)

    total_pages_movie = _int(cfg.get("TOTAL_PAGES_MOVIE", 500), 500)
    total_pages_tv = _int(cfg.get("TOTAL_PAGES_TV", 500), 500)

    now = int(time.time())
    slot = math.floor(now / (rotate_minutes * 60))

    movie_pages_used = _deterministic_pages(total_pages_movie, movie_pages, slot, "movie")
    tv_pages_used = _deterministic_pages(total_pages_tv, tv_pages, slot, "tv")

    provider_names = _split_csv(cfg.get("SUBS_INCLUDE", os.getenv("SUBS_INCLUDE", "")))
    language = cfg.get("LANGUAGE", os.getenv("LANGUAGE", "en-US"))
    with_original_language = cfg.get("ORIGINAL_LANGS", os.getenv("ORIGINAL_LANGS", "en"))
    watch_region = cfg.get("REGION", os.getenv("REGION", "US"))

    return {
        "movie_pages": movie_pages,
        "tv_pages": tv_pages,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
        "total_pages_movie": total_pages_movie,
        "total_pages_tv": total_pages_tv,
        "movie_pages_used": movie_pages_used,
        "tv_pages_used": tv_pages_used,
        "provider_names": provider_names,
        "language": language,
        "with_original_language": with_original_language,
        "watch_region": watch_region,
    }


def _discover_batch(
    kind: str,
    pages: Iterable[int],
    provider_names: List[str],
    language: str,
    with_original_language: str,
    watch_region: str,
    api_key: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for pg in pages:
        out.extend(
            _tmdb_discover(
                kind=kind,
                page=int(pg),
                provider_names=provider_names,
                language=language,
                with_original_language=with_original_language,
                watch_region=watch_region,
                api_key=api_key,
            )
        )
    return out


def build_pool(cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Dict-driven entry point (matches runner.py calling build_pool(cfg)).

    Steps:
      1) Make a deterministic page plan
      2) Discover TMDB (movie + tv) for planned pages
      3) Merge into persistent catalog store (append-only)
      4) Return (current_run_pool, meta)
    """
    api_key = os.getenv("TMDB_API_KEY") or str(cfg.get("TMDB_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required for discover calls")

    # 1) Plan
    plan = _make_page_plan(cfg)

    # 2) Discover
    movie_batch = _discover_batch(
        "movie",
        plan["movie_pages_used"],
        plan["provider_names"],
        plan["language"],
        plan["with_original_language"],
        plan["watch_region"],
        api_key=api_key,
    )
    tv_batch = _discover_batch(
        "tv",
        plan["tv_pages_used"],
        plan["provider_names"],
        plan["language"],
        plan["with_original_language"],
        plan["watch_region"],
        api_key=api_key,
    )

    # 3) Merge into persistent store
    store = load_store()
    added_m = merge_discover_batch(store, movie_batch)
    added_t = merge_discover_batch(store, tv_batch)
    save_store(store)

    # 4) Current run pool + meta
    current_pool: List[Dict[str, Any]] = movie_batch + tv_batch

    meta: Dict[str, Any] = {
        "pool_counts": {
            "movie": sum(1 for x in current_pool if (x.get("type") or "").lower().startswith("mov")),
            "tv": sum(1 for x in current_pool if (x.get("type") or "").lower().startswith("tv")),
        },
        "store_counts": {
            "movie": sum(1 for _ in all_items(store, "movie")),
            "tv": sum(1 for _ in all_items(store, "tv")),
        },
        "added_this_run": {
            "movie": added_m.get("movie", 0) + added_t.get("movie", 0),
            "tv": added_m.get("tv", 0) + added_t.get("tv", 0),
        },
        "page_plan": {
            "movie_pages": plan["movie_pages"],
            "tv_pages": plan["tv_pages"],
            "rotate_minutes": plan["rotate_minutes"],
            "slot": plan["slot"],
            "movie_pages_used": plan["movie_pages_used"],
            "tv_pages_used": plan["tv_pages_used"],
            "provider_names": plan["provider_names"],
            "language": plan["language"],
            "with_original_language": plan["with_original_language"],
            "watch_region": plan["watch_region"],
        },
    }

    return current_pool, meta