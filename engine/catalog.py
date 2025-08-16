# engine/catalog.py
from __future__ import annotations

import os
import random
from typing import Any, Dict, List, Tuple

from .catalog_store import load_store, save_store, merge_discover_batch, all_items

# Import TMDB discover helpers exposed by engine.tmdb
try:
    from .tmdb import discover_movie_page, discover_tv_page
except Exception:
    raise ImportError(
        "engine.catalog: unable to import discover_movie_page/discover_tv_page from engine.tmdb. "
        "Ensure engine/tmdb.py exports those symbols."
    )

# -------------------------
# small utils
# -------------------------

def _int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _cfg_get(cfg, key, default=None):
    """
    Read a setting from:
      1) cfg.get(key, default) if available
      2) cfg[key] if cfg is a dict
      3) getattr(cfg, key) if attribute exists
      4) environment variable
      5) provided default
    """
    try:
        if hasattr(cfg, "get") and callable(getattr(cfg, "get")):
            return cfg.get(key, default)
    except Exception:
        pass
    if isinstance(cfg, dict):
        return cfg.get(key, default)
    if hasattr(cfg, key):
        return getattr(cfg, key)
    return os.getenv(key, default)


# -------------------------
# page planning
# -------------------------

def _make_page_plan(cfg) -> Dict[str, Any]:
    movie_pages = _int(_cfg_get(cfg, "TMDB_PAGES_MOVIE", 24), 24)
    tv_pages = _int(_cfg_get(cfg, "TMDB_PAGES_TV", 24), 24)
    slot = random.randint(1_000_000, 9_999_999)
    rotate_minutes = 15
    return {
        "movie_pages": movie_pages,
        "tv_pages": tv_pages,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
    }


def _provider_names(cfg) -> List[str]:
    raw = str(_cfg_get(cfg, "SUBS_INCLUDE", "") or "")
    return [p.strip() for p in raw.split(",") if p.strip()]


def _provider_param_for_tmdb(names: List[str]) -> str | None:
    """
    TMDB 'with_watch_providers' expects numeric IDs.
    If the concatenated string has no digits (likely names like 'netflix,hulu'),
    skip the filter to avoid getting 0 results.
    """
    if not names:
        return None
    s = ",".join(names)
    return s if any(ch.isdigit() for ch in s) else None


# -------------------------
# build pool
# -------------------------

def build_pool(cfg) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    1) Load cumulative store (data/catalog_store.json)
    2) Discover TMDB titles for configured pages/providers/lang/region
    3) Merge into store and save
    4) Return (pool_items, meta)
    """
    plan = _make_page_plan(cfg)
    movie_pages = plan["movie_pages"]
    tv_pages = plan["tv_pages"]

    provider_names = _provider_names(cfg)
    provider_param = _provider_param_for_tmdb(provider_names)

    language = (str(_cfg_get(cfg, "ORIGINAL_LANGS", "en")) or "en").strip()
    watch_region = (str(_cfg_get(cfg, "REGION", "US")) or "US").strip()
    include_tv_seasons = str(_cfg_get(cfg, "INCLUDE_TV_SEASONS", "true")).lower() in {"1", "true", "yes", "y"}
    max_catalog = _int(_cfg_get(cfg, "MAX_CATALOG", 10000), 10000)

    store_path = "data/catalog_store.json"
    store = load_store(store_path)

    pool_items: List[Dict[str, Any]] = []
    added_movie = updated_movie = 0
    added_tv = updated_tv = 0

    # --- movies ---
    total_pages_seen_movie = 0
    for page in range(1, movie_pages + 1):
        results, total_pages = discover_movie_page(
            page=page,
            watch_region=watch_region,
            with_watch_providers=provider_param,
            with_original_language=language,
        )
        total_pages_seen_movie = total_pages
        a, u = merge_discover_batch(
            store,
            results,
            media_type="movie",
            region=watch_region,
            providers=provider_names,
        )
        added_movie += a
        updated_movie += u
        pool_items.extend(
            {
                "type": "movie",
                "id": r.get("id"),
                "title": r.get("title") or r.get("original_title"),
                "year": (r.get("release_date") or "")[:4] if r.get("release_date") else None,
            }
            for r in (results or [])
        )
        if len(all_items(store)) >= max_catalog:
            break

    # --- TV ---
    total_pages_seen_tv = 0
    for page in range(1, tv_pages + 1):
        results, total_pages = discover_tv_page(
            page=page,
            watch_region=watch_region,
            with_watch_providers=provider_param,
            with_original_language=language,
        )
        total_pages_seen_tv = total_pages
        a, u = merge_discover_batch(
            store,
            results,
            media_type="tv",
            region=watch_region,
            providers=provider_names,
        )
        added_tv += a
        updated_tv += u
        pool_items.extend(
            {
                "type": "tvSeries",
                "id": r.get("id"),
                "title": r.get("name") or r.get("original_name"),
                "year": (r.get("first_air_date") or "")[:4] if r.get("first_air_date") else None,
            }
            for r in (results or [])
        )
        if len(all_items(store)) >= max_catalog:
            break

    # Persist the cumulative store
    save_store(store, store_path)

    # Pool counts for the current run (what runner.py expects)
    pool_count_movie = sum(1 for x in pool_items if x["type"] == "movie")
    pool_count_tv = sum(1 for x in pool_items if x["type"] == "tvSeries")
    pool_total = pool_count_movie + pool_count_tv

    # Full metadata
    meta: Dict[str, Any] = {
        "page_plan": {
            **plan,
            "movie_total_pages_seen": total_pages_seen_movie,
            "tv_total_pages_seen": total_pages_seen_tv,
        },
        "provider_names": provider_names,
        "language": language,
        "watch_region": watch_region,
        "include_tv_seasons": include_tv_seasons,
        "limits": {"max_catalog": max_catalog},
        "store_counts": {
            "movie": len(store.get("movie", {})),
            "tv": len(store.get("tv", {})),
            "added_this_run": {"movie": added_movie, "tv": added_tv},
            "updated_this_run": {"movie": updated_movie, "tv": updated_tv},
        },
        # 👇 this block fixes the KeyError in runner.py
        "pool_counts": {
            "movie": pool_count_movie,
            "tv": pool_count_tv,
            "total": pool_total,
        },
    }

    return pool_items, meta