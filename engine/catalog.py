# engine/catalog.py
from __future__ import annotations

import os
import random
import time
from typing import Any, Dict, List, Tuple

from .catalog_store import load_store, save_store, merge_discover_batch, all_items

# TMDB discover functions (either legacy or the ones we provide in engine.tmdb)
try:
    from .tmdb import discover_movie_page, discover_tv_page
except Exception:
    # If import fails, raise a clean error with guidance
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
    Read a setting from (in priority order):
      1) cfg.get(key) if present
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


def _log(s: str) -> None:
    print(s, flush=True)


# -------------------------
# page planning
# -------------------------

def _make_page_plan(cfg) -> Dict[str, Any]:
    """
    Decide how many TMDB pages to pull this run, and log a slot so
    pagination rotates across runs.
    """
    movie_pages = _int(_cfg_get(cfg, "TMDB_PAGES_MOVIE", 24), 24)
    tv_pages = _int(_cfg_get(cfg, "TMDB_PAGES_TV", 24), 24)

    # Rotation seed (not critical, but keeps the "slot" message you had)
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


# -------------------------
# build pool
# -------------------------

def build_pool(cfg) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Core entry-point used by runner.py.
    1) Load cumulative store (catalog_store.json) if present
    2) Discover TMDB titles (movie & tv) for the configured pages/providers/lang/region
    3) Merge into store and save back
    4) Return (pool, meta) where pool is the newly pulled batch (movie+tv) and meta has telemetry
    """
    _log("[hb] | catalog:begin")

    plan = _make_page_plan(cfg)
    movie_pages = plan["movie_pages"]
    tv_pages = plan["tv_pages"]

    provider_names = _provider_names(cfg)
    # TMDB expects provider ids as a comma-separated list. If you’re passing human names,
    # leave them as-is; upstream mapping (if any) can be added later.
    provider_param = ",".join(provider_names) if provider_names else None

    language = (str(_cfg_get(cfg, "ORIGINAL_LANGS", "en")) or "en").strip()
    watch_region = (str(_cfg_get(cfg, "REGION", "US")) or "US").strip()

    include_tv_seasons = str(_cfg_get(cfg, "INCLUDE_TV_SEASONS", "true")).lower() in {
        "1", "true", "yes", "y"
    }
    max_catalog = _int(_cfg_get(cfg, "MAX_CATALOG", 10000), 10000)

    store_path = "data/catalog_store.json"
    store = load_store(store_path)

    pool_items: List[Dict[str, Any]] = []
    added_movie = updated_movie = 0
    added_tv = updated_tv = 0

    # --- movies ---
    total_pages_seen = 0
    for page in range(1, movie_pages + 1):
        results, total_pages = discover_movie_page(
            page=page,
            watch_region=watch_region,
            with_watch_providers=provider_param,
            with_original_language=language,
        )
        total_pages_seen = total_pages
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

    save_store(store, store_path)

    # Telemetry to keep your markdown summary & logs consistent
    pool_count_movie = sum(1 for x in pool_items if x["type"] == "movie")
    pool_count_tv = sum(1 for x in pool_items if x["type"] == "tvSeries")
    pool_total = pool_count_movie + pool_count_tv

    _log(f"[hb] | catalog:end pool={pool_total} movie={pool_count_movie} tv={pool_count_tv}")

    meta: Dict[str, Any] = {
        "page_plan": {
            **plan,
            "movie_total_pages_seen": total_pages_seen,
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
    }

    return pool_items, meta