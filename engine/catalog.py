# engine/catalog.py
from __future__ import annotations
import os
from typing import Any, Dict, List, Tuple
from types import SimpleNamespace

from .catalog_store import (
    load_store,
    save_store,
    merge_discover_batch,
    all_items,
)

# We import the discover callables from engine.tmdb.
# They must exist with these exact names.
try:
    from .tmdb import discover_movie_page as _tmdb_discover_movie
    from .tmdb import discover_tv_page as _tmdb_discover_tv
except Exception as e:
    raise ImportError(
        "engine.catalog: unable to import discover_movie_page/discover_tv_page from engine.tmdb. "
        "Ensure engine/tmdb.py exports those symbols."
    ) from e


def _coerce_cfg(cfg: Any) -> SimpleNamespace:
    """Accepts dict, SimpleNamespace, or any object with attributes/env."""
    if isinstance(cfg, dict):
        return SimpleNamespace(**cfg)
    return cfg  # assume it already exposes attributes


def _get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _get_env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or str(v).strip() == "" else str(v)


def _get_attr_or_env(cfg: SimpleNamespace, name: str, default: Any) -> Any:
    if hasattr(cfg, name):
        val = getattr(cfg, name)
        if val is not None:
            return val
    # fall back to env
    if isinstance(default, int):
        return _get_env_int(name, default)
    elif isinstance(default, str):
        return _get_env_str(name, default)
    else:
        # no good env decoding — just return default
        return default


def _make_page_plan(cfg: SimpleNamespace) -> Dict[str, Any]:
    movie_pages = _get_attr_or_env(cfg, "TMDB_PAGES_MOVIE", 24)
    tv_pages = _get_attr_or_env(cfg, "TMDB_PAGES_TV", 24)

    # A deterministic slot (optional cosmetics)
    rotate_minutes = 15
    slot = (movie_pages * 100000) + (tv_pages * 1000) + rotate_minutes
    return {
        "movie_pages": int(movie_pages),
        "tv_pages": int(tv_pages),
        "rotate_minutes": rotate_minutes,
        "slot": slot,
    }


def _default_meta() -> Dict[str, Any]:
    # Always present keys so downstream code never KeyErrors.
    return {
        "pool_counts": {"movie": 0, "tv": 0},
        "telemetry": {
            "counts": {
                "tmdb_pool": 0,
                "eligible_unseen": 0,
                "shortlist": 0,
                "shown": 0,
            }
        },
        "page_plan": {"movie_pages": 0, "tv_pages": 0, "rotate_minutes": 0, "slot": 0},
        "providers": [],
    }


def _provider_names_list(cfg: SimpleNamespace) -> List[str]:
    raw = _get_attr_or_env(cfg, "SUBS_INCLUDE", "")
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def build_pool(cfg: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build the nightly candidate pool.
    Returns (pool_list, meta_dict).
    Meta ALWAYS has the keys used by runner & summary.
    """
    cfg = _coerce_cfg(cfg)

    plan = _make_page_plan(cfg)
    provider_names = _provider_names_list(cfg)
    language = _get_attr_or_env(cfg, "ORIGINAL_LANGS", "en")
    with_original_language = language or "en"
    watch_region = _get_attr_or_env(cfg, "REGION", "US")
    include_tv_seasons = str(_get_attr_or_env(cfg, "INCLUDE_TV_SEASONS", "true")).lower() == "true"
    max_catalog = int(_get_attr_or_env(cfg, "MAX_CATALOG", 10000))

    # Load cumulative store first (may be empty)
    store = load_store("data/catalog_store.json")

    print("[hb] | catalog:begin", flush=True)

    # Discover fresh pages (new titles each run) — robust fallback in tmdb.* functions.
    pool_movie: List[Dict[str, Any]] = []
    for p in range(1, plan["movie_pages"] + 1):
        batch = _tmdb_discover_movie(
            page=p,
            provider_names=provider_names,
            watch_region=watch_region,
            with_original_language=with_original_language,
        )
        if not batch:
            break
        pool_movie.extend(batch)
        if len(pool_movie) >= max_catalog:
            break

    pool_tv: List[Dict[str, Any]] = []
    for p in range(1, plan["tv_pages"] + 1):
        batch = _tmdb_discover_tv(
            page=p,
            provider_names=provider_names,
            watch_region=watch_region,
            with_original_language=with_original_language,
            include_tv_seasons=include_tv_seasons,
        )
        if not batch:
            break
        pool_tv.extend(batch)
        if len(pool_tv) >= max_catalog:
            break

    # Merge into store (growing over time)
    added_m = merge_discover_batch(store, pool_movie)
    added_t = merge_discover_batch(store, pool_tv)

    # Persist the store so future runs pick it up
    save_store("data/catalog_store.json", store)

    # Compose tonight’s pool = store items limited to MAX_CATALOG (latest-first)
    cumulative = all_items(store)
    if len(cumulative) > max_catalog:
        cumulative = cumulative[:max_catalog]

    # Meta & telemetry
    movie_count = sum(1 for it in cumulative if it.get("type") == "movie")
    tv_count = sum(1 for it in cumulative if it.get("type") == "tv")

    meta = _default_meta()
    meta["pool_counts"] = {"movie": movie_count, "tv": tv_count}
    meta["telemetry"]["counts"]["tmdb_pool"] = movie_count + tv_count
    meta["page_plan"] = plan
    meta["providers"] = provider_names
    meta["store_added"] = {"movie": int(added_m), "tv": int(added_t)}

    print(f"[hb] | catalog:end pool={movie_count + tv_count} movie={movie_count} tv={tv_count}", flush=True)
    return cumulative, meta