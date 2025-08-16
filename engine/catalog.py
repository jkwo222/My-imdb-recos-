# engine/catalog.py
from __future__ import annotations

import os
from typing import Dict, List, Tuple

from .config import Config
from .catalog_store import load_store, save_store, merge_discover_batch, all_items
from .tmdb import discover_movie_page, discover_tv_page, providers_from_env


def _make_page_plan(cfg: Config) -> Dict[str, int]:
    """Decide how many pages to fetch for movie/tv."""
    return {
        "movie": int(cfg.tmdb_pages_movie),
        "tv": int(cfg.tmdb_pages_tv),
    }


def _fetch_all_tmdb(cfg: Config) -> Tuple[List[Dict], Dict]:
    """
    Fetch both movie and tv discovery across *all* user services (OR union),
    page by page, then merge to one pool; also compute simple meta counts.
    """
    subs_csv = cfg.subs_include  # e.g., "netflix,prime_video,hulu,..."
    provider_ids = providers_from_env(subs_csv)
    region = cfg.watch_region
    langs = [s.strip() for s in (cfg.with_original_language or "").split(",") if s.strip()]

    movie_pages = int(cfg.tmdb_pages_movie)
    tv_pages = int(cfg.tmdb_pages_tv)

    pool: List[Dict] = []
    meta = {"pool_counts": {"movie": 0, "tv": 0}}

    # Movies
    for p in range(1, movie_pages + 1):
        items, _page = discover_movie_page(p, region=region, provider_ids=provider_ids, original_langs=langs)
        for it in items:
            it["type"] = "movie"
        pool.extend(items)
    meta["pool_counts"]["movie"] = len([x for x in pool if x.get("type") == "movie"])

    # TV
    for p in range(1, tv_pages + 1):
        items, _page = discover_tv_page(p, region=region, provider_ids=provider_ids, original_langs=langs)
        for it in items:
            it["type"] = "tv"
        pool.extend(items)
    meta["pool_counts"]["tv"] = len([x for x in pool if x.get("type") == "tv"])

    # Cap if needed
    if cfg.max_catalog and len(pool) > int(cfg.max_catalog):
        pool = pool[: int(cfg.max_catalog)]

    return pool, meta


def build_pool(cfg: Config) -> Tuple[List[Dict], Dict]:
    """
    Public entry: builds the full pool (movies + tv) guaranteed to include titles
    available on ANY of the user's subscribed services for this run.
    Also persists/merges into the store.
    """
    print("[hb] | catalog:begin", flush=True)

    # fetch fresh pages each run (new titles) – no provider filtering later:
    fresh_pool, meta = _fetch_all_tmdb(cfg)

    # merge into cumulative store (avoid losing seen metadata/history)
    store = load_store()
    merged = merge_discover_batch(store, fresh_pool)
    save_store(merged)

    # Produce final one-run pool from merged store to hand back to runner
    # (all newly discovered + anything we kept historically if you want).
    # For "pool includes everything found now", we just pass fresh_pool onward.
    print(f"[hb] | catalog:end pool={len(fresh_pool)} movie={meta['pool_counts']['movie']} tv={meta['pool_counts']['tv']}", flush=True)
    return fresh_pool, meta