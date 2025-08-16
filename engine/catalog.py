from __future__ import annotations
import hashlib
import math
import random
from typing import Any, Dict, List, Tuple

from .config import Config
from .tmdb import TMDB, normalize_provider_names

def _slot_number(rotate_minutes: int) -> int:
    # 15-min rotation => same “slot” within a 15-min bucket
    import time
    return int(time.time() // (rotate_minutes * 60))

def _choose_pages(total_pages: int, want_pages: int, seed: int) -> List[int]:
    """
    Deterministic, widely-spaced page selection using an odd step (cycle through ring).
    Handles tiny totals gracefully.
    """
    total_pages = max(1, int(total_pages))
    want_pages = max(1, int(want_pages))
    want_pages = min(want_pages, total_pages)

    if total_pages == 1:
        return [1]

    rng = random.Random(seed)
    # Odd step in [1, total_pages-1], ensures a full cycle
    step = (rng.randrange(1, total_pages) * 2 + 1) % total_pages
    if step == 0:
        step = 1
    start = rng.randrange(0, total_pages)
    pages = []
    cur = start
    for _ in range(want_pages):
        pages.append(cur + 1)  # TMDB pages are 1-based
        cur = (cur + step) % total_pages
    return pages

def _want_pages(cfg: Config) -> Tuple[int, int]:
    return (max(1, cfg.tmdb_pages_movie), max(1, cfg.tmdb_pages_tv))

def _seed_for(kind: str, cfg: Config, slot: int) -> int:
    key = f"{kind}|{slot}|{cfg.region}|{cfg.language}|{','.join(cfg.with_original_langs)}|{','.join(cfg.subs_include)}"
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)

def _coerce_item(kind: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    # TMDB discover returns slightly different fields for tv vs movie
    if kind == "movie":
        title = raw.get("title") or raw.get("original_title") or ""
        year = (raw.get("release_date") or "")[:4] or None
    else:
        title = raw.get("name") or raw.get("original_name") or ""
        year = (raw.get("first_air_date") or "")[:4] or None
    return {
        "kind": kind,
        "tmdb_id": int(raw.get("id")),
        "title": title,
        "year": int(year) if (isinstance(year, str) and year.isdigit()) else year,
        "popularity": float(raw.get("popularity") or 0.0),
        "vote_average": float(raw.get("vote_average") or 0.0),  # 0..10
        "original_language": raw.get("original_language"),
    }

def build_pool(cfg: Config) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Collects items from TMDB discover across many pages (randomized but deterministic per slot),
    filters by region/providers (your services only), and returns (pool, meta).
    """
    tmdb = TMDB(cfg.tmdb_api_key, cache=_cache(cfg))
    rotate_minutes = 15
    slot = _slot_number(rotate_minutes)

    # Determine total pages once for each kind
    total_pages_movie = tmdb.total_pages("movie", cfg.language, cfg.with_original_langs, cfg.region)
    total_pages_tv = tmdb.total_pages("tv", cfg.language, cfg.with_original_langs, cfg.region)

    want_movie, want_tv = _want_pages(cfg)
    seed_m = _seed_for("movie", cfg, slot)
    seed_t = _seed_for("tv", cfg, slot)

    movie_pages_used = _choose_pages(total_pages_movie, want_movie, seed_m)
    tv_pages_used = _choose_pages(total_pages_tv, want_tv, seed_t)

    pool: List[Dict[str, Any]] = []
    def collect(kind: str, pages: List[int]) -> List[Dict[str, Any]]:
        out = []
        for p in pages:
            data = tmdb.discover(kind, p, cfg.language, cfg.with_original_langs, cfg.region)
            for raw in data.get("results", []) or []:
                item = _coerce_item(kind, raw)
                out.append(item)
        return out

    movie_items = collect("movie", movie_pages_used)
    tv_items    = collect("tv", tv_pages_used)

    # Filter by providers (post-filter; avoids brittle provider-ID matching)
    def enrich_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        keep: List[Dict[str, Any]] = []
        for it in items:
            prov_names = tmdb.providers_for_title(it["kind"], it["tmdb_id"], cfg.region)
            prov_slugs = normalize_provider_names(prov_names)
            it["providers"] = prov_slugs
            if any(s in cfg.subs_include for s in prov_slugs):
                keep.append(it)
        return keep

    movie_items = enrich_and_filter(movie_items)
    tv_items    = enrich_and_filter(tv_items)

    # Merge, sort by popularity as a first-pass, truncate to max_catalog
    pool = (movie_items + tv_items)
    pool.sort(key=lambda x: x.get("popularity", 0.0), reverse=True)
    if cfg.max_catalog > 0:
        pool = pool[: cfg.max_catalog]

    meta = {
        "movie_pages": want_movie,
        "tv_pages": want_tv,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
        "total_pages_movie": total_pages_movie,
        "total_pages_tv": total_pages_tv,
        "movie_pages_used": movie_pages_used,
        "tv_pages_used": tv_pages_used,
        "provider_names": cfg.subs_include,
        "language": cfg.language,
        "with_original_language": ",".join(cfg.with_original_langs),
        "watch_region": cfg.region,
        "pool_counts": {"movie": len(movie_items), "tv": len(tv_items)},
        "total_pages": [total_pages_movie, total_pages_tv],
    }
    return pool, meta

# Cache singleton (lazy)
__cache = None
def _cache(cfg: Config):
    global __cache
    if __cache is None:
        from .util.cache import DiskCache
        __cache = DiskCache(cfg.cache_dir, cfg.cache_ttl_secs)
    return __cache