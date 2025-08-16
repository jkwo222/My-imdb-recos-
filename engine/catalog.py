from __future__ import annotations
import hashlib
from typing import Any, Dict, List, Tuple

from .config import Config
from .tmdb import TMDB, normalize_provider_names
from .util.cache import DiskCache
from .catalog_store import load_store, save_store, merge_discover_batch, all_items

def _slot_number(rotate_minutes: int) -> int:
    import time
    return int(time.time() // (rotate_minutes * 60))

# Cache singleton (lazy)
__cache = None
def _cache(cfg: Config):
    global __cache
    if __cache is None:
        __cache = DiskCache(cfg.cache_dir, cfg.cache_ttl_secs)
    return __cache

def _seed_for(kind: str, cfg: Config, slot: int) -> int:
    key = f"{kind}|{slot}|{cfg.region}|{cfg.language}|{','.join(cfg.with_original_langs)}|{','.join(cfg.subs_include)}|{cfg.tmdb_pages_movie}|{cfg.tmdb_pages_tv}"
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)

def _choose_pages(total_pages: int, want_pages: int, seed: int) -> List[int]:
    import random
    total_pages = max(1, int(total_pages))
    want_pages = max(1, min(int(want_pages), total_pages))
    if total_pages == 1:
        return [1]
    rng = random.Random(seed)
    step = (rng.randrange(1, total_pages) * 2 + 1) % total_pages
    if step == 0: step = 1
    start = rng.randrange(0, total_pages)
    pages = []
    cur = start
    for _ in range(want_pages):
        pages.append(cur + 1)
        cur = (cur + step) % total_pages
    return pages

def _coerce_item(kind: str, raw: Dict[str, Any]) -> Dict[str, Any]:
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
        "vote_average": float(raw.get("vote_average") or 0.0),
        "original_language": raw.get("original_language"),
    }

def build_pool(cfg: Config) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    NEW: persistent catalog store.
      1) Discover pages (deterministic per-slot) -> merge into store (union over time).
      2) Build pool from ALL stored items, applying provider filters.
      3) Return pool + meta (with store stats).
    """
    tmdb = TMDB(cfg.tmdb_api_key, _cache(cfg))
    rotate_minutes = 15
    slot = _slot_number(rotate_minutes)

    # Load existing store
    store = load_store()

    # Total pages (once)
    total_pages_movie = tmdb.total_pages("movie", cfg.language, cfg.with_original_langs, cfg.region)
    total_pages_tv    = tmdb.total_pages("tv",    cfg.language, cfg.with_original_langs, cfg.region)

    # Page plans (deterministic, rotating)
    seed_m = _seed_for("movie", cfg, slot)
    seed_t = _seed_for("tv", cfg, slot)
    movie_pages_used = _choose_pages(total_pages_movie, cfg.tmdb_pages_movie, seed_m)
    tv_pages_used    = _choose_pages(total_pages_tv,    cfg.tmdb_pages_tv,    seed_t)

    # Collect current-slot pages
    def collect(kind: str, pages: List[int]) -> List[Dict[str, Any]]:
        out = []
        for p in pages:
            data = tmdb.discover(
                kind=kind,
                page=p,
                language=cfg.language,
                with_original_language=cfg.with_original_langs,
                watch_region=cfg.region,
            )
            for raw in data.get("results", []) or []:
                out.append(_coerce_item(kind, raw))
        return out

    movie_batch = collect("movie", movie_pages_used)
    tv_batch    = collect("tv", tv_pages_used)

    # Merge into persistent store
    added_m, updated_m = merge_discover_batch("movie", movie_batch, store)
    added_t, updated_t = merge_discover_batch("tv",    tv_batch,    store)
    save_store(store)

    # Build from ALL stored items, apply provider filter
    def enrich_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        keep: List[Dict[str, Any]] = []
        for it in items:
            prov_names = tmdb.providers_for_title(it["kind"], it["tmdb_id"], cfg.region)
            prov_slugs = normalize_provider_names(prov_names)
            if any(s in cfg.subs_include for s in prov_slugs):
                x = dict(it)
                x["providers"] = prov_slugs
                keep.append(x)
        return keep

    stored_all = all_items(store)
    movies_all = [x for x in stored_all if x.get("kind") == "movie"]
    tv_all     = [x for x in stored_all if x.get("kind") == "tv"]

    movie_items = enrich_and_filter(movies_all)
    tv_items    = enrich_and_filter(tv_all)

    pool = movie_items + tv_items
    pool.sort(key=lambda x: x.get("popularity", 0.0), reverse=True)
    if cfg.max_catalog > 0:
        pool = pool[: cfg.max_catalog]

    meta = {
        "movie_pages": cfg.tmdb_pages_movie,
        "tv_pages": cfg.tmdb_pages_tv,
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
        # store stats
        "store_counts": {
            "movie": len(store.get("movie", {})),
            "tv": len(store.get("tv", {})),
            "added_this_run": {"movie": added_m, "tv": added_t},
            "updated_this_run": {"movie": updated_m, "tv": updated_t},
        },
    }
    return pool, meta