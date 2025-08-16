# FILE: engine/catalog.py
from __future__ import annotations
import hashlib
import random
from typing import Any, Dict, List, Tuple

from .config import Config
from .tmdb import TMDB, normalize_provider_names
from .util.cache import DiskCache
from .util.omdb import fetch_omdb_enrich  # NEW helper (see util/omdb.py)

def _slot_number(rotate_minutes: int) -> int:
    import time
    return int(time.time() // (rotate_minutes * 60))

def _choose_pages(total_pages: int, want_pages: int, seed: int) -> List[int]:
    total_pages = max(1, int(total_pages))
    want_pages = max(1, min(int(want_pages), total_pages))
    if total_pages == 1:
        return [1]
    rng = random.Random(seed)
    step = (rng.randrange(1, total_pages) * 2 + 1) % total_pages
    if step == 0:
        step = 1
    start = rng.randrange(0, total_pages)
    pages, cur = [], start
    for _ in range(want_pages):
        pages.append(cur + 1)
        cur = (cur + step) % total_pages
    return pages

def _want_pages(cfg: Config) -> Tuple[int, int]:
    return (max(1, cfg.tmdb_pages_movie), max(1, cfg.tmdb_pages_tv))

def _seed_for(kind: str, cfg: Config, slot: int) -> int:
    key = f"{kind}|{slot}|{cfg.region}|{cfg.language}|{','.join(cfg.with_original_langs)}|{','.join(cfg.subs_include)}"
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)

def _coerce_item(kind: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    if kind == "movie":
        title = raw.get("title") or raw.get("original_title") or ""
        year = (raw.get("release_date") or "")[:4] or None
        item_type = "movie"
    else:
        title = raw.get("name") or raw.get("original_name") or ""
        year = (raw.get("first_air_date") or "")[:4] or None
        item_type = "tvSeries"
    return {
        "kind": kind,
        "type": item_type,     # required for seen filter
        "tmdb_id": int(raw.get("id")),
        "title": title,
        "year": int(year) if (isinstance(year, str) and year.isdigit()) else year,
        "popularity": float(raw.get("popularity") or 0.0),
        "vote_average": float(raw.get("vote_average") or 0.0),
        "original_language": raw.get("original_language"),
        # imdb_id will be added by OMDb enrich below
    }

def _cache(cfg: Config) -> DiskCache:
    return DiskCache(cfg.cache_dir, cfg.cache_ttl_secs)

def build_pool(cfg: Config) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    tmdb = TMDB(cfg.tmdb_api_key, cache=_cache(cfg))
    rotate_minutes = 15
    slot = _slot_number(rotate_minutes)

    total_pages_movie = tmdb.total_pages("movie", cfg.language, cfg.with_original_langs, cfg.region)
    total_pages_tv    = tmdb.total_pages("tv",    cfg.language, cfg.with_original_langs, cfg.region)

    want_movie, want_tv = _want_pages(cfg)
    seed_m = _seed_for("movie", cfg, slot)
    seed_t = _seed_for("tv", cfg, slot)

    movie_pages_used = _choose_pages(total_pages_movie, want_movie, seed_m)
    tv_pages_used    = _choose_pages(total_pages_tv,    want_tv,    seed_t)

    def collect(kind: str, pages: List[int]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for p in pages:
            data = tmdb.discover(kind, p, cfg.language, cfg.with_original_langs, cfg.region)
            for raw in data.get("results", []) or []:
                out.append(_coerce_item(kind, raw))
        return out

    movie_items = collect("movie", movie_pages_used)
    tv_items    = collect("tv", tv_pages_used)

    # Enrich providers & filter to your subscriptions
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

    pool: List[Dict[str, Any]] = movie_items + tv_items

    # ---- NEW: OMDb enrichment for IMDb IDs (cached) ----
    # We only need imdb_id (and optionally languages/genres if you want later)
    api_key = (cfg.omdb_api_key or "").strip()
    if api_key:
        for it in pool:
            # Skip if already enriched (future-proof)
            if it.get("imdb_id"):
                continue
            enrich = fetch_omdb_enrich(
                title=it.get("title") or "",
                year=int(it["year"]) if isinstance(it.get("year"), int) else None,
                media_type=("series" if it.get("type") == "tvSeries" else "movie"),
                api_key=api_key,
                cache=_cache(cfg),
            )
            if enrich.get("imdb_id"):
                it["imdb_id"] = enrich["imdb_id"].lower()

    # Sort by popularity; truncate
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