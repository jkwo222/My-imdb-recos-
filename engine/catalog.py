# FILE: engine/catalog.py
from __future__ import annotations

import hashlib
import json
import os
import random
import time
from typing import Any, Dict, List, Tuple

from .config import Config
from .tmdb import TMDB, normalize_provider_names


COVERAGE_PATH = "data/coverage.json"


def _load_cov() -> dict:
    if os.path.exists(COVERAGE_PATH):
        try:
            with open(COVERAGE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"movie": {}, "tv": {}, "_meta": {"updated": 0}}


def _save_cov(d: dict) -> None:
    os.makedirs("data", exist_ok=True)
    tmp = COVERAGE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(tmp, COVERAGE_PATH)


def _slot_number(rotate_minutes: int) -> int:
    return int(time.time() // (rotate_minutes * 60))


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
        "type": item_type,  # <<< added so filtering sees the right kind
        "tmdb_id": int(raw.get("id")),
        "title": title,
        "year": int(year) if (isinstance(year, str) and year.isdigit()) else year,
        "popularity": float(raw.get("popularity") or 0.0),
        "vote_average": float(raw.get("vote_average") or 0.0),
        "original_language": raw.get("original_language"),
    }


def _want_pages(cfg: Config) -> Tuple[int, int]:
    return (max(1, cfg.tmdb_pages_movie), max(1, cfg.tmdb_pages_tv))


def _choose_pages_coverage(kind: str, total_pages: int, want_pages: int, seed: int, cov: dict) -> List[int]:
    """
    Coverage-aware selection:
      - Use per-kind visit counts to bias toward least-visited pages.
      - Break ties deterministically with seeded RNG.
    """
    total_pages = max(1, int(total_pages))
    want_pages = max(1, min(int(want_pages), total_pages))

    if total_pages == 1:
        return [1]

    visits_map: Dict[str, int] = cov.get(kind, {})
    # Build list [ (page, visits) ]
    stats = [(p, int(visits_map.get(str(p), 0))) for p in range(1, total_pages + 1)]

    rng = random.Random(seed)
    # Sort by (visits ASC, random tiebreaker) and take the first want_pages
    stats.sort(key=lambda t: (t[1], rng.random()))
    pages = [p for (p, _) in stats[:want_pages]]
    return pages


def build_pool(cfg: Config) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build the largest feasible pool pre-scoring:
      - Discover many pages for movie+tv with coverage bias
      - Keep titles available on your providers (flatrate/ads/free)
      - Sort by popularity
      - Truncate once at the end by MAX_CATALOG
    """
    tmdb = TMDB(cfg.tmdb_api_key, cache=_cache(cfg))

    rotate_minutes = 15
    slot = _slot_number(rotate_minutes)

    # Determine total pages once for each kind (bounded by 500 by TMDB)
    total_pages_movie = tmdb.total_pages("movie", cfg.language, cfg.with_original_langs, cfg.region)
    total_pages_tv = tmdb.total_pages("tv", cfg.language, cfg.with_original_langs, cfg.region)

    want_movie, want_tv = _want_pages(cfg)
    seed_m = _seed_for("movie", cfg, slot)
    seed_t = _seed_for("tv", cfg, slot)

    cov = _load_cov()
    movie_pages_used = _choose_pages_coverage("movie", total_pages_movie, want_movie, seed_m, cov)
    tv_pages_used = _choose_pages_coverage("tv", total_pages_tv, want_tv, seed_t, cov)

    def collect(kind: str, pages: List[int]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for p in pages:
            data = tmdb.discover(kind, p, cfg.language, cfg.with_original_langs, cfg.region)
            for raw in data.get("results", []) or []:
                out.append(_coerce_item(kind, raw))
        return out

    movie_raw = collect("movie", movie_pages_used)
    tv_raw = collect("tv", tv_pages_used)

    def enrich_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        keep: List[Dict[str, Any]] = []
        for it in items:
            names = tmdb.providers_for_title(it["kind"], it["tmdb_id"], cfg.region)
            slugs = normalize_provider_names(names)
            it["providers"] = slugs
            if any(s in cfg.subs_include for s in slugs):
                keep.append(it)
        return keep

    movie_items = enrich_and_filter(movie_raw)
    tv_items = enrich_and_filter(tv_raw)

    # Merge & sort
    pool = movie_items + tv_items
    pool.sort(key=lambda x: x.get("popularity", 0.0), reverse=True)
    if cfg.max_catalog > 0:
        pool = pool[: cfg.max_catalog]

    # Update coverage counts for pages we just used (after successful collection)
    for p in movie_pages_used:
        cov.setdefault("movie", {})[str(p)] = int(cov.get("movie", {}).get(str(p), 0)) + 1
    for p in tv_pages_used:
        cov.setdefault("tv", {})[str(p)] = int(cov.get("tv", {}).get(str(p), 0)) + 1
    cov["_meta"]["updated"] = int(time.time())
    _save_cov(cov)

    meta: Dict[str, Any] = {
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