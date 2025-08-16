from __future__ import annotations
import math
import random
from typing import Dict, Any, List, Tuple
from .config import Config
from .cache import JsonCache
from .tmdb import TMDB, MAX_DISCOVER_PAGES
from .providers import any_allowed

def _first_date(parts: Dict[str, Any]) -> int | None:
    for k in ("release_date", "first_air_date"):
        v = parts.get(k)
        if v and isinstance(v, str) and len(v) >= 4 and v[:4].isdigit():
            return int(v[:4])
    return None

def _choose_pages(total_pages: int, want_pages: int, seed: int) -> List[int]:
    # Clamp total_pages to TMDB max for discover (defense in depth)
    total_pages = max(1, min(int(total_pages or 1), MAX_DISCOVER_PAGES))
    want_pages = max(1, int(want_pages or 1))
    want = min(want_pages, total_pages)

    rng = random.Random(seed)

    # choose a coprime step so we traverse widely
    step = None
    for _ in range(64):
        candidate = rng.randrange(1, total_pages)  # 1..total-1
        if math.gcd(candidate, total_pages) == 1:
            step = candidate
            break
    if step is None:
        step = 1

    start = rng.randrange(1, total_pages + 1)
    pages, seen = [], set()
    x = start
    while len(pages) < want:
        if x not in seen:
            pages.append(x)
            seen.add(x)
        x = ((x - 1 + step) % total_pages) + 1
    return pages

def _basic_item_from_result(kind: str, r: Dict[str, Any]) -> Dict[str, Any]:
    title = r.get("title") or r.get("name") or "Unknown"
    year = _first_date(r) or 0
    tmdb_id = int(r.get("id"))
    popularity = float(r.get("popularity", 0.0) or 0.0)
    runtime = int(r.get("runtime") or 0)  # discover usually lacks runtime; left for future enrichers
    return {
        "title": title,
        "year": year,
        "type": "tvSeries" if kind == "tv" else "movie",
        "tmdb_id": tmdb_id,
        "popularity": popularity,
        "runtime": runtime,
        "imdb_id": None,
        "_providers": [],
    }

def _enrich_ids_and_providers(api: TMDB, kind: str, item: Dict[str, Any], region: str) -> Dict[str, Any]:
    tmdb_id = int(item["tmdb_id"])
    try:
        ids = api.external_ids_for(kind, tmdb_id)
        imdb_id = ids.get("imdb_id")
        if imdb_id:
            item["imdb_id"] = imdb_id
    except Exception:
        pass
    try:
        provs = api.watch_providers_for(kind, tmdb_id, region=region)
        item["_providers"] = provs
    except Exception:
        item["_providers"] = []
    return item

def build_pool(cfg: Config) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache = JsonCache(cfg.cache_dir)
    tmdb = TMDB(cfg.tmdb_api_key, cache)

    slot = cfg.rotation_slot()

    total_pages_movie = tmdb.total_pages("movie", cfg.language, cfg.with_original_langs, cfg.region)
    total_pages_tv    = tmdb.total_pages("tv",    cfg.language, cfg.with_original_langs, cfg.region)

    movie_pages = _choose_pages(total_pages_movie, cfg.tmdb_pages_movie, seed=slot * 1009 + 7)
    tv_pages    = _choose_pages(total_pages_tv,    cfg.tmdb_pages_tv,    seed=slot * 1013 + 11)

    pool: List[Dict[str, Any]] = []
    per_kind_cap = max(1, cfg.max_catalog // 2)

    # Movies
    added_movies = 0
    for p in movie_pages:
        if added_movies >= per_kind_cap:
            break
        data = tmdb.discover("movie", p, cfg.language, cfg.with_original_langs, cfg.region)
        for r in data.get("results", []):
            item = _basic_item_from_result("movie", r)
            item = _enrich_ids_and_providers(tmdb, "movie", item, cfg.region)
            if any_allowed(item.get("_providers"), cfg.subs_include):
                pool.append(item)
                added_movies += 1
                if added_movies >= per_kind_cap:
                    break

    # TV
    added_tv = 0
    for p in tv_pages:
        if added_tv >= per_kind_cap:
            break
        data = tmdb.discover("tv", p, cfg.language, cfg.with_original_langs, cfg.region)
        for r in data.get("results", []):
            item = _basic_item_from_result("tv", r)
            item = _enrich_ids_and_providers(tmdb, "tv", item, cfg.region)
            if any_allowed(item.get("_providers"), cfg.subs_include):
                pool.append(item)
                added_tv += 1
                if added_tv >= per_kind_cap:
                    break

    meta = {
        "movie_pages": cfg.tmdb_pages_movie,
        "tv_pages": cfg.tmdb_pages_tv,
        "rotate_minutes": cfg.rotate_minutes,
        "slot": slot,
        "total_pages_movie": total_pages_movie,  # already capped
        "total_pages_tv": total_pages_tv,        # already capped
        "movie_pages_used": movie_pages,
        "tv_pages_used": tv_pages,
        "provider_names": cfg.subs_include,
        "language": cfg.language,
        "with_original_language": ",".join(cfg.with_original_langs),
        "watch_region": cfg.region,
        "pool_counts": {
            "movie": sum(1 for x in pool if x["type"] == "movie"),
            "tv": sum(1 for x in pool if x["type"] == "tvSeries"),
        },
        "total_pages": [total_pages_movie, total_pages_tv],
    }

    return pool, meta