import json
import math
import os
import random
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .config import Config
from .http import DiskCache, TMDB

JSON = Dict[str, Any]

_STATE_PATH = "data/cache/discover_state.json"

def _load_state() -> JSON:
    p = Path(_STATE_PATH)
    if not p.exists():
        return {"movie": {"cursor": 1, "total_pages": 1},
                "tv":    {"cursor": 1, "total_pages": 1}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"movie": {"cursor": 1, "total_pages": 1},
                "tv":    {"cursor": 1, "total_pages": 1}}

def _save_state(st: JSON) -> None:
    p = Path(_STATE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(st, indent=2), encoding="utf-8")

def _provider_ids_for_names(tmdb: TMDB, names: List[str]) -> List[int]:
    """
    Map human-friendly names to TMDB provider IDs.
    Names should be provided in snake/lower or plain words.
    We normalize both sides to loose-match (lowercase, spaces->spaces).
    """
    name_variants = {
        "netflix": ["netflix"],
        "prime_video": ["amazon prime video", "prime video"],
        "hulu": ["hulu"],
        "max": ["max", "hbo max"],
        "disney_plus": ["disney plus", "disney+"],
        "apple_tv_plus": ["apple tv+", "apple tv plus"],
        "peacock": ["peacock"],
        "paramount_plus": ["paramount plus", "paramount+"],
    }

    prov_map = tmdb.providers_map(country=tmdb.region)
    out: List[int] = []

    for key in names:
        needle = key.strip().lower()
        candidates = name_variants.get(needle, [needle])
        found = None
        for cand in candidates:
            cand2 = cand.replace("_", " ").strip()
            if cand2 in prov_map:
                found = prov_map[cand2]
                break
        if found is not None:
            out.append(int(found))
    # Dedup
    return sorted(set(out))

def _unique_items(items: List[JSON]) -> List[JSON]:
    seen = set()
    out = []
    for it in items:
        k = (it.get("media_type"), int(it.get("id")))
        if k not in seen:
            seen.add(k)
            out.append(it)
    return out

def _results_to_pool(kind: str, page_blob: JSON) -> List[JSON]:
    pool: List[JSON] = []
    for r in page_blob.get("results", []):
        tmdb_id = int(r.get("id"))
        title = (r.get("title") or r.get("name") or "").strip()
        release = (r.get("release_date") or r.get("first_air_date") or "")[:4]
        year = int(release) if release.isdigit() else None
        pool.append({
            "media_type": kind,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "popularity": r.get("popularity"),
            "vote_average": r.get("vote_average"),
            "vote_count": r.get("vote_count"),
        })
    return pool

def _next_chunk(start: int, count: int, total_pages: int) -> List[int]:
    if total_pages <= 0:
        return []
    pages = []
    cur = start
    for _ in range(max(0, count)):
        pages.append(cur)
        cur += 1
        if cur > total_pages:
            cur = 1
    return sorted(set(pages))

def _sample_pages(seed: int, count: int, total_pages: int) -> List[int]:
    if total_pages <= 1:
        return [1]
    n = min(count, total_pages)
    rng = random.Random(seed)
    return sorted(rng.sample(range(1, total_pages + 1), n))

def build_pool(cfg: Config, slot: int) -> Tuple[List[JSON], JSON]:
    # Init cache + client
    cache = DiskCache(cfg.cache_dir) if cfg.enable_discover_cache else None
    tmdb = TMDB(cfg.tmdb_api_key, cfg.region, cfg.language, cache)

    # Provider IDs from runtime lookup (cached)
    pids = _provider_ids_for_names(tmdb, cfg.subs_include)
    with_provider_ids = ",".join(str(x) for x in pids) if pids else ""

    state = _load_state()

    # Fetch total pages (cached briefly) to guard randrange issues & know bounds
    total_movie = tmdb.total_pages(
        "movie", with_provider_ids, cfg.with_original_language,
        slot, cfg.discover_cache_ttl_min, cfg.enable_discover_cache
    )
    total_tv = tmdb.total_pages(
        "tv", with_provider_ids, cfg.with_original_language,
        slot, cfg.discover_cache_ttl_min, cfg.enable_discover_cache
    )

    state["movie"]["total_pages"] = int(total_movie or 1)
    state["tv"]["total_pages"] = int(total_tv or 1)
    movie_cursor = int(state["movie"].get("cursor", 1))
    tv_cursor = int(state["tv"].get("cursor", 1))

    # Build page lists
    # 1) rotating sample that changes with slot
    movie_sample = _sample_pages(seed=hash(("movie", slot)), count=cfg.sample_pages_movie, total_pages=total_movie)
    tv_sample = _sample_pages(seed=hash(("tv", slot)), count=cfg.sample_pages_tv, total_pages=total_tv)

    # 2) sequential fill to grow local cache toward full coverage
    movie_fill = _next_chunk(start=movie_cursor, count=cfg.fill_pages_movie, total_pages=total_movie)
    tv_fill = _next_chunk(start=tv_cursor, count=cfg.fill_pages_tv, total_pages=total_tv)

    movie_pages = sorted(set(movie_sample + movie_fill))
    tv_pages = sorted(set(tv_sample + tv_fill))

    # Advance cursors for next run
    if total_movie > 0 and cfg.fill_pages_movie > 0:
        new_movie_cursor = movie_fill[-1] + 1 if movie_fill else movie_cursor
        if new_movie_cursor > total_movie:
            new_movie_cursor = 1
        state["movie"]["cursor"] = new_movie_cursor

    if total_tv > 0 and cfg.fill_pages_tv > 0:
        new_tv_cursor = tv_fill[-1] + 1 if tv_fill else tv_cursor
        if new_tv_cursor > total_tv:
            new_tv_cursor = 1
        state["tv"]["cursor"] = new_tv_cursor

    _save_state(state)

    # Collect pages
    pool_movie: List[JSON] = []
    for pg in movie_pages:
        blob = tmdb.discover("movie", pg, with_provider_ids, cfg.with_original_language,
                             slot, cfg.discover_cache_ttl_min, cfg.enable_discover_cache)
        pool_movie += _results_to_pool("movie", blob)

    pool_tv: List[JSON] = []
    for pg in tv_pages:
        blob = tmdb.discover("tv", pg, with_provider_ids, cfg.with_original_language,
                             slot, cfg.discover_cache_ttl_min, cfg.enable_discover_cache)
        pool_tv += _results_to_pool("tv", blob)

    # Dedup and cap
    pool = _unique_items(pool_movie + pool_tv)
    if cfg.max_catalog > 0 and len(pool) > cfg.max_catalog:
        pool = pool[:cfg.max_catalog]

    meta = {
        "movie_pages_used": movie_pages,
        "tv_pages_used": tv_pages,
        "total_pages_movie": total_movie,
        "total_pages_tv": total_tv,
        "provider_names": cfg.subs_include,
        "language": cfg.language,
        "with_original_language": cfg.with_original_language,
        "watch_region": cfg.region,
        "pool_counts": {
            "movie": len(pool_movie),
            "tv": len(pool_tv),
        }
    }

    return pool, meta