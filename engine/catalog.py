# engine/catalog.py
from __future__ import annotations

import os
import time
import math
import hashlib
import random
from typing import Dict, Any, Iterable, List, Tuple

from .catalog_store import load_store, save_store, merge_discover_batch, all_items

# We assume you already have these helpers in engine/tmdb.py
# and that they accept the kwargs below.
try:
    from .tmdb import discover_movie_page, discover_tv_page
except Exception as e:
    # Provide a clear error if the tmdb helpers can't be imported.
    raise ImportError(
        "engine.catalog: unable to import discover_movie_page/discover_tv_page "
        "from engine.tmdb. Ensure engine/tmdb.py exports those symbols."
    ) from e


def _int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _split_csv(s: str) -> List[str]:
    out: List[str] = []
    if not s:
        return out
    for p in s.split(","):
        p = p.strip()
        if p:
            out.append(p)
    return out


def _deterministic_pages(
    total_pages: int, how_many: int, seed: int, salt: str
) -> List[int]:
    """
    Deterministically sample `how_many` distinct pages in [1..total_pages]
    using a stable seed so the same 15-minute slot yields the same pages.
    """
    how_many = max(0, min(how_many, total_pages))
    rng = random.Random()
    # Mix seed + salt for separation of movie/tv streams
    h = hashlib.sha256(f"{seed}:{salt}".encode("utf-8")).digest()
    # Convert first 8 bytes to an int seed
    mixed_seed = int.from_bytes(h[:8], "big", signed=False)
    rng.seed(mixed_seed)
    population = list(range(1, total_pages + 1))
    # sample without replacement
    return rng.sample(population, how_many)


def _make_page_plan(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Inputs (with sensible defaults if absent)
    movie_pages = _int(cfg.get("TMDB_PAGES_MOVIE", os.getenv("TMDB_PAGES_MOVIE", 24)), 24)
    tv_pages = _int(cfg.get("TMDB_PAGES_TV", os.getenv("TMDB_PAGES_TV", 24)), 24)
    rotate_minutes = _int(cfg.get("ROTATE_MINUTES", 15), 15)

    # TMDB "discover" endpoints commonly expose 500 pages max
    total_pages_movie = _int(cfg.get("TOTAL_PAGES_MOVIE", 500), 500)
    total_pages_tv = _int(cfg.get("TOTAL_PAGES_TV", 500), 500)

    # Slot changes every rotate_minutes to rotate what we crawl
    now = int(time.time())
    slot = math.floor(now / (rotate_minutes * 60))

    movie_pages_used = _deterministic_pages(total_pages_movie, movie_pages, slot, "movie")
    tv_pages_used = _deterministic_pages(total_pages_tv, tv_pages, slot, "tv")

    # Providers / language / region
    provider_names = _split_csv(
        cfg.get("SUBS_INCLUDE", os.getenv("SUBS_INCLUDE", ""))
    )
    language = cfg.get("LANGUAGE", os.getenv("LANGUAGE", "en-US"))
    with_original_language = cfg.get(
        "ORIGINAL_LANGS", os.getenv("ORIGINAL_LANGS", "en")
    )
    watch_region = cfg.get("REGION", os.getenv("REGION", "US"))

    return {
        "movie_pages": movie_pages,
        "tv_pages": tv_pages,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
        "total_pages_movie": total_pages_movie,
        "total_pages_tv": total_pages_tv,
        "movie_pages_used": movie_pages_used,
        "tv_pages_used": tv_pages_used,
        "provider_names": provider_names,
        "language": language,
        "with_original_language": with_original_language,
        "watch_region": watch_region,
        "total_pages": [total_pages_movie, total_pages_tv],
    }


def _discover_batch(
    kind: str,
    pages: Iterable[int],
    provider_names: List[str],
    language: str,
    with_original_language: str,
    watch_region: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if kind == "movie":
        for pg in pages:
            out.extend(
                discover_movie_page(
                    page=int(pg),
                    provider_names=provider_names,
                    language=language,
                    with_original_language=with_original_language,
                    watch_region=watch_region,
                )
            )
    else:
        for pg in pages:
            out.extend(
                discover_tv_page(
                    page=int(pg),
                    provider_names=provider_names,
                    language=language,
                    with_original_language=with_original_language,
                    watch_region=watch_region,
                )
            )
    return out


def build_pool(cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Entry point expected by runner.py — accepts a single config dict.

    Responsibilities:
      1) Compute a page plan (deterministic per 15-min slot)
      2) Discover TMDB pages (movie + tv) for this run
      3) Merge results into the persistent catalog store (append-only)
      4) Return the *current-run* pool plus meta/telemetry (incl. page_plan & store counts)
    """
    # 1) Page planning
    page_plan = _make_page_plan(cfg)

    # 2) Discover
    movie_batch = _discover_batch(
        "movie",
        page_plan["movie_pages_used"],
        page_plan["provider_names"],
        page_plan["language"],
        page_plan["with_original_language"],
        page_plan["watch_region"],
    )
    tv_batch = _discover_batch(
        "tv",
        page_plan["tv_pages_used"],
        page_plan["provider_names"],
        page_plan["language"],
        page_plan["with_original_language"],
        page_plan["watch_region"],
    )

    # 3) Merge into persistent store
    store = load_store()
    added_m = merge_discover_batch(store, movie_batch)
    added_t = merge_discover_batch(store, tv_batch)
    save_store(store)

    # 4) Build current-run pool and meta
    current_pool: List[Dict[str, Any]] = movie_batch + tv_batch

    meta: Dict[str, Any] = {
        "pool_counts": {
            "movie": sum(1 for x in current_pool if (x.get("type") or "").lower().startswith("mov")),
            "tv": sum(1 for x in current_pool if (x.get("type") or "").lower().startswith("tv")),
        },
        "store_counts": {
            "movie": sum(1 for _ in all_items(store, "movie")),
            "tv": sum(1 for _ in all_items(store, "tv")),
        },
        "added_this_run": {
            "movie": added_m.get("movie", 0) + added_t.get("movie", 0),
            "tv": added_m.get("tv", 0) + added_t.get("tv", 0),
        },
        "page_plan": page_plan,
    }

    return current_pool, meta