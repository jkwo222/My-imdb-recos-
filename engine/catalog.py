# engine/catalog.py
from __future__ import annotations

import os
import time
import math
import hashlib
import random
from typing import Dict, Any, Iterable, List, Tuple, Callable

from .catalog_store import load_store, save_store, merge_discover_batch, all_items

# Import the tmdb module and resolve callable names dynamically so we work
# whether you export discover_movie_page/discover_tv_page or
# tmdb_discover_movie/tmdb_discover_tv.
try:
    from . import tmdb as _tmdb_mod  # type: ignore
except Exception as e:
    raise ImportError("engine.catalog: unable to import engine.tmdb") from e


def _resolve_callable(*names: str) -> Callable[..., List[Dict[str, Any]]]:
    for nm in names:
        fn = getattr(_tmdb_mod, nm, None)
        if callable(fn):
            return fn  # type: ignore[return-value]
    raise ImportError(
        f"engine.catalog: none of the expected functions exist in engine.tmdb: {names}"
    )


_DISCOVER_MOVIE = _resolve_callable("discover_movie_page", "tmdb_discover_movie")
_DISCOVER_TV = _resolve_callable("discover_tv_page", "tmdb_discover_tv")


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


def _deterministic_pages(total_pages: int, how_many: int, seed: int, salt: str) -> List[int]:
    """Deterministically sample `how_many` distinct pages in [1..total_pages]."""
    how_many = max(0, min(how_many, total_pages))
    rng = random.Random()
    h = hashlib.sha256(f"{seed}:{salt}".encode("utf-8")).digest()
    rng.seed(int.from_bytes(h[:8], "big", signed=False))
    population = list(range(1, total_pages + 1))
    return rng.sample(population, how_many)


def _make_page_plan(cfg: Dict[str, Any]) -> Dict[str, Any]:
    movie_pages = _int(cfg.get("TMDB_PAGES_MOVIE", os.getenv("TMDB_PAGES_MOVIE", 24)), 24)
    tv_pages = _int(cfg.get("TMDB_PAGES_TV", os.getenv("TMDB_PAGES_TV", 24)), 24)
    rotate_minutes = _int(cfg.get("ROTATE_MINUTES", 15), 15)

    total_pages_movie = _int(cfg.get("TOTAL_PAGES_MOVIE", 500), 500)
    total_pages_tv = _int(cfg.get("TOTAL_PAGES_TV", 500), 500)

    now = int(time.time())
    slot = math.floor(now / (rotate_minutes * 60))

    movie_pages_used = _deterministic_pages(total_pages_movie, movie_pages, slot, "movie")
    tv_pages_used = _deterministic_pages(total_pages_tv, tv_pages, slot, "tv")

    provider_names = _split_csv(cfg.get("SUBS_INCLUDE", os.getenv("SUBS_INCLUDE", "")))
    language = cfg.get("LANGUAGE", os.getenv("LANGUAGE", "en-US"))
    with_original_language = cfg.get("ORIGINAL_LANGS", os.getenv("ORIGINAL_LANGS", "en"))
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
                _DISCOVER_MOVIE(
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
                _DISCOVER_TV(
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
    Dict-driven entry point (matches runner.py calling build_pool(cfg)).

    Steps:
      1) Make a deterministic page plan
      2) Discover TMDB (movie + tv) for planned pages
      3) Merge into persistent catalog store (append-only)
      4) Return (current_run_pool, meta)
    """
    # 1) Plan
    plan = _make_page_plan(cfg)

    # 2) Discover
    movie_batch = _discover_batch(
        "movie",
        plan["movie_pages_used"],
        plan["provider_names"],
        plan["language"],
        plan["with_original_language"],
        plan["watch_region"],
    )
    tv_batch = _discover_batch(
        "tv",
        plan["tv_pages_used"],
        plan["provider_names"],
        plan["language"],
        plan["with_original_language"],
        plan["watch_region"],
    )

    # 3) Merge into persistent store
    store = load_store()
    added_m = merge_discover_batch(store, movie_batch)
    added_t = merge_discover_batch(store, tv_batch)
    save_store(store)

    # 4) Current run pool + meta
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
        "page_plan": {
            "movie_pages": plan["movie_pages"],
            "tv_pages": plan["tv_pages"],
            "rotate_minutes": plan["rotate_minutes"],
            "slot": plan["slot"],
            "movie_pages_used": plan["movie_pages_used"],
            "tv_pages_used": plan["tv_pages_used"],
            "provider_names": plan["provider_names"],
            "language": plan["language"],
            "with_original_language": plan["with_original_language"],
            "watch_region": plan["watch_region"],
        },
    }

    return current_pool, meta