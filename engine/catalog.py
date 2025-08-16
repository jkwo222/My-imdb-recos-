# engine/catalog.py
from __future__ import annotations

from typing import Dict, Any, Iterable, List, Tuple
from .catalog_store import load_store, save_store, merge_discover_batch, all_items

def build_pool(
    movie_pages_used: Iterable[int],
    tv_pages_used: Iterable[int],
    provider_names: Iterable[str],
    language: str,
    with_original_language: str,
    watch_region: str,
    tmdb_discover_movie: callable,
    tmdb_discover_tv: callable,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build the in-memory candidate pool by:
      1) Restoring the on-disk store
      2) Discovering fresh pages from TMDB
      3) Merging them into the store
      4) Returning the *current run* pool (not the whole store) + telemetry

    `tmdb_discover_movie` / `tmdb_discover_tv` are callables you already have that accept:
       (page, provider_names, language, with_original_language, watch_region) -> List[items]
    Each item should include: type/movie|tv, tmdb_id, imdb_id (if known), title, year, providers, etc.
    """
    store = load_store()

    # 1) Discover movies
    movie_batch: List[Dict[str, Any]] = []
    for pg in movie_pages_used:
        movie_batch.extend(
            tmdb_discover_movie(
                page=int(pg),
                provider_names=provider_names,
                language=language,
                with_original_language=with_original_language,
                watch_region=watch_region,
            )
        )

    # 2) Discover tv
    tv_batch: List[Dict[str, Any]] = []
    for pg in tv_pages_used:
        tv_batch.extend(
            tmdb_discover_tv(
                page=int(pg),
                provider_names=provider_names,
                language=language,
                with_original_language=with_original_language,
                watch_region=watch_region,
            )
        )

    # 3) Merge both batches into the catalog store
    added_m = merge_discover_batch(store, movie_batch)
    added_t = merge_discover_batch(store, tv_batch)
    # added_m/added_t are dicts like {"movie": <n>, "tv": 0} / {"movie": 0, "tv": <n>}

    # Persist the updated store
    save_store(store)

    # 4) Build the *current run* pool list (not the entire store: only fresh batches)
    #    Your scoring/filters should operate on the current pool (movie_batch + tv_batch).
    current_pool = movie_batch + tv_batch

    telemetry = {
        "pool_counts": {
            "movie": len([x for x in current_pool if (x.get("type") or "").lower().startswith("mov")]),
            "tv": len([x for x in current_pool if (x.get("type") or "").lower().startswith("tv")]),
        },
        "store_counts": {
            "movie": len(list(all_items(store, "movie"))),
            "tv": len(list(all_items(store, "tv"))),
        },
        "added_this_run": {
            "movie": added_m.get("movie", 0) + added_t.get("movie", 0),
            "tv": added_m.get("tv", 0) + added_t.get("tv", 0),
        },
    }

    return current_pool, telemetry