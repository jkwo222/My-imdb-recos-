# engine/catalog.py
import os
from typing import Callable, Dict, List, Tuple
from .config import Config
from .catalog_store import load_store, save_store, merge_discover_batch, all_items

# Try both historic and current symbol names (to avoid import errors).
def _resolve_callable(*names: str) -> Callable:
    import importlib
    tm = importlib.import_module("engine.tmdb")
    for n in names:
        if hasattr(tm, n):
            return getattr(tm, n)
    raise ImportError(f"engine.catalog: none of the expected functions exist in engine.tmdb: {names}")

_DISCOVER_MOVIE = _resolve_callable("discover_movie_page")
_DISCOVER_TV = _resolve_callable("discover_tv_page")

def _make_page_plan(cfg: Config) -> Dict:
    movie_pages = int(cfg.tmdb_pages_movie)
    tv_pages = int(cfg.tmdb_pages_tv)
    return {"movie_pages": movie_pages, "tv_pages": tv_pages}

def _rank(items: List[Dict], critic_weight: float, audience_weight: float) -> List[Dict]:
    # For now, use TMDB vote_average as audience-ish proxy. Hook in RT/IMDb later.
    ranked = []
    for it in items:
        va = (it.get("vote_average") or 0) * 10  # TMDB is 0..10 → 0..100
        critic = it.get("metascore") or 0       # place-holder slot for future RT/Meta
        score = critic_weight*critic + audience_weight*va
        ranked.append({**it, "match": round(float(score), 1)})
    ranked.sort(key=lambda x: (x.get("match") or 0, x.get("vote_count") or 0, x.get("popularity") or 0), reverse=True)
    return ranked

def _load_seen_ids(cfg: Config) -> Dict[str, bool]:
    # Read IMDb CSV (ids-only OK)
    seen = {}
    path = cfg.imdb_ratings_csv_path
    if os.path.exists(path):
        try:
            import csv
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    tid = (row.get("const") or row.get("tconst") or "").strip()
                    if tid:
                        seen[tid] = True
        except Exception:
            pass
    # Optional: future — scrape user list by cfg.imdb_user_id
    return seen

def _filter_unseen(items: List[Dict], seen_index: Dict[str, bool]) -> List[Dict]:
    # We don't have IMDb IDs here; conservatively keep all items (you can add matching later)
    # If you map TMDB -> IMDb elsewhere, filter using that map.
    return items

def build_pool(cfg: Config) -> Tuple[List[Dict], Dict]:
    """
    1) Load cumulative store
    2) Discover new pages from TMDB and merge
    3) Produce today's candidate pool (bounded by max_catalog)
    4) Return (pool, meta) where meta['pool_counts'] always exists
    """
    print("[hb] | catalog:begin", flush=True)

    store = load_store()
    before_movie = len(store.get("movie", {}))
    before_tv = len(store.get("tv", {}))

    plan = _make_page_plan(cfg)

    # Discover & merge
    added_movie = 0
    for p in range(1, plan["movie_pages"] + 1):
        batch = _DISCOVER_MOVIE(
            cfg.tmdb_api_key, p, cfg.region, cfg.with_original_language, cfg.provider_names
        )
        a, _ = merge_discover_batch(store, batch, "movie")
        added_movie += a

    added_tv = 0
    for p in range(1, plan["tv_pages"] + 1):
        batch = _DISCOVER_TV(
            cfg.tmdb_api_key, p, cfg.region, cfg.with_original_language, cfg.provider_names
        )
        a, _ = merge_discover_batch(store, batch, "tv")
        added_tv += a

    # Persist cumulative store (so later steps & next runs can use it)
    save_store(store)

    total_movie = len(store.get("movie", {}))
    total_tv = len(store.get("tv", {}))
    pool_items = all_items(store)[: cfg.max_catalog]

    # Filtering
    seen_index = _load_seen_ids(cfg)
    unseen = _filter_unseen(pool_items, seen_index)

    # Ranking
    ranked = _rank(unseen, cfg.critic_weight, cfg.audience_weight)

    meta = {
        "pool_counts": {"movie": total_movie, "tv": total_tv},
        "added_this_run": {"movie": added_movie, "tv": added_tv},
        "telemetry": {
            "counts": {
                "tmdb_pool": len(pool_items),
                "eligible_unseen": len(unseen),
                "shortlist": min(50, len(ranked)),
                "shown": min(10, len(ranked)),
            },
            "weights": {"critic": cfg.critic_weight, "audience": cfg.audience_weight},
            "plan": {
                "movie_pages": plan["movie_pages"],
                "tv_pages": plan["tv_pages"],
                "providers": list(cfg.provider_names),
                "region": cfg.region,
                "with_original_language": cfg.with_original_language,
            },
        },
    }

    print(f"[hb] | catalog:end pool={len(pool_items)} movie={total_movie} tv={total_tv}", flush=True)
    return ranked, meta