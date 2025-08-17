from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple, Any

from .tmdb import (
    discover_movie_page,
    discover_tv_page,
    providers_from_env,
)
from .exclusions import build_exclusion_index, filter_excluded

# ========== helpers ==========

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default

def _cfg_get(cfg: Any, key: str, default: Any) -> Any:
    if cfg is not None and hasattr(cfg, key):
        try:
            return getattr(cfg, key)
        except Exception:
            pass
    env_key = key.upper()
    if isinstance(default, bool):
        return _env_bool(env_key, default)
    if isinstance(default, int):
        return _env_int(env_key, default)
    return _env_str(env_key, default)

# ---------- persistent files ----------

def _store_path() -> str:
    return os.path.join("data", "catalog_store.json")

def _cursor_path() -> str:
    return os.path.join("data", "catalog_cursor.json")

def _load_store() -> List[Dict]:
    p = _store_path()
    if not os.path.exists(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else data.get("items", [])
    except Exception:
        return []

def _save_store(items: List[Dict]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(_store_path(), "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False)

def _load_cursor() -> Dict[str, int]:
    p = _cursor_path()
    if not os.path.exists(p):
        return {"movie_next": 1, "tv_next": 1}
    try:
        with open(p, "r", encoding="utf-8") as f:
            c = json.load(f)
        if not isinstance(c, dict):
            return {"movie_next": 1, "tv_next": 1}
        c.setdefault("movie_next", 1)
        c.setdefault("tv_next", 1)
        return c
    except Exception:
        return {"movie_next": 1, "tv_next": 1}

def _save_cursor(cursor: Dict[str, int]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(_cursor_path(), "w", encoding="utf-8") as f:
        json.dump(cursor, f, ensure_ascii=False)

# ---------- list ops ----------

def _dedupe(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for it in items:
        k = (it.get("type"), it.get("tmdb_id"))
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

def _trim(items: List[Dict], max_catalog: int) -> List[Dict]:
    def popkey(it: Dict) -> float:
        return float(it.get("popularity", 0.0))
    return sorted(items, key=popkey, reverse=True)[:max_catalog]

# ---------- paging plan ----------

_TMDB_MAX_PAGES = 500

def _page_window(start: int, count: int) -> List[int]:
    if count <= 0:
        return []
    pages = []
    p = max(1, min(start, _TMDB_MAX_PAGES))
    for _ in range(count):
        pages.append(p)
        p += 1
        if p > _TMDB_MAX_PAGES:
            p = 1
    return pages

# ---------- fetch ----------

def _fetch_all_tmdb(cfg: Any) -> Tuple[List[Dict], Dict]:
    subs_csv = _cfg_get(
        cfg, "subs_include",
        _env_str("SUBS_INCLUDE", "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"),
    )
    region = _cfg_get(cfg, "watch_region", _env_str("REGION", "US"))
    langs  = _cfg_get(cfg, "original_langs", _env_str("ORIGINAL_LANGS", "en"))

    movie_pages_per_run = int(_cfg_get(cfg, "tmdb_pages_movie", _env_int("TMDB_PAGES_MOVIE", 24)))
    tv_pages_per_run    = int(_cfg_get(cfg, "tmdb_pages_tv",    _env_int("TMDB_PAGES_TV", 24)))

    cursor = _load_cursor()
    movie_start = int(cursor.get("movie_next", 1))
    tv_start    = int(cursor.get("tv_next", 1))

    movie_pages = _page_window(movie_start, movie_pages_per_run)
    tv_pages    = _page_window(tv_start, tv_pages_per_run)

    provider_ids = providers_from_env(subs_csv, region)

    fresh: List[Dict] = []

    # Movies
    for p in movie_pages:
        items, _ = discover_movie_page(
            p, region=region, provider_ids=provider_ids, original_langs=langs
        )
        fresh.extend(items)

    # TV
    for p in tv_pages:
        items, _ = discover_tv_page(
            p, region=region, provider_ids=provider_ids, original_langs=langs
        )
        fresh.extend(items)

    next_movie = movie_pages[-1] + 1 if movie_pages else movie_start
    next_tv    = tv_pages[-1] + 1 if tv_pages else tv_start
    if next_movie > _TMDB_MAX_PAGES: next_movie = 1
    if next_tv    > _TMDB_MAX_PAGES: next_tv    = 1
    _save_cursor({"movie_next": next_movie, "tv_next": next_tv})

    meta = {
        "counts": {
            "tmdb_pool": len(fresh),
            "movie_pages_fetched": len(movie_pages),
            "tv_pages_fetched": len(tv_pages),
            "movie_start_page": movie_start,
            "tv_start_page": tv_start,
        },
        "filters": {
            "region": region,
            "original_langs": langs,
            "providers_env": subs_csv,
            "provider_ids": provider_ids,
        },
        "cursor_after": {"movie_next": next_movie, "tv_next": next_tv},
    }
    return fresh, meta

# ---------- simple ranking placeholder ----------

def _rank(unseen: List[Dict], critic_weight: float, audience_weight: float) -> List[Dict]:
    for it in unseen:
        va  = float(it.get("vote_average", 0.0))
        pop = float(it.get("popularity", 0.0))
        score = (critic_weight * va * 10.0) + (audience_weight * min(pop, 100.0) * 0.1)
        it["match"] = round(score, 1)
    return sorted(unseen, key=lambda x: x.get("match", 0.0), reverse=True)

# ---------- entrypoint ----------

def build_pool(cfg: Any) -> Tuple[List[Dict], Dict]:
    """
    - Fetch next window of TMDB pages (cursorized) for both movies & TV
    - Merge with previous cumulative store
    - **Exclude** any items present in the user's CSV list (multi-check)
    - Trim to MAX_CATALOG
    - Save store and return the cumulative pool
    """
    print("[hb] | catalog:begin", flush=True)

    max_catalog = int(_cfg_get(cfg, "max_catalog", _env_int("MAX_CATALOG", 10000)))
    csv_path = _cfg_get(cfg, "ratings_csv", _env_str("RATINGS_CSV", os.path.join("data", "ratings.csv")))

    # Build exclusion index once
    excl_idx = build_exclusion_index(csv_path)

    fresh, meta = _fetch_all_tmdb(cfg)
    fresh = _dedupe(fresh)

    # Exclude from the fresh batch right away (prevents re-adding to store)
    fresh_kept, fresh_excluded = filter_excluded(fresh, excl_idx)

    prev = _load_store()
    # Also sanitize previous store in case older runs added something before this logic existed
    prev_kept, prev_excluded = filter_excluded(prev, excl_idx)

    merged = _dedupe(prev_kept + fresh_kept)
    merged = _trim(merged, max_catalog)
    _save_store(merged)

    pool = merged

    # telemetry (accurate counts)
    meta.setdefault("counts", {})
    meta["counts"]["cumulative"] = len(merged)
    meta["counts"]["eligible_unseen"] = len(pool)

    fresh_movie = sum(1 for it in fresh_kept if it.get("type") == "movie")
    fresh_tv    = sum(1 for it in fresh_kept if it.get("type") == "tvSeries")
    pool_movie  = sum(1 for it in pool       if it.get("type") == "movie")
    pool_tv     = sum(1 for it in pool       if it.get("type") == "tvSeries")

    meta["counts"]["fresh_movie"] = fresh_movie
    meta["counts"]["fresh_tv"] = fresh_tv
    meta["counts"]["pool_movie"] = pool_movie
    meta["counts"]["pool_tv"] = pool_tv

    # exclusion telemetry
    meta["exclusions"] = {
        "fresh_excluded": fresh_excluded,
        "prev_excluded": prev_excluded,
        "csv_path": csv_path,
    }

    # weights (defaults if not provided)
    meta["weights"] = {
        "critic_weight": float(_cfg_get(cfg, "critic_weight", 0.6)),
        "audience_weight": float(_cfg_get(cfg, "audience_weight", 0.4)),
    }

    print(
        f"[hb] | catalog:end pool={len(pool)} movie={pool_movie} tv={pool_tv}",
        flush=True
    )
    return pool, meta