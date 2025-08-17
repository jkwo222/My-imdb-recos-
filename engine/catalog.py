# engine/catalog.py
from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple, Any

from .tmdb import (
    discover_movie_page,
    discover_tv_page,
    providers_from_env,
)

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
    # 1) try attribute on cfg
    if cfg is not None and hasattr(cfg, key):
        try:
            return getattr(cfg, key)
        except Exception:
            pass
    # 2) env fallback (key uppercased)
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
        # sanity defaults
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

# TMDB caps discover at 500 pages
_TMDB_MAX_PAGES = 500

def _page_window(start: int, count: int) -> List[int]:
    """
    Return a list of 'count' pages starting at 'start', wrapping after 500.
    Pages are 1-based.
    """
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
    # subscriptions / filters
    subs_csv = _cfg_get(
        cfg, "subs_include",
        _env_str("SUBS_INCLUDE", "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"),
    )
    region = _cfg_get(cfg, "watch_region", _env_str("REGION", "US"))
    langs  = _cfg_get(cfg, "original_langs", _env_str("ORIGINAL_LANGS", "en"))

    # pages per run
    movie_pages_per_run = int(_cfg_get(cfg, "tmdb_pages_movie", _env_int("TMDB_PAGES_MOVIE", 24)))
    tv_pages_per_run    = int(_cfg_get(cfg, "tmdb_pages_tv",    _env_int("TMDB_PAGES_TV", 24)))

    # paging cursor (advances each run)
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

    # advance and persist cursor for next run
    next_movie = movie_pages[-1] + 1 if movie_pages else movie_start
    next_tv    = tv_pages[-1] + 1 if tv_pages else tv_start
    if next_movie > _TMDB_MAX_PAGES: next_movie = 1
    if next_tv    > _TMDB_MAX_PAGES: next_tv    = 1
    _save_cursor({"movie_next": next_movie, "tv_next": next_tv})

    meta = {
        "counts": {
            "tmdb_pool": len(fresh),      # fresh batch this run
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
        va  = float(it.get("vote_average", 0.0))  # 0..10
        pop = float(it.get("popularity", 0.0))
        score = (critic_weight * va * 10.0) + (audience_weight * min(pop, 100.0) * 0.1)
        it["match"] = round(score, 1)
    return sorted(unseen, key=lambda x: x.get("match", 0.0), reverse=True)

# ---------- entrypoint ----------

def build_pool(cfg: Any) -> Tuple[List[Dict], Dict]:
    """
    - Fetch next window of TMDB pages (cursorized) for both movies & TV
    - Merge with previous cumulative store
    - Trim to MAX_CATALOG
    - Save store and return the cumulative pool
    """
    print("[hb] | catalog:begin", flush=True)

    max_catalog = int(_cfg_get(cfg, "max_catalog", _env_int("MAX_CATALOG", 10000)))

    fresh, meta = _fetch_all_tmdb(cfg)
    fresh = _dedupe(fresh)

    prev = _load_store()
    merged = _dedupe(prev + fresh)
    merged = _trim(merged, max_catalog)
    _save_store(merged)

    pool = merged

    # telemetry
    meta.setdefault("counts", {})
    meta["counts"]["cumulative"] = len(merged)
    # keep this equal to pool unless/ until you exclude already-seen here
    meta["counts"]["eligible_unseen"] = len(pool)

    # weights (defaults if not provided)
    meta["weights"] = {
        "critic_weight": float(_cfg_get(cfg, "critic_weight", 0.6)),
        "audience_weight": float(_cfg_get(cfg, "audience_weight", 0.4)),
    }

    # basic movie/tv counts for the pretty log line (approximation: pages*20)
    movie_pages_fetched = int(meta["counts"].get("movie_pages_fetched", 0))
    tv_pages_fetched    = int(meta["counts"].get("tv_pages_fetched", 0))
    print(f"[hb] | catalog:end pool={len(pool)} movie={movie_pages_fetched*20} tv={tv_pages_fetched*20}", flush=True)
    return pool, meta