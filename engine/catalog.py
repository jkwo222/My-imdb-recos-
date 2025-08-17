# engine/catalog.py
from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple, Any

# Import TMDB helpers
from .tmdb import (
    discover_movie_page,
    discover_tv_page,
    providers_from_env,
)

# ---------- small utilities ----------

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None and v != "" else default
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default

def _cfg_get(cfg: Any, key: str, default: Any) -> Any:
    """
    Safe accessor that prefers cfg.<key> if present, else env var,
    else default. This avoids AttributeError when Config is missing keys.
    Converts to correct types based on 'default' type.
    """
    # 1) try attribute on cfg
    if cfg is not None and hasattr(cfg, key):
        try:
            return getattr(cfg, key)
        except Exception:
            pass

    # 2) env var (we map pythonic key -> ENV_STYLE)
    env_key = key.upper()
    if isinstance(default, bool):
        return _env_bool(env_key, default)
    if isinstance(default, int):
        return _env_int(env_key, default)
    # strings / others
    return _env_str(env_key, default)


def _store_path() -> str:
    return os.path.join("data", "catalog_store.json")


def _load_store() -> List[Dict]:
    path = _store_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        # tolerate older dict-shaped stores
        return data.get("items", [])
    except Exception:
        return []


def _save_store(items: List[Dict]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(_store_path(), "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False)


def _dedupe(items: List[Dict]) -> List[Dict]:
    """
    Deduplicate by (type, tmdb_id). Keep the first occurrence.
    """
    seen = set()
    out = []
    for it in items:
        t = it.get("type")
        i = it.get("tmdb_id")
        k = (t, i)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out


def _trim(items: List[Dict], max_catalog: int) -> List[Dict]:
    """
    Keep at most max_catalog items. Simple heuristic:
    - prefer keeping items with higher popularity first if available
    - otherwise just keep first N
    """
    def popkey(it: Dict) -> float:
        # Higher popularity first; default 0.0
        return float(it.get("popularity", 0.0))

    # sort descending by popularity, stable
    items_sorted = sorted(items, key=popkey, reverse=True)
    return items_sorted[:max_catalog]


# ---------- TMDB fetch plan ----------

def _make_page_plan(cfg: Any) -> Dict[str, int]:
    # default pages
    movie_pages = _cfg_get(cfg, "tmdb_pages_movie", 24)
    tv_pages    = _cfg_get(cfg, "tmdb_pages_tv", 24)

    # hard floor
    if movie_pages < 0: movie_pages = 0
    if tv_pages    < 0: tv_pages    = 0

    return {"movie_pages": int(movie_pages), "tv_pages": int(tv_pages)}


def _fetch_all_tmdb(cfg: Any) -> Tuple[List[Dict], Dict]:
    subs_csv = _cfg_get(
        cfg, "subs_include",
        "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",
    )
    region = _cfg_get(cfg, "watch_region", _env_str("REGION", "US"))
    langs = _cfg_get(cfg, "original_langs", _env_str("ORIGINAL_LANGS", "en"))
    include_tv_seasons = _cfg_get(cfg, "include_tv_seasons", True)

    plan = _make_page_plan(cfg)
    mpages = plan["movie_pages"]
    tpages = plan["tv_pages"]

    provider_ids = providers_from_env(subs_csv, region)

    fresh: List[Dict] = []
    # Movies
    for p in range(1, mpages + 1):
        items, _ = discover_movie_page(
            p,
            region=region,
            provider_ids=provider_ids,
            original_langs=langs,
        )
        fresh.extend(items)

    # TV
    for p in range(1, tpages + 1):
        items, _ = discover_tv_page(
            p,
            region=region,
            provider_ids=provider_ids,
            original_langs=langs,
            include_seasons=include_tv_seasons,
        )
        fresh.extend(items)

    meta = {
        "counts": {
            "tmdb_pool": len(fresh),
            "movie_pages": mpages,
            "tv_pages": tpages,
        },
        "filters": {
            "region": region,
            "original_langs": langs,
            "providers_env": subs_csv,
            "provider_ids": provider_ids,
        },
    }
    return fresh, meta


# ---------- Ranking placeholder ----------
# Keep simple; your runner computes shortlist/shown later.
def _rank(unseen: List[Dict], critic_weight: float, audience_weight: float) -> List[Dict]:
    # Very basic score: vote_average (0..10) scaled
    for it in unseen:
        va = float(it.get("vote_average", 0.0))
        pop = float(it.get("popularity", 0.0))
        # lightweight blend; you can wire your profile DNA upstream into this later
        score = (critic_weight * va * 10.0) + (audience_weight * min(pop, 100.0) * 0.1)
        it["match"] = round(score, 1)
    # sort descending by match
    return sorted(unseen, key=lambda x: x.get("match", 0.0), reverse=True)


# ---------- main entry called by runner ----------

def build_pool(cfg: Any) -> Tuple[List[Dict], Dict]:
    """
    Returns (pool, meta)

    Behavior:
    - fetch fresh TMDB results using env/cfg
    - merge with previous store (on disk) to make a cumulative catalog
    - trim to MAX_CATALOG, then return as the pool
    - write the merged store back to disk
    - meta contains 'counts.tmdb_pool' (fresh only) and 'counts.cumulative'
    """
    print("[hb] | catalog:begin", flush=True)

    max_catalog = _cfg_get(cfg, "max_catalog", _env_int("MAX_CATALOG", 10000))

    # Get fresh batch from TMDB
    fresh, meta = _fetch_all_tmdb(cfg)
    fresh = _dedupe(fresh)

    # Load previous
    prev = _load_store()

    # Merge previous + fresh (union by (type, tmdb_id))
    merged = _dedupe(prev + fresh)

    # Trim to cap
    merged = _trim(merged, max_catalog)

    # Persist cumulative store
    _save_store(merged)

    # The pool we feed downstream is the cumulative set
    pool = merged

    # Enrich meta for telemetry
    meta.setdefault("counts", {})
    meta["counts"]["cumulative"] = len(merged)
    meta["counts"]["eligible_unseen"] = len(pool)  # if you later exclude seen, adjust here

    # Optional weights for ranking later (fall back to sensible defaults if absent)
    meta["weights"] = {
        "critic_weight": float(_cfg_get(cfg, "critic_weight", 0.6)),
        "audience_weight": float(_cfg_get(cfg, "audience_weight", 0.4)),
    }

    print(f"[hb] | catalog:end pool={len(pool)} movie={meta['counts'].get('movie_pages', 0)*20} tv={meta['counts'].get('tv_pages', 0)*20}", flush=True)
    return pool, meta