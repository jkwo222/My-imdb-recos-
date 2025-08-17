# engine/catalog_builder.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
from .env import Env
from . import tmdb
from . import pool as pool_mod

def _attach_imdb_ids(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        kind = it.get("media_type")
        tid = int(it.get("tmdb_id") or 0)
        if not tid or kind not in ("movie", "tv"):
            out.append(it)
            continue
        try:
            ids = tmdb.get_external_ids(kind, tid)
            imdb_id = ids.get("imdb_id")
            if imdb_id:
                it = dict(it)
                it["imdb_id"] = imdb_id
        except Exception:
            pass
        out.append(it)
    return out

def build_catalog(env: Env) -> List[Dict[str, Any]]:
    region = env.get("REGION", "US")
    langs = env.get("ORIGINAL_LANGS", ["en"])
    subs = env.get("SUBS_INCLUDE", [])
    pages = int(env.get("DISCOVER_PAGES", 12))

    pool_max = int(env.get("POOL_MAX_ITEMS", 20000))
    prune_at = int(env.get("POOL_PRUNE_AT", 0) or 0)
    prune_keep = int(env.get("POOL_PRUNE_KEEP", max(0, prune_at - 5000)) or 0)

    provider_ids, used_map = tmdb.providers_from_env(subs, region)
    unmatched = [k for k, v in (used_map or {}).items() if not v]

    fresh: List[Dict[str, Any]] = []
    diag_pages: List[Dict[str, Any]] = []

    for kind in ("movie", "tv"):
        for p in range(1, max(1, pages) + 1):
            if kind == "movie":
                items, d = tmdb.discover_movie_page(p, region, langs, provider_ids, slot=p % 3)
            else:
                items, d = tmdb.discover_tv_page(p, region, langs, provider_ids, slot=p % 3)
            fresh.extend(items)
            d["kind"] = kind
            diag_pages.append(d)

    for period in ("day", "week"):
        fresh.extend(tmdb.trending("movie", period))
        fresh.extend(tmdb.trending("tv", period))

    # De-dupe fresh
    seen_fresh = set()
    uniq_fresh: List[Dict[str, Any]] = []
    for it in fresh:
        k = (it.get("media_type"), int(it.get("tmdb_id") or 0))
        if not k[0] or not k[1] or k in seen_fresh:
            continue
        uniq_fresh.append(it)
        seen_fresh.add(k)

    with_ids = _attach_imdb_ids(uniq_fresh)

    # --- telemetry fix: measure BEFORE append ---
    stats_before = pool_mod.pool_stats(sample_unique=False)

    appended = pool_mod.append_candidates(with_ids)

    if prune_at and stats_before.get("file_lines", 0) + appended > prune_at and prune_keep > 0:
        pool_mod.prune_pool(keep_last_lines=prune_keep)

    stats_after = pool_mod.pool_stats(sample_unique=True, sample_limit=200000)

    pool = pool_mod.load_pool(max_items=pool_max, unique_only=True, prefer_recent=True)

    combined_keys = set()
    combined: List[Dict[str, Any]] = []
    for it in pool:
        k = (it.get("media_type"), int(it.get("tmdb_id") or 0))
        if k[0] and k[1] and k not in combined_keys:
            combined.append(it); combined_keys.add(k)
    for it in with_ids:
        k = (it.get("media_type"), int(it.get("tmdb_id") or 0))
        if k[0] and k[1] and k not in combined_keys:
            combined.append(it); combined_keys.add(k)

    env["DISCOVERED_COUNT"] = len(fresh)
    env["ELIGIBLE_COUNT"] = len(combined)
    env["PROVIDER_MAP"] = used_map
    env["PROVIDER_UNMATCHED"] = unmatched
    env["DISCOVER_PAGE_TELEMETRY"] = diag_pages[:]
    env["POOL_TELEMETRY"] = {
        "appended_this_run": int(appended),
        "file_lines_before": int(stats_before.get("file_lines", 0)),
        "file_lines_after": int(stats_after.get("file_lines", 0)),
        "unique_keys_est": int(stats_after.get("unique_keys_est", 0)),
        "loaded_unique": int(len(pool)),
        "pool_max_items": int(pool_max),
        "prune_at": int(prune_at),
        "prune_keep": int(prune_keep),
    }
    return combined