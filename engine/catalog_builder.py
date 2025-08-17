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
    """
    Build the candidate set *for this run*:
      1) Discover pages for movie+tv with provider filters and langs.
      2) Add trending (day+week).
      3) Attach imdb_ids to support unseen filtering.
      4) Persist to pool; then return (pool ∪ fresh) de-duped, newest-first.

    NOTE: Filtering for "unseen" occurs later in runner via engine.exclusions.
    """
    region = env.get("REGION", "US")
    langs = env.get("ORIGINAL_LANGS", ["en"])
    subs = env.get("SUBS_INCLUDE", [])
    pages = int(env.get("DISCOVER_PAGES", 12))

    provider_ids, used_map = tmdb.providers_from_env(subs, region)
    unmatched = [k for k, v in (used_map or {}).items() if not v]

    all_items: List[Dict[str, Any]] = []
    diag_pages: List[Dict[str, Any]] = []

    # Discover movie / tv
    for kind in ("movie", "tv"):
        for p in range(1, max(1, pages) + 1):
            if kind == "movie":
                items, d = tmdb.discover_movie_page(p, region, langs, provider_ids, slot=p % 3)
            else:
                items, d = tmdb.discover_tv_page(p, region, langs, provider_ids, slot=p % 3)
            all_items.extend(items)
            d["kind"] = kind
            diag_pages.append(d)

    # Trending
    for period in ("day", "week"):
        all_items.extend(tmdb.trending("movie", period))
        all_items.extend(tmdb.trending("tv", period))

    # De-dupe by (media_type, tmdb_id)
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in all_items:
        key = (it.get("media_type"), int(it.get("tmdb_id") or 0))
        if not key[1] or key in seen:
            continue
        uniq.append(it)
        seen.add(key)

    # Attach imdb_ids for unseen filtering
    with_ids = _attach_imdb_ids(uniq)

    # Persist to pool and return pool ∪ fresh (newest-first)
    pool_mod.append_candidates(with_ids)
    pool = pool_mod.load_pool(max_items=5000)

    def _key_sort(it: Dict[str, Any]):
        return (float(it.get("added_at") or 0.0), float(it.get("tmdb_vote") or 0.0))

    combined = sorted(pool + with_ids, key=_key_sort, reverse=True)

    # Telemetry for runner summary/diag
    env["DISCOVERED_COUNT"] = len(all_items)
    env["ELIGIBLE_COUNT"] = len(combined)  # pre-exclusions
    env["PROVIDER_MAP"] = used_map
    env["PROVIDER_UNMATCHED"] = unmatched
    env["DISCOVER_PAGE_TELEMETRY"] = diag_pages[:]

    return combined