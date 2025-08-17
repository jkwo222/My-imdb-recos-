# engine/catalog_builder.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple
from pathlib import Path

from .tmdb import discover_movie_page, discover_tv_page, providers_from_env
from .tmdb_detail import enrich_item

def _log(msg: str) -> None:
    print(msg, flush=True)

def _unique_key(x: Dict[str, Any]) -> Tuple[str, int]:
    return (x.get("media_type") or "movie", int(x.get("tmdb_id")))

def _discover_pool(env: Dict[str, Any]) -> List[Dict[str, Any]]:
    region = env.get("REGION", "US")
    subs = env.get("SUBS_INCLUDE", "")
    langs = env.get("ORIGINAL_LANGS", "en")
    pages = int(env.get("DISCOVER_PAGES", "3") or 3)

    providers = providers_from_env(subs, region=region)

    agg: List[Dict[str, Any]] = []
    # Always hit TMDB Discover (fresh pages) — we still respect cache TTL in tmdb.py
    for p in range(1, pages + 1):
        movies, _ = discover_movie_page(p, region=region, provider_ids=providers, original_langs=langs)
        tv, _ = discover_tv_page(p, region=region, provider_ids=providers, original_langs=langs)
        agg.extend(movies)
        agg.extend(tv)

    # Dedup
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in agg:
        k = _unique_key(it)
        if k not in seen:
            uniq.append(it)
            seen.add(k)

    return uniq

def build_catalog(env: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns:
      {
        "items": [enriched items],
        "telemetry": {...}
      }
    """
    _log(" | catalog:begin")
    raw = _discover_pool(env)

    # Enrich with detail endpoints (imdb_id, genres, year, providers, etc.)
    region = env.get("REGION", "US")
    items: List[Dict[str, Any]] = []
    errors = 0
    for r in raw:
        try:
            items.append(enrich_item(r, region))
        except Exception as e:
            errors += 1

    tel = {
        "discover_total": len(raw),
        "enriched_total": len(items),
        "enrich_errors": errors,
        "region": region,
        "discover_pages": int(env.get("DISCOVER_PAGES", "3") or 3),
        "subs_include": env.get("SUBS_INCLUDE", ""),
    }

    _log(f" | catalog:end kept={len(items)}")
    return {"items": items, "telemetry": tel}