# engine/catalog_builder.py
from __future__ import annotations
from typing import Any, Dict, List

from .env import Env
from .tmdb import (
    providers_from_env,
    discover_movie_page,
    discover_tv_page,
)

def _discover_pool(env: Env) -> Dict[str, Any]:
    """
    Build the raw discovery pool (movies + tv) from TMDB according to env.
    """
    # Accept attribute-style and dict-style access
    region = getattr(env, "REGION", None) or env.get("REGION", "US")
    langs = getattr(env, "ORIGINAL_LANGS", None) or env.get("ORIGINAL_LANGS", ["en"])
    subs = getattr(env, "SUBS_INCLUDE", None) or env.get("SUBS_INCLUDE", [])
    pages = int(getattr(env, "DISCOVER_PAGES", None) or env.get("DISCOVER_PAGES", 9))  # expanded pages per run

    provider_ids: List[int] = []
    if subs:
        try:
            provider_ids = providers_from_env(subs, region)
        except Exception as ex:
            print(f"[catalog] providers_from_env({subs}, {region}) failed: {ex!r}", flush=True)
            provider_ids = []

    use_provider_filter = len(provider_ids) > 0
    if not use_provider_filter and subs:
        print(f"[catalog] No provider IDs resolved for SUBS_INCLUDE={subs} in region={region}; "
              f"falling back to unfiltered discovery.", flush=True)

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for p in range(1, pages + 1):
        # Movies
        try:
            movies, _ = discover_movie_page(
                p,
                region=region,
                provider_ids=(provider_ids if use_provider_filter else None),
                original_langs=langs,
            )
            items.extend(movies)
        except Exception as ex:
            errors.append(f"discover_movie_page(p={p}): {ex!r}")

        # TV
        try:
            shows, _ = discover_tv_page(
                p,
                region=region,
                provider_ids=(provider_ids if use_provider_filter else None),
                original_langs=langs,
            )
            items.extend(shows)
        except Exception as ex:
            errors.append(f"discover_tv_page(p={p}): {ex!r}")

    if errors:
        print("[catalog] Discovery errors:\n  - " + "\n  - ".join(errors), flush=True)

    return {
        "items": items,
        "errors": errors,
        "region": region,
        "langs": langs,
        "subs": subs,
        "pages": pages,
        "provider_ids": provider_ids,
        "provider_filter": use_provider_filter,
    }

def build_catalog(env: Env) -> List[Dict[str, Any]]:
    """
    Public API used by runner.main(). Returns a flat list of items.
    """
    raw = _discover_pool(env)
    return list(raw["items"])