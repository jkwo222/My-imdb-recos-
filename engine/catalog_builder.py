from __future__ import annotations
from typing import Any, Dict, List, Tuple

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
    # Support both attribute and dict-like access (runner code historically mixed styles)
    region = getattr(env, "REGION", None) or env.get("REGION", "US")
    langs = getattr(env, "ORIGINAL_LANGS", None) or env.get("ORIGINAL_LANGS", ["en"])
    subs = getattr(env, "SUBS_INCLUDE", None) or env.get("SUBS_INCLUDE", [])
    pages = getattr(env, "DISCOVER_PAGES", None) or env.get("DISCOVER_PAGES", 3)

    provider_ids = providers_from_env(subs, region) if subs else []

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for p in range(1, int(pages) + 1):
        try:
            movies, _ = discover_movie_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=langs,
            )
            items.extend(movies)
        except Exception as ex:
            errors.append(f"discover_movie_page(p={p}): {ex!r}")

        try:
            shows, _ = discover_tv_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=langs,
            )
            items.extend(shows)
        except Exception as ex:
            errors.append(f"discover_tv_page(p={p}): {ex!r}")

    return {
        "items": items,
        "errors": errors,
        "region": region,
        "langs": langs,
        "subs": subs,
        "pages": pages,
        "provider_ids": provider_ids,
    }


def build_catalog(env: Env) -> List[Dict[str, Any]]:
    """
    Public API used by runner.main(). Returns a flat list of items.
    """
    raw = _discover_pool(env)
    # Runner prints its own begin/end guards; keep this lean and predictable.
    return list(raw["items"])