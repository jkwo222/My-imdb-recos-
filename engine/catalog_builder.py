# engine/catalog_builder.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple

from .env import Env
from .tmdb import (
    providers_from_env,
    discover_movie_page,
    discover_tv_page,
)
from .rotation import plan_pages

def _discover_pool(env: Env) -> Dict[str, Any]:
    """
    Build the raw discovery pool (movies + tv) from TMDB according to env.
    Uses deterministic page rotation so you see a different slice over time.
    """
    # Support both attribute and dict-like access
    region = getattr(env, "REGION", None) or env.get("REGION", "US")
    langs = getattr(env, "ORIGINAL_LANGS", None) or env.get("ORIGINAL_LANGS", ["en"])
    subs = getattr(env, "SUBS_INCLUDE", None) or env.get("SUBS_INCLUDE", [])
    pages_req = int(getattr(env, "DISCOVER_PAGES", None) or env.get("DISCOVER_PAGES", 3))

    # Optional rotation knobs
    rotate_minutes = int(getattr(env, "ROTATE_MINUTES", None) or env.get("ROTATE_MINUTES", 180))
    page_cap = int(getattr(env, "DISCOVER_PAGE_CAP", None) or env.get("DISCOVER_PAGE_CAP", 200))
    step = int(getattr(env, "ROTATE_STEP", None) or env.get("ROTATE_STEP", 17))  # prime-ish step for spread

    # Provider filter (accept list or CSV)
    provider_ids = providers_from_env(subs, region) if subs else []

    # Pages to hit this run (1-based)
    pages = plan_pages(
        pages_requested=pages_req,
        step=step,
        rotate_minutes=rotate_minutes,
        cap=page_cap,
    )

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for p in pages:
        try:
            movies, _ = discover_movie_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=",".join(langs),
            )
            items.extend(movies)
        except Exception as ex:
            errors.append(f"discover_movie_page(p={p}): {ex!r}")

        try:
            shows, _ = discover_tv_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=",".join(langs),
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
    return list(raw["items"])