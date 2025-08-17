# engine/catalog_builder.py
from __future__ import annotations
import os
from typing import Any, Dict, List

from .env import Env
from .tmdb import (
    providers_from_env,
    discover_movie_page,
    discover_tv_page,
)

def _log(env: Env, msg: str) -> None:
    """Log to Actions console and append to OUT_DIR/runner.log."""
    print(msg, flush=True)
    try:
        # Support both attr and dict-like Env access
        out_dir = getattr(env, "OUT_DIR", None) or env.get("OUT_DIR", "data/out")
        os.makedirs(out_dir, exist_ok=True)
        log_path = os.path.join(out_dir, "runner.log")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        # Never let logging break discovery
        pass


def _discover_pool(env: Env) -> Dict[str, Any]:
    """
    Build the raw discovery pool (movies + tv) from TMDB according to env.
    Supports both attribute and dict-like access.
    """
    region = getattr(env, "REGION", None) or env.get("REGION", "US")
    langs = getattr(env, "ORIGINAL_LANGS", None) or env.get("ORIGINAL_LANGS", ["en"])
    subs = getattr(env, "SUBS_INCLUDE", None) or env.get("SUBS_INCLUDE", [])
    pages = int(getattr(env, "DISCOVER_PAGES", None) or env.get("DISCOVER_PAGES", 3))

    provider_ids = providers_from_env(subs, region) if subs else []

    _log(env, "[catalog_builder] Starting discovery")
    _log(env, f"[catalog_builder] Region={region}, Langs={langs}, Subs={subs}")
    _log(env, f"[catalog_builder] Provider IDs={provider_ids}, Pages={pages}")

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for p in range(1, pages + 1):
        try:
            movies, _ = discover_movie_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=langs,
            )
            _log(env, f"[catalog_builder] Page {p}: {len(movies)} movies discovered")
            items.extend(movies)
        except Exception as ex:
            msg = f"[catalog_builder][ERROR] discover_movie_page(p={p}): {ex!r}"
            _log(env, msg)
            errors.append(msg)

        try:
            shows, _ = discover_tv_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=langs,
            )
            _log(env, f"[catalog_builder] Page {p}: {len(shows)} shows discovered")
            items.extend(shows)
        except Exception as ex:
            msg = f"[catalog_builder][ERROR] discover_tv_page(p={p}): {ex!r}"
            _log(env, msg)
            errors.append(msg)

    _log(env, f"[catalog_builder] Discovery complete: {len(items)} total items, {len(errors)} errors")

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
    _log(env, (f"[catalog_builder] build_catalog returning {len(raw['items'])} items "
               f"(Region={raw['region']}, Langs={raw['langs']}, Subs={raw['subs']}, "
               f"ProviderIDs={raw['provider_ids']}, Pages={raw['pages']})"))
    return list(raw["items"])