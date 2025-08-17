from __future__ import annotations
from typing import Any, Dict, List, Tuple, Iterable

from .env import Env
from .tmdb import (
    providers_from_env,
    discover_movie_page,
    discover_tv_page,
)


def _as_list(v: Any) -> List[str]:
    """
    Normalize common env variants into a list of strings.
    - None -> []
    - "en" -> ["en"]
    - "en,es,fr" -> ["en","es","fr"]
    - ["en","es"] -> ["en","es"]
    """
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if "," in s:
        return [t.strip() for t in s.split(",") if t.strip()]
    return [s]


def _env_region(env: Env) -> str:
    r = getattr(env, "REGION", None) or env.get("REGION", "US")
    r = str(r).strip().upper() or "US"
    return r


def _env_langs(env: Env) -> List[str]:
    langs = getattr(env, "ORIGINAL_LANGS", None) or env.get("ORIGINAL_LANGS", ["en"])
    langs = _as_list(langs)
    return langs or ["en"]


def _env_providers(env: Env) -> List[str]:
    subs = getattr(env, "SUBS_INCLUDE", None) or env.get("SUBS_INCLUDE", [])
    return _as_list(subs)


def _env_pages(env: Env) -> int:
    p = getattr(env, "DISCOVER_PAGES", None) or env.get("DISCOVER_PAGES", 3)
    try:
        p = int(p)
    except Exception:
        p = 3
    return max(1, min(50, p))  # hard cap to keep runs tame


def _query_once(
    pages: int,
    region: str,
    provider_ids: List[int],
    langs: List[str],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    items: List[Dict[str, Any]] = []
    errors: List[str] = []
    for p in range(1, pages + 1):
        # Movies
        try:
            mv, _ = discover_movie_page(
                p,
                region=region,
                provider_ids=provider_ids or None,
                original_langs=langs or None,
            )
            items.extend(mv or [])
        except Exception as ex:
            errors.append(f"discover_movie_page(p={p}, region={region}, providers={bool(provider_ids)}, langs={','.join(langs) if langs else '-'}) -> {ex!r}")

        # TV
        try:
            tv, _ = discover_tv_page(
                p,
                region=region,
                provider_ids=provider_ids or None,
                original_langs=langs or None,
            )
            items.extend(tv or [])
        except Exception as ex:
            errors.append(f"discover_tv_page(p={p}, region={region}, providers={bool(provider_ids)}, langs={','.join(langs) if langs else '-'}) -> {ex!r}")
    return items, errors


def _discover_pool(env: Env) -> Dict[str, Any]:
    """
    Build the raw discovery pool (movies + tv) from TMDB according to env,
    with robust fallbacks and verbose telemetry to runner.log.
    """
    region = _env_region(env)
    langs = _env_langs(env)
    subs = _env_providers(env)
    pages = _env_pages(env)

    # Map subscription slugs -> TMDB provider IDs
    provider_ids = []
    provider_errors: List[str] = []
    try:
        provider_ids = providers_from_env(subs, region) if subs else []
    except Exception as ex:
        provider_errors.append(f"providers_from_env(subs={subs}, region={region}) -> {ex!r}")
        provider_ids = []

    print(f" | discover: region={region} langs={langs} subs={subs} pages={pages}")
    if provider_errors:
        for e in provider_errors:
            print(f" | discover: provider-map-error: {e}")
    print(f" | discover: provider_ids={provider_ids or 'NONE'}")

    # Pass 1: as configured
    items, errs = _query_once(pages, region, provider_ids, langs)
    print(f" | discover: pass1 found={len(items)} errors={len(errs)}")
    for e in errs[:10]:
        print(f" | discover: err: {e}")

    # If nothing found, progressively relax constraints:
    # Pass 2: drop provider filter
    if len(items) == 0 and (provider_ids and subs):
        print(" | discover: FALLBACK #1 (drop providers, keep langs)")
        items2, errs2 = _query_once(pages, region, [], langs)
        items.extend(items2)
        errs.extend(errs2)
        print(f" | discover: pass2 found={len(items2)} cumulative={len(items)}")
        for e in errs2[:10]:
            print(f" | discover: err: {e}")

    # Pass 3: drop original_langs too (broadest)
    if len(items) == 0:
        print(" | discover: FALLBACK #2 (drop providers & langs)")
        items3, errs3 = _query_once(pages, region, [], [])
        items.extend(items3)
        errs.extend(errs3)
        print(f" | discover: pass3 found={len(items3)} cumulative={len(items)}")
        for e in errs3[:10]:
            print(f" | discover: err: {e}")

    return {
        "items": items,
        "errors": errs,
        "region": region,
        "langs": langs,
        "subs": subs,
        "pages": pages,
        "provider_ids": provider_ids,
    }


def build_catalog(env: Env) -> List[Dict[str, Any]]:
    """
    Public API used by runner.main(). Returns a flat list of items and prints compact telemetry.
    """
    raw = _discover_pool(env)
    items = list(raw["items"])
    print(f" | discover: summary region={raw['region']} pages={raw['pages']} langs={raw['langs']} subs={raw['subs']}")
    print(f" | discover: total_items={len(items)} provider_ids={raw['provider_ids'] or 'NONE'} errors={len(raw['errors'])}")
    return items