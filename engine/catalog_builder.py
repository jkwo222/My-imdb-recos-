from __future__ import annotations
from typing import Any, Dict, List, Optional

# TMDB helpers
from .tmdb import discover_movie_page, discover_tv_page  # type: ignore

# providers_from_env may vary by repo/version; treat as optional & handle signatures
try:
    from .tmdb import providers_from_env  # type: ignore
except Exception:
    providers_from_env = None  # type: ignore[assignment]

# Optional rotation support
try:
    from .rotation import plan_pages  # type: ignore
except Exception:
    plan_pages = None  # type: ignore[assignment]


def _as_list(v: Any, default: Optional[List[str]] = None) -> List[str]:
    if v is None:
        return list(default or [])
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _pages(env: Dict[str, Any]) -> List[int]:
    pages_requested = int(env.get("DISCOVER_PAGES", 3))
    cap = int(env.get("PAGE_CAP", 1000))
    step = int(env.get("STEP", 37))
    rotate_minutes = int(env.get("ROTATE_MINUTES", 60))
    if plan_pages:
        return plan_pages(pages_requested=pages_requested, step=step, rotate_minutes=rotate_minutes, cap=cap)
    pages_requested = max(1, min(pages_requested, cap))
    return list(range(1, pages_requested + 1))


def _provider_ids(env: Dict[str, Any], region: str) -> Optional[List[int]]:
    subs = _as_list(env.get("SUBS_INCLUDE", []))
    if not subs or not providers_from_env:
        return None
    # Try modern signature first, then older
    try:
        return providers_from_env(subs, region=region)  # type: ignore[misc]
    except TypeError:
        try:
            return providers_from_env(subs)  # type: ignore[misc]
        except Exception:
            return None
    except Exception:
        return None


def _discover_pool(env: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the raw discovery pool (movies + tv) from TMDB according to env.
    """
    # dict-style access expected by current runner
    region = str(env.get("REGION", env.get("TMDB_REGION", "US")) or "US").upper()
    langs = _as_list(env.get("ORIGINAL_LANGS", env.get("WITH_ORIGINAL_LANGS", ["en"])), default=["en"])
    provider_ids = _provider_ids(env, region=region)

    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for p in _pages(env):
        try:
            movies, _ = discover_movie_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=langs or None,
            )
            if movies:
                items.extend(movies)
        except Exception as ex:
            errors.append(f"discover_movie_page(p={p}): {ex!r}")

        try:
            shows, _ = discover_tv_page(
                p,
                region=region,
                provider_ids=provider_ids,
                original_langs=langs or None,
            )
            if shows:
                items.extend(shows)
        except Exception as ex:
            errors.append(f"discover_tv_page(p={p}): {ex!r}")

    return {
        "items": items,
        "errors": errors,
        "region": region,
        "langs": langs,
        "subs": _as_list(env.get("SUBS_INCLUDE", [])),
        "pages": env.get("DISCOVER_PAGES", 3),
        "provider_ids": provider_ids or [],
    }


def build_catalog(env: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Public API used by runner.main(). Returns a flat list of items.
    """
    raw = _discover_pool(env)
    return list(raw["items"])