# engine/config.py
from __future__ import annotations

import os
from typing import Any, Dict


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _as_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _as_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class Config:
    """
    Config built from process environment with safe defaults, type casting,
    alias handling, and both dict- & attribute-style access.
    """

    # Sensible defaults
    _DEFAULTS: Dict[str, Any] = {
        # Region / language
        "region": "US",
        "watch_region": None,            # will fall back to region
        "original_langs": "en",
        "language": "en-US",
        "with_original_language": None,  # will fall back to original_langs

        # Providers / subscriptions
        "subs_include": "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",

        # TMDB discovery sweep sizes
        "tmdb_pages_movie": 24,
        "tmdb_pages_tv": 24,

        # Catalog / behavior
        "max_catalog": 10000,
        "include_tv_seasons": True,
        "skip_window_days": 4,

        # Ranking weights
        "critic_weight": 0.5,
        "audience_weight": 0.5,
    }

    # Primary ENV → internal key mappings
    _ENV_MAP: Dict[str, str] = {
        # region / language
        "REGION": "region",
        "WATCH_REGION": "watch_region",
        "ORIGINAL_LANGS": "original_langs",
        "LANGUAGE": "language",
        "WITH_ORIGINAL_LANGUAGE": "with_original_language",

        # providers
        "SUBS_INCLUDE": "subs_include",
        "PROVIDER_NAMES": "subs_include",  # alias we accept

        # sweep sizes
        "TMDB_PAGES_MOVIE": "tmdb_pages_movie",
        "TMDB_PAGES_TV": "tmdb_pages_tv",

        # behavior
        "MAX_CATALOG": "max_catalog",
        "INCLUDE_TV_SEASONS": "include_tv_seasons",
        "SKIP_WINDOW_DAYS": "skip_window_days",

        # ranking weights
        "CRITIC_WEIGHT": "critic_weight",
        "AUDIENCE_WEIGHT": "audience_weight",
    }

    # Type casters
    _CASTERS: Dict[str, Any] = {
        "tmdb_pages_movie": _as_int,
        "tmdb_pages_tv": _as_int,
        "max_catalog": _as_int,
        "skip_window_days": _as_int,
        "include_tv_seasons": _as_bool,
        "critic_weight": _as_float,
        "audience_weight": _as_float,
    }

    def __init__(self, data: Dict[str, Any]):
        merged = dict(self._DEFAULTS)
        merged.update(data or {})

        # Backfills/aliases computed from other values
        # watch_region defaults to region if unset
        if not merged.get("watch_region"):
            merged["watch_region"] = merged.get("region") or self._DEFAULTS["region"]

        # with_original_language defaults to original_langs if unset
        if not merged.get("with_original_language"):
            merged["with_original_language"] = merged.get("original_langs") or self._DEFAULTS["original_langs"]

        # Normalize types
        for k, caster in self._CASTERS.items():
            merged[k] = caster(merged.get(k), self._DEFAULTS.get(k))  # type: ignore[arg-type]

        self._d = merged

    # ---- construction helpers ----

    @classmethod
    def from_env(cls) -> "Config":
        """
        Build Config from whitelisted env vars and reasonable aliases.
        Also accepts already-normalized snake_case keys in env (advanced use).
        """
        data: Dict[str, Any] = {}

        # Primary mapping
        for env_key, cfg_key in cls._ENV_MAP.items():
            if env_key in os.environ:
                data[cfg_key] = os.environ[env_key]

        # Accept snake_case directly, e.g. export subs_include=...
        for k, v in os.environ.items():
            kk = k.strip().lower()
            if kk in cls._DEFAULTS and kk not in data:
                data[kk] = v

        # Ensure REGION influences watch_region if WATCH_REGION missing
        if "watch_region" not in data and "region" in data:
            data["watch_region"] = data["region"]

        # Ensure ORIGINAL_LANGS influences with_original_language if missing
        if "with_original_language" not in data and "original_langs" in data:
            data["with_original_language"] = data["original_langs"]

        return cls(data)

    # ---- mapping-like API ----
    def to_dict(self) -> Dict[str, Any]:
        return dict(self._d)

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._d[key]

    def __contains__(self, key: str) -> bool:
        return key in self._d

    # ---- attribute API ----
    def __getattr__(self, key: str) -> Any:
        try:
            return self._d[key]
        except KeyError as e:
            raise AttributeError(key) from e

    def __repr__(self) -> str:
        return f"Config({self._d!r})"