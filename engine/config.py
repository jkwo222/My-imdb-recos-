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


class Config:
    """
    Simple config holder that can be constructed from environment variables.
    - Attribute access: cfg.key
    - Mapping access:   cfg["key"], cfg.get("key", default)
    """

    # Defaults used if no environment value is present
    _DEFAULTS: Dict[str, Any] = {
        # discovery & region/language
        "region": "US",
        "original_langs": "en",
        "subs_include": "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",

        # TMDB discovery sweep sizes
        "tmdb_pages_movie": 24,
        "tmdb_pages_tv": 24,

        # Catalog / selection behavior
        "max_catalog": 10000,
        "include_tv_seasons": True,
        "skip_window_days": 4,

        # Ranking weights (used by _rank)
        "critic_weight": 0.5,
        "audience_weight": 0.5,
    }

    # Mapping of ENV -> internal key
    _ENV_MAP: Dict[str, str] = {
        # region/language/providers
        "REGION": "region",
        "ORIGINAL_LANGS": "original_langs",
        "SUBS_INCLUDE": "subs_include",

        # sweep sizes
        "TMDB_PAGES_MOVIE": "tmdb_pages_movie",
        "TMDB_PAGES_TV": "tmdb_pages_tv",

        # behavior toggles/limits
        "MAX_CATALOG": "max_catalog",
        "INCLUDE_TV_SEASONS": "include_tv_seasons",
        "SKIP_WINDOW_DAYS": "skip_window_days",

        # ranking weights
        "CRITIC_WEIGHT": "critic_weight",
        "AUDIENCE_WEIGHT": "audience_weight",
    }

    # Which keys should be cast to which types
    _CASTERS: Dict[str, Any] = {
        "tmdb_pages_movie": _as_int,
        "tmdb_pages_tv": _as_int,
        "max_catalog": _as_int,
        "skip_window_days": _as_int,
        "include_tv_seasons": _as_bool,
        # weights can arrive as str or float; coerce to float if possible
        "critic_weight": float,
        "audience_weight": float,
    }

    def __init__(self, data: Dict[str, Any]):
        # merge defaults with provided data
        merged = dict(self._DEFAULTS)
        merged.update(data or {})
        # final type normalization
        for k, caster in self._CASTERS.items():
            if k in merged:
                try:
                    if caster is float:
                        merged[k] = float(merged[k])
                    else:
                        # custom caster like _as_int/_as_bool
                        merged[k] = caster(merged[k], self._DEFAULTS.get(k))  # type: ignore
                except Exception:
                    # fall back to default if cast fails
                    merged[k] = self._DEFAULTS.get(k)
        self._d = merged

    # --- construction helpers ---

    @classmethod
    def from_env(cls) -> "Config":
        """
        Build Config from process environment (plus defaults).
        Only whitelisted env vars are read via _ENV_MAP.
        """
        data: Dict[str, Any] = {}
        for env_key, cfg_key in cls._ENV_MAP.items():
            if env_key in os.environ:
                data[cfg_key] = os.environ[env_key]

        # Also accept already-normalized keys present in env (advanced use)
        # e.g., export tmdb_pages_movie=12
        for k in list(os.environ.keys()):
            kk = k.strip().lower()
            if kk in cls._DEFAULTS and kk not in data:
                data[kk] = os.environ[k]

        return cls(data)

    # --- dict-like API ---

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._d)

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._d[key]

    def __contains__(self, key: str) -> bool:
        return key in self._d

    # --- attribute API ---

    def __getattr__(self, key: str) -> Any:
        try:
            return self._d[key]
        except KeyError as e:
            # surface the real missing attribute name
            raise AttributeError(key) from e

    def __repr__(self) -> str:
        return f"Config({self._d!r})"