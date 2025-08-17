# engine/config.py
from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional, Sequence


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if v is not None else default


def _csv_env(key: str, default: str = "") -> str:
    """
    Returns a normalized, comma-separated value string (no spaces).
    """
    raw = _env(key, default) or ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return ",".join(parts)


DEFAULTS: Dict[str, Any] = {
    # TMDB
    "tmdb_api_key": None,  # must be provided via TMDB_API_KEY
    "watch_region": "US",  # ISO-3166-1 alpha-2
    "subs_include": "netflix,prime_video,hulu,disney_plus,hbo_max,apple_tv,peacock,paramount_plus",
    "langs_include": "en",  # TMDB original_language filters, comma-separated ISO 639-1
    "monetization_types": "flatrate|free|ads",  # TMDB discover monetization types

    # pagination for discover
    "tmdb_pages_movies": 20,
    "tmdb_pages_tv": 20,

    # local paths
    "data_dir": "data",
    "cache_dir": "data/cache",

    # pool sizes (catalog will slice these)
    "pool_target_movie": 480,
    "pool_target_tv": 480,

    # output
    "out_dir": "data/out",
}


class Config:
    """
    Thin config wrapper that:
      * reads from env with sensible defaults
      * exposes attributes used around the engine
    """

    def __init__(self, d: Dict[str, Any]):
        self._d = d

    # ---- canonical accessors (so AttributeError never surprises the caller)

    @property
    def tmdb_api_key(self) -> Optional[str]:
        return self._d.get("tmdb_api_key")

    @property
    def watch_region(self) -> str:
        return self._d.get("watch_region", DEFAULTS["watch_region"])

    @property
    def subs_include(self) -> str:
        return self._d.get("subs_include", DEFAULTS["subs_include"])

    @property
    def langs_include(self) -> str:
        return self._d.get("langs_include", DEFAULTS["langs_include"])

    @property
    def monetization_types(self) -> str:
        return self._d.get("monetization_types", DEFAULTS["monetization_types"])

    @property
    def tmdb_pages_movies(self) -> int:
        return int(self._d.get("tmdb_pages_movies", DEFAULTS["tmdb_pages_movies"]))

    @property
    def tmdb_pages_tv(self) -> int:
        return int(self._d.get("tmdb_pages_tv", DEFAULTS["tmdb_pages_tv"]))

    @property
    def data_dir(self) -> str:
        return self._d.get("data_dir", DEFAULTS["data_dir"])

    @property
    def cache_dir(self) -> str:
        return self._d.get("cache_dir", DEFAULTS["cache_dir"])

    @property
    def out_dir(self) -> str:
        return self._d.get("out_dir", DEFAULTS["out_dir"])

    @property
    def pool_target_movie(self) -> int:
        return int(self._d.get("pool_target_movie", DEFAULTS["pool_target_movie"]))

    @property
    def pool_target_tv(self) -> int:
        return int(self._d.get("pool_target_tv", DEFAULTS["pool_target_tv"]))

    # ---- dict-like helpers

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._d)

    def __repr__(self) -> str:
        return f"Config({json.dumps(self._d, indent=2, sort_keys=True)})"

    # ---- legacy fallback (avoid AttributeError crashes seen in logs)
    def __getattr__(self, key: str) -> Any:
        if key in self._d:
            return self._d[key]
        if key in DEFAULTS:
            return DEFAULTS[key]
        raise AttributeError(key)

    # ---- constructor

    @staticmethod
    def from_env() -> "Config":
        d: Dict[str, Any] = dict(DEFAULTS)

        # Required API key
        d["tmdb_api_key"] = _env("TMDB_API_KEY", DEFAULTS["tmdb_api_key"])

        # Filters / controls
        d["watch_region"] = _env("WATCH_REGION", DEFAULTS["watch_region"]) or DEFAULTS["watch_region"]
        d["subs_include"] = _csv_env("SUBS_INCLUDE", DEFAULTS["subs_include"])
        d["langs_include"] = _csv_env("LANGS_INCLUDE", DEFAULTS["langs_include"])
        d["monetization_types"] = _env("MONETIZATION_TYPES", DEFAULTS["monetization_types"]) or DEFAULTS["monetization_types"]

        # Paging
        d["tmdb_pages_movies"] = int(_env("TMDB_PAGES_MOVIES", str(DEFAULTS["tmdb_pages_movies"])))
        d["tmdb_pages_tv"] = int(_env("TMDB_PAGES_TV", str(DEFAULTS["tmdb_pages_tv"])))

        # Paths
        d["data_dir"] = _env("DATA_DIR", DEFAULTS["data_dir"]) or DEFAULTS["data_dir"]
        d["cache_dir"] = _env("CACHE_DIR", DEFAULTS["cache_dir"]) or DEFAULTS["cache_dir"]
        d["out_dir"] = _env("OUT_DIR", DEFAULTS["out_dir"]) or DEFAULTS["out_dir"]

        # Pool targets
        d["pool_target_movie"] = int(_env("POOL_TARGET_MOVIE", str(DEFAULTS["pool_target_movie"])))
        d["pool_target_tv"] = int(_env("POOL_TARGET_TV", str(DEFAULTS["pool_target_tv"])))

        return Config(d)