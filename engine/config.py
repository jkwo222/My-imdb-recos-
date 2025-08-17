from __future__ import annotations

import os
from typing import Any, Dict, Optional


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


_DEFAULTS: Dict[str, Any] = {
    # Discovery / filtering
    "subs_include": _env_str("SUBS_INCLUDE",
                             "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"),
    "watch_region": _env_str("REGION", "US"),
    "original_langs": _env_str("ORIGINAL_LANGS", "en"),

    # Paging
    "tmdb_pages_movie": _env_int("TMDB_PAGES_MOVIE", 24),
    "tmdb_pages_tv": _env_int("TMDB_PAGES_TV", 24),

    # Catalog size
    "max_catalog": _env_int("MAX_CATALOG", 10000),

    # Personalization weights (fallbacks; rank.py may expand on top)
    "critic_weight": float(_env_str("CRITIC_WEIGHT", "0.6")),
    "audience_weight": float(_env_str("AUDIENCE_WEIGHT", "0.4")),

    # Feed sizes
    "shortlist_size": _env_int("SHORTLIST_SIZE", 50),
    "shown_count": _env_int("SHOWN_COUNT", 10),

    # Paths
    "ratings_csv": _env_str("RATINGS_CSV", "data/ratings.csv"),
    "exclude_csv": _env_str("EXCLUDE_CSV", "data/ratings.csv"),  # “my CSV list” doubles as exclusion set

    # TMDB client
    "tmdb_language": _env_str("TMDB_LANGUAGE", "en-US"),
    "tmdb_read_timeout": _env_int("TMDB_TIMEOUT", 20),
    "tmdb_use_bearer": _env_bool("TMDB_USE_BEARER", True),

    # Optional: enrich ratings with TMDB (for genre DNA)
    "augment_profile": _env_bool("AUGMENT_PROFILE", True),
    "augment_profile_limit": _env_int("AUGMENT_PROFILE_LIMIT", 200),
}


class Config:
    """
    Simple config:
      - sensible defaults
      - env overrides
      - explicit kwargs overrides last
    Access via attributes: cfg.watch_region, etc.
    """

    def __init__(self, **overrides: Any) -> None:
        self._d: Dict[str, Any] = dict(_DEFAULTS)
        self._d.update(overrides or {})

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._d.get(key, default)

    def __getattr__(self, key: str) -> Any:
        if key in self._d:
            return self._d[key]
        # Graceful unknowns to avoid runner crashes; return None instead of raising
        return None

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._d)