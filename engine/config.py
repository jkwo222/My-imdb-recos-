# engine/config.py
from __future__ import annotations

import os
from typing import Any, Dict


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


class Config:
    """
    Minimal config object, reading from environment variables.
    Provides attributes that other modules expect (including legacy names).
    """

    def __init__(self, d: Dict[str, Any]) -> None:
        self._d = d

    @classmethod
    def from_env(cls) -> "Config":
        d: Dict[str, Any] = {}

        # Region & languages
        d["watch_region"] = os.getenv("REGION", "US").strip() or "US"
        d["original_langs"] = os.getenv("ORIGINAL_LANGS", "en").strip() or "en"

        # Streaming providers (comma-separated slugs)
        # Example: netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus
        d["subs_include"] = os.getenv("SUBS_INCLUDE", "").strip()

        # TMDB discovery page counts
        d["tmdb_pages_movie"] = _get_int("TMDB_PAGES_MOVIE", 24)
        d["tmdb_pages_tv"] = _get_int("TMDB_PAGES_TV", 24)

        # Limits / options
        d["max_catalog"] = _get_int("MAX_CATALOG", 10_000)
        d["include_tv_seasons"] = _get_bool("INCLUDE_TV_SEASONS", True)
        d["skip_window_days"] = _get_int("SKIP_WINDOW_DAYS", 4)

        # Scoring knobs (keep defaults if not used)
        d["critic_weight"] = float(os.getenv("CRITIC_WEIGHT", "0.5"))
        d["audience_weight"] = float(os.getenv("AUDIENCE_WEIGHT", "0.5"))

        # OMDB/IMDb paths (used elsewhere)
        d["imdb_ratings_csv_path"] = os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

        return cls(d)

    # convenience: dict-like getters (but code elsewhere uses attributes)
    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def __getattr__(self, key: str) -> Any:  # attribute access
        try:
            return self._d[key]
        except KeyError as e:
            # Make it obvious what key was missing
            raise AttributeError(key) from e

    def __repr__(self) -> str:
        keys = ", ".join(sorted(self._d.keys()))
        return f"Config(keys=[{keys}])"