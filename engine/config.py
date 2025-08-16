from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _to_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _to_int(val: Optional[str], default: int) -> int:
    try:
        return int(str(val).strip())
    except Exception:
        return default


def _csv_list(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [x.strip() for x in str(val).split(",") if x.strip()]


class Config:
    """
    Small config wrapper with:
      - Config.from_env(): build from environment variables
      - .get(key, default)
      - attribute access (cfg.REGION or cfg.region)
      - item access (cfg["REGION"])
      - .to_dict()
    """

    def __init__(self, data: Dict[str, Any]):
        self._d = dict(data)

    # ---- builders ----
    @staticmethod
    def from_env() -> "Config":
        # API keys / IDs
        tmdb_key = os.getenv("TMDB_API_KEY", "")
        omdb_key = os.getenv("OMDB_API_KEY", "")
        imdb_user = os.getenv("IMDB_USER_ID", "")
        ratings_csv = os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

        # Discovery / region / language
        region = os.getenv("REGION", "US")
        original_langs = _csv_list(os.getenv("ORIGINAL_LANGS", "en"))
        with_original_language = original_langs[:]  # alias used by some modules

        # Streaming providers to include
        provider_names = _csv_list(
            os.getenv(
                "SUBS_INCLUDE",
                "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",
            )
        )

        # Pagination / limits
        tmdb_pages_movie = _to_int(os.getenv("TMDB_PAGES_MOVIE"), 24)
        tmdb_pages_tv = _to_int(os.getenv("TMDB_PAGES_TV"), 24)
        max_catalog = _to_int(os.getenv("MAX_CATALOG"), 10000)
        include_tv_seasons = _to_bool(os.getenv("INCLUDE_TV_SEASONS"), True)
        skip_window_days = _to_int(os.getenv("SKIP_WINDOW_DAYS"), 4)

        # Base dictionary (UPPER-CASE canonical keys)
        data: Dict[str, Any] = {
            "TMDB_API_KEY": tmdb_key,
            "OMDB_API_KEY": omdb_key,
            "IMDB_USER_ID": imdb_user,
            "IMDB_RATINGS_CSV_PATH": ratings_csv,

            "REGION": region,
            "watch_region": region,  # alias some modules expect

            "ORIGINAL_LANGS": original_langs,
            "with_original_language": with_original_language,  # alias

            "provider_names": provider_names,

            "TMDB_PAGES_MOVIE": tmdb_pages_movie,
            "TMDB_PAGES_TV": tmdb_pages_tv,
            "MAX_CATALOG": max_catalog,
            "INCLUDE_TV_SEASONS": include_tv_seasons,
            "SKIP_WINDOW_DAYS": skip_window_days,
        }

        # ---- lower-case mirrors (for modules that access lower-case attrs) ----
        lc_aliases = {
            "tmdb_api_key": tmdb_key,
            "omdb_api_key": omdb_key,
            "imdb_user_id": imdb_user,
            "imdb_ratings_csv_path": ratings_csv,

            "region": region,
            "watch_region": region,  # already lower-case above; keep as-is

            "original_langs": original_langs,
            "with_original_language": with_original_language,

            "provider_names": provider_names,

            "tmdb_pages_movie": tmdb_pages_movie,
            "tmdb_pages_tv": tmdb_pages_tv,
            "max_catalog": max_catalog,
            "include_tv_seasons": include_tv_seasons,
            "skip_window_days": skip_window_days,
        }
        data.update(lc_aliases)

        return Config(data)

    # ---- conveniences ----
    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._d)

    # dict-like
    def __getitem__(self, key: str) -> Any:
        return self._d[key]

    # attribute-like
    def __getattr__(self, key: str) -> Any:
        try:
            return self._d[key]
        except KeyError as e:
            raise AttributeError(key) from e

    def __repr__(self) -> str:
        return f"Config({self._d!r})"