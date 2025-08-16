# engine/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# Map your simple slugs -> TMDB watch provider IDs (US region).
# Source: TMDB provider list (common ones). You can add more if needed.
_PROVIDER_MAP_US: Dict[str, int] = {
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "disney_plus": 337,
    "max": 384,             # formerly HBO Max
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
    # aliases (optional)
    "amazon": 9,
    "amazon_prime": 9,
    "disney+": 337,
    "hbomax": 384,
    "max_hbo": 384,
    "atv+": 350,
}


def _get_bool(env_name: str, default: bool) -> bool:
    raw = os.getenv(env_name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _split_csv(env_name: str) -> List[str]:
    raw = os.getenv(env_name, "")
    if not raw.strip():
        return []
    return [p.strip().lower() for p in raw.split(",") if p.strip()]


@dataclass
class Config:
    # API keys
    tmdb_api_key: str
    omdb_api_key: Optional[str] = None

    # IMDB / ratings input
    imdb_user_id: Optional[str] = None
    imdb_ratings_csv_path: Optional[str] = None

    # discovery constraints
    region: str = "US"
    original_langs: List[str] = field(default_factory=lambda: ["en"])
    with_original_language: Optional[str] = None  # derived from original_langs
    include_tv_seasons: bool = True

    # providers
    provider_slugs: List[str] = field(default_factory=list)  # from SUBS_INCLUDE
    provider_ids: List[int] = field(default_factory=list)    # mapped via _PROVIDER_MAP_US

    # pagination / limits
    tmdb_pages_movie: int = 24
    tmdb_pages_tv: int = 24
    max_catalog: int = 10_000
    skip_window_days: int = 4

    # ranking weights (keep defaults; tweak later if you want)
    critic_weight: float = 0.5
    audience_weight: float = 0.5

    @staticmethod
    def from_env() -> "Config":
        tmdb_key = os.getenv("TMDB_API_KEY", "")
        if not tmdb_key:
            raise RuntimeError("TMDB_API_KEY is required")

        # languages
        langs = _split_csv("ORIGINAL_LANGS") or ["en"]
        with_orig = ",".join(langs)

        # providers
        slugs = _split_csv("SUBS_INCLUDE")
        ids: List[int] = []
        for s in slugs:
            if s in _PROVIDER_MAP_US:
                ids.append(_PROVIDER_MAP_US[s])
            else:
                # you can add a print if you want to see unknown slugs in logs
                pass

        return Config(
            tmdb_api_key=tmdb_key,
            omdb_api_key=os.getenv("OMDB_API_KEY") or None,
            imdb_user_id=os.getenv("IMDB_USER_ID") or None,
            imdb_ratings_csv_path=os.getenv("IMDB_RATINGS_CSV_PATH") or None,
            region=os.getenv("REGION", "US"),
            original_langs=langs,
            with_original_language=with_orig,
            include_tv_seasons=_get_bool("INCLUDE_TV_SEASONS", True),
            provider_slugs=slugs,
            provider_ids=ids,
            tmdb_pages_movie=int(os.getenv("TMDB_PAGES_MOVIE", "24")),
            tmdb_pages_tv=int(os.getenv("TMDB_PAGES_TV", "24")),
            max_catalog=int(os.getenv("MAX_CATALOG", "10000")),
            skip_window_days=int(os.getenv("SKIP_WINDOW_DAYS", "4")),
            critic_weight=float(os.getenv("CRITIC_WEIGHT", "0.5")),
            audience_weight=float(os.getenv("AUDIENCE_WEIGHT", "0.5")),
        )