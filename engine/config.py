# File: engine/config.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List

def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "", "None", "null") else default

def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = _env(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

@dataclass(frozen=True)
class Config:
    tmdb_api_key: str
    omdb_api_key: str | None
    imdb_ratings_csv_path: str
    region: str
    language: str
    with_original_langs: List[str]
    subs_include: List[str]
    tmdb_pages_movie: int
    tmdb_pages_tv: int
    max_catalog: int
    include_tv_seasons: bool
    skip_window_days: int
    critic_weight: float
    audience_weight: float
    novelty_pressure: float
    commitment_cost_scale: float
    cache_dir: str
    cache_ttl_secs: int

def from_env() -> Config:
    region = _env("REGION", "US")
    language = _env("LANGUAGE", "en-US")
    orig_langs = _env("ORIGINAL_LANGS", "en")
    with_original_langs = [s.strip() for s in (orig_langs or "").split(",") if s.strip()]

    subs_raw = _env("SUBS_INCLUDE","netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus")
    subs_include = [s.strip() for s in (subs_raw or "").split(",") if s.strip()]

    tmdb_pages_movie = _env_int("TMDB_PAGES_MOVIE", 200)
    tmdb_pages_tv    = _env_int("TMDB_PAGES_TV", 200)
    max_catalog      = _env_int("MAX_CATALOG", 20000)

    include_tv_seasons = _env_bool("INCLUDE_TV_SEASONS", True)
    skip_window_days   = _env_int("SKIP_WINDOW_DAYS", 4)

    critic_weight = float(_env("CRITIC_WEIGHT", "0.25") or "0.25")
    audience_weight = float(_env("AUDIENCE_WEIGHT", "0.75") or "0.75")
    novelty_pressure = float(_env("NOVELTY_PRESSURE", "0.15") or "0.15")
    commitment_cost_scale = float(_env("COMMITMENT_COST_SCALE", "1.0") or "1.0")

    cache_dir = _env("CACHE_DIR", "data/cache")
    cache_ttl_secs = _env_int("CACHE_TTL_SECS", 7 * 24 * 3600)

    return Config(
        tmdb_api_key=os.environ["TMDB_API_KEY"],
        omdb_api_key=_env("OMDB_API_KEY"),
        imdb_ratings_csv_path=_env("IMDB_RATINGS_CSV_PATH", "data/ratings.csv"),
        region=region or "US",
        language=language or "en-US",
        with_original_langs=with_original_langs,
        subs_include=subs_include,
        tmdb_pages_movie=tmdb_pages_movie,
        tmdb_pages_tv=tmdb_pages_tv,
        max_catalog=max_catalog,
        include_tv_seasons=include_tv_seasons,
        skip_window_days=skip_window_days,
        critic_weight=critic_weight,
        audience_weight=audience_weight,
        novelty_pressure=novelty_pressure,
        commitment_cost_scale=commitment_cost_scale,
        cache_dir=cache_dir or "data/cache",
        cache_ttl_secs=cache_ttl_secs,
    )