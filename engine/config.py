# engine/config.py
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import List

def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "").strip() or default)
    except Exception:
        return default

def _get_csv(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    return [p.strip().lower() for p in raw.split(",") if p.strip()]

@dataclass
class Config:
    # API keys & paths
    tmdb_api_key: str = field(default_factory=lambda: os.getenv("TMDB_API_KEY", "").strip())
    omdb_api_key: str = field(default_factory=lambda: os.getenv("OMDB_API_KEY", "").strip())
    imdb_ratings_csv_path: str = field(default_factory=lambda: os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv"))
    out_dir: str = field(default_factory=lambda: os.getenv("OUT_DIR", "data/out/daily"))
    cache_dir: str = field(default_factory=lambda: os.getenv("CACHE_DIR", "data/cache/tmdb"))
    state_dir: str = field(default_factory=lambda: os.getenv("STATE_DIR", "data/runtime"))

    # Market / language / availability
    region: str = field(default_factory=lambda: os.getenv("REGION", "US"))
    original_langs: List[str] = field(default_factory=lambda: _get_csv("ORIGINAL_LANGS", ["en"]))
    subs_include: List[str] = field(default_factory=lambda: _get_csv(
        "SUBS_INCLUDE",
        ["netflix","prime_video","hulu","max","disney_plus","apple_tv_plus","peacock","paramount_plus"]
    ))
    include_tv_seasons: bool = field(default_factory=lambda: _get_bool("INCLUDE_TV_SEASONS", True))

    # TMDB discovery volume & rotation
    tmdb_pages_movie: int = field(default_factory=lambda: _get_int("TMDB_PAGES_MOVIE", 200))
    tmdb_pages_tv: int = field(default_factory=lambda: _get_int("TMDB_PAGES_TV", 200))
    tmdb_movie_sort: str = field(default_factory=lambda: os.getenv("TMDB_MOVIE_SORT", "vote_count.desc"))
    tmdb_tv_sort: str = field(default_factory=lambda: os.getenv("TMDB_TV_SORT", "vote_count.desc"))
    tmdb_page_cap: int = field(default_factory=lambda: _get_int("TMDB_PAGE_CAP", 500))
    tmdb_rotate_minutes: int = field(default_factory=lambda: _get_int("TMDB_ROTATE_MINUTES", 15))
    tmdb_rotate_step: int | None = field(default=None)
    tmdb_rotate_step_movie: int | None = field(default=None)
    tmdb_rotate_step_tv: int | None = field(default=None)

    # Hydration & limits
    max_catalog: int = field(default_factory=lambda: _get_int("MAX_CATALOG", 8000))
    max_id_hydration: int = field(default_factory=lambda: _get_int("MAX_ID_HYDRATION", 2000))
    skip_window_days: int = field(default_factory=lambda: _get_int("SKIP_WINDOW_DAYS", 4))

    # Scoring
    critic_weight: float = field(default_factory=lambda: _get_float("CRITIC_WEIGHT", 0.30))
    audience_weight: float = field(default_factory=lambda: _get_float("AUDIENCE_WEIGHT", 0.70))
    novelty_pressure: float = field(default_factory=lambda: _get_float("NOVELTY_PRESSURE", 0.15))
    commitment_cost_scale: float = field(default_factory=lambda: _get_float("COMMITMENT_COST_SCALE", 1.0))

    # Logging / heartbeat
    heartbeat_every: int = field(default_factory=lambda: _get_int("HEARTBEAT_EVERY", 50))

    def finalize(self) -> "Config":
        # Ensure pages within cap & sane
        self.tmdb_pages_movie = max(1, min(self.tmdb_pages_movie, self.tmdb_page_cap))
        self.tmdb_pages_tv = max(1, min(self.tmdb_pages_tv, self.tmdb_page_cap))

        # Rotation step defaults (window hop size)
        if self.tmdb_rotate_step is None:
            self.tmdb_rotate_step = None  # per-type fallback below
        if self.tmdb_rotate_step_movie is None:
            self.tmdb_rotate_step_movie = self.tmdb_rotate_step or self.tmdb_pages_movie
        if self.tmdb_rotate_step_tv is None:
            self.tmdb_rotate_step_tv = self.tmdb_rotate_step or self.tmdb_pages_tv

        # Normalize scoring weights
        total = (self.critic_weight or 0) + (self.audience_weight or 0)
        if total <= 0:
            self.critic_weight, self.audience_weight = 0.3, 0.7
        else:
            self.critic_weight /= total
            self.audience_weight /= total

        # Guarantee language list has content and is lowercase
        if not self.original_langs:
            self.original_langs = ["en"]
        self.original_langs = [x.lower() for x in self.original_langs]

        # Provider slugs normalized
        self.subs_include = [x.lower() for x in self.subs_include]

        return self

def load_config() -> Config:
    return Config().finalize()