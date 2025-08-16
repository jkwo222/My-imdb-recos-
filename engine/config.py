from __future__ import annotations
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

def _as_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _as_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _as_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

def _as_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    return [x.strip() for x in raw.split(",") if x.strip()]

@dataclass
class Config:
    # API keys
    tmdb_api_key: str = field(default_factory=lambda: os.getenv("TMDB_API_KEY", "").strip())
    omdb_api_key: str = field(default_factory=lambda: os.getenv("OMDB_API_KEY", "").strip())

    # User data
    imdb_user_id: str = field(default_factory=lambda: os.getenv("IMDB_USER_ID", "").strip())
    imdb_ratings_csv_path: str = field(default_factory=lambda: os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv"))

    # Discovery filters
    region: str = field(default_factory=lambda: os.getenv("REGION", "US"))
    language: str = field(default_factory=lambda: os.getenv("LANGUAGE", "en-US"))
    with_original_langs: List[str] = field(default_factory=lambda: _as_list("ORIGINAL_LANGS", ["en"]))

    # Subscriptions (strict allowlist)
    subs_include: List[str] = field(default_factory=lambda: _as_list(
        "SUBS_INCLUDE",
        ["netflix","prime_video","hulu","max","disney_plus","apple_tv_plus","peacock","paramount_plus"]
    ))

    # Paging / rotation
    tmdb_pages_movie: int = field(default_factory=lambda: _as_int("TMDB_PAGES_MOVIE", 40))
    tmdb_pages_tv: int = field(default_factory=lambda: _as_int("TMDB_PAGES_TV", 40))
    rotate_minutes: int = field(default_factory=lambda: _as_int("TMDB_ROTATE_MINUTES", 15))

    # Pools & filters
    max_catalog: int = field(default_factory=lambda: _as_int("MAX_CATALOG", 12000))
    include_tv_seasons: bool = field(default_factory=lambda: _as_bool("INCLUDE_TV_SEASONS", True))
    skip_window_days: int = field(default_factory=lambda: _as_int("SKIP_WINDOW_DAYS", 4))

    # Scoring weights
    critic_weight: float = field(default_factory=lambda: _as_float("CRITIC_WEIGHT", 0.25))
    audience_weight: float = field(default_factory=lambda: _as_float("AUDIENCE_WEIGHT", 0.75))
    novelty_pressure: float = field(default_factory=lambda: _as_float("NOVELTY_PRESSURE", 0.15))
    commitment_cost_scale: float = field(default_factory=lambda: _as_float("COMMITMENT_COST_SCALE", 1.0))

    # Paths
    cache_dir: str = field(default="data/cache")
    out_dir: str = field(default="data/out")
    debug_dir: str = field(default="data/debug")

    def rotation_slot(self) -> int:
        """Changes every rotate_minutes, UTC, so page choices reshuffle frequently."""
        now = datetime.now(timezone.utc)
        if self.rotate_minutes <= 0:
            return int(now.timestamp() // 900)  # fallback: 15 min
        return int(now.timestamp() // (self.rotate_minutes * 60))