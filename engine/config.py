# engine/config.py
import os
from dataclasses import dataclass
from typing import List

@dataclass
class Config:
    # Core env
    tmdb_api_key: str = os.getenv("TMDB_API_KEY", "")
    omdb_api_key: str = os.getenv("OMDB_API_KEY", "")

    # Discovery / filters
    region: str = os.getenv("REGION", "US")
    with_original_language: str = os.getenv("ORIGINAL_LANGS", "en")
    provider_names: List[str] = tuple(
        p.strip() for p in os.getenv(
            "SUBS_INCLUDE",
            "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"
        ).split(",") if p.strip()
    )

    # Pagination
    tmdb_pages_movie: int = int(os.getenv("TMDB_PAGES_MOVIE", "24"))
    tmdb_pages_tv: int = int(os.getenv("TMDB_PAGES_TV", "24"))

    # Engine controls
    include_tv_seasons: bool = os.getenv("INCLUDE_TV_SEASONS", "true").lower() == "true"
    max_catalog: int = int(os.getenv("MAX_CATALOG", "10000"))
    skip_window_days: int = int(os.getenv("SKIP_WINDOW_DAYS", "4"))

    # IMDb ingest
    imdb_user_id: str = os.getenv("IMDB_USER_ID", "")
    imdb_ratings_csv_path: str = os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

    # Scoring weights
    critic_weight: float = float(os.getenv("CRITIC_WEIGHT", "0.25"))
    audience_weight: float = float(os.getenv("AUDIENCE_WEIGHT", "0.75"))