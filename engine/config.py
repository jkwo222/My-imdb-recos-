import os
from dataclasses import dataclass
from typing import List

def _as_list(csv: str) -> List[str]:
    return [x.strip() for x in csv.split(",") if x.strip()]

@dataclass(frozen=True)
class Config:
    tmdb_api_key: str
    omdb_api_key: str | None
    imdb_user_id: str | None
    imdb_ratings_csv_path: str | None

    region: str
    language: str
    with_original_language: str

    # Providers by *name* (we map to TMDB IDs at runtime and cache)
    subs_include: List[str]

    # Rotate sampling every N minutes (drives page variance)
    rotate_minutes: int

    # How many pages to SAMPLE each run (varies by slot)
    sample_pages_movie: int
    sample_pages_tv: int

    # How many pages to FILL (sequential crawl) each run
    fill_pages_movie: int
    fill_pages_tv: int

    # Cache settings for discover endpoints
    enable_discover_cache: bool
    discover_cache_ttl_min: int  # short TTL keeps lists fresh-ish but avoids hammering

    # Cap the combined pool if desired (0 = unlimited)
    max_catalog: int

    include_tv_seasons: bool
    skip_window_days: int

    cache_dir: str = "data/cache"
    debug_dir: str = "data/debug"
    out_dir: str = "data/out"

def load_config() -> Config:
    return Config(
        tmdb_api_key=os.environ.get("TMDB_API_KEY", "").strip(),
        omdb_api_key=os.environ.get("OMDB_API_KEY"),
        imdb_user_id=os.environ.get("IMDB_USER_ID"),
        imdb_ratings_csv_path=os.environ.get("IMDB_RATINGS_CSV_PATH"),

        region=os.environ.get("REGION", "US"),
        language=os.environ.get("LANGUAGE", "en-US"),
        with_original_language=os.environ.get("ORIGINAL_LANGS", "en"),

        subs_include=_as_list(os.environ.get(
            "SUBS_INCLUDE",
            "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"
        )),

        rotate_minutes=int(os.environ.get("ROTATE_MINUTES", "15")),

        # Bigger rotating sample per run (20 results/page)
        sample_pages_movie=int(os.environ.get("SAMPLE_PAGES_MOVIE", "100")),
        sample_pages_tv=int(os.environ.get("SAMPLE_PAGES_TV", "100")),

        # Sequential fill pages per run (grows local cache toward full catalog)
        fill_pages_movie=int(os.environ.get("FILL_PAGES_MOVIE", "50")),
        fill_pages_tv=int(os.environ.get("FILL_PAGES_TV", "50")),

        enable_discover_cache=os.environ.get("ENABLE_DISCOVER_CACHE", "1") == "1",
        discover_cache_ttl_min=int(os.environ.get("DISCOVER_CACHE_TTL_MIN", "60")),

        max_catalog=int(os.environ.get("MAX_CATALOG", "6000")),
        include_tv_seasons=os.environ.get("INCLUDE_TV_SEASONS", "true").lower() == "true",
        skip_window_days=int(os.environ.get("SKIP_WINDOW_DAYS", "4")),
    )