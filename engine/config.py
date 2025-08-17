# engine/config.py
from __future__ import annotations

import os
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Any


def _getenv_csv(name: str, default_csv: str) -> List[str]:
    raw = os.getenv(name, default_csv) or ""
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class Config:
    # Required
    tmdb_api_key: str

    # Discovery constraints
    watch_region: str = "US"
    original_langs: List[str] = None
    subs_include: List[str] = None

    # Page & rate limits (kept intentionally small)
    discover_pages_movie: int = 10
    discover_pages_tv: int = 10
    tmdb_concurrency: int = 2
    tmdb_min_delay_s: float = 0.20  # ~5 req/sec cap across the job

    # Personalization / ranking
    ratings_csv: str = "data/ratings.csv"
    shortlist_size: int = 50      # shortlist taken from catalog pool before ranking
    show_n: int = 10              # items shown in final feed
    weight_critic: float = 0.25   # blend factor for critic/audience fields if present
    weight_audience: float = 0.25

    # I/O
    cache_dir: str = "data/cache"
    store_path: str = "data/catalog_store.json"
    cursor_path: str = "data/catalog_cursor.json"
    out_dir: str = "data/out"
    latest_dir: str = "data/out/latest"

    # Debug
    debug_log_path: str = "data/debug/runner.log"

    def to_meta(self) -> Dict[str, Any]:
        d = asdict(self)
        d["original_langs"] = self.original_langs or []
        d["subs_include"] = self.subs_include or []
        # don't leak secrets
        d["tmdb_api_key"] = "***"
        return d


def load_config() -> Config:
    """
    Build a Config from environment variables with safe defaults.
    This function is what runner.py imports and what your workflow expects.
    """
    api = os.getenv("TMDB_API_KEY", "").strip()
    if not api:
        raise RuntimeError("TMDB_API_KEY is required (set it in repo secrets / workflow).")

    region = os.getenv("REGION", "US").strip() or "US"
    langs = _getenv_csv("ORIGINAL_LANGS", "en")
    subs = _getenv_csv(
        "SUBS_INCLUDE",
        # sensible default set
        "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",
    )

    # Optional tuning
    pages_movie = int(os.getenv("DISCOVER_PAGES_MOVIE", "10"))
    pages_tv = int(os.getenv("DISCOVER_PAGES_TV", "10"))
    shortlist = int(os.getenv("SHORTLIST_SIZE", "50"))
    shown = int(os.getenv("SHOW_N", "10"))
    w_c = float(os.getenv("WEIGHT_CRITIC", "0.25"))
    w_a = float(os.getenv("WEIGHT_AUDIENCE", "0.25"))
    conc = int(os.getenv("TMDB_CONCURRENCY", "2"))
    delay = float(os.getenv("TMDB_MIN_DELAY_S", "0.20"))

    cfg = Config(
        tmdb_api_key=api,
        watch_region=region,
        original_langs=langs,
        subs_include=subs,
        discover_pages_movie=pages_movie,
        discover_pages_tv=pages_tv,
        shortlist_size=shortlist,
        show_n=shown,
        weight_critic=w_c,
        weight_audience=w_a,
        tmdb_concurrency=conc,
        tmdb_min_delay_s=delay,
    )

    # Ensure folders exist
    for path in [cfg.cache_dir, cfg.out_dir, cfg.latest_dir, os.path.dirname(cfg.debug_log_path)]:
        os.makedirs(path, exist_ok=True)

    # Write a small debug meta for inspection
    try:
        meta_path = os.path.join(cfg.latest_dir, "config_meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(cfg.to_meta(), f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    return cfg