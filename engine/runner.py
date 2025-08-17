# engine/runner.py
from __future__ import annotations

import os, sys, json, time, pathlib
from typing import List, Dict, Any, Tuple

from rich import print as rprint

# our pipeline modules
from .ratings_ingest import load_user_ratings_combined
from .catalog_builder import build_catalog
from .seen_index import load_seen_index, filter_unseen
from .taste import build_taste, taste_boost_for
from .feed import build_feed

def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]

def _ratings_csv_path() -> str:
    return os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

def _make_seen_filter(csv_path: str):
    idx = load_seen_index(csv_path)  # dict with ids + normalized title/year pairs
    def _apply(pool: List[Dict]) -> List[Dict]:
        return filter_unseen(pool, idx)
    return _apply

def main() -> None:
    rprint("[bold]| catalog:begin[/bold]")

    # 1) Load your IMDb ratings (CSV + optional public page incremental)
    rows, meta = load_user_ratings_combined()
    rprint(f"[green]IMDb ratings loaded[/green] → rows={len(rows)} meta={meta}")

    # 2) Build TMDB/OMDb-enriched catalog
    catalog = build_catalog()
    rprint(f"[green]catalog built[/green] → {len(catalog)} items")

    # 3) Taste profile from your ratings
    taste_profile = build_taste(rows)
    rprint(f"[green]taste profile[/green] → {len(taste_profile)} genres")

    # 4) Seen filter (by CSV + optional public page)
    seen_filter = _make_seen_filter(_ratings_csv_path())

    # 5) Build personalized feed (handles provider filtering, seen, recency, ranking)
    out = build_feed(catalog, seen_filter=seen_filter, taste_profile=taste_profile)

    out_path = pathlib.Path("data/out/daily") / time.strftime("%Y-%m-%d", time.gmtime()) / "assistant_feed.json"
    rprint(f"[bold]| catalog:end[/bold] pool={out['meta']['pool_sizes']['initial']} feed={out['count']} → {out_path}")

if __name__ == "__main__":
    main()