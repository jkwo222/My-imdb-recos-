# engine/runner.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Dict, Any, List
import json

from .catalog_builder import build_catalog
from .imdb_sync import _load_user_profile as load_user_profile  # reuse
from .personalize import genre_weights_from_profile, apply_personal_score
from .summarize import write_summary_md

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
FEED_JSON = OUT_DIR / "assistant_feed.json"

def main():
    env = dict(os.environ)

    # 1) Build or refresh the catalog with providers filter (writes assistant_feed.json)
    items = build_catalog(env)

    # 2) Pull user evidence (ratings.csv + public IMDb ratings)
    profile = load_user_profile(env)

    # 3) Genre weights from evidence intersecting items we know
    genre_weights = genre_weights_from_profile(items, profile)

    # 4) Apply personalized scoring to items (0–100)
    apply_personal_score(items, genre_weights, base_key="imdb_rating")

    # 5) Write back feed with scores for transparency
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FEED_JSON.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    # 6) Summary markdown (uses genre_weights if present)
    write_summary_md(env, genre_weights=genre_weights, picks_limit=15)

if __name__ == "__main__":
    main()