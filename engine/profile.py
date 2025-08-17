# engine/profile.py
from __future__ import annotations
import csv
import json
from pathlib import Path
from typing import Dict, Any

ROOT = Path(__file__).resolve().parents[1]

# Where we look for your local data:
RATINGS_CSV = ROOT / "data" / "user" / "ratings.csv"
# Optional cache of your public IMDb list (to catch new titles not yet in ratings.csv).
# This is expected to be a JSON Lines file with objects that at least include {"tconst": "tt..."}.
IMDB_LIST_CACHE = ROOT / "data" / "cache" / "imdb" / "public_list.jsonl"


def _load_ratings_csv() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not RATINGS_CSV.exists():
        return out

    with RATINGS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Expected columns: tconst,my_rating (others ignored)
        for row in reader:
            tconst = (row.get("tconst") or "").strip()
            if not tconst:
                continue
            rating = row.get("my_rating")
            try:
                my_rating = float(rating) if rating not in (None, "", "NaN") else None
            except Exception:
                my_rating = None
            out[tconst] = {"my_rating": my_rating}
    return out


def _load_public_list_cache() -> Dict[str, Dict[str, Any]]:
    """
    Pulls tconsts from a local cache of your public IMDb list (no network here).
    If an item isn't in ratings.csv we add it with a neutral rating baseline (6.0)
    so your genre weights start to see it as 'watched/added'.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not IMDB_LIST_CACHE.exists():
        return out

    with IMDB_LIST_CACHE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            tconst = (obj.get("tconst") or "").strip()
            if not tconst:
                continue
            # Neutral baseline if not explicitly rated
            out[tconst] = {"my_rating": 6.0}
    return out


def load_user_profile(env: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """
    Returns a dict keyed by tconst -> {"my_rating": float | None}.

    Sources:
      1) data/user/ratings.csv         (authoritative when present)
      2) data/cache/imdb/public_list.jsonl  (optional; neutral baseline)

    If a tconst appears in both, ratings.csv wins.
    """
    ratings = _load_ratings_csv()
    cached_list = _load_public_list_cache()

    # Merge with ratings priority
    merged = dict(cached_list)
    merged.update(ratings)
    return merged