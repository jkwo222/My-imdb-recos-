# engine/catalog_builder.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
import csv
import os

from .imdb_sync import (
    load_ratings_csv,
    fetch_user_ratings_web,
    merge_user_sources,
    to_user_profile,
)
from .tmdb_detail import enrich_items_with_tmdb  # assumes you already added this file

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
OUT_DIR = ROOT / "data" / "out" / "latest"
IMDB_CACHE = CACHE_DIR / "imdb"
IMDB_CACHE.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

IMDB_BASICS = IMDB_CACHE / "title.basics.tsv"
IMDB_RATINGS = IMDB_CACHE / "title.ratings.tsv"

# --- IMDb TSV loading (if present) -------------------------------------------

def _load_tsv(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            rows.append(r)
    return rows

def _merge_basics_ratings(basics: List[Dict[str, str]], ratings: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    by_id = {r["tconst"]: r for r in basics if r.get("tconst")}
    for r in ratings:
        t = r.get("tconst")
        if not t or t not in by_id:
            continue
        by_id[t]["imdb_rating"] = r.get("averageRating")
        by_id[t]["numVotes"] = r.get("numVotes")
    items: List[Dict[str, Any]] = []
    for tconst, row in by_id.items():
        title_type = row.get("titleType")
        if title_type not in {"movie", "tvSeries", "tvMiniSeries"}:
            continue
        start_year = row.get("startYear") or row.get("year")
        try:
            year = int(start_year) if start_year and str(start_year).isdigit() else None
        except Exception:
            year = None
        genres = [g for g in (row.get("genres", "").split(",") if row.get("genres") else []) if g and g != r"\N"]
        items.append({
            "tconst": tconst,
            "title": row.get("primaryTitle") or row.get("originalTitle"),
            "type": "tvSeries" if (title_type or "").startswith("tv") and title_type != "tvMovie" else "movie",
            "year": year,
            "genres": genres,
            "imdb_rating": row.get("imdb_rating"),
        })
    return items

# --- User evidence ------------------------------------------------------------

def _load_user_profile(env: Dict[str, str]) -> Dict[str, Dict]:
    """
    Returns profile map {tconst: {my_rating, rated_at}} by merging local CSV
    and (if IMDB_USER_ID is present) scraped IMDb user ratings.
    """
    local = load_ratings_csv()  # data/user/ratings.csv
    remote: List[Dict[str, Any]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

# --- Public API ---------------------------------------------------------------

def build_catalog(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Builds filtered, enriched items and writes:
      - data/out/latest/assistant_feed.json (raw filtered list used downstream)
      - data/out/latest/run_meta.json (metadata/telemetry for summary)
    Resilient: if IMDb TSVs are missing, starts from an empty list but still writes outputs.
    """
    # Load IMDb TSVs if present
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    used_imdb = bool(basics and ratings)
    if used_imdb:
        items = _merge_basics_ratings(basics, ratings)
    else:
        items = []  # may still get enrichment via tmdb_detail fallback in upstream steps

    # User profile (for exclusion)
    profile = _load_user_profile(env)
    exclude_ids = set(profile.keys()) if profile else set()

    # Enrich with TMDB (adds imdb_id when discover-based, providers, etc.)
    region = (env.get("REGION") or "US").upper()
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key:
        enrich_items_with_tmdb(items, api_key=api_key, region=region)

    # Exclusion pass: remove anything in your profile (seen/rated)
    pre_exclude = len(items)
    kept_after_exclude: List[Dict[str, Any]] = []
    excluded_count = 0
    for it in items:
        tid = it.get("tconst") or it.get("imdb_id")
        if tid and str(tid) in exclude_ids:
            excluded_count += 1
            continue
        kept_after_exclude.append(it)

    # Providers filter using human-readable names (as enriched)
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    filtered: List[Dict[str, Any]] = []
    for it in kept_after_exclude:
        provs = it.get("providers") or []
        if subs:
            low = [p.lower() for p in provs]
            keep = any(any(s in p for p in low) for s in subs)
            if not keep:
                continue
        filtered.append(it)

    # Write raw feed
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assistant_feed.json").write_text(
        json.dumps({"items": filtered}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Telemetry
    meta = {
        "using_imdb": used_imdb,
        "candidates_total": len(items),
        "excluded_by_user_lists": excluded_count,
        "kept_after_exclusion": len(kept_after_exclude),
        "kept_after_provider_filter": len(filtered),
        "env": {
            "region": region,
            "original_langs": env.get("ORIGINAL_LANGS", ""),
            "subs_include": subs,
        },
        "profile": {
            "loaded": bool(profile),
            "size": len(profile),
        },
    }
    (OUT_DIR / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return filtered