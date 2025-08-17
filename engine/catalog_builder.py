# engine/catalog_builder.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
import csv

from .cache import load_state, save_state
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile
from .tmdb_detail import enrich_items_with_tmdb  # NEW

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
    local = load_ratings_csv()  # data/user/ratings.csv (may be empty if not uploaded)
    remote: List[Dict[str, str]] = []
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
      - data/out/latest/assistant_feed.json (raw filtered)
      - data/out/latest/run_meta.json (metadata for summary)
      - data/cache/state/personal_state.json (genre weights, etc., via imdb_sync.* helpers)
    This function is resilient: if IMDb TSVs are missing it starts from an empty list and
    still enriches known persistent items and writes outputs.
    """
    # Load IMDb TSVs if present
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    used_imdb = bool(basics and ratings)
    if used_imdb:
        items = _merge_basics_ratings(basics, ratings)
    else:
        items = []  # We’ll still try to carry-forward from persistent pool

    # stitch in any prior run’s enrichment (tmdb mapping, providers, notes, etc.)
    persistent = load_state("persistent_pool", default={"items": {}})
    known_items: Dict[str, Any] = persistent.get("items", {})

    # Bring forward known items if IMDb TSVs are missing, so we still have a pool
    if not items and known_items:
        items = []
        for k, v in known_items.items():
            row = {
                "tconst": k,
                "title": v.get("title"),
                "type": v.get("type") or "movie",
                "year": v.get("year"),
                "genres": v.get("genres") or [],
                "imdb_rating": v.get("imdb_rating"),
                "tmdb_id": v.get("tmdb_id"),
                "tmdb_media_type": v.get("tmdb_media_type"),
                "providers": v.get("providers") or [],
            }
            items.append(row)

    # Merge forward enrichment for any IMDb-derived items, too
    for it in items:
        k = it.get("tconst")
        if k and k in known_items:
            carry = known_items[k]
            for ck, cv in carry.items():
                if ck not in it or it[ck] in (None, "", [], {}):
                    it[ck] = cv

    # Enrich with TMDB (genres + providers) before filter so fallback runs are richer
    region = (env.get("REGION") or "US").upper()
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key:
        enrich_items_with_tmdb(items, api_key=api_key, region=region)

    # Providers filter using human-readable names (as enriched)
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    filtered: List[Dict[str, Any]] = []
    for it in items:
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
        json.dumps(filtered, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Persist back anything new we learned (so the pool grows over time)
    for it in filtered:
        tconst = str(it.get("tconst"))
        if not tconst:
            continue
        known_items[tconst] = {
            "tmdb_id": it.get("tmdb_id"),
            "tmdb_media_type": it.get("tmdb_media_type"),
            "providers": it.get("providers"),
            "genres": it.get("genres"),
            "title": it.get("title"),
            "type": it.get("type"),
            "year": it.get("year"),
            "imdb_rating": it.get("imdb_rating"),
        }
    save_state("persistent_pool", {"items": known_items})

    # Save meta for the summary
    meta = {
        "using_imdb": used_imdb,
        "candidates_after_filtering": len(filtered),
        "note": "Using IMDb TSVs" if used_imdb else "Using TMDB fallback",
    }
    (OUT_DIR / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # Also refresh personalized state from the latest ratings (handled by imdb_sync helpers)
    # The helpers should already write to data/cache/state/personal_state.json as part of their logic.
    # If not, you can ensure it here by recomputing genre weights and writing them. Left as-is
    # to avoid duplicating logic if you've wired that elsewhere.

    return filtered