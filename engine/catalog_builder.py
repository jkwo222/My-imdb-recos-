# engine/catalog_builder.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
import csv

from .cache import load_state, save_state, tmdb_providers_cached
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
OUT_DIR = ROOT / "data" / "out" / "latest"
IMDB_CACHE = CACHE_DIR / "imdb"
IMDB_CACHE.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

IMDB_BASICS = IMDB_CACHE / "title.basics.tsv"
IMDB_RATINGS = IMDB_CACHE / "title.ratings.tsv"

# --- IMDb TSV loading (already cached by your workflow) ----------------------

def _load_tsv(path: Path) -> List[Dict[str,str]]:
    rows: List[Dict[str,str]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            rows.append(r)
    return rows

def _merge_basics_ratings(basics: List[Dict[str,str]], ratings: List[Dict[str,str]]) -> List[Dict[str,Any]]:
    by_id = {r["tconst"]: r for r in basics if r.get("tconst")}
    for r in ratings:
        t = r.get("tconst")
        if not t or t not in by_id:
            continue
        by_id[t]["imdb_rating"] = r.get("averageRating")
        by_id[t]["numVotes"] = r.get("numVotes")
    items: List[Dict[str,Any]] = []
    for tconst, row in by_id.items():
        title_type = row.get("titleType")
        if title_type not in {"movie", "tvSeries", "tvMiniSeries"}:
            continue
        start_year = row.get("startYear") or row.get("year")
        try:
            year = int(start_year) if start_year and start_year.isdigit() else None
        except Exception:
            year = None
        genres = [g for g in (row.get("genres","").split(",") if row.get("genres") else []) if g and g != r"\N"]
        items.append({
            "tconst": tconst,
            "title": row.get("primaryTitle") or row.get("originalTitle"),
            "type": "tvSeries" if title_type.startswith("tv") and title_type!="tvMovie" else "movie",
            "year": year,
            "genres": genres,
            "imdb_rating": row.get("imdb_rating"),
        })
    return items

# --- Providers via TMDB (cached) ---------------------------------------------

def _providers_for_item(it: Dict[str,Any], api_key: str, region: str) -> List[str]:
    """We expect items to carry 'tmdb_id' and 'tmdb_media_type' if known; if not, return []."""
    tmdb_id = it.get("tmdb_id")
    mtype = it.get("tmdb_media_type") or ("tv" if it["type"]!="movie" else "movie")
    if not tmdb_id or not api_key:
        return []
    prov = tmdb_providers_cached(int(tmdb_id), api_key, mtype)
    if not prov or "results" not in prov:
        return []
    r = prov["results"].get(region.upper()) or {}
    flatrate = [p.get("provider_name") for p in r.get("flatrate", []) if p.get("provider_name")]
    ads = [p.get("provider_name") for p in r.get("ads", []) if p.get("provider_name")]
    return sorted(set(flatrate + ads))

# --- User evidence ------------------------------------------------------------

def _load_user_profile(env: Dict[str,str]) -> Dict[str,Dict]:
    local = load_ratings_csv()  # data/user/ratings.csv
    remote: List[Dict[str,str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

# --- Public API ---------------------------------------------------------------

def ensure_imdb_cache() -> None:
    """
    Your workflow already downloads IMDb TSVs; this function can be used to assert they exist.
    """
    missing = [p for p in (IMDB_BASICS, IMDB_RATINGS) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing IMDb TSVs: {[str(p) for p in missing]}")

def build_catalog(env: Dict[str,str]) -> List[Dict[str,Any]]:
    """
    Returns the filtered, enriched items and writes assistant_feed.json.
    """
    ensure_imdb_cache()

    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    items = _merge_basics_ratings(basics, ratings)

    # stitch in any prior run’s enrichment (tmdb_id mapping, etc.)
    persistent = load_state("persistent_pool", default={"items":{}})
    known_items: Dict[str,Any] = persistent.get("items", {})

    # Merge known enrichment forward
    for it in items:
        k = it["tconst"]
        if k in known_items:
            # carry forward tmdb mappings, prior providers, notes, etc.
            it.update({kk: vv for kk, vv in known_items[k].items() if kk not in it or it[kk] in (None, "", [], {})})

    # Providers filter
    region = (env.get("REGION") or "US").upper()
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    api_key = env.get("TMDB_API_KEY") or ""

    filtered: List[Dict[str,Any]] = []
    for it in items:
        # Resolve providers lazily and cache
        provs = it.get("providers")
        if provs is None:
            provs = _providers_for_item(it, api_key, region)
            it["providers"] = provs
        if subs:
            low = [p.lower() for p in provs]
            keep = any(any(s in p for p in low) for s in subs)
            if not keep:
                continue
        filtered.append(it)

    # Write out feed
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assistant_feed.json").write_text(
        json.dumps(filtered, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Persist back anything new we learned
    for it in filtered:
        known_items[it["tconst"]] = {
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

    return filtered