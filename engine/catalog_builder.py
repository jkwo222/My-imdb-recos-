# engine/catalog_builder.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
import csv

from .cache import load_state, save_state, tmdb_providers_cached, ensure_dirs
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
            year = int(start_year) if start_year and str(start_year).isdigit() else None
        except Exception:
            year = None
        raw_genres = row.get("genres")
        genres = [g for g in (raw_genres.split(",") if raw_genres else []) if g and g != r"\N"]
        # Normalize type to 'movie' or 'tv'
        norm_type = "movie"
        if title_type and title_type.startswith("tv") and title_type != "tvMovie":
            norm_type = "tv"
        items.append({
            "tconst": tconst,
            "title": row.get("primaryTitle") or row.get("originalTitle"),
            "type": norm_type,
            "year": year,
            "genres": genres,
            "imdb_rating": row.get("imdb_rating"),
        })
    return items

# --- Providers via TMDB (cached) ---------------------------------------------

def _providers_for_item(it: Dict[str,Any], api_key: str, region: str) -> List[str]:
    """
    Fetch (cached) provider names for this item, resolving TMDB id on the fly when missing.
    """
    tmdb_id = it.get("tmdb_id")
    mtype = (it.get("tmdb_media_type") or it.get("type") or "movie")
    # Resolve & cache providers (uses imdb tconst/title/year hints if tmdb_id is absent)
    prov = tmdb_providers_cached(
        int(tmdb_id) if tmdb_id else None,
        api_key,
        "movie" if mtype == "movie" else "tv",
        imdb_tconst=it.get("tconst"),
        title=it.get("title"),
        year=it.get("year"),
    )
    if not prov or "results" not in prov:
        return []
    r = prov["results"].get(region.upper()) or {}
    flatrate = [p.get("provider_name") for p in r.get("flatrate", []) if p.get("provider_name")]
    ads = [p.get("provider_name") for p in r.get("ads", []) if p.get("provider_name")]
    free = [p.get("provider_name") for p in r.get("free", []) if p.get("provider_name")]
    return sorted(set(flatrate + ads + free))

# --- User evidence ------------------------------------------------------------

def _load_user_profile(env: Dict[str,str]) -> Dict[str,Dict]:
    """
    Loads and merges local ratings.csv and (optionally) your public IMDb ratings via IMDB_USER_ID.
    The returned dict is keyed by tconst with fields like {"my_rating": 8.0, ...}.
    """
    local = load_ratings_csv()  # data/user/ratings.csv
    remote: List[Dict[str,str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        try:
            remote = fetch_user_ratings_web(uid)
        except Exception:
            remote = []
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

# --- Public API ---------------------------------------------------------------

def ensure_imdb_cache() -> None:
    """
    Your workflow already downloads IMDb TSVs; this function asserts they exist.
    """
    missing = [p for p in (IMDB_BASICS, IMDB_RATINGS) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing IMDb TSVs: {[str(p) for p in missing]}")

def build_catalog(env: Dict[str,str]) -> List[Dict[str,Any]]:
    """
    Returns the filtered, enriched items and writes assistant_feed.json.
    Persists enrichment across runs so the pool grows and the mapping is reused.
    """
    ensure_dirs()
    ensure_imdb_cache()

    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    items = _merge_basics_ratings(basics, ratings)

    # Bring forward prior run enrichment (tmdb_id mapping, providers, etc.)
    persistent = load_state("persistent_pool", default={"items": {}})
    known_items: Dict[str,Any] = dict(persistent.get("items") or {})

    # Merge known enrichment forward into current items
    for it in items:
        k = it["tconst"]
        if k in known_items:
            prev = known_items[k]
            # Only fill blanks; don't clobber newly-parsed fields
            for kk, vv in prev.items():
                if kk not in it or it[kk] in (None, "", [], {}):
                    it[kk] = vv

    # Provider filter setup
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

        # Respect subscription filter, if provided
        if subs:
            low = [p.lower() for p in provs]
            keep = any(any(s in p for p in low) for s in subs)
            if not keep:
                continue

        filtered.append(it)

    # Write out feed used by later stages
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assistant_feed.json").write_text(
        json.dumps(filtered, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Persist back anything new we learned for next run
    for it in filtered:
        known_items[it["tconst"]] = {
            "tmdb_id": it.get("tmdb_id"),
            "tmdb_media_type": it.get("tmdb_media_type") or ("movie" if (it.get("type") == "movie") else "tv"),
            "providers": it.get("providers"),
            "genres": it.get("genres"),
            "title": it.get("title"),
            "type": it.get("type"),
            "year": it.get("year"),
            "imdb_rating": it.get("imdb_rating"),
        }

    save_state("persistent_pool", {"items": known_items})

    return filtered