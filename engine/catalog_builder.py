# engine/catalog_builder.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
import csv

from .cache import load_state, save_state
from .imdb_sync import (
    load_ratings_csv,
    fetch_user_ratings_web,
    merge_user_sources,
    to_user_profile,
)
from .tmdb_detail import enrich_items_with_tmdb
from .tmdb import (
    discover_movie_page,
    discover_tv_page,
    providers_from_env,
)

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
OUT_DIR = ROOT / "data" / "out" / "latest"
IMDB_CACHE = CACHE_DIR / "imdb"
IMDB_CACHE.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

IMDB_BASICS = IMDB_CACHE / "title.basics.tsv"
IMDB_RATINGS = IMDB_CACHE / "title.ratings.tsv"

# ---------- IMDb TSV helpers --------------------------------------------------

def _load_tsv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows.extend(reader)
    return rows

def _merge_basics_ratings(
    basics: List[Dict[str, str]],
    ratings: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    by_id = {r["tconst"]: r for r in basics if r.get("tconst")}
    for r in ratings:
        t = r.get("tconst")
        if t and t in by_id:
            by_id[t]["imdb_rating"] = r.get("averageRating")
            by_id[t]["numVotes"] = r.get("numVotes")

    items: List[Dict[str, Any]] = []
    for tconst, row in by_id.items():
        title_type = (row.get("titleType") or "").strip()
        if title_type not in {"movie", "tvSeries", "tvMiniSeries"}:
            continue
        start_year = row.get("startYear") or row.get("year")
        try:
            year = int(start_year) if start_year and str(start_year).isdigit() else None
        except Exception:
            year = None
        genres = [
            g
            for g in (row.get("genres", "").split(",") if row.get("genres") else [])
            if g and g != r"\N"
        ]
        items.append(
            {
                "tconst": tconst,
                "title": row.get("primaryTitle") or row.get("originalTitle"),
                "type": "tvSeries"
                if title_type.startswith("tv") and title_type != "tvMovie"
                else "movie",
                "year": year,
                "genres": genres,
                "imdb_rating": row.get("imdb_rating"),
            }
        )
    return items

# ---------- TMDB discover (always on) ----------------------------------------

def _seed_from_tmdb(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Always pull a few TMDB discover pages to keep the pool fresh.
    Enrichment will add IMDb id, genres, vote averages and provider names later.
    """
    api_key = env.get("TMDB_API_KEY") or ""
    if not api_key:
        return []

    region = (env.get("REGION") or "US").upper()
    original_langs = (env.get("ORIGINAL_LANGS") or "").strip() or None
    subs = env.get("SUBS_INCLUDE") or ""
    provider_ids = providers_from_env(subs, region=region)

    # Tune these if you want more/less discovery per run
    MOVIE_PAGES = int(env.get("DISCOVER_MOVIE_PAGES", "3"))
    TV_PAGES = int(env.get("DISCOVER_TV_PAGES", "3"))

    items: List[Dict[str, Any]] = []

    # Movies
    for p in range(1, MOVIE_PAGES + 1):
        page_items, _ = discover_movie_page(
            p, region=region, provider_ids=provider_ids, original_langs=original_langs
        )
        for r in page_items:
            title = r.get("title") or r.get("original_title")
            year = None
            rd = (r.get("release_date") or "")[:4]
            if rd.isdigit():
                try:
                    year = int(rd)
                except Exception:
                    year = None
            items.append(
                {
                    "tmdb_id": r.get("id"),
                    "tmdb_media_type": "movie",
                    "title": title,
                    "year": year,
                    "type": "movie",
                }
            )

    # TV
    for p in range(1, TV_PAGES + 1):
        page_items, _ = discover_tv_page(
            p, region=region, provider_ids=provider_ids, original_langs=original_langs
        )
        for r in page_items:
            title = r.get("name") or r.get("original_name")
            year = None
            fd = (r.get("first_air_date") or "")[:4]
            if fd.isdigit():
                try:
                    year = int(fd)
                except Exception:
                    year = None
            items.append(
                {
                    "tmdb_id": r.get("id"),
                    "tmdb_media_type": "tv",
                    "title": title,
                    "year": year,
                    "type": "tvSeries",
                }
            )

    return items

# ---------- Merge helpers -----------------------------------------------------

def _key_for_item(it: Dict[str, Any]) -> Tuple[str, str]:
    """
    Prefer stable IMDb key when present; otherwise use TMDB typed key.
    """
    tconst = str(it.get("tconst") or it.get("imdb_id") or "").strip()
    if tconst:
        return ("imdb", tconst)
    tmdb_id = it.get("tmdb_id")
    media = (it.get("tmdb_media_type") or ("tv" if it.get("type") != "movie" else "movie")).strip()
    if tmdb_id:
        return ("tmdb", f"{media}:{tmdb_id}")
    # Last resort: title+year+type (not great, but avoids total loss)
    return ("title", f"{(it.get('type') or '').lower()}|{(it.get('title') or '').strip()}|{it.get('year') or ''}")

def _merge_sources(*sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for src in sources:
        for it in src:
            key = _key_for_item(it)
            if key not in merged:
                merged[key] = dict(it)
            else:
                # Shallow merge: prefer existing non-empty, fill blanks from new
                cur = merged[key]
                for k, v in it.items():
                    if k not in cur or cur[k] in (None, "", [], {}):
                        cur[k] = v
    return list(merged.values())

# ---------- User profile (available for future scoring) ----------------------

def _load_user_profile(env: Dict[str, str]) -> Dict[str, Dict]:
    local = load_ratings_csv()  # data/user/ratings.csv (may be empty)
    remote: List[Dict[str, str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

# ---------- Public API --------------------------------------------------------

def build_catalog(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Build the daily candidate pool:
      1) Load IMDb TSV items (if TSVs exist).
      2) Load persistent pool from prior runs.
      3) ALWAYS seed fresh items from TMDB Discover.
      4) Merge + enrich via TMDB (adds imdb_id, vote averages, genres, providers).
      5) Filter by SUBS_INCLUDE provider names.
      6) Persist back a slim map so the pool grows over time.
      7) Write assistant_feed.json and run_meta.json.
    """
    # (1) IMDb TSVs (optional)
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    used_imdb = bool(basics and ratings)
    items_imdb: List[Dict[str, Any]] = _merge_basics_ratings(basics, ratings) if used_imdb else []

    # (2) Persistent pool
    persistent = load_state("persistent_pool", default={"items": {}})
    known_items_map: Dict[str, Any] = persistent.get("items", {})
    items_persist: List[Dict[str, Any]] = []
    for k, v in known_items_map.items():
        items_persist.append(
            {
                "tconst": k,
                "title": v.get("title"),
                "type": v.get("type") or "movie",
                "year": v.get("year"),
                "genres": v.get("genres") or [],
                "imdb_rating": v.get("imdb_rating"),
                "tmdb_id": v.get("tmdb_id"),
                "tmdb_media_type": v.get("tmdb_media_type"),
                "providers": v.get("providers") or [],
                "imdb_id": v.get("imdb_id") or k,  # in case we stored imdb_id instead of tconst
            }
        )

    # (3) Fresh TMDB discover (always)
    items_tmdb = _seed_from_tmdb(env)

    # (4a) Merge sources (IMDb TSVs + persisted + fresh discover)
    items = _merge_sources(items_imdb, items_persist, items_tmdb)

    # (4b) Enrich via TMDB (adds imdb_id/tconst mapping, providers, votes, genres…)
    region = (env.get("REGION") or "US").upper()
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key and items:
        enrich_items_with_tmdb(items, api_key=api_key, region=region)

    # (5) Provider filter using human-readable provider names
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

    # (6) Persist back a slim record so the pool grows
    for it in filtered:
        # prefer tconst / imdb_id as map key if present
        tconst = (it.get("tconst") or it.get("imdb_id") or "").strip()
        key = tconst if tconst else f"{(it.get('tmdb_media_type') or 'movie')}:{it.get('tmdb_id')}"
        if not key:
            continue
        known_items_map[key] = {
            "tmdb_id": it.get("tmdb_id"),
            "tmdb_media_type": it.get("tmdb_media_type"),
            "providers": it.get("providers"),
            "genres": it.get("genres"),
            "title": it.get("title"),
            "type": it.get("type"),
            "year": it.get("year"),
            "imdb_rating": it.get("imdb_rating"),
            "imdb_id": it.get("imdb_id") or it.get("tconst"),
        }
    save_state("persistent_pool", {"items": known_items_map})

    # (7) Write outputs
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assistant_feed.json").write_text(
        json.dumps({"items": filtered}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    meta = {
        "telemetry": {
            "using_imdb": used_imdb,
            "counts": {
                "imdb_tsv": len(items_imdb),
                "persist_prior": len(items_persist),
                "tmdb_discover": len(items_tmdb),
                "merged_total_before_filter": len(items),
                "kept_after_filter": len(filtered),
            },
            "region": region,
            "original_langs": env.get("ORIGINAL_LANGS") or None,
            "subs_include": subs,
        }
    }
    (OUT_DIR / "run_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return filtered