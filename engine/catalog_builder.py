from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
import csv
import os

from .cache import load_state, save_state
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile
from .tmdb_detail import enrich_items_with_tmdb
from .tmdb import discover_movie_page, discover_tv_page, providers_from_env

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
OUT_DIR = ROOT / "data" / "out" / "latest"
IMDB_CACHE = CACHE_DIR / "imdb"
IMDB_CACHE.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

IMDB_BASICS = IMDB_CACHE / "title.basics.tsv"
IMDB_RATINGS = IMDB_CACHE / "title.ratings.tsv"

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
        media_type = "tv" if (title_type or "").startswith("tv") else "movie"
        items.append({
            "tconst": tconst,
            "title": row.get("primaryTitle") or row.get("originalTitle"),
            "type": "tvSeries" if media_type == "tv" else "movie",
            "tmdb_media_type": media_type,
            "year": year,
            "genres": genres,
            "imdb_rating": row.get("imdb_rating"),
            "source": "imdb_tsv",
        })
    return items

def _load_user_profile(env: Dict[str, str]):
    local = load_ratings_csv()
    remote: List[Dict[str, str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

def _discover_pool(env: Dict[str, str]) -> List[Dict[str, Any]]:
    region = (env.get("REGION") or "US").upper()
    subs = env.get("SUBS_INCLUDE", "")
    providers = providers_from_env(subs, region=region)
    langs = env.get("ORIGINAL_LANGS") or None
    pages = int(env.get("DISCOVER_PAGES") or "2")

    pool: List[Dict[str, Any]] = []
    # Movies
    for p in range(1, pages + 1):
        items, _ = discover_movie_page(p, region=region, provider_ids=providers, original_langs=langs)
        for it in items:
            pool.append({
                "title": it.get("title") or it.get("name"),
                "tmdb_id": it.get("id"),
                "tmdb_media_type": "movie",
                "year": (it.get("release_date") or "0000")[:4].isdigit() and int((it.get("release_date") or "0000")[:4]) or None,
                "genres": [],  # will fill via tmdb_detail enrichment
                "source": "tmdb_discover",
            })
    # TV
    for p in range(1, pages + 1):
        items, _ = discover_tv_page(p, region=region, provider_ids=providers, original_langs=langs)
        for it in items:
            pool.append({
                "title": it.get("name") or it.get("title"),
                "tmdb_id": it.get("id"),
                "tmdb_media_type": "tv",
                "year": (it.get("first_air_date") or "0000")[:4].isdigit() and int((it.get("first_air_date") or "0000")[:4]) or None,
                "genres": [],
                "source": "tmdb_discover",
            })
    return pool

def build_catalog(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Build a candidate pool using:
      - IMDb TSVs (if present)
      - TMDB Discover (always)
      - Carry-forward enrichment via persistent pool
    Enrich via tmdb_detail, filter by providers, persist, and write telemetry.
    """
    # IMDb TSVs (optional)
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    used_imdb = bool(basics and ratings)
    imdb_items = _merge_basics_ratings(basics, ratings) if used_imdb else []

    # TMDB discover (always)
    tmdb_items = _discover_pool(env)

    # stitch in prior run enrichment
    persistent = load_state("persistent_pool", default={"items": {}})
    known_items: Dict[str, Any] = persistent.get("items", {})

    # Combine and carry forward enrichment
    by_key: Dict[str, Dict[str, Any]] = {}
    def _key(it: Dict[str, Any]) -> str:
        if it.get("tconst"):
            return f"imdb:{it['tconst']}"
        if it.get("tmdb_id"):
            return f"tmdb:{it['tmdb_media_type']}:{it['tmdb_id']}"
        return f"title:{(it.get('title') or '').strip().lower()}:{it.get('year') or ''}"

    for it in (imdb_items + tmdb_items):
        by_key[_key(it)] = it

    items = list(by_key.values())

    # Carry forward known enrichment
    for it in items:
        # prefer imdb key if present else tmdb key
        tconst = it.get("tconst")
        if tconst and tconst in known_items:
            carry = known_items[tconst]
        else:
            carry = None
            for k, v in known_items.items():
                if v.get("tmdb_id") == it.get("tmdb_id") and v.get("tmdb_media_type") == it.get("tmdb_media_type"):
                    carry = v
                    break
        if carry:
            for ck, cv in carry.items():
                if ck not in it or it[ck] in (None, "", [], {}):
                    it[ck] = cv

    # Enrich with TMDB details (genres, providers, imdb tconst resolution)
    region = (env.get("REGION") or "US").upper()
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key:
        enrich_items_with_tmdb(items, api_key=api_key, region=region)

    # Providers include filter
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

    # Persist enrichment
    for it in filtered:
        # index by tconst if known else tmdb composite
        key = it.get("tconst") or f"{it.get('tmdb_media_type','?')}:{it.get('tmdb_id')}"
        if not key:
            continue
        known_items[key] = {
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

    # Telemetry
    stats = {
        "using_imdb": used_imdb,
        "imdb_items": len(imdb_items),
        "tmdb_items": len(tmdb_items),
        "combined_unique": len(items),
        "after_provider_filter": len(filtered),
        "note": "IMDb TSV + TMDB discover" if used_imdb else "TMDB discover only",
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "catalog_stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    # Write raw feed
    (OUT_DIR / "assistant_feed.json").write_text(json.dumps(filtered, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT_DIR / "run_meta.json").write_text(json.dumps({"using_imdb": used_imdb}, ensure_ascii=False, indent=2), encoding="utf-8")
    return filtered