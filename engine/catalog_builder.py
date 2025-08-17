# engine/catalog_builder.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
import csv
import time

from .cache import load_state, save_state
from .imdb_sync import (
    load_ratings_csv,
    fetch_user_ratings_web,
    merge_user_sources,
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
                "_origin_imdb_tsv": True,   # mark source
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
                    "_origin_tmdb_discover": True,  # mark source
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
                    "_origin_tmdb_discover": True,  # mark source
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
    # Last resort: title+year+type (not ideal, but avoids total loss)
    return ("title", f"{(it.get('type') or '').lower()}|{(it.get('title') or '').strip()}|{it.get('year') or ''}")


def _merge_sources(*sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for src in sources:
        for it in src:
            key = _key_for_item(it)
            if key not in merged:
                merged[key] = dict(it)
            else:
                cur = merged[key]
                # Shallow merge: prefer existing non-empty, fill blanks from new
                for k, v in it.items():
                    if k not in cur or cur[k] in (None, "", [], {}):
                        cur[k] = v
                # keep source flags if any
                for flag in ("_origin_imdb_tsv", "_origin_tmdb_discover", "_origin_persist"):
                    if it.get(flag):
                        cur[flag] = True
    return list(merged.values())


# ---------- Public API --------------------------------------------------------

def build_catalog(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Build the daily candidate pool with telemetry.
    """
    t0 = time.time()

    # (A) IMDb TSVs (optional)
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    used_imdb = bool(basics and ratings)
    items_imdb: List[Dict[str, Any]] = _merge_basics_ratings(basics, ratings) if used_imdb else []

    # (B) Persistent pool
    persistent = load_state("persistent_pool", default={"items": {}})
    known_items_map: Dict[str, Any] = persistent.get("items", {})
    items_persist: List[Dict[str, Any]] = []
    for k, v in known_items_map.items():
        row = {
            "tconst": k if k.startswith("tt") else v.get("imdb_id"),
            "title": v.get("title"),
            "type": v.get("type") or "movie",
            "year": v.get("year"),
            "genres": v.get("genres") or [],
            "imdb_rating": v.get("imdb_rating"),
            "tmdb_id": v.get("tmdb_id"),
            "tmdb_media_type": v.get("tmdb_media_type"),
            "providers": v.get("providers") or [],
            "imdb_id": v.get("imdb_id") or k,
            "_origin_persist": True,  # mark source
        }
        items_persist.append(row)

    # (C) Fresh TMDB discover (always)
    items_tmdb = _seed_from_tmdb(env)

    # (D) Merge all sources
    items = _merge_sources(items_imdb, items_persist, items_tmdb)

    # (E) Enrich via TMDB (adds imdb_id/tconst mapping, providers, votes, genres…)
    region = (env.get("REGION") or "US").upper()
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key and items:
        enrich_items_with_tmdb(items, api_key=api_key, region=region)

    # (F) Exclude items from user's lists (ratings.csv + public IMDb list)
    #     We count each exclusion reason separately.
    local_rows = load_ratings_csv()  # may be empty
    uid = (env.get("IMDB_USER_ID") or "").strip()
    remote_rows = fetch_user_ratings_web(uid) if uid else []

    # sets of imdb ids/tconsts
    local_ids = {str(r.get("tconst") or r.get("imdb_id")).strip() for r in local_rows if (r.get("tconst") or r.get("imdb_id"))}
    remote_ids = {str(r.get("tconst") or r.get("imdb_id")).strip() for r in remote_rows if (r.get("tconst") or r.get("imdb_id"))}

    excl_local = 0
    excl_remote = 0
    remaining_after_user = []
    for it in items:
        tconst = (it.get("tconst") or it.get("imdb_id") or "").strip()
        if tconst and tconst in local_ids:
            excl_local += 1
            continue
        if tconst and tconst in remote_ids:
            excl_remote += 1
            continue
        remaining_after_user.append(it)
    items = remaining_after_user

    # (G) Provider filter using human-readable provider names
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    excl_providers = 0
    filtered: List[Dict[str, Any]] = []
    for it in items:
        provs = it.get("providers") or []
        if subs:
            low = [p.lower() for p in provs]
            keep = any(any(s in p for p in low) for s in subs)
            if not keep:
                excl_providers += 1
                continue
        filtered.append(it)

    # (H) Persist back a slim record so the pool grows
    pre_keys = set(known_items_map.keys())
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
    post_keys = set(known_items_map.keys())
    newly_added_to_pool = len(post_keys - pre_keys)

    # (I) Count source usage among final candidates
    src_counts = {
        "from_imdb_tsv": sum(1 for it in filtered if it.get("_origin_imdb_tsv")),
        "from_tmdb_discover": sum(1 for it in filtered if it.get("_origin_tmdb_discover")),
        "from_persist": sum(1 for it in filtered if it.get("_origin_persist")),
    }

    # (J) Write outputs
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assistant_feed.json").write_text(
        json.dumps({"items": filtered}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    meta = {
        "telemetry": {
            "region": region,
            "original_langs": env.get("ORIGINAL_LANGS") or None,
            "subs_include": subs,
            "counts": {
                "imdb_tsv_loaded": len(items_imdb),
                "persist_loaded": len(items_persist),
                "tmdb_discover_loaded": len(items_tmdb),
                "merged_total_before_user_filter": len(_merge_sources(items_imdb, items_persist, items_tmdb)),
                "excluded_user_ratings_csv": excl_local,
                "excluded_public_imdb_list": excl_remote,
                "after_user_filters": len(items),
                "excluded_by_provider_filter": excl_providers,
                "kept_after_filter": len(filtered),
            },
            "final_source_mix": src_counts,
            "pool": {
                "pool_size_after_save": len(known_items_map),
                "newly_added_this_run": newly_added_to_pool,
                "cached_reused_this_run": len(filtered) - newly_added_to_pool if len(filtered) >= newly_added_to_pool else 0,
            },
            "using_imdb_tsv": used_imdb,
            "timing_sec": round(time.time() - t0, 2),
        }
    }
    (OUT_DIR / "run_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Clean transient flags so downstream consumers don’t rely on them
    for it in filtered:
        for flag in ("_origin_imdb_tsv", "_origin_tmdb_discover", "_origin_persist"):
            if flag in it:
                del it[flag]

    return filtered