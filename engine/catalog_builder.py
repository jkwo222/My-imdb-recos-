# engine/catalog_builder.py
from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .tmdb import discover_movie_page, discover_tv_page, providers_from_env
from .tmdb_detail import enrich_items_with_tmdb
from .imdb_sync import (
    load_ratings_csv,
    fetch_user_ratings_web,
    merge_user_sources,
    to_user_profile,
)

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
OUT_DIR = ROOT / "data" / "out" / "latest"
IMDB_CACHE = CACHE_DIR / "imdb"
STATE_DIR = CACHE_DIR / "state"
PERSIST_PATH = STATE_DIR / "persistent_pool.json"

IMDB_CACHE.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

IMDB_BASICS = IMDB_CACHE / "title.basics.tsv"
IMDB_RATINGS = IMDB_CACHE / "title.ratings.tsv"

# ---------- tiny JSON helpers ----------

def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- IMDb TSV loading (optional) ----------

def _load_tsv(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            rows.append(r)
    return rows

def _merge_basics_ratings(
    basics: List[Dict[str, str]], ratings: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
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
                if (title_type or "").startswith("tv") and title_type != "tvMovie"
                else "movie",
                "year": year,
                "genres": genres,
                "imdb_rating": row.get("imdb_rating"),
            }
        )
    return items

# ---------- User profile (ratings.csv + IMDb site) ----------

def _load_user_profile(env: Dict[str, str]) -> Tuple[Dict[str, Dict], bool]:
    """Returns (profile_by_tconst, loaded_ok)."""
    local = load_ratings_csv()  # data/user/ratings.csv if present
    remote: List[Dict[str, str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        try:
            remote = fetch_user_ratings_web(uid)
        except Exception:
            remote = []
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile, bool(profile)

# ---------- Provider filter helpers ----------

SLUG_TO_NAMES = {
    "netflix": {"netflix", "netflix standard with ads"},
    "hulu": {"hulu"},
    "max": {"max", "hbo max", "hbo max amazon channel", "hbo max  amazon channel"},
    "peacock": {"peacock", "peacock premium", "peacock premium plus"},
    "disney_plus": {"disney plus"},
    "prime_video": {
        "amazon prime video",
        "amazon prime video with ads",
        "amazon prime video free with ads",
    },
    "apple_tv_plus": {"apple tv+"},
    "paramount_plus": {
        "paramount plus",
        "paramount+ amazon channel",
        "paramount+ roku premium channel",
        "paramount plus apple tv channel ",
        "paramount+ originals amazon channel",
        "paramount+ mtv amazon channel",
    },
}

def _normalize_provider_names(names: List[str]) -> List[str]:
    return [n.strip().lower() for n in (names or []) if n]

def _subs_name_whitelist(subs_slugs: List[str]) -> List[str]:
    allowed: List[str] = []
    for s in subs_slugs:
        allowed.extend(sorted(SLUG_TO_NAMES.get(s, {s})))
    return sorted(set(a.strip().lower() for a in allowed))

def _passes_subs_filter(item: Dict[str, Any], allowed_provider_names: List[str]) -> bool:
    if not allowed_provider_names:
        return True
    low = set(_normalize_provider_names(item.get("providers") or []))
    if not low:
        return False
    for name in allowed_provider_names:
        if name in low:
            return True
    return False

# ---------- Always Discover fresh titles ----------

def _discover_fresh(env: Dict[str, str]) -> List[Dict[str, Any]]:
    region = (env.get("REGION") or "US").upper()
    original_langs = env.get("ORIGINAL_LANGS") or None
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    provider_ids = providers_from_env(subs, region=region)

    pages = int(float(env.get("DISCOVER_PAGES", "3")))
    payload: List[Dict[str, Any]] = []

    # Movies
    for p in range(1, pages + 1):
        items, _ = discover_movie_page(
            p, region=region, provider_ids=provider_ids, original_langs=original_langs
        )
        for d in items:
            payload.append(
                {
                    "tmdb_id": d.get("id"),
                    "tmdb_media_type": "movie",
                    "type": "movie",
                    "title": d.get("title") or d.get("original_title"),
                    "year": int(str(d.get("release_date", "")).split("-")[0])
                    if d.get("release_date")
                    else None,
                    "tmdb_vote": d.get("vote_average"),
                }
            )

    # TV
    for p in range(1, pages + 1):
        items, _ = discover_tv_page(
            p, region=region, provider_ids=provider_ids, original_langs=original_langs
        )
        for d in items:
            payload.append(
                {
                    "tmdb_id": d.get("id"),
                    "tmdb_media_type": "tv",
                    "type": "tvSeries",
                    "title": d.get("name") or d.get("original_name"),
                    "year": int(str(d.get("first_air_date", "")).split("-")[0])
                    if d.get("first_air_date")
                    else None,
                    "tmdb_vote": d.get("vote_average"),
                }
            )

    # remove any null tmdb_id
    payload = [x for x in payload if x.get("tmdb_id") is not None]
    return payload

# ---------- Build catalog (main) ----------

def build_catalog(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Builds filtered, enriched items and writes:
      - data/out/latest/assistant_feed.json
      - data/out/latest/run_meta.json
      - data/out/latest/debug_status.json (extra diagnostics)

    Pipeline:
      1) Optionally load IMDb TSVs (basics+ratings).
      2) Load persistent pool (carry-forward enrichment).
      3) Always run TMDB Discover for fresh titles (movies + TV).
      4) Enrich with TMDB details (incl. imdb_id for TV) + providers.
      5) Exclude anything in your lists (ratings.csv + IMDb public list).
      6) Filter by subscriptions.
      7) Persist back to pool; write outputs + telemetry/diagnostics.
    """
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # (1) IMDb TSVs (optional)
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    using_imdb = bool(basics and ratings)
    imdb_items: List[Dict[str, Any]] = _merge_basics_ratings(basics, ratings) if using_imdb else []

    # (2) Persistent pool
    persistent = _read_json(PERSIST_PATH, {"items": {}})
    known_items: Dict[str, Any] = persistent.get("items", {})

    # index by tmdb_id and imdb_id for cross-checks
    known_by_tmdb: Dict[str, Dict[str, Any]] = {}
    imdb_to_tmdb: Dict[str, str] = {}
    for _, v in known_items.items():
        tmdb_id = v.get("tmdb_id")
        imdb_id = v.get("imdb_id")
        if tmdb_id is not None:
            known_by_tmdb[str(tmdb_id)] = v
        if imdb_id:
            imdb_to_tmdb[str(imdb_id)] = str(tmdb_id) if tmdb_id is not None else ""

    # (3) Discover fresh every run
    discovered = _discover_fresh(env)
    discovered_total = len(discovered)

    # Working set is discovered
    items: List[Dict[str, Any]] = discovered[:]

    # Carry-forward enrichment for same TMDB id
    by_tmdb: Dict[str, Dict] = {str(it.get("tmdb_id")): it for it in items if it.get("tmdb_id") is not None}
    for key, carry in known_by_tmdb.items():
        if key in by_tmdb:
            it = by_tmdb[key]
            for ck, cv in carry.items():
                if ck not in it or it[ck] in (None, "", [], {}):
                    it[ck] = cv

    # (4) Enrich via TMDB (adds imdb_id for TV, genres, providers)
    region = (env.get("REGION") or "US").upper()
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key:
        enrich_items_with_tmdb(items, api_key=api_key, region=region)

    # (5) Exclude anything that appears in your profile lists
    profile, profile_loaded = _load_user_profile(env)
    user_tconsts = set(profile.keys()) if profile else set()

    # — helper: should exclude?
    def _is_excluded(it: Dict[str, Any]) -> Tuple[bool, str]:
        imdb_id = (it.get("imdb_id") or "").strip()
        tconst = (it.get("tconst") or "").strip()
        tmdb_id = it.get("tmdb_id")
        # direct IMDb id or tconst in your list
        if imdb_id and imdb_id in user_tconsts:
            return True, "imdb_id_in_user_lists"
        if tconst and tconst in user_tconsts:
            return True, "tconst_in_user_lists"
        # if we already had this tmdb_id mapped to an imdb_id you've rated
        if tmdb_id is not None:
            k = str(tmdb_id)
            prior = known_by_tmdb.get(k)
            if prior:
                prior_imdb = (prior.get("imdb_id") or "").strip()
                if prior_imdb and prior_imdb in user_tconsts:
                    return True, "tmdb_linked_to_rated_imdb"
        return False, ""

    # (6) Subscriptions filter (robust slug->name mapping)
    subs_slugs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    allowed_names = _subs_name_whitelist(subs_slugs)

    excluded_by_providers = 0
    excluded_by_user = 0
    excluded_reasons: Dict[str, int] = {}
    excluded_samples: List[Dict[str, Any]] = []
    filtered: List[Dict[str, Any]] = []

    for it in items:
        # subs filter
        if not _passes_subs_filter(it, allowed_names):
            excluded_by_providers += 1
            continue
        # user exclusions
        ex, reason = _is_excluded(it)
        if ex:
            excluded_by_user += 1
            excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
            if len(excluded_samples) < 25:
                excluded_samples.append({
                    "title": it.get("title"),
                    "year": it.get("year"),
                    "tmdb_id": it.get("tmdb_id"),
                    "imdb_id": it.get("imdb_id"),
                    "reason": reason,
                })
            continue
        filtered.append(it)

    kept_after_filter = len(filtered)

    # (7) Persist pool growth + write outputs
    pool_size_before = len(known_items)
    pool_new_this_run = 0
    for it in filtered:
        tmdb_id = it.get("tmdb_id")
        if tmdb_id is None:
            continue
        key = str(tmdb_id)
        prev = known_items.get(key)
        snapshot = {
            "tmdb_id": it.get("tmdb_id"),
            "tmdb_media_type": it.get("tmdb_media_type"),
            "providers": it.get("providers"),
            "genres": it.get("genres"),
            "title": it.get("title"),
            "type": it.get("type"),
            "year": it.get("year"),
            "imdb_id": it.get("imdb_id"),
            "imdb_rating": it.get("imdb_rating"),
            "tmdb_vote": it.get("tmdb_vote"),
        }
        if not prev:
            pool_new_this_run += 1
        else:
            for ck, cv in snapshot.items():
                if ck not in prev or prev[ck] in (None, "", [], {}):
                    prev[ck] = cv
            snapshot = prev
        known_items[key] = snapshot

    pool_size_after = len(known_items)
    _write_json(PERSIST_PATH, {"items": known_items})

    # Outputs
    _write_json(OUT_DIR / "assistant_feed.json", {"items": filtered})
    meta = {
        "using_imdb": using_imdb,
        "profile_loaded": profile_loaded,
        "discovered_total": discovered_total,
        "excluded_by_providers": excluded_by_providers,
        "excluded_by_user_ratings_or_imdb_list": excluded_by_user,
        "excluded_reasons": excluded_reasons,
        "kept_after_filter": kept_after_filter,
        "movies_kept": sum(1 for x in filtered if (x.get("tmdb_media_type") or "").startswith("movie")),
        "tv_kept": sum(1 for x in filtered if (x.get("tmdb_media_type") or "").startswith("tv")),
        "pool_size_before": pool_size_before,
        "pool_new_this_run": pool_new_this_run,
        "pool_size_after": pool_size_after,
        "region": region,
        "original_langs": env.get("ORIGINAL_LANGS") or None,
        "subs_include": subs_slugs,
        "elapsed_sec": round(time.time() - t0, 2),
        "note": "Discover + enrich pipeline; IMDb TSVs optional.",
    }
    _write_json(OUT_DIR / "run_meta.json", meta)

    # extra diagnostics to help audit exclusions
    debug_status = {
        "excluded_samples": excluded_samples,  # up to 25 examples with reasons
        "profile_tconst_count": len(user_tconsts),
        "known_pool_items": pool_size_after,
    }
    _write_json(OUT_DIR / "debug_status.json", debug_status)

    # also drop raw lists we’ll include in the debug zip
    _write_json(OUT_DIR / "raw_discovered.json", {"items": items})
    _write_json(OUT_DIR / "raw_filtered.json", {"items": filtered})

    return filtered