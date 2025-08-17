# engine/catalog_builder.py
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .tmdb import (
    discover_movie_page,
    discover_tv_page,
    find_by_imdb_id,
    search_title_year,
)
from .cache import load_state, save_state

# Optional helpers (we use them if present)
try:
    from .imdb_sync import (
        load_ratings_csv,
        fetch_user_ratings_web,
        merge_user_sources,
        to_user_profile,
    )
    _IMDB_SYNC_OK = True
except Exception:
    _IMDB_SYNC_OK = False

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
OUT_DIR = ROOT / "data" / "out" / "latest"
IMDB_CACHE = CACHE_DIR / "imdb"
IMDB_CACHE.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

IMDB_BASICS = IMDB_CACHE / "title.basics.tsv"
IMDB_RATINGS = IMDB_CACHE / "title.ratings.tsv"


# -----------------------------
# IMDb TSV loading (if present)
# -----------------------------
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
            for g in (
                row.get("genres", "").split(",") if row.get("genres") else []
            )
            if g and g != r"\N"
        ]
        items.append(
            {
                "tconst": tconst,
                "imdb_id": tconst,
                "title": row.get("primaryTitle") or row.get("originalTitle"),
                "type": "tvSeries"
                if (title_type or "").startswith("tv") and title_type != "tvMovie"
                else "movie",
                "year": year,
                "genres": genres,
                "imdb_rating": _to_float(row.get("imdb_rating")),
                "source": "imdb_tsv",
            }
        )
    return items


# -----------------------------
# TMDB discover (fresh titles)
# -----------------------------
def _to_float(x) -> Optional[float]:
    try:
        if x is None or x == "" or x == r"\N":
            return None
        return float(x)
    except Exception:
        return None


def _tmdb_discover_items(
    *, region: str, original_langs: Optional[str], pages: int
) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    for p in range(1, max(1, pages) + 1):
        movs, _ = discover_movie_page(
            p, region=region, provider_ids=None, original_langs=original_langs
        )
        tvs, _ = discover_tv_page(
            p, region=region, provider_ids=None, original_langs=original_langs
        )

        for m in movs or []:
            all_items.append(
                {
                    "tmdb_id": m.get("id"),
                    "tmdb_media_type": "movie",
                    "title": m.get("title") or m.get("original_title"),
                    "year": _safe_year(m.get("release_date")),
                    "tmdb_vote": _to_float(m.get("vote_average")),
                    "type": "movie",
                    "source": "tmdb_discover",
                }
            )
        for t in tvs or []:
            all_items.append(
                {
                    "tmdb_id": t.get("id"),
                    "tmdb_media_type": "tv",
                    "title": t.get("name") or t.get("original_name"),
                    "year": _safe_year(t.get("first_air_date")),
                    "tmdb_vote": _to_float(t.get("vote_average")),
                    "type": "tvSeries",
                    "source": "tmdb_discover",
                }
            )
    return all_items


def _safe_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        y = int((date_str or "")[:4])
        return y
    except Exception:
        return None


def _resolve_imdb_for_tmdb_seed(seed: Dict[str, Any]) -> None:
    """
    Side-effect: adds imdb_id (tconst) when we can find it.
    """
    tmdb_id = seed.get("tmdb_id")
    mtype = seed.get("tmdb_media_type") or ("movie" if seed.get("type") == "movie" else "tv")
    if not tmdb_id:
        return
    # Try direct /find if we somehow already have an imdb_id (we don't here),
    # otherwise do a title/year search fallback.
    # Best is: try /search then pick best first result’s external_ids via /find?  We’ll do a
    # cheap pass: /search on the media type and take first exact-year match if available.
    title = seed.get("title")
    year = seed.get("year")
    try:
        # We don’t have the discovery item’s external_ids in this path, so try /search for (title,year).
        data = search_title_year(title or "", year, mtype)
        results = data.get("results", []) or []
        if results:
            # If the search result has 'id', try /find using any attached imdb_id? We don't get imdb here.
            # Instead, we’ll rely on runner/enrichment to fetch external_ids later.
            pass
    except Exception:
        pass
    # We’ll let downstream enrichment fill imdb_id if available.


# -----------------------------
# User profile (ratings & list)
# -----------------------------
def _load_user_profile(env: Dict[str, str]) -> Dict[str, Dict]:
    if not _IMDB_SYNC_OK:
        return {}
    local = load_ratings_csv()  # data/user/ratings.csv (ok if empty)
    remote: List[Dict[str, str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        # NOTE: This fetches your public IMDb ratings page (not arbitrary lists)
        # If you later add a function to fetch a custom list, merge it here too.
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)  # { tconst: {my_rating, ...}, ... }
    return profile


# -----------------------------
# Public API
# -----------------------------
def build_catalog(env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Builds filtered, enriched items and writes:
      - data/out/latest/assistant_feed.json
      - data/out/latest/run_meta.json
    We now ALWAYS include TMDB discover to bring in fresh titles, and we exclude anything
    you’ve rated/seen (from ratings.csv and your public IMDb ratings).
    """
    region = (env.get("REGION") or "US").upper()
    original_langs = (env.get("ORIGINAL_LANGS") or "").strip() or None
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    discover_pages = int(float(env.get("DISCOVER_PAGES") or 3))

    # 1) Load IMDb TSVs if present
    basics = _load_tsv(IMDB_BASICS)
    ratings = _load_tsv(IMDB_RATINGS)
    imdb_used = bool(basics and ratings)
    imdb_items = _merge_basics_ratings(basics, ratings) if imdb_used else []

    # 2) TMDB discover seeds (fresh)
    discover_items = _tmdb_discover_items(
        region=region, original_langs=original_langs, pages=discover_pages
    )
    for s in discover_items:
        _resolve_imdb_for_tmdb_seed(s)  # best-effort; downstream enrichment can fill

    # 3) Bring forward anything we’ve already learned (persistent pool)
    persistent = load_state("persistent_pool", default={"items": {}})
    known_items: Dict[str, Any] = persistent.get("items", {})

    # 4) Merge all sources -> map by key (prefer imdb_id if present; else tmdb_id)
    by_key: Dict[str, Dict[str, Any]] = {}

    def _k(it: Dict[str, Any]) -> Optional[str]:
        tid = it.get("imdb_id") or it.get("tconst")
        if tid:
            return f"imdb:{tid}"
        tm = it.get("tmdb_id")
        if tm:
            return f"tmdb:{tm}"
        return None

    source_counts = {"imdb_tsv": 0, "tmdb_discover": 0, "persistent": 0}

    for it in imdb_items:
        key = _k(it)
        if not key:
            continue
        it.setdefault("imdb_id", it.get("tconst"))
        it["source"] = it.get("source") or "imdb_tsv"
        by_key[key] = it
        source_counts["imdb_tsv"] += 1

    for it in discover_items:
        key = _k(it)
        if not key:
            # use tmdb key if imdb missing
            tm = it.get("tmdb_id")
            if tm:
                by_key[f"tmdb:{tm}"] = it
                source_counts["tmdb_discover"] += 1
            continue
        # prefer existing imdb-based (don’t overwrite)
        if key not in by_key:
            by_key[key] = it
            source_counts["tmdb_discover"] += 1

    # carry forward enrichment from persistent
    for k, v in known_items.items():
        key = f"imdb:{k}" if not str(k).startswith(("imdb:", "tmdb:")) else str(k)
        if str(k).startswith(("imdb:", "tmdb:")):
            key = str(k)
        # Create a minimal row if it’s not already here
        row = by_key.get(key)
        if not row:
            row = {
                "imdb_id": v.get("imdb_id") or (k if str(k).startswith("tt") else None),
                "tmdb_id": v.get("tmdb_id"),
                "tmdb_media_type": v.get("tmdb_media_type"),
                "title": v.get("title"),
                "type": v.get("type") or ("tvSeries" if v.get("tmdb_media_type") == "tv" else "movie"),
                "year": v.get("year"),
                "genres": v.get("genres") or [],
                "imdb_rating": _to_float(v.get("imdb_rating")),
                "providers": v.get("providers") or [],
                "source": "persistent",
            }
            # compute key
            pk = _k(row)
            if pk:
                by_key[pk] = row
                source_counts["persistent"] += 1
            continue

        # merge forward missing enrichment fields
        for ck, cv in v.items():
            if ck in {"providers", "tmdb_id", "tmdb_media_type", "genres", "title", "type", "year", "imdb_rating"}:
                if row.get(ck) in (None, "", [], {}):
                    row[ck] = cv

    merged_items: List[Dict[str, Any]] = list(by_key.values())

    # 5) Exclude anything you’ve rated/seen (ratings.csv + public IMDb ratings)
    profile = _load_user_profile(env)
    profile_ids: Set[str] = set(profile.keys())
    excluded_rated = 0
    kept_after_exclude: List[Dict[str, Any]] = []
    for it in merged_items:
        tconst = it.get("imdb_id") or it.get("tconst")
        if tconst and tconst in profile_ids:
            excluded_rated += 1
            continue
        kept_after_exclude.append(it)

    # 6) Filter by providers, if SUBS_INCLUDE is set
    subs_lower = [s.lower() for s in subs]
    filtered: List[Dict[str, Any]] = []
    for it in kept_after_exclude:
        provs = it.get("providers") or []
        if subs_lower:
            low = [p.lower() for p in provs]
            keep = any(any(s in p for p in low) for s in subs_lower)
            if not keep:
                continue
        filtered.append(it)

    # 7) Write raw feed
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    feed = {
        "items": filtered,
        "telemetry": {
            "total_sources_merged": len(merged_items),
            "kept_after_exclude": len(kept_after_exclude),
            "kept_after_subs": len(filtered),
            "excluded_rated_or_list": excluded_rated,
            "discover_pages": discover_pages,
            "region": region,
            "original_langs": original_langs,
            "subs_include": subs,
            "sources": source_counts,
            "imdb_tsv_present": imdb_used,
        },
    }
    (OUT_DIR / "assistant_feed.json").write_text(
        json.dumps(feed, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 8) Persist pool (grows over time)
    for it in filtered:
        tconst = str(it.get("imdb_id") or it.get("tconst") or "")
        if not tconst:
            # fall back to tmdb id
            tm = it.get("tmdb_id")
            if tm:
                tconst = f"tmdb:{tm}"
            else:
                continue
        known_items[tconst] = {
            "imdb_id": it.get("imdb_id") or it.get("tconst"),
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

    # 9) Run meta (for summarize)
    run_meta = {
        "using_imdb": imdb_used,
        "candidates_after_filtering": len(filtered),
        "user_profile_loaded": bool(profile),
        "excluded_rated_or_list": excluded_rated,
        "sources": source_counts,
        "discover_pages": discover_pages,
        "region": region,
        "original_langs": original_langs,
        "subs_include": subs,
    }
    (OUT_DIR / "run_meta.json").write_text(
        json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return filtered