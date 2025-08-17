from __future__ import annotations
from typing import List, Dict, Any, Tuple
from pathlib import Path
import json

from .tmdb_detail import discover, enrich_items_with_tmdb
from .imdb_sync import (
    load_ratings_csv, fetch_user_ratings_web, merge_user_sources,
    to_user_profile, compute_genre_weights, build_exclusion_set, save_personal_state
)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
CACHE = DATA / "cache"
STATE = CACHE / "state"
OUT = DATA / "out" / "latest"
OUT.mkdir(parents=True, exist_ok=True)

def _normalize_langs(original_langs: str | None) -> str | None:
    if not original_langs: return None
    return original_langs.split(",")[0].strip()

def _providers_from_env(subs_csv: str | None, region: str) -> List[int]:
    # Minimal static mapping (can expand later). Matches earlier helper you had.
    mapping = {
        "US": {
            "netflix": 8,
            "prime_video": 9,
            "hulu": 15,
            "max": 384,
            "disney_plus": 337,
            "apple_tv_plus": 350,
            "peacock": 386,
            "paramount_plus": 531,
        }
    }
    subs = []
    if subs_csv:
        subs = [s.strip().lower() for s in subs_csv.split(",") if s.strip()]
    prov = mapping.get(region.upper(), mapping["US"])
    ids = []
    seen=set()
    for s in subs:
        pid = prov.get(s)
        if pid and pid not in seen:
            seen.add(pid)
            ids.append(pid)
    return ids

def _discover_pages(env: Dict[str,str]) -> int:
    try:
        return max(1, min(10, int(env.get("DISCOVER_PAGES","3"))))
    except Exception:
        return 3

def _build_discover_pool(env: Dict[str,str]) -> Tuple[List[Dict[str,Any]], Dict[str,int]]:
    region = (env.get("REGION") or "US").upper()
    langs = _normalize_langs(env.get("ORIGINAL_LANGS"))
    provider_ids = _providers_from_env(env.get("SUBS_INCLUDE"), region)
    pages = _discover_pages(env)

    items: List[Dict[str,Any]] = []
    tally = {"discover_calls": 0, "discover_movies": 0, "discover_tv": 0}

    for page in range(1, pages+1):
        dm = discover("movie", page, region, provider_ids, langs)
        dv = discover("tv", page, region, provider_ids, langs)
        tally["discover_calls"] += 2
        for r in dm.get("results", []) or []:
            items.append({
                "tmdb_id": r.get("id"),
                "tmdb_media_type": "movie",
                "type": "movie",
                "title": r.get("title"),
                "year": int((r.get("release_date") or "0000")[:4]) if r.get("release_date") else None,
                "tmdb_vote": r.get("vote_average"),
                "providers": [],
            })
        tally["discover_movies"] += len(dm.get("results", []) or [])

        for r in dv.get("results", []) or []:
            items.append({
                "tmdb_id": r.get("id"),
                "tmdb_media_type": "tv",
                "type": "tvSeries",
                "title": r.get("name"),
                "year": int((r.get("first_air_date") or "0000")[:4]) if r.get("first_air_date") else None,
                "tmdb_vote": r.get("vote_average"),
                "providers": [],
            })
        tally["discover_tv"] += len(dv.get("results", []) or [])
    return items, tally

def _apply_exclusions(items: List[Dict[str,Any]], ex_ids: set[str], ex_pairs: set[tuple[str,int|None]]) -> Tuple[List[Dict[str,Any]], Dict[str,int]]:
    def norm(t: str) -> str:
        return " ".join((t or "").strip().lower().split())
    kept: List[Dict[str,Any]] = []
    tallies = {"excluded_imdb": 0, "excluded_titleyear": 0}
    for it in items:
        imdb_id = it.get("imdb_id")
        if imdb_id and imdb_id in ex_ids:
            tallies["excluded_imdb"] += 1
            continue
        title = norm(it.get("title") or "")
        year = it.get("year")
        if title and (title, year) in ex_pairs:
            tallies["excluded_titleyear"] += 1
            continue
        kept.append(it)
    return kept, tallies

def build_catalog(env: Dict[str,str]) -> List[Dict[str,Any]]:
    # — Load user sources —
    local_rows = load_ratings_csv()
    remote_rows = fetch_user_ratings_web((env.get("IMDB_USER_ID") or "").strip())
    merged_rows = merge_user_sources(local_rows, remote_rows)
    profile = to_user_profile(merged_rows)
    genre_weights = compute_genre_weights(profile)
    save_personal_state(genre_weights, merged_rows)

    ex_ids, ex_pairs = build_exclusion_set(merged_rows)

    # — Discover fresh catalog —
    discover_items, discover_stats = _build_discover_pool(env)

    # — Enrich with TMDB (adds imdb_id, genres, providers, etc.) —
    api_key = env.get("TMDB_API_KEY") or ""
    if api_key:
        enrich_items_with_tmdb(discover_items, api_key=api_key, region=(env.get("REGION") or "US"))

    # — Exclude anything you’ve seen/rated —
    after_ex, excl_stats = _apply_exclusions(discover_items, ex_ids, ex_pairs)

    # — Provider filtering by human names (post-enrichment) —
    subs = [s.strip().lower() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]
    filtered: List[Dict[str,Any]] = []
    for it in after_ex:
        provs = [p.lower() for p in (it.get("providers") or [])]
        if subs:
            if not any(any(s in p for p in provs) for s in subs):
                continue
        filtered.append(it)

    # — Telemetry (saved to run_meta.json and used by summarize.py) —
    meta = {
        "discover": discover_stats,
        "exclusions": excl_stats,
        "profile_size": len(profile),
        "genre_weights_nonzero": len([g for g,v in genre_weights.items() if v != 0.5]),
        "candidates_after_filtering": len(filtered),
        "subs_include": subs,
        "region": env.get("REGION") or "US",
        "original_langs": env.get("ORIGINAL_LANGS") or "",
    }
    (OUT / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # Raw feed (downstream scoring will turn this into assistant_ranked.json)
    (OUT / "assistant_feed.json").write_text(json.dumps(filtered, ensure_ascii=False, indent=2), encoding="utf-8")
    return filtered