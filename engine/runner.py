from __future__ import annotations
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from .catalog_builder import build_catalog
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile, UserProfile
from .summarize import write_summary_md

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _load_user_profile(env: Dict[str, str]) -> UserProfile:
    local = load_ratings_csv()
    remote: List[Dict[str, Any]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

def _build_exclusion_sets(profile: UserProfile) -> Dict[str, Any]:
    """
    Build strong exclusion sets:
      - tconsts from profile
      - (title_lower, year) pairs from profile
    """
    return {
        "tconsts": set(profile.seen_tconsts),
        "title_year": set(profile.seen_titles),
    }

def _genre_weights(profile: UserProfile) -> Dict[str, float]:
    # normalize to 0..1
    total = sum(profile.genre_counts.values()) or 1.0
    return {g: round(v / total, 4) for g, v in profile.genre_counts.items()}

def _dir_weights(profile: UserProfile) -> Dict[str, float]:
    total = sum(profile.director_counts.values()) or 1.0
    return {d: round(v / total, 4) for d, v in profile.director_counts.items()}

def _score_item(it: Dict[str, Any], gweights: Dict[str, float], dweights: Dict[str, float]) -> float:
    # Genre match
    genres = it.get("genres") or []
    g = sum(gweights.get(gg, 0.0) for gg in genres)

    # Director boost if we have director info (from tmdb_detail enrichment you may add later)
    boost = 0.0
    for d in it.get("directors", []) or []:
        boost += dweights.get(d, 0.0) * 0.5

    # IMDb rating gentle nudge
    try:
        ir = float(it.get("imdb_rating") or 0.0)
    except Exception:
        ir = 0.0
    rating_bonus = max(0.0, (ir - 6.0)) * 0.03  # +0..0.12 for 10

    # Freshness: light preference to 2010+
    year = it.get("year") or 0
    fresh = 0.06 if year >= 2020 else (0.03 if year >= 2010 else 0.0)

    score = g + boost + rating_bonus + fresh
    # scale to approx 0..100
    return round(min(100.0, score * 100.0), 1)

def _rank_items(items: List[Dict[str, Any]], env: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    profile = _load_user_profile(env)
    exclusions = _build_exclusion_sets(profile)
    gweights = _genre_weights(profile)
    dweights = _dir_weights(profile)

    # pre-exclusion
    pre = len(items)

    kept: List[Dict[str, Any]] = []
    excluded_seen_tconst = 0
    excluded_seen_titleyear = 0

    for it in items:
        # tconst exclusion
        tconst = it.get("tconst")
        if tconst and tconst in exclusions["tconsts"]:
            excluded_seen_tconst += 1
            continue
        # title/year fallback
        title = (it.get("title") or "").strip().lower()
        year = it.get("year") if isinstance(it.get("year"), int) else None
        if title and (title, year) in exclusions["title_year"]:
            excluded_seen_titleyear += 1
            continue

        # compute score
        it["score"] = _score_item(it, gweights, dweights)
        kept.append(it)

    # score cut
    min_cut = float(env.get("MIN_MATCH_CUT") or "58")
    scored = len(kept)
    shortlist = [k for k in kept if k.get("score", 0) >= min_cut]

    telemetry = {
        "pre_candidates": pre,
        "excluded_seen_tconst": excluded_seen_tconst,
        "excluded_seen_titleyear": excluded_seen_titleyear,
        "after_exclusions": scored,
        "min_cut": min_cut,
        "shortlist": len(shortlist),
        "genre_weights": gweights,
    }

    # write ranked file (all kept)
    (OUT_DIR / "assistant_ranked.json").write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT_DIR / "debug_status.json").write_text(json.dumps(telemetry, ensure_ascii=False, indent=2), encoding="utf-8")

    return shortlist, telemetry

def main():
    env = {k: v for k, v in os.environ.items()}
    print(" | catalog:begin")
    items = build_catalog(env)
    print(f" | catalog:end kept={len(items)}")

    shortlist, telemetry = _rank_items(items, env)

    # Compose final summary
    write_summary_md(
        env=env,
        shortlist=shortlist,
        telemetry=telemetry,
    )

if __name__ == "__main__":
    main()