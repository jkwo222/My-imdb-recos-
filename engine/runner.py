# engine/runner.py
from __future__ import annotations
import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from math import exp

import pandas as pd

from .catalog_builder import build_catalog
from .summarize import write_summary_md

# Optional IMDb web sync (survive if missing)
try:
    from .imdb_sync import fetch_user_ratings_web  # noqa
except Exception:
    fetch_user_ratings_web = None  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

USER_DIR = ROOT / "data" / "user"
USER_DIR.mkdir(parents=True, exist_ok=True)

STATE_DIR = ROOT / "data" / "cache" / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

def _env() -> Dict[str, Any]:
    return {
        "REGION": os.getenv("REGION", "US"),
        "ORIGINAL_LANGS": os.getenv("ORIGINAL_LANGS", "en"),
        "SUBS_INCLUDE": os.getenv("SUBS_INCLUDE", ""),
        "MIN_MATCH_CUT": float(os.getenv("MIN_MATCH_CUT", "58")),
        "DISCOVER_PAGES": int(os.getenv("DISCOVER_PAGES", "3") or 3),
        "IMDB_USER_ID": os.getenv("IMDB_USER_ID", "").strip(),
    }

# -------------------------
# Profile / exclusions
# -------------------------

def _load_local_ratings_csv() -> pd.DataFrame:
    p = USER_DIR / "ratings.csv"
    if not p.exists():
        return pd.DataFrame(columns=["const","yourrating","date","genres","title","url","titleType","year","numVotes","directors"])
    df = pd.read_csv(p)
    # Normalize column names we care about; tolerate variations
    if "const" not in df.columns and "tconst" in df.columns:
        df = df.rename(columns={"tconst": "const"})
    for col in ("yourrating","genres","year","titleType","numVotes","directors"):
        if col not in df.columns:
            df[col] = None
    return df

def _build_profile(df: pd.DataFrame) -> Dict[str, Any]:
    # Genre weights weighted by yourrating if present (default 6/10 neutral)
    weights: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    for _, row in df.iterrows():
        gstr = row.get("genres") or ""
        if not isinstance(gstr, str):
            continue
        rating = row.get("yourrating")
        try:
            r = float(rating)
        except Exception:
            r = 6.0
        for g in [x.strip() for x in gstr.split(",") if x.strip()]:
            weights[g] = weights.get(g, 0.0) + r
            counts[g] = counts.get(g, 0) + 1
    # Normalize to 0..1
    total = sum(weights.values()) or 1.0
    norm = {k: v / total for k, v in weights.items()}
    return {"genre_weights": norm, "sample_size": int(df.shape[0])}

def _exclusion_set(local: pd.DataFrame, imdb_web: Optional[Dict[str, Any]]) -> set:
    excl = set()
    if not local.empty:
        for t in local["const"].dropna().astype(str):
            excl.add(t.strip())
    # imdb_web is a dict with "ratings": [{"tconst":"tt...."}, ...]
    if imdb_web and isinstance(imdb_web, dict):
        for x in imdb_web.get("ratings", []) or []:
            t = str(x.get("tconst") or "").strip()
            if t:
                excl.add(t)
    return excl

# -------------------------
# Scoring
# -------------------------

def _genre_score(item: Dict[str, Any], weights: Dict[str, float]) -> float:
    gs = 0.0
    have = item.get("genres") or []
    if not have:
        return 0.0
    for g in have:
        gs += weights.get(g, 0.0)
    # Convert 0..1 total-scale to 0..100 more visible
    return min(100.0, gs * 100.0)

def _rating_score(item: Dict[str, Any]) -> float:
    # prefer IMDb rating if present later; for now use tmdb_vote (0..10)
    r = None
    for key in ("imdb_rating", "tmdb_vote", "vote_average"):
        if key in item and item[key] is not None:
            try:
                r = float(item[key])
                break
            except Exception:
                pass
    if r is None:
        return 0.0
    return max(0.0, min(100.0, r * 10.0))

def _recency_score(item: Dict[str, Any]) -> float:
    y = item.get("year")
    if not y:
        return 50.0
    try:
        y = int(y)
    except Exception:
        return 50.0
    age = max(0, (2025 - y))  # stable for CI; adjust yearly
    # 100 when 0 yrs old; ~50 at ~10 yrs; ~20 at ~20 yrs
    return max(10.0, 100.0 * exp(-age / 12.0))

def _score_item(item: Dict[str, Any], genre_weights: Dict[str, float]) -> float:
    g = _genre_score(item, genre_weights)
    a = _rating_score(item)
    r = _recency_score(item)
    score = 0.5 * g + 0.3 * a + 0.2 * r
    # Clamp
    return float(max(0.0, min(100.0, score)))

# -------------------------
# Main
# -------------------------

def main() -> None:
    env = _env()

    # Build catalog (discover + enrich)
    payload = build_catalog(env)
    items = payload["items"]
    tel = payload["telemetry"]

    # Load profile + exclusions
    local = _load_local_ratings_csv()
    profile = _build_profile(local)
    imdb_web_remote = None
    if env["IMDB_USER_ID"] and fetch_user_ratings_web:
        try:
            imdb_web_remote = fetch_user_ratings_web(env["IMDB_USER_ID"], ttl_days=2)
        except Exception:
            imdb_web_remote = None

    exclusions = _exclusion_set(local, imdb_web_remote)

    # Filter out seen/rated by imdb_id
    filtered: List[Dict[str, Any]] = []
    excluded_csv = 0
    for it in items:
        tconst = (it.get("imdb_id") or "").strip()
        if tconst and tconst in exclusions:
            excluded_csv += 1
            continue
        filtered.append(it)

    # Score & shortlist
    gw = profile.get("genre_weights", {})
    MIN_MATCH = float(env["MIN_MATCH_CUT"])
    scored: List[Dict[str, Any]] = []
    for it in filtered:
        s = _score_item(it, gw)
        it_out = dict(it)
        it_out["score"] = round(s, 2)
        if s >= MIN_MATCH:
            scored.append(it_out)

    # Rank
    ranked = sorted(scored, key=lambda x: x["score"], reverse=True)

    # Telemetry
    telemetry = {
        **tel,
        "exclusions_total": len(exclusions),
        "excluded_from_seen": excluded_csv,
        "input_items": len(items),
        "eligible_after_exclusions": len(filtered),
        "eligible_above_cut": len(ranked),
        "min_match_cut": MIN_MATCH,
    }

    # Write outputs
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with (OUT_DIR / "assistant_feed.json").open("w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)
    with (OUT_DIR / "assistant_ranked.json").open("w", encoding="utf-8") as f:
        json.dump(ranked, f, ensure_ascii=False, indent=2)
    with (ROOT / "data" / "cache" / "state" / "personal_state.json").open("w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)

    write_summary_md(env, telemetry=telemetry, genre_weights=gw, ranked=ranked[:20])

if __name__ == "__main__":
    main()