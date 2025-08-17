from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# --- project paths
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- project modules
from .catalog_builder import build_catalog
from .summarize import write_summary_md
from .imdb_sync import (
    load_ratings_csv,
    fetch_user_ratings_web,
    merge_user_sources,
    to_user_profile,
    compute_genre_weights,
)

# -----------------------
# Small utility functions
# -----------------------

def _env(env: Dict[str, str], key: str, default: str = "") -> str:
    v = env.get(key)
    if v is None or (isinstance(v, str) and v.strip() == ""):
        v = os.getenv(key, default)
    return v

def _env_float(env: Dict[str, str], key: str, default: float) -> float:
    raw = _env(env, key, "")
    try:
        return float(raw) if raw != "" else default
    except Exception:
        return default

def _read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _title_key(title: Optional[str], year: Optional[int | str]) -> Optional[str]:
    if not title:
        return None
    t = str(title).strip().lower()
    if not t:
        return None
    y = ""
    if isinstance(year, int):
        y = str(year)
    elif isinstance(year, str) and year.isdigit():
        y = year
    return f"{t}::{y}" if y else t

def _first_year_from_item(it: Dict[str, Any]) -> Optional[int]:
    y = it.get("year")
    if isinstance(y, int):
        return y
    if isinstance(y, str) and y.isdigit():
        try:
            return int(y)
        except Exception:
            pass
    # fallback to release/air date
    for k in ("release_date", "first_air_date"):
        d = it.get(k)
        if isinstance(d, str) and len(d) >= 4 and d[:4].isdigit():
            try:
                return int(d[:4])
            except Exception:
                pass
    return None

def _providers_as_names(providers_val: Any) -> List[str]:
    if not providers_val:
        return []
    if isinstance(providers_val, list):
        return [str(p) for p in providers_val if str(p).strip()]
    # tolerate dicts like {"US": {...}}
    try:
        return list(providers_val.values())
    except Exception:
        return [str(providers_val)]

# -----------------------
# Profile / exclusions
# -----------------------

def _load_user_profile(env: Dict[str, str]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
      profile (dict suitable for compute_genre_weights),
      local_rows (from ratings.csv),
      remote_rows (from imdb web)
    """
    local_rows = load_ratings_csv()  # may be []
    remote_rows: List[Dict[str, Any]] = []
    uid = (_env(env, "IMDB_USER_ID", "") or "").strip()
    if uid:
        remote_rows = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local_rows, remote_rows)
    profile = to_user_profile(merged)
    return profile, local_rows, remote_rows

def _seen_sets(local_rows: List[Dict[str, Any]], remote_rows: List[Dict[str, Any]]) -> Dict[str, set]:
    """
    Build separate sets so we can count CSV vs IMDb-web exclusions.
    Returns dict with keys: csv_tconst, csv_titlekey, web_tconst, web_titlekey
    """
    def rows_to_sets(rows: List[Dict[str, Any]]) -> Tuple[set, set]:
        tset, kset = set(), set()
        for r in rows:
            t = (r.get("tconst") or "").strip()
            if t:
                tset.add(t)
            title = r.get("title") or r.get("primaryTitle") or r.get("originalTitle")
            year = r.get("year") or r.get("startYear")
            key = _title_key(title, year)
            if key:
                kset.add(key)
        return tset, kset

    csv_t, csv_k = rows_to_sets(local_rows)
    web_t, web_k = rows_to_sets(remote_rows)
    return {
        "csv_tconst": csv_t,
        "csv_titlekey": csv_k,
        "web_tconst": web_t,
        "web_titlekey": web_k,
    }

def _is_seen(item: Dict[str, Any], sets: Dict[str, set]) -> Tuple[bool, str]:
    """
    Return (seen?, source_tag in {"csv","web",""}).
    Check by tconst, then by (title, year) signature.
    """
    tconst = (item.get("tconst") or "").strip()
    if tconst:
        if tconst in sets["csv_tconst"]:
            return True, "csv"
        if tconst in sets["web_tconst"]:
            return True, "web"
    key = _title_key(item.get("title") or item.get("name"), _first_year_from_item(item))
    if key:
        if key in sets["csv_titlekey"]:
            return True, "csv"
        if key in sets["web_titlekey"]:
            return True, "web"
    return False, ""

# -----------------------
# Scoring / ranking
# -----------------------

def _genres_of(item: Dict[str, Any]) -> List[str]:
    g = item.get("genres")
    if isinstance(g, list):
        return [str(x) for x in g if str(x).strip()]
    if isinstance(g, str) and g.strip():
        return [x.strip() for x in g.split(",") if x.strip()]
    return []

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x in (None, "", "NaN", "nan"):
            return None
        return float(x)
    except Exception:
        return None

def _score_item(item: Dict[str, Any], genre_weights: Dict[str, float]) -> Tuple[float, str]:
    """
    Returns (score_0_to_100, why_text)
    Heuristic:
      - genre affinity: sum of weights for item genres normalized by sum of top weights
      - ratings: IMDb / TMDB votes nudged in
    """
    # Genre affinity
    genres = _genres_of(item)
    if genre_weights:
        # sum weights for present genres
        gsum = 0.0
        contribs = []
        for g in genres:
            w = float(genre_weights.get(g, 0.0))
            if w > 0:
                gsum += w
                contribs.append((g, w))
        # normalize by sum of top-N weights to avoid runaway values
        top_weights = sorted(genre_weights.values(), reverse=True)[:6]
        norm_base = sum(float(w) for w in top_weights) or 1.0
        g_aff = max(0.0, min(1.0, gsum / norm_base))
        contribs.sort(key=lambda x: x[1], reverse=True)
        why = ", ".join(f"{g}({w:.2f})" for g, w in contribs[:3]) if contribs else ""
    else:
        g_aff = 0.3  # modest baseline if we lack weights
        why = ""
    # Ratings
    imdb = _safe_float(item.get("imdb_rating"))
    tmdb = _safe_float(item.get("tmdb_vote")) or _safe_float(item.get("vote_average"))
    # map 0..10 rating to 0..1
    def rate01(v: Optional[float]) -> float:
        if v is None:
            return 0.0
        return max(0.0, min(1.0, v / 10.0))

    r_aff = max(rate01(imdb), rate01(tmdb))  # optimistic blend

    # Combine
    score01 = 0.7 * g_aff + 0.3 * r_aff
    score = round(100.0 * score01, 2)
    why_text = f"genres: {why}" if why else None
    return score, (why_text or "")

# -----------------------
# Main pipeline
# -----------------------

def main() -> None:
    # Pull env once
    env: Dict[str, str] = dict(os.environ)
    env.setdefault("REGION", "US")
    env.setdefault("ORIGINAL_LANGS", "en")

    # Build catalog (already applies provider/subs filtering + TMDB enrich)
    print(" | catalog:begin")
    items = build_catalog(env)  # list[dict], already filtered by SUBS_INCLUDE if set
    print(f" | catalog:end kept={len(items)}")

    # Telemetry seed
    telemetry: Dict[str, Any] = {
        "discover_new": 0,               # fill by catalog if you populate meta later
        "imdb_rows": 0,                  # likewise from meta if you load TSVs
        "pool_before_filter": 0,         # we'll compute before exclusions if we had a larger pre-filter pool
        "pool_after_exclusions": 0,
        "pool_after_subs_filter": len(items),  # catalog output is after subs
        "excluded_user_csv": 0,
        "excluded_user_web": 0,
        "ranked_total": 0,
        "scored_cut": 0,
        "min_match_cut": _env_float(env, "MIN_MATCH_CUT", 58.0),
        "region": _env(env, "REGION", "US"),
    }

    # If catalog wrote meta, harvest counts
    meta = _read_json(OUT_DIR / "run_meta.json", default={})
    if isinstance(meta, dict):
        if "candidates_after_filtering" in meta:
            telemetry["pool_after_subs_filter"] = int(meta.get("candidates_after_filtering", telemetry["pool_after_subs_filter"]))
        if "using_imdb" in meta:
            telemetry["imdb_rows"] = int(meta.get("imdb_rows", meta.get("using_imdb") and 1 or 0))
        if "discover_new" in meta:
            telemetry["discover_new"] = int(meta.get("discover_new", 0))
        # For completeness, expose threshold if meta carried it
        if "min_match_cut" in meta:
            telemetry["min_match_cut"] = float(meta.get("min_match_cut"))

    # Load profile + exclusions
    profile, local_rows, remote_rows = _load_user_profile(env)
    genre_weights = compute_genre_weights(profile) if profile else {}

    sets = _seen_sets(local_rows, remote_rows)

    # Exclude anything already seen/rated
    excluded_csv = 0
    excluded_web = 0
    kept: List[Dict[str, Any]] = []
    for it in items:
        seen, src = _is_seen(it, sets)
        if seen:
            if src == "csv":
                excluded_csv += 1
            elif src == "web":
                excluded_web += 1
            else:
                excluded_web += 1
            continue
        kept.append(it)

    telemetry["excluded_user_csv"] = excluded_csv
    telemetry["excluded_user_web"] = excluded_web
    telemetry["pool_after_exclusions"] = len(kept)

    # Score and rank
    min_cut = float(telemetry["min_match_cut"])
    ranked: List[Dict[str, Any]] = []
    for it in kept:
        score, why = _score_item(it, genre_weights)
        it2 = dict(it)
        it2["match_score"] = score
        # surface ratings consistently for summary
        if it2.get("imdb_rating") is None and it2.get("imdb_vote") is not None:
            it2["imdb_rating"] = it2.get("imdb_vote")
        if it2.get("tmdb_vote") is None and it2.get("vote_average") is not None:
            it2["tmdb_vote"] = it2.get("vote_average")
        # providers normalized to human strings
        it2["providers"] = _providers_as_names(it2.get("providers"))
        if why:
            it2["why"] = why
        ranked.append(it2)

    ranked.sort(key=lambda r: (r.get("match_score") or 0.0), reverse=True)
    telemetry["ranked_total"] = len(ranked)

    # Score cut
    kept_after_cut = [r for r in ranked if (r.get("match_score") or 0.0) >= min_cut]
    telemetry["scored_cut"] = len(kept_after_cut)

    # Persist outputs
    _write_json(OUT_DIR / "assistant_ranked.json", kept_after_cut)
    _write_json(OUT_DIR / "debug_status.json", {"ok": True, "counts": telemetry})
    # (assistant_feed.json is written by catalog_builder; keep it as-is)

    # Write email/issue summary
    write_summary_md(
        env,
        ranked_items=kept_after_cut,
        telemetry=telemetry,
        genre_weights=genre_weights,
    )

if __name__ == "__main__":
    main()