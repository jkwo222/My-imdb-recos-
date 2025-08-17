from __future__ import annotations
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
import math

from .util import clamp01
from .taste import compute_taste_weights, recency_bonus_for_item, genre_affinity_bonus

def _to_list(x) -> List[str]:
    if not x:
        return []
    if isinstance(x, (list, tuple)):
        return [str(i) for i in x if i]
    return [str(x)]

def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def genre_weights_from_profile(profile: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    acc = defaultdict(float); cnt = defaultdict(int)
    for _, row in profile.items():
        r = row.get("my_rating")
        if r is None:
            continue
        delta = float(r) - 6.0
        for g in _to_list(row.get("genres")):
            acc[g] += delta
            cnt[g] += 1
    if not acc:
        return {}
    mx = max(abs(v) for v in acc.values()) or 1.0
    return {g: round(0.5 + 0.5 * (v / mx), 4) for g, v in acc.items()}

def director_weights_from_profile(profile: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    acc = defaultdict(float); cnt = defaultdict(int)
    for _, row in profile.items():
        r = row.get("my_rating")
        if r is None:
            continue
        delta = float(r) - 6.0
        for d in _to_list(row.get("directors")):
            acc[d] += delta
            cnt[d] += 1
    if not acc:
        return {}
    mx = max(abs(v) for v in acc.values()) or 1.0
    return {d: round(0.5 + 0.5 * (v / mx), 4) for d, v in acc.items()}

def _avg_rating_10(it: Dict[str, Any]) -> float:
    imdb = _safe_float(it.get("imdb_rating"))
    tmdb_raw = it.get("tmdb_vote", it.get("vote_average"))
    tmdb = _safe_float(tmdb_raw)
    vals = [v for v in (imdb, tmdb) if v is not None]
    if not vals:
        return 6.0
    return sum(vals) / len(vals)

def _authority_bonus(it: Dict[str, Any]) -> float:
    nv = _safe_float(it.get("numVotes"), 0.0) or 0.0
    return min(6.0, math.log10(1.0 + nv)) * 0.8

def _genre_fit(it: Dict[str, Any], gw: Dict[str, float]) -> float:
    g = _to_list(it.get("genres"))
    if not g or not gw:
        return 0.0
    fit = sum(gw.get(x, 0.5) for x in g) / len(g)
    return (fit - 0.5) * 30.0

def _director_fit(it: Dict[str, Any], dw: Dict[str, float]) -> float:
    if not dw:
        return 0.0
    ds = _to_list(it.get("directors"))
    if not ds:
        return 0.0
    best = max((dw.get(d, 0.5) for d in ds), default=0.5)
    return (best - 0.5) * 20.0

def apply_personal_score(
    items: List[Dict[str, Any]],
    genre_weights: Dict[str, float],
    director_weights: Dict[str, float],
) -> None:
    for it in items:
        base10 = _avg_rating_10(it)
        base100 = base10 * 10.0
        adj = 0.0
        adj += _genre_fit(it, genre_weights)
        adj += _director_fit(it, director_weights)
        adj += _authority_bonus(it)
        score = max(0.0, min(100.0, base100 + adj))
        it["match_score"] = round(score, 2)
        imdb = _safe_float(it.get("imdb_rating"))
        tmdb = _safe_float(it.get("tmdb_vote", it.get("vote_average")))
        bits = []
        if imdb is not None: bits.append(f"IMDb {imdb:g}")
        if tmdb is not None: bits.append(f"TMDB {tmdb:g}")
        if it.get("year"): bits.append(str(it["year"]))
        it["why"] = "; ".join(bits)

def _popularity_bonus(it: Dict[str, Any]) -> float:
    p = _safe_float(it.get("popularity"), 0.0) or 0.0
    if p <= 0:
        return 0.0
    v = min(10.0, math.log10(1.0 + p) * 2.5)
    return v

def apply_personalization(
    env: Dict[str, Any],
    items: List[Dict[str, Any]],
    *,
    ratings_csv_path: Optional[str] = None,
    taste: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    t = taste if isinstance(taste, dict) else compute_taste_weights(env, ratings_csv_path=ratings_csv_path)
    gw = t.get("genre_weights", {}) if isinstance(t, dict) else {}
    out: List[Dict[str, Any]] = []
    for it in items:
        boost = 0.0
        boost += recency_bonus_for_item(it)
        boost += genre_affinity_bonus(it, gw)
        boost += _popularity_bonus(it) * 0.5
        pre = clamp01((boost) / 10.0) * 10.0
        it2 = dict(it)
        it2["pre_match_hint"] = round(pre, 2)
        it2["_personalize"] = {
            "recency_bonus": round(recency_bonus_for_item(it), 2),
            "genre_bonus": round(genre_affinity_bonus(it, gw), 2),
            "popularity_bonus": round(_popularity_bonus(it) * 0.5, 2),
        }
        out.append(it2)
    return out

personalize = apply_personalization