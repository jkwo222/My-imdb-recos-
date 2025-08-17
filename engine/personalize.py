# engine/personalize.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import math
from datetime import datetime

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

# -------------------------
# Affinity (Genres/Directors)
# -------------------------

def genre_weights_from_profile(profile: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """
    Weight each genre by (my_rating - 6.0). Normalize to [0,1].
    Uses genres stored in the profile rows themselves.
    """
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
    return {g: round(0.5 + 0.5*(v/mx), 4) for g, v in acc.items()}

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
    return {d: round(0.5 + 0.5*(v/mx), 4) for d, v in acc.items()}

# -------------------------
# Scoring
# -------------------------

def _avg_rating_10(it: Dict[str, Any]) -> float:
    """
    Blend IMDb (0-10) and TMDB vote (0-10).
    """
    imdb = _safe_float(it.get("imdb_rating"))
    tmdb_raw = _safe_float(it.get("tmdb_vote"))
    tmdb = tmdb_raw if tmdb_raw is None else float(tmdb_raw)
    vals = [v for v in [imdb, tmdb] if v is not None]
    if not vals:
        return 6.0  # neutral-ish default
    return sum(vals)/len(vals)

def _authority_bonus(it: Dict[str, Any]) -> float:
    """
    Small positive bonus for high vote counts (IMDb numVotes).
    Scale: log10(numVotes+1) clamped to [0..6] -> up to +6 * 0.8 ≈ +4.8 points.
    """
    nv = _safe_float(it.get("numVotes"), 0.0)
    if nv is None:
        nv = 0.0
    import math
    return min(6.0, math.log10(1.0 + nv)) * 0.8

def _genre_fit(it: Dict[str, Any], gw: Dict[str, float]) -> float:
    g = _to_list(it.get("genres"))
    if not g or not gw:
        return 0.0
    fit = sum(gw.get(x, 0.5) for x in g) / len(g)
    return (fit - 0.5) * 30.0  # ±15

def _director_fit(it: Dict[str, Any], dw: Dict[str, float]) -> float:
    if not dw:
        return 0.0
    ds = _to_list(it.get("directors"))
    if not ds:
        return 0.0
    # Take top director affinity
    best = max((dw.get(d, 0.5) for d in ds), default=0.5)
    return (best - 0.5) * 20.0  # ±10

def apply_personal_score(
    items: List[Dict[str, Any]],
    genre_weights: Dict[str, float],
    director_weights: Dict[str, float],
) -> None:
    """
    Mutates items: adds 'match_score' (0–100) and 'why'.
    """
    for it in items:
        base10 = _avg_rating_10(it)
        base100 = base10 * 10.0
        adj = 0.0
        adj += _genre_fit(it, genre_weights)
        adj += _director_fit(it, director_weights)
        adj += _authority_bonus(it)
        score = max(0.0, min(100.0, base100 + adj))
        it["match_score"] = round(score, 2)
        # Build a simple "why"
        imdb = _safe_float(it.get("imdb_rating"))
        tmdb = _safe_float(it.get("tmdb_vote"))
        bits = []
        if imdb is not None: bits.append(f"IMDb {imdb:g}")
        if tmdb is not None: bits.append(f"TMDB {tmdb:g}")
        if it.get("year"): bits.append(str(it["year"]))
        it["why"] = "; ".join(bits)