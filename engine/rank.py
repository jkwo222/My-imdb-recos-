# engine/rank.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from math import copysign

from .taste import taste_boost_for

def _in(val, lo, hi) -> float:
    try:
        x = float(val)
    except Exception:
        return 0.0
    if x < lo: return lo
    if x > hi: return hi
    return x

def _boost_from_rating(r: float) -> float:
    """
    Map a raw rating in [0,1] → boost/penalty in [-1, +1] with a
    neutral band around 0.50–0.65.

    - r <= 0.35 → ~strong penalty approaching -1
    - 0.50–0.65 → ~0 (neutral)
    - r >= 0.85 → strong boost approaching +1
    Piecewise-linear for clarity & debuggability.
    """
    r = _in(r, 0.0, 1.0)
    if r <= 0.35:
        # 0.00 .. 0.35 → -1 .. -0.5
        return -1.0 + (r / 0.35) * 0.5
    if r < 0.50:
        # 0.35 .. 0.50 → -0.5 .. -0.15
        return -0.5 + ((r - 0.35) / 0.15) * 0.35
    if r <= 0.65:
        return 0.0
    if r < 0.85:
        # 0.65 .. 0.85 → 0 .. +0.6
        return ((r - 0.65) / 0.20) * 0.6
    # 0.85 .. 1.00 → 0.6 .. 1.0
    return 0.6 + ((r - 0.85) / 0.15) * 0.4

def _audience_score(it: Dict[str, Any]) -> float:
    # prefer OMDb IMDb audience (0..1), else TMDB vote (0..10 → 0..1)
    if (it.get("audience") or 0) > 0:
        return float(it.get("audience"))
    va = it.get("tmdb_vote")
    return float(va) / 10.0 if va else 0.0

def _critic_score(it: Dict[str, Any]) -> float:
    # prefer OMDb RT (0..1); if missing, you can optionally fall back later
    return float(it.get("critic") or 0.0)

def explain_reasons(it: Dict[str, Any], weights: Dict[str, float], taste_b: float, base_boost: float) -> List[str]:
    reasons: List[str] = []
    aud = _audience_score(it); cri = _critic_score(it)

    if aud >= 0.75:
        reasons.append("strong audience rating")
    elif aud <= 0.45 and aud > 0:
        reasons.append("audience rating is low (penalized)")

    if cri >= 0.75:
        reasons.append("critically rated")
    elif cri <= 0.45 and cri > 0:
        reasons.append("critic rating is low (penalized)")

    if abs(taste_b) >= 0.02:
        reasons.append("genre fit with your past ratings")

    prov = it.get("providers") or []
    if prov:
        reasons.append(f"available on: {', '.join(prov)}")

    if (it.get("type") == "tvSeries") and int(it.get("seasons") or 1) >= 3:
        reasons.append("commitment penalty for multi-season series")

    # when both ratings are missing, still say why (providers / taste)
    if not reasons:
        reasons.append("popular on your services")

    return reasons

def rank_candidates(items: List[Dict[str, Any]],
                    weights: Dict[str, float],
                    taste_profile: Dict[str, float]) -> List[Dict[str, Any]]:
    """
    Produce a ranked list with 'match' and 'why' for each item.
    weights:
      - audience_weight (dominant)
      - critic_weight
      - novelty_weight (0..1)
      - commitment_cost_scale
    """
    aw = float(weights.get("audience_weight", 0.65))
    cw = float(weights.get("critic_weight", 0.35))
    # ensure audience > critic, gently renormalize if not
    if cw >= aw:
        aw = min(0.8, max(0.5, aw))
        cw = max(0.2, min(0.5, 1.0 - aw))

    novelty_w = float(weights.get("novelty_weight", 0.15))
    cc_scale = float(weights.get("commitment_cost_scale", 1.0))

    ranked: List[Dict[str, Any]] = []
    for it in items:
        aud = _audience_score(it)
        cri = _critic_score(it)

        aud_boost = _boost_from_rating(aud)
        cri_boost = _boost_from_rating(cri)

        # Taste boost in [-0.08, +0.15] from your profile (already clamped there)
        taste_b = taste_boost_for(it.get("genres") or [], taste_profile)  # typically small

        # Commitment penalty for multi-season shows
        commit_pen = 0.0
        if (it.get("type") == "tvSeries"):
            seasons = int(it.get("seasons") or 1)
            if seasons >= 3:
                commit_pen = 0.09 * cc_scale   # ~9 pts
            elif seasons == 2:
                commit_pen = 0.04 * cc_scale   # ~4 pts

        # Novelty: if we ever pass a novelty signal (e.g., popularity), reward a bit
        pop = float(it.get("popularity") or 0.0)
        # quick sigmoid-ish:  pop_normalized ~ 0..1 for 0..200
        pop_n = max(0.0, min(1.0, pop / 200.0))
        novelty = novelty_w * pop_n

        base = 60.0
        # 20 points of headroom for ratings; taste scales to ~ +/-5,
        # novelty up to ~ +3; commitment subtracts absolute points.
        rating_part = 20.0 * (aw * aud_boost + cw * cri_boost)
        taste_part  = 35.0 * taste_b   # scale taste into points ([-2.8, +5.25] approx)
        novelty_part = 20.0 * novelty  # up to ~+3
        s = base + rating_part + taste_part + novelty_part
        s -= (commit_pen * 100.0)

        # clamp to [0, 100], then floor at 55 to keep obvious duds out
        match = round(max(0.0, min(100.0, s)), 1)

        reasons = explain_reasons(it, weights, taste_b, rating_part)

        ranked.append({
            **it,
            "match": match,
            "why": reasons
        })

    ranked.sort(key=lambda x: x["match"], reverse=True)
    return ranked