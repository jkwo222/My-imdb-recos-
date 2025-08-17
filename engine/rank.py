# engine/rank.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import math

JSON = Dict[str, Any]

def _clamp01(x: float) -> float:
    return 0.0 if x is None else max(0.0, min(1.0, float(x)))

def _score_from_pct(pct: Optional[float], neutral: float = 0.60, scale: float = 2.0) -> float:
    """
    Map 0..1 → [-1..+1] with a neutral band around 0.60.
    scale controls how steeply we boost/penalize away from neutral.
    """
    if pct is None:
        return 0.0
    p = _clamp01(pct)
    return max(-1.0, min(1.0, scale * (p - neutral)))

def _audience_component(item: JSON) -> float:
    """
    Prefer OMDb's IMDb audience (0..1). Fallback to TMDB vote_average / 10.
    """
    if item.get("audience") is not None:
        return float(item["audience"])
    va = item.get("vote_average")
    return (float(va) / 10.0) if va is not None else 0.0

def _critic_component(item: JSON) -> float:
    """
    Prefer RottenTomatoes % from OMDb (0..1). Fallback to TMDB vote_average / 10.
    """
    if item.get("critic") is not None:
        return float(item["critic"])
    va = item.get("vote_average")
    return (float(va) / 10.0) if va is not None else 0.0

def _novelty_bonus(year: Optional[int]) -> float:
    """
    Small bonus for newer releases; ~0..+0.2 across 2005→2025.
    """
    try:
        y = int(year or 0)
    except Exception:
        return 0.0
    if y <= 0:
        return 0.0
    bonus = (y - 2005) / 20.0  # 2005→2025
    return max(0.0, min(1.0, bonus)) * 0.2

def _commitment_penalty(item: JSON, scale: float) -> float:
    """
    Penalize very long shows a bit to reflect commitment cost.
    (applied as negative points in final 0..100 scale)
    """
    if (item.get("type") or "").lower() not in {"tvseries", "tvminiseries", "tv"}:
        return 0.0
    seasons = int(item.get("seasons") or 1)
    if seasons >= 5:
        return 10.0 * scale
    if seasons >= 3:
        return 6.0 * scale
    if seasons == 2:
        return 3.0 * scale
    return 0.0

def _taste_boost_for(genres: List[str] | None, profile: Dict[str, float]) -> float:
    if not genres or not profile:
        return 0.0
    vals = [profile.get(g.lower(), 0.0) for g in genres]
    return sum(vals) / len(vals) if vals else 0.0  # already small (e.g., -0.08..+0.15)

def rank_candidates(
    catalog: List[JSON],
    weights: Dict[str, float],
    taste_profile: Dict[str, float] | None = None,
    *,
    top_k: int = 500
) -> List[JSON]:
    """
    Returns ranked items with `match` (0..100) and a `why` breakdown.
    Audience is weighted more than critic (weights come from weights.py).
    """
    aw = float(weights.get("audience_weight", 0.65))
    cw = float(weights.get("critic_weight", 0.30))
    nw = float(weights.get("novelty_weight", 0.05))
    cc = float(weights.get("commitment_cost_scale", 1.0))

    # Normalize trio just in case
    s = max(aw + cw + nw, 1e-9)
    aw, cw, nw = aw / s, cw / s, nw / s

    ranked: List[JSON] = []

    for it in catalog:
        aud_pct = _audience_component(it)         # 0..1
        cri_pct = _critic_component(it)           # 0..1
        aud = _score_from_pct(aud_pct, neutral=0.60, scale=2.2)  # heavier swing
        cri = _score_from_pct(cri_pct, neutral=0.60, scale=1.6)  # lighter than audience
        nov = _novelty_bonus(it.get("year"))

        taste = _taste_boost_for(it.get("genres") or [], taste_profile or {})  # ~ -0.08..+0.15
        # Base in [-1..+1]
        base = (aw * aud) + (cw * cri) + (nw * nov) + (0.25 * taste)

        # Map to ~50..98, then subtract commitment penalty
        match = 60.0 + 25.0 * base
        match -= _commitment_penalty(it, cc)
        match = max(50.0, min(98.0, match))

        ranked.append({
            **it,
            "match": round(match, 1),
            "why": {
                "audience": round(aud, 3),
                "critic": round(cri, 3),
                "novelty": round(nov, 3),
                "taste": round(taste, 3),
                "weights": {"audience": aw, "critic": cw, "novelty": nw},
            }
        })

    ranked.sort(key=lambda x: (x.get("match") or 0.0), reverse=True)
    return ranked[:top_k]