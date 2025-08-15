# engine/scoring.py
from __future__ import annotations
from typing import List, Dict, Any

def _commitment_cost(item: Dict[str, Any]) -> float:
    """
    Very light penalty for longer commitment (TV vs movie), scaled by vote scarcity.
    You can evolve this later with runtime/seasons if you enrich items.
    """
    typ = item.get("type")
    votes = max(1, int(item.get("tmdb_votes") or 1))
    base = 0.0 if typ == "movie" else 2.0  # tiny nudge against very long series
    return base * (1000 / votes) ** 0.15  # smaller penalty if many votes

def _novelty_bonus(item: Dict[str, Any]) -> float:
    """
    Tiny recency/niche bonus: helps newer or less-exposed content bubble up.
    """
    year = item.get("year") or 0
    recent = max(0, year - 2000) / 25.0  # 0..1 for ~2000..2025
    pop = float(item.get("pop") or 0.0)
    niche = 1.0 / (1.0 + pop)**0.15  # very gentle
    return 3.0 * recent * niche  # 0..~3

def score_and_rank(items: List[Dict[str, Any]],
                   critic_weight: float = 0.35,
                   audience_weight: float = 0.65,
                   novelty_pressure: float = 0.15,
                   commitment_cost_scale: float = 1.0) -> List[Dict[str, Any]]:
    out = []
    for x in items:
        aud10 = float(x.get("tmdb_vote") or 0.0)           # 0..10
        aud = max(0.0, min(100.0, aud10 * 10.0))          # 0..100
        # Until (if) you enrich with a real critic score, map critic≈audience
        crit = aud
        blend = audience_weight * aud + critic_weight * crit
        bonus = novelty_pressure * _novelty_bonus(x)
        penalty = commitment_cost_scale * _commitment_cost(x)
        match = blend + bonus - penalty
        y = dict(x)
        y["match"] = match
        out.append(y)
    out.sort(key=lambda z: (z.get("match", 0.0), z.get("tmdb_votes", 0)), reverse=True)
    return out