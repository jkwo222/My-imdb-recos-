import os
import math
from typing import List, Dict, Any, Tuple

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default

def _bayes_rating(r: float, v: int, m: int = 150, mu: float = 6.5) -> float:
    r = float(r or 0.0); v = int(v or 0)
    return (v/(v+m))*r + (m/(v+m))*mu

def score_items(
    items: List[dict],
    weights: Dict[str, float],
    shortlist_size: int = 250,
) -> Tuple[List[dict], Dict[str, Any]]:
    cw = float(weights.get("critic", 0.25))
    aw = float(weights.get("audience", 0.75))
    np = float(weights.get("novelty_pressure", 0.15))
    cc = float(weights.get("commitment_cost_scale", 1.0))

    enriched = []
    for it in items:
        r = float(it.get("tmdb_vote_average", 0.0))
        v = int(it.get("tmdb_vote_count", 0))
        bayes = _bayes_rating(r, v)
        critic = r
        audience = bayes

        # Novelty bump (recentness)
        year = it.get("year")
        if isinstance(year, int) and year >= 2000:
            age = max(0, 2025 - year)
            novelty = 1.0 - min(1.0, age / 15.0)
        else:
            novelty = 0.2

        score = (cw * critic + aw * audience) * (1.0 + np * novelty)
        it2 = dict(it)
        it2["__score_stage1"] = score
        it2["match"] = round(max(0.0, min(100.0, score * 10.0)), 1)
        enriched.append(it2)

    enriched.sort(key=lambda x: (-x["__score_stage1"], -x.get("tmdb_vote_count", 0), -x.get("popularity", 0.0)))
    final = enriched[:max(50, shortlist_size)]
    return final, {"ranked": len(final), "weights": {"critic": cw, "audience": aw, "novelty_pressure": np, "commitment_cost_scale": cc}}