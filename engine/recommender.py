# engine/recommender.py
from typing import List, Dict, Any
from .seen_index import is_seen

def score(c: Dict[str,Any], w: Dict[str,Any]) -> float:
    # Normalize inputs
    crit = float(c.get("critic", 0.0))      # RT 0..1
    aud  = float(c.get("audience", 0.0))    # IMDb 0..1
    consensus = (w.get("critic_weight",0.5) * crit) + (w.get("audience_weight",0.5) * aud)

    s = 60.0 + 30.0 * consensus  # 60..90 before adjustments

    # Commitment cost: apply to ALL unseen multi-season series (miniseries exempt)
    if c.get("type") == "tvSeries":
        seasons = int(c.get("seasons", 1))
        if seasons >= 3:
            s -= 10.0 * w.get("commitment_cost_scale", 1.0)
        elif seasons == 2:
            s -= 5.0 * w.get("commitment_cost_scale", 1.0)

    # Novelty pressure (light)
    s += 5.0 * float(w.get("novelty_pressure", 0.15))

    return max(50.0, min(98.0, round(s, 1)))

def recommend(catalog: List[Dict[str,Any]], w: Dict[str,Any]) -> List[Dict[str,Any]]:
    out = []
    for c in catalog:
        if is_seen(c.get("title",""), c.get("imdb_id",""), int(c.get("year",0))):
            continue
        x = dict(c)
        x["match"] = score(c, w)
        out.append(x)
    out.sort(key=lambda x: x["match"], reverse=True)
    return out[:50]