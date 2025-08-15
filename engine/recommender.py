# engine/recommender.py
from typing import List, Dict, Any
from .seen_index import is_seen

def score(c: Dict[str, Any], w: Dict[str, Any]) -> float:
    base = 70.0
    crit = float(c.get("critic", 0))
    aud  = float(c.get("audience", 0))
    s = base + 15.0 * (w.get("critic_weight", 0.5) * crit + w.get("audience_weight", 0.5) * aud)

    # commitment cost for multi-season unseen TV
    if c.get("type") == "tvSeries":
        seasons = int(c.get("seasons", 1))
        if seasons >= 3:
            s -= 9.0 * w.get("commitment_cost_scale", 1.0)
        elif seasons == 2:
            s -= 4.0 * w.get("commitment_cost_scale", 1.0)

    # light penalty when both signals are missing to avoid "all 70.0"
    if crit == 0.0 and aud == 0.0:
        s -= 8.0

    return max(60.0, min(98.0, s))

def recommend(catalog: List[Dict[str, Any]], w: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for c in catalog:
        if is_seen(c.get("title",""), c.get("imdb_id",""), int(c.get("year",0))):
            continue
        x = dict(c)
        x["match"] = round(score(c, w), 1)
        out.append(x)
    out.sort(key=lambda x: x["match"], reverse=True)
    return out[:50]