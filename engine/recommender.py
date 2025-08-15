# engine/recommender.py
from typing import List, Dict, Any
from .seen_index import is_seen
from .taste import taste_boost_for

def score(c: Dict[str,Any], w: Dict[str,Any], taste_profile: Dict[str,float]) -> float:
    crit = float(c.get("critic", 0.0))      # RT (0..1)
    aud  = float(c.get("audience", 0.0))    # IMDb (0..1)
    consensus = (w.get("critic_weight",0.52) * crit) + (w.get("audience_weight",0.48) * aud)

    # Taste boost from your genre affinities (0..~0.15) → scaled to ~0..+6 points
    tb = taste_boost_for(c.get("genres") or [], taste_profile)
    taste_points = 40.0 * tb  # up to about +6.0

    s = 60.0 + 28.0 * consensus + taste_points

    # Commitment cost: all unseen multi-season shows are penalized; miniseries exempt
    if c.get("type") == "tvSeries":
        seasons = int(c.get("seasons", 1))
        if seasons >= 3:
            s -= 10.0 * w.get("commitment_cost_scale", 1.0)
        elif seasons == 2:
            s -= 5.0 * w.get("commitment_cost_scale", 1.0)

    # Light novelty pressure
    s += 5.0 * float(w.get("novelty_pressure",0.15))

    return max(50.0, min(98.0, round(s, 1)))

def recommend(catalog: List[Dict[str,Any]], w: Dict[str,Any], taste_profile: Dict[str,float]) -> List[Dict[str,Any]]:
    out = []
    for c in catalog:
        if is_seen(c.get("title",""), c.get("imdb_id",""), int(c.get("year",0))):
            continue
        x = dict(c)
        x["match"] = score(c, w, taste_profile)
        out.append(x)
    out.sort(key=lambda x: x["match"], reverse=True)
    return out