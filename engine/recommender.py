from typing import List, Dict, Any
from .seen_index import is_seen

def _norm(v, lo, hi):
    try:
        x = float(v)
    except Exception:
        return 0.0
    if hi == lo: return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))

def score(c: Dict[str,Any], w: Dict[str,Any]) -> float:
    # critic: RT if available, else TMDB vote
    critic = None
    if c.get("rt"):
        critic = float(c.get("rt", 0.0)) / 100.0
    elif c.get("tmdb_vote"):
        critic = float(c.get("tmdb_vote", 0.0)) / 10.0
    else:
        critic = 0.0

    # audience: IMDb if available, else TMDB vote
    audience = None
    if c.get("imdb_rating"):
        audience = float(c.get("imdb_rating", 0.0)) / 10.0
    elif c.get("tmdb_vote"):
        audience = float(c.get("tmdb_vote", 0.0)) / 10.0
    else:
        audience = 0.0

    base = 60.0
    s = base + 20.0*(w.get("critic_weight",0.5)*critic + w.get("audience_weight",0.5)*audience)

    # Commitment penalty for unseen multi-season series
    if c.get("type")=="tvSeries":
        seasons = int(c.get("seasons") or 1)
        if seasons >= 3:
            s -= 9.0 * w.get("commitment_cost_scale", 1.0)
        elif seasons == 2:
            s -= 4.0 * w.get("commitment_cost_scale", 1.0)

    return round(max(55.0, min(98.0, s)), 1)

def recommend(catalog: List[Dict[str,Any]], w: Dict[str,Any]) -> List[Dict[str,Any]]:
    out=[]
    for c in catalog:
        if is_seen(c.get("title",""), c.get("imdb_id",""), int(c.get("year",0))): 
            continue
        x=dict(c); x["match"]=score(c,w); out.append(x)
    out.sort(key=lambda x:x["match"], reverse=True)
    return out[:50]