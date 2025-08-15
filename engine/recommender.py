from typing import List, Dict, Any
from .seen_index import is_seen

def _safe01(x):
    try:
        if x is None: return None
        x = float(x)
        if x <= 0: return None
        if x > 1: 
            # if someone passed 0..100 accidentally
            return min(1.0, x/100.0)
        return x
    except:
        return None

def score(c: Dict[str,Any], w: Dict[str,Any]) -> float:
    base = 70.0
    crit = _safe01(c.get("critic"))
    aud  = _safe01(c.get("audience"))
    parts = []
    if crit is not None: parts.append(w.get("critic_weight",0.5)*crit)
    if aud  is not None: parts.append(w.get("audience_weight",0.5)*aud)
    s = base + 15.0*(sum(parts) if parts else 0.5)  # if both missing, neutral .5

    # Penalty for multi-season unseen (apply equally to all multi-season shows)
    if c.get("type") == "tvSeries":
        seasons = c.get("seasons") or 0
        if seasons >= 2:
            s -= 8.0 * w.get("commitment_cost_scale",1.0)

    # Clamp
    return round(max(60.0, min(98.0, s)), 1)

def recommend(catalog: List[Dict[str,Any]], w: Dict[str,Any]) -> List[Dict[str,Any]]:
    out=[]
    for c in catalog:
        if is_seen(c.get("title",""), c.get("imdb_id",""), int(c.get("year",0))):
            continue
        x=dict(c); x["match"]=score(c,w)
        # quick “why” string
        reasons=[]
        if c.get("critic") is not None: reasons.append(f"critic {int(round(c['critic']*100))}%")
        if c.get("audience") is not None: reasons.append(f"audience {int(round(c['audience']*100))}%")
        if c.get("type")=="tvSeries" and (c.get("seasons") or 0) >= 2:
            reasons.append("multi-season penalty")
        x["why"]="; ".join(reasons) if reasons else "signal sparse"
        out.append(x)
    out.sort(key=lambda x:x["match"], reverse=True)
    return out[:50]