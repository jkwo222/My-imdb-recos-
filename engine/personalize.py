# engine/personalize.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import math

def _to_list(x) -> List[str]:
    if not x: return []
    if isinstance(x, (list, tuple)): return [str(i) for i in x if i]
    return [str(x)]

def genre_weights_from_profile(
    items: List[Dict[str,Any]],
    user_profile: Dict[str,Dict],
    imdb_id_field: str="tconst",
) -> Dict[str, float]:
    """
    Weight genres by (my_rating - 6.0) across rated titles found in items (where we know the mapping).
    6.0 ~ neutral; >6 favors, <6 disfavors.
    """
    acc = defaultdict(float)
    cnt = defaultdict(int)

    # index items by imdb id for genre lookup
    by_imdb = {}
    for it in items:
        tid = it.get(imdb_id_field) or it.get("imdb_id") or it.get("tconst")
        if not tid: 
            continue
        by_imdb[str(tid)] = it

    for tid, row in user_profile.items():
        it = by_imdb.get(str(tid))
        if not it:
            continue
        genres = _to_list(it.get("genres")) or []
        r = row.get("my_rating")
        if r is None:
            continue
        delta = float(r) - 6.0
        for g in genres:
            acc[g] += delta
            cnt[g] += 1

    # normalize to [0, 1] with soft cap
    if not acc:
        return {}
    mx = max(abs(v) for v in acc.values()) or 1.0
    out = {g: (0.5 + 0.5*(v/mx)) for g,v in acc.items()}  # map [-mx,+mx] -> [0,1]
    # Recenter so min ~ 0 and emphasize >0.5
    return {g: round(w, 4) for g,w in out.items()}

def apply_personal_score(
    items: List[Dict[str,Any]],
    genre_weights: Dict[str,float],
    base_key: str="imdb_rating",
) -> None:
    """
    Mutates items: adds 'score' (0–100). Combines base score (IMDb/10) with genre fit.
    """
    for it in items:
        base = it.get(base_key)
        try:
            base10 = float(base) if base is not None else math.nan
        except Exception:
            base10 = math.nan
        base100 = (base10 * 10.0) if not math.isnan(base10) else 60.0  # default mid

        g = _to_list(it.get("genres"))
        if genre_weights and g:
            fit = sum(genre_weights.get(x, 0.5) for x in g) / len(g)
            # center around 0.5 => [-0.5..+0.5], scale to ±15 points
            adj = (fit - 0.5) * 30.0
        else:
            adj = 0.0

        it["score"] = max(0.0, min(100.0, base100 + adj))