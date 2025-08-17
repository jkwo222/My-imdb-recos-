# engine/rank.py
from __future__ import annotations
from typing import Dict, List, Any

# Default: audience > critic
DEFAULT_WEIGHTS = {
    "audience_weight": 0.65,   # you care more about the audience score
    "critic_weight":   0.35,
    "commitment_cost_scale": 1.0,
    "novelty_weight":  0.15,   # small tie-breaker
    "min_match_cut":   58.0,
}

def _to_01(v, lo, hi) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if hi == lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))

def _commitment_penalty(it: Dict[str, Any], scale: float) -> float:
    t = (it.get("type") or "").strip()
    if t == "tvSeries":
        seasons = int(it.get("seasons") or 1)
        if seasons >= 3:
            return 0.09 * scale
        if seasons == 2:
            return 0.04 * scale
    return 0.0

def _novelty_bonus(it: Dict[str, Any], w: float) -> float:
    year = it.get("year")
    pop  = it.get("popularity")
    yb = 0.03 if isinstance(year, int) and year >= 2022 else (0.02 if isinstance(year, int) and year >= 2018 else 0.0)
    pb = 0.02 * _to_01(pop or 0.0, 0.0, 2000.0)
    return w * (yb + pb)

def score_item(it: Dict[str, Any], w: Dict[str, Any], taste_boost: float = 0.0) -> float:
    aw = float(w.get("audience_weight", DEFAULT_WEIGHTS["audience_weight"]))
    cw = float(w.get("critic_weight",   DEFAULT_WEIGHTS["critic_weight"]))
    cc = float(w.get("commitment_cost_scale", DEFAULT_WEIGHTS["commitment_cost_scale"]))
    nw = float(w.get("novelty_weight", DEFAULT_WEIGHTS["novelty_weight"]))

    # audience (prefer imdb/audience -> tmdb)
    if it.get("audience") not in (None, ""):
        audience_01 = float(it["audience"])
        if audience_01 > 1.0:  # if given as 0..10
            audience_01 = _to_01(audience_01, 0.0, 10.0)
    elif it.get("imdb_rating") not in (None, ""):
        audience_01 = _to_01(it["imdb_rating"], 0.0, 10.0)
    else:
        audience_01 = _to_01(it.get("tmdb_vote", 0.0), 0.0, 10.0)

    # critic (prefer rt -> tmdb)
    if it.get("critic") not in (None, ""):
        critic_01 = float(it["critic"])
        if critic_01 > 1.0:  # if given as %
            critic_01 = _to_01(critic_01, 0.0, 100.0)
    elif it.get("rt") not in (None, ""):
        critic_01 = _to_01(it["rt"], 0.0, 100.0)
    else:
        critic_01 = _to_01(it.get("tmdb_vote", 0.0), 0.0, 10.0)

    base = aw * audience_01 + cw * critic_01
    penalty = _commitment_penalty(it, cc)
    bonus   = _novelty_bonus(it, nw)
    match = base + taste_boost + bonus - penalty

    # map to 0..100 with neutral area ~50–65
    score = 60.0 + 20.0 * match
    return round(max(40.0, min(99.0, score)), 1)

def rank_items(items: List[Dict[str, Any]],
               w: Dict[str, Any],
               taste_for: callable | None = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        genres = it.get("genres") or []
        tboost = 0.0
        if taste_for:
            try:
                tboost = float(taste_for(genres))
            except Exception:
                tboost = 0.0
        m = score_item(it, w, tboost)
        row = dict(it)
        row["match"] = m
        if "type" not in row:
            kind = it.get("kind")
            row["type"] = "tvSeries" if kind == "tv" else "movie"
        out.append(row)
    out.sort(key=lambda r: r.get("match", 0.0), reverse=True)
    return out