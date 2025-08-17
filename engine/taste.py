# engine/taste.py
from __future__ import annotations
import os, json, pathlib
from typing import Dict, List
from . import imdb_bulk

PROFILE = pathlib.Path("data/taste_profile.json")

def _norm_rating(your: float) -> float:
    # Center at 6/10; scale so +/-4 points ~ +/-1.0
    return max(-1.5, min(1.5, (your - 6.0) / 4.0))

def build_taste(rows: List[Dict]) -> Dict[str, float]:
    """
    Build per-genre affinities from your rated titles using IMDb TSV genres.
    """
    if not rows:
        return json.load(open(PROFILE,"r")) if PROFILE.exists() else {}

    imdb_bulk.load()
    sums, counts = {}, {}
    for r in rows:
        try:
            your = float(r.get("your_rating") or 0.0)
        except Exception:
            continue
        if your <= 0: 
            continue
        imdb_id = (r.get("imdb_id") or "").strip()
        if not imdb_id:
            continue
        genres = imdb_bulk.get_genres(imdb_id) or []
        if not genres:
            continue
        val = _norm_rating(your)
        for g in genres:
            sums[g] = sums.get(g, 0.0) + val
            counts[g] = counts.get(g, 0) + 1

    prof = {}
    for g, s in sums.items():
        mean = s / max(1, counts[g])
        # Clamp: -0.08 .. +0.15 (later scaled in scorer)
        prof[g] = float(max(-0.08, min(0.15, mean * 0.12)))

    os.makedirs("data", exist_ok=True)
    json.dump(prof, open(PROFILE,"w"), indent=2)
    return prof

def taste_boost_for(genres: List[str], profile: Dict[str, float]) -> float:
    if not genres or not profile: return 0.0
    vals = [profile.get(g, 0.0) for g in genres]
    if not vals: return 0.0
    return sum(vals)/len(vals)