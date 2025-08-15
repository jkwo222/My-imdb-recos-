# engine/taste.py
import os, json, pathlib
from typing import Dict, List
from .catalog_builder import _omdb_enrich

PROFILE = pathlib.Path("data/taste_profile.json")

def _norm_rating(your: float) -> float:
    # Center at 6/10; scale so +/-4 points ~ +/-1.0
    return max(-1.5, min(1.5, (your - 6.0) / 4.0))

def build_taste(rows: List[Dict]) -> Dict[str, float]:
    """
    Build per-genre/tag affinities from your rated titles.
    Uses OMDb (cached) to fetch genres for rated items if needed.
    """
    if not rows:
        return json.load(open(PROFILE,"r")) if PROFILE.exists() else {}

    sums, counts = {}, {}
    for r in rows:
        your = float(r.get("your_rating") or 0.0)
        if your <= 0: continue
        imdb_id = (r.get("imdb_id") or "").strip()
        title = r.get("title",""); year = int(r.get("year") or 0)
        e = _omdb_enrich(title, year, "tv" if "tv" in (r.get("type") or "") else "movie", imdb_id=imdb_id)
        genres = e.get("genres") or []
        if not genres: continue
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