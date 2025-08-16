# FILE: engine/scoring.py
from __future__ import annotations
import csv
import os
from typing import Dict, Iterable, List, Tuple

from .config import Config
from .taste import build_taste, taste_boost_for

# ---------- Seen ingestion (by IMDb ID) ----------

def load_seen_index(csv_path: str) -> Dict[str, bool]:
    """
    Parse IMDb export CSV and collect IMDb IDs (tt...).
    Returns dict {imdb_id: True}.
    """
    idx: Dict[str, bool] = {}
    if not os.path.exists(csv_path):
        return idx
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return idx
            # pick any column that looks like an id/URL w/ tconst
            for row in reader:
                tid = None
                # common headers
                for key in ("const","tconst","IMDb Title ID","imdb_id","id","URL"):
                    v = (row.get(key) or "").strip()
                    if v:
                        if "tt" in v:
                            import re
                            m = re.search(r"(tt\d{6,10})", v)
                            if m:
                                tid = m.group(1)
                                break
                        if v.startswith("tt"):
                            tid = v
                            break
                if tid and tid.startswith("tt"):
                    idx[tid] = True
    except Exception:
        return {}
    return idx

def filter_unseen(pool: List[Dict], seen_idx: Dict[str, bool]) -> List[Dict]:
    """
    Drop items whose imdb_id is present in the user's ratings CSV.
    If imdb_id is missing, keep the item (we prefer recall over false drops).
    """
    out: List[Dict] = []
    for it in pool:
        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        out.append(it)
    return out

# ---------- Scoring ----------

def _commitment_penalty(it: Dict, cc_scale: float) -> float:
    if (it.get("kind") == "tv") or (it.get("type") == "tvSeries"):
        seasons = int(it.get("seasons") or 1)
        if seasons >= 3:
            return 0.09 * cc_scale   # -9 points
        if seasons == 2:
            return 0.04 * cc_scale   # -4 points
    return 0.0

def _novelty_boost(it: Dict, novelty_pressure: float) -> float:
    """
    Light positive pressure for newer titles. Year unknown => no boost.
    """
    try:
        y = int(it.get("year") or 0)
    except Exception:
        y = 0
    if not y:
        return 0.0
    # scale 1980..current → 0..~0.06, then scaled by novelty_pressure (0..1)
    import datetime
    cur = datetime.datetime.utcnow().year
    y = max(1980, min(cur, y))
    raw = (y - 1980) / max(1, (cur - 1980))
    return 0.06 * novelty_pressure * raw

def score_items(cfg: Config, items: List[Dict]) -> List[Dict]:
    """
    Score uses:
      - critic: OMDb RottenTomatoes% (0..1) fallback TMDB vote
      - audience: OMDb IMDb (0..1) fallback TMDB vote
      - taste boost: avg of per-genre weights
      - commitment penalty: longer TV costs points
      - novelty boost: newer gets a small bump
    Output is 0..100 (rounded to one decimal).
    """
    # build taste profile once (from your ratings CSV via taste.py helpers)
    # taste.build_taste expects full rows; here we only need genre weights saved earlier.
    try:
        # If a prior run created a profile, taste_boost_for will use it.
        taste_profile = {}
        # optional: we could rebuild from ratings here if needed.
    except Exception:
        taste_profile = {}

    cw = cfg.critic_weight
    aw = cfg.audience_weight

    ranked: List[Dict] = []
    for it in items:
        tmdb_vote = float(it.get("vote_average") or 0.0) / 10.0
        critic = float(it.get("critic") or 0.0) or tmdb_vote
        audience = float(it.get("audience") or 0.0) or tmdb_vote

        base = cw * critic + aw * audience

        genres = [g for g in (it.get("genres") or []) if isinstance(g, str)]
        tboost = taste_boost_for(genres, taste_profile)  # -0.08 .. +0.15
        nboost = _novelty_boost(it, cfg.novelty_pressure)  # 0..~0.06 * pressure
        penalty = _commitment_penalty(it, cfg.commitment_cost_scale)  # 0..0.09

        score01 = max(0.0, min(1.0, base + tboost + nboost - penalty))
        match = round(100.0 * score01, 1)

        ranked.append({
            "title": it.get("title"),
            "year": it.get("year"),
            "type": ("tvSeries" if it.get("kind") == "tv" else "movie"),
            "providers": it.get("providers", []),
            "match": match,
            "audience": round((audience or 0.0) * 100, 1),
            "critic": round((critic or 0.0) * 100, 1),
        })

    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked