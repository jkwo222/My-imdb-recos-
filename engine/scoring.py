# FILE: engine/scoring.py
from __future__ import annotations
from typing import Dict, Iterable, List

from .config import Config
from .seen_index import SeenIndex, load_imdb_ratings_csv_auto
from .filtering import filter_unseen as _filter_unseen_titles

def load_seen_index(csv_path_ignored: str) -> SeenIndex:
    """
    Build a SeenIndex from the user's IMDb ratings CSV (title+year tolerant).
    Runner calls len() on this; SeenIndex implements __len__.
    """
    idx, count, _ = load_imdb_ratings_csv_auto()
    return idx

def filter_unseen(pool: Iterable[Dict], seen_idx: SeenIndex) -> List[Dict]:
    """
    Drop items that match seen titles (tolerant title+year + kind).
    Requires items to carry:
      - title (str), year (int|None)
      - type ('movie'|'tvSeries')
    """
    return _filter_unseen_titles(pool, seen_idx)

def score_items(cfg: Config, items: List[Dict]) -> List[Dict]:
    """
    Simple match score:
      - audience proxy from TMDB vote_average (0..10) -> 0..1
      - critic score currently 0 (extend with OMDb/RT if desired)
      - combine via weights, scale to 100
      - slight penalty for tv commitment
    """
    cw = cfg.critic_weight
    aw = cfg.audience_weight
    cc = cfg.commitment_cost_scale

    ranked = []
    for it in items:
        aud = max(0.0, min(1.0, (it.get("vote_average", 0.0) or 0.0) / 10.0))
        cri = 0.0
        base = aw * aud + cw * cri
        penalty = 0.02 * cc if (it.get("type") == "tvSeries") else 0.0
        match = round(100.0 * max(0.0, base - penalty), 1)
        ranked.append({
            "title": it.get("title"),
            "year": it.get("year"),
            "type": it.get("type") or ("tvSeries" if it.get("kind")=="tv" else "movie"),
            "audience": round(aud * 100, 1),
            "critic": round(cri * 100, 1),
            "match": match,
            "providers": it.get("providers", []),
        })
    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked