from __future__ import annotations
import csv
import os
from typing import Dict, Iterable, List, Tuple

from .config import Config

def load_seen_index(csv_path: str) -> Dict[str, bool]:
    """
    Robustly parse an IMDb export or custom list CSV.
    Accepts common headers:
      - 'const' / 'tconst' / 'Const' / 'IMDb Title ID' (tt1234567)
    Returns dict {imdb_id: True}
    If file missing or empty, returns {} (and caller can warn).
    """
    idx: Dict[str, bool] = {}
    if not os.path.exists(csv_path):
        return idx
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            sniffer = csv.Sniffer()
            sample = f.read(4096)
            f.seek(0)
            dialect = sniffer.sniff(sample) if sample else csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            # Normalize headers
            norm = {h.lower().strip(): h for h in (reader.fieldnames or [])}
            key_name = None
            for cand in ("const", "tconst", "imdb title id", "imdb_id", "id"):
                if cand in norm:
                    key_name = norm[cand]
                    break
            if key_name is None:
                # Best effort: look for a "tt..." column
                for h in (reader.fieldnames or []):
                    if h.lower().startswith("tt"):
                        key_name = h
                        break
            for row in reader:
                val = (row.get(key_name) or "").strip()
                if val.startswith("tt") and len(val) >= 7:
                    idx[val] = True
    except Exception:
        # If anything goes wrong, we just return empty index.
        return {}
    return idx

def filter_unseen(pool: List[Dict], seen_idx: Dict[str, bool]) -> List[Dict]:
    """
    We don't have IMDb IDs at discover time; leave pool unchanged here.
    (IMDb matching typically happens later; if you have a mapping step, plug it here.)
    For now we return the pool as-is so we don't erroneously drop everything.
    """
    return pool

def score_items(cfg: Config, items: List[Dict]) -> List[Dict]:
    """
    Compute a simple match score:
      - audience proxy from TMDB vote_average (0..10) -> 0..1
      - critic score defaults to 0 unless you augment later
      - combine via weights, scale to 100
      - light penalty for "commitment cost" (tv series)
    """
    cw = cfg.critic_weight
    aw = cfg.audience_weight
    cc = cfg.commitment_cost_scale

    ranked = []
    for it in items:
        aud = max(0.0, min(1.0, (it.get("vote_average", 0.0) or 0.0) / 10.0))
        cri = 0.0  # placeholder; integrate OMDb/Metascore later if desired
        base = aw * aud + cw * cri
        # rough commitment penalty: tv costs slightly more than movie
        penalty = 0.0
        if it.get("kind") == "tv":
            penalty = 0.02 * cc  # 2 points off after scaling
        match = round(100.0 * max(0.0, base - penalty), 1)
        ranked.append({
            "title": it.get("title"),
            "year": it.get("year"),
            "type": "tvSeries" if it.get("kind") == "tv" else "movie",
            "audience": round(aud * 100, 1),
            "critic": round(cri * 100, 1),
            "match": match,
            "providers": it.get("providers", []),
        })
    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked