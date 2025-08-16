# File: engine/scoring.py
from __future__ import annotations
import csv
import os
from typing import Dict, List
from .config import Config

def load_seen_index(csv_path: str) -> Dict[str, bool]:
    """
    Parse IMDb export for 'const'/'tconst'/etc. → {imdb_id: True}.
    Returns {} if file missing or cannot be parsed.
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
            norm = {h.lower().strip(): h for h in (reader.fieldnames or [])}
            key_name = None
            for cand in ("const", "tconst", "imdb title id", "imdb_id", "id"):
                if cand in norm:
                    key_name = norm[cand]; break
            if key_name is None:
                for h in (reader.fieldnames or []):
                    if (h or "").lower().startswith("tt"):
                        key_name = h; break
            for row in reader:
                val = (row.get(key_name) or "").strip()
                if val.startswith("tt") and len(val) >= 7:
                    idx[val] = True
    except Exception:
        return {}
    return idx

def filter_unseen(pool: List[Dict], seen_idx: Dict[str, bool]) -> List[Dict]:
    """
    Pool currently lacks reliable IMDb IDs → no-op.
    Keep as-is until mapping is added.
    """
    return pool

def score_items(cfg: Config, items: List[Dict]) -> List[Dict]:
    """
    Match score from TMDB vote_average (audience proxy) with small TV penalty.
    """
    cw = cfg.critic_weight
    aw = cfg.audience_weight
    cc = cfg.commitment_cost_scale

    ranked = []
    for it in items:
        aud = max(0.0, min(1.0, (it.get("vote_average", 0.0) or 0.0) / 10.0))
        cri = 0.0
        base = aw * aud + cw * cri
        penalty = 0.02 * cc if it.get("kind") == "tv" else 0.0
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