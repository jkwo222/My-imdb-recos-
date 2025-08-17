# engine/exclusions.py
from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

def load_seen_index(csv_path: Path) -> Dict[str, bool]:
    """
    Parse data/user/ratings.csv and build a set-like dict of IMDb IDs (tt...).
    Accepts columns: imdb_id OR tconst OR a 'url' that contains the tt id.
    """
    idx: Dict[str, bool] = {}
    if not csv_path.exists():
        return idx

    with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            imdb = (row.get("imdb_id") or row.get("tconst") or "").strip()
            if not imdb and row.get("url"):
                # extract from a URL like https://www.imdb.com/title/tt1234567/
                import re
                m = re.search(r"(tt\d+)", row.get("url"))
                imdb = m.group(1) if m else ""
            if imdb and imdb.startswith("tt"):
                idx[imdb] = True
    return idx


def filter_unseen(items: List[dict], seen_idx: Dict[str, bool]) -> List[dict]:
    """
    Drop any item with an imdb_id in seen_idx (if available).
    If an item lacks imdb_id, keep it for now (we try to attach ids upstream).
    """
    out: List[dict] = []
    for it in items:
        imdb = str(it.get("imdb_id") or "")
        if imdb and imdb in seen_idx:
            continue
        out.append(it)
    return out