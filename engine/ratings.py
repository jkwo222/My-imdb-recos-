from __future__ import annotations
import csv
import os
import re
from typing import Set

_TCONST_RE = re.compile(r"(tt\d{6,9})")

def _maybe_tconst(cell: str) -> str | None:
    if not cell:
        return None
    m = _TCONST_RE.search(cell.strip())
    if m:
        return m.group(1)
    return None

def load_seen_ids(path: str = "data/ratings.csv") -> Set[str]:
    """Load IMDb IDs from the user's ratings CSV (const or URL field)."""
    seen: Set[str] = set()
    if not os.path.exists(path):
        return seen
    with open(path, "r", encoding="utf-8") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = sniffer.sniff(sample) if sample else csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        headers = [h.strip().lower() for h in reader.fieldnames or []]

        # Common column names in IMDb exports
        possible_id_cols = [c for c in headers if c in {"const","tconst","imdb id","imdb_id"}]
        for row in reader:
            tid = None
            for c in possible_id_cols:
                if tid:
                    break
                tid = _maybe_tconst(row.get(c, ""))
            if not tid:
                # try URL-based
                for c in headers:
                    tid = _maybe_tconst(row.get(c, ""))
                    if tid:
                        break
            if tid:
                seen.add(tid)
    return seen