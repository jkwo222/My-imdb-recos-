# engine/seen_index.py
from __future__ import annotations
import csv, os, re
from dataclasses import dataclass, field
from typing import Dict, Set, Tuple, List

IMDB_RATINGS_CSV_PATH = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

_norm_re = re.compile(r"[^a-z0-9]+")

def _norm_title(t: str) -> str:
    t = (t or "").lower()
    t = _norm_re.sub("", t)
    return t

@dataclass
class SeenIndex:
    ttids: Set[str] = field(default_factory=set)
    by_title_year: Set[Tuple[str,int]] = field(default_factory=set)

    @property
    def keys(self) -> Set[str]:
        # mainly for size logging
        return set(self.ttids) | {f"{t}:{y}" for t,y in self.by_title_year}

def load_imdb_ratings_csv_auto() -> Tuple[List[Dict[str,str]], str | None]:
    """
    Load IMDb ratings CSV from env path (repo), or fallback to /mnt/data for local runs.
    Returns (rows, path_used)
    """
    candidates = []
    if IMDB_RATINGS_CSV_PATH:
        candidates.append(IMDB_RATINGS_CSV_PATH)
    # local dev fallback
    candidates.append("/mnt/data/ratings.csv")

    for path in candidates:
        try:
            if path and os.path.exists(path):
                rows: List[Dict[str,str]] = []
                with open(path, "r", encoding="utf-8", newline="") as f:
                    r = csv.DictReader(f)
                    for row in r:
                        rows.append(row)
                return rows, path
        except Exception:
            continue
    return [], None

def update_seen_from_ratings(rows: List[Dict[str,str]]) -> Tuple[SeenIndex,int]:
    seen = SeenIndex()
    added = 0
    for row in rows:
        # IMDb's export commonly uses "const" for tt id, "Title", "Year"
        tid = (row.get("const") or row.get("tconst") or "").strip()
        title = (row.get("Title") or row.get("title") or "").strip()
        year_str = (row.get("Year") or row.get("year") or "").strip()
        year = None
        try:
            year = int(year_str) if year_str else None
        except:
            year = None

        if tid.startswith("tt"):
            if tid not in seen.ttids:
                seen.ttids.add(tid)
                added += 1

        if title and year:
            key = (_norm_title(title), year)
            if key not in seen.by_title_year:
                seen.by_title_year.add(key)
                # don't count twice toward 'added'
    return seen, added

def load_seen() -> SeenIndex:
    # empty fallback when CSV absent
    return SeenIndex()