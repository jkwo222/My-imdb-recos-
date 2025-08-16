# FILE: engine/seen_index.py
from __future__ import annotations
import csv
import os
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

RATINGS_PATH_ENV = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

def _norm_title(t: str) -> str:
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    repl = [("’","'"), ("&"," and "), (":"," "), ("-"," "), ("/"," "), ("."," "), (","," "), ("!"," "), ("?"," ")]
    for a,b in repl: t = t.replace(a,b)
    for art in (" the ", " a ", " an "):
        if t.startswith(art.strip()+" "): t = t[len(art):]
    t = " ".join(t.split())
    return t

def _kind_from_title_type(tt: str) -> Optional[str]:
    tt = (tt or "").strip().lower()
    if tt in ("movie", "feature", "video", "tvmovie"): return "movie"
    if tt in ("tv series", "tvseries", "tv miniseries", "tvminiseries"): return "tvSeries"
    if tt in ("tvepisode", "episode"): return None  # ignore episodes
    return None

def _parse_int(x: str) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

class SeenIndex:
    """Title+year keyed index with tolerant lookups."""
    def __init__(self) -> None:
        self.keys = set()  # e.g. "movie:normalized:1997" or "tvSeries:normalized:*"

    def __len__(self) -> int:
        return len(self.keys)

    def add(self, kind: str, title: str, year: Optional[int]) -> None:
        if not title or not kind: return
        nt = _norm_title(title)
        if not nt: return
        y = str(year) if year else "*"
        self.keys.add(f"{kind}:{nt}:{y}")

    def has(self, kind: str, title: str, year: Optional[int]) -> bool:
        if not title or not kind: return False
        nt = _norm_title(title)
        if not nt: return False
        y = str(year) if year else "*"
        if f"{kind}:{nt}:{y}" in self.keys: return True
        if f"{kind}:{nt}:*" in self.keys: return True
        if year is not None:
            if f"{kind}:{nt}:{year-1}" in self.keys: return True
            if f"{kind}:{nt}:{year+1}" in self.keys: return True
        return False

def _read_csv(path: str) -> List[dict]:
    rows: List[dict] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({(k or "").strip(): (v or "").strip() for k,v in r.items()})
    return rows

def update_seen_from_ratings(rows: Iterable[dict]) -> Tuple[SeenIndex, int]:
    idx = SeenIndex()
    added = 0
    for r in rows:
        # Flexible schema
        title = r.get("Title") or r.get("title") or r.get("originalTitle") or r.get("Original Title")
        year = _parse_int(r.get("Year") or r.get("year") or r.get("startYear") or r.get("Release Year") or "")
        ttype = r.get("Title Type") or r.get("Title type") or r.get("titleType") or r.get("Type") or r.get("constType") or ""
        kind = _kind_from_title_type(ttype) or ("tvSeries" if (ttype or "").lower()=="tvepisode" else "movie")
        if title:
            idx.add(kind, title, year)
            added += 1
    return idx, added

def load_imdb_ratings_csv_auto() -> Tuple[SeenIndex, int, Optional[str]]:
    rows = _read_csv(RATINGS_PATH_ENV)
    if not rows:
        return SeenIndex(), 0, None
    idx, added = update_seen_from_ratings(rows)
    return idx, added, RATINGS_PATH_ENV