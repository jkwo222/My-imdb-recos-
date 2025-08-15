# engine/seen_index.py
from __future__ import annotations
import csv, os, unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

RATINGS_PATH_ENV = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")

def _norm_title(t: str) -> str:
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    repl = [("’","'"), ("&"," and "), (":"," "), ("-"," "), ("/"," "), ("."," "), (","," "), ("!"," "), ("?"," ")]
    for a,b in repl: t = t.replace(a,b)
    # strip articles
    for art in (" the ", " a ", " an "):
        if t.startswith(art.strip()+" "): t = t[len(art):]
    t = " ".join(t.split())
    return t

def _kind_from_title_type(tt: str) -> Optional[str]:
    tt = (tt or "").lower()
    if tt in ("movie", "video", "tvmovie"): return "movie"
    if tt in ("tvseries", "tvminiseries"): return "tvSeries"
    # ignore episodes, shorts, etc., for seen filter purposes
    return None

class SeenIndex:
    def __init__(self) -> None:
        self.keys = set()  # strings like "movie:normalized:1997" or "tvSeries:normalized:*"

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
        # tolerant checks
        if f"{kind}:{nt}:*" in self.keys: return True
        if year is not None:
            # allow +/- 1 year wiggle
            if f"{kind}:{nt}:{year-1}" in self.keys: return True
            if f"{kind}:{nt}:{year+1}" in self.keys: return True
        return False

def _parse_int(x: str) -> Optional[int]:
    try:
        return int(x)
    except:
        return None

def _read_csv(path: str) -> List[dict]:
    rows: List[dict] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({k.strip(): (v or "").strip() for k,v in r.items()})
    return rows

def load_imdb_ratings_csv_auto() -> Tuple[List[dict], Optional[str]]:
    rows = _read_csv(RATINGS_PATH_ENV)
    return rows, RATINGS_PATH_ENV if rows else ([], None)[0]

def update_seen_from_ratings(rows: Iterable[dict]) -> Tuple[SeenIndex, int]:
    idx = SeenIndex()
    added = 0
    for r in rows:
        # Try multiple schema variants
        title = r.get("Title") or r.get("title") or r.get("originalTitle") or r.get("Original Title")
        year = _parse_int(r.get("Year") or r.get("year") or r.get("startYear") or r.get("Release Year") or "")
        ttype = r.get("Title type") or r.get("titleType") or r.get("Type") or r.get("constType") or ""
        kind = _kind_from_title_type(ttype)
        if not kind:
            # best-effort: infer TV if "Episode" present, else movie
            kind = "tvSeries" if (ttype.lower() == "tvepisode") else "movie"
        if title:
            idx.add(kind, title, year)
            added += 1
    return idx, added

def load_seen() -> SeenIndex:
    return SeenIndex()