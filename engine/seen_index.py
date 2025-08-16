# FILE: engine/seen_index.py
from __future__ import annotations
import csv
import os
import re
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

RATINGS_PATH_ENV = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")
_TCONST_RE = re.compile(r"(tt\d{6,9})", re.IGNORECASE)

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
    if tt in ("tvepisode", "episode"): return None
    return None

def _parse_int(x: str) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def _maybe_tconst(cell: str) -> Optional[str]:
    if not cell: return None
    m = _TCONST_RE.search(cell)
    return m.group(1).lower() if m else None

class SeenIndex:
    """
    Stores:
      - exact IMDb IDs (set[str])
      - tolerant title+year keys for movie/tvSeries
    """
    def __init__(self) -> None:
        self.ids: set[str] = set()
        self.keys: set[str] = set()  # "movie:normalized:1997" or "tvSeries:normalized:*"

    def __len__(self) -> int:
        # report total unique signals (ids + title keys)
        return len(self.ids) + len(self.keys)

    def add_id(self, imdb_id: Optional[str]) -> None:
        if imdb_id and imdb_id.startswith("tt"):
            self.ids.add(imdb_id.lower())

    def add_title(self, kind: str, title: str, year: Optional[int]) -> None:
        if not title or not kind: return
        nt = _norm_title(title)
        if not nt: return
        y = str(year) if year else "*"
        self.keys.add(f"{kind}:{nt}:{y}")

    def has_id(self, imdb_id: Optional[str]) -> bool:
        if not imdb_id: return False
        return imdb_id.lower() in self.ids

    def has_title(self, kind: str, title: str, year: Optional[int]) -> bool:
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
        # IDs — prefer direct match, else sniff any column for tt…
        imdb_id = (
            r.get("Const") or r.get("const") or r.get("tconst") or
            r.get("IMDb Title ID") or r.get("imdb_id") or r.get("id") or ""
        ).strip()
        imdb_id = _maybe_tconst(imdb_id) or imdb_id.lower() if imdb_id.startswith("tt") else None
        if not imdb_id:
            for v in r.values():
                imdb_id = _maybe_tconst(v)
                if imdb_id:
                    break
        if imdb_id:
            idx.add_id(imdb_id)
            added += 1

        # Titles (fallback signal)
        title = r.get("Title") or r.get("title") or r.get("originalTitle") or r.get("Original Title")
        year = _parse_int(r.get("Year") or r.get("year") or r.get("startYear") or r.get("Release Year") or "")
        ttype = r.get("Title Type") or r.get("Title type") or r.get("titleType") or r.get("Type") or r.get("constType") or ""
        kind = _kind_from_title_type(ttype) or ("tvSeries" if (ttype or "").lower()=="tvepisode" else "movie")
        if title:
            idx.add_title(kind, title, year)

    return idx, added

def load_imdb_ratings_csv_auto() -> Tuple[SeenIndex, int, Optional[str]]:
    rows = _read_csv(RATINGS_PATH_ENV)
    if not rows:
        return SeenIndex(), 0, None
    idx, added = update_seen_from_ratings(rows)
    return idx, added, RATINGS_PATH_ENV