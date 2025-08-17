# engine/imdb_ingest.py
from __future__ import annotations
import csv, gzip, io, os, pathlib, time, urllib.request
from typing import Dict, Tuple

IMDB_DUMP = "https://datasets.imdbws.com"
CACHE_DIR = pathlib.Path("data/cache/imdb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASICS = CACHE_DIR / "title.basics.tsv.gz"
RATINGS = CACHE_DIR / "title.ratings.tsv.gz"

def _http_get(url: str, dest: pathlib.Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as r:
        data = r.read()
    dest.write_bytes(data)

def _maybe_fetch(fname: pathlib.Path, url: str, ttl_hours: int) -> None:
    if fname.exists():
        age = time.time() - fname.stat().st_mtime
        if age < ttl_hours * 3600:
            return
    _http_get(url, fname)

def load_imdb_maps(ttl_hours: int = 72) -> Tuple[Dict[str, Tuple[str, int, str]], Dict[str, Tuple[float, int]]]:
    """
    Returns:
      basics_map: tconst -> (primaryTitle, startYear, titleType)
      ratings_map: tconst -> (averageRating, numVotes)
    """
    _maybe_fetch(BASICS, f"{IMDB_DUMP}/title.basics.tsv.gz", ttl_hours)
    _maybe_fetch(RATINGS, f"{IMDB_DUMP}/title.ratings.tsv.gz", ttl_hours)

    basics_map: Dict[str, Tuple[str, int, str]] = {}
    ratings_map: Dict[str, Tuple[float, int]] = {}

    # basics
    with gzip.open(BASICS, "rb") as f:
        tsv = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
        rdr = csv.DictReader(tsv, delimiter="\t")
        for r in rdr:
            tid = (r.get("tconst") or "").strip()
            if not tid: continue
            title = (r.get("primaryTitle") or r.get("originalTitle") or "").strip()
            tt = (r.get("titleType") or "").strip()
            y = r.get("startYear") or ""
            try:
                yy = int(y) if y.isdigit() else 0
            except:
                yy = 0
            basics_map[tid] = (title, yy, tt)

    # ratings
    with gzip.open(RATINGS, "rb") as f:
        tsv = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
        rdr = csv.DictReader(tsv, delimiter="\t")
        for r in rdr:
            tid = (r.get("tconst") or "").strip()
            if not tid: continue
            try:
                avg = float(r.get("averageRating") or 0.0)
                nv = int(r.get("numVotes") or 0)
            except:
                avg, nv = 0.0, 0
            ratings_map[tid] = (avg, nv)

    print(f"[IMDb TSV] basics loaded: {len(basics_map):,}")
    print(f"[IMDb TSV] ratings loaded: {len(ratings_map):,}")
    return basics_map, ratings_map