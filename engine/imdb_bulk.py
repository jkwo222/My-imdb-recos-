# engine/imdb_bulk.py
from __future__ import annotations
import csv, gzip, io, os, pathlib, time, requests

IMDB_BASE = "https://datasets.imdbws.com"
CACHE = pathlib.Path("data/cache/imdb")
CACHE.mkdir(parents=True, exist_ok=True)

RATINGS_GZ = CACHE / "title.ratings.tsv.gz"
BASICS_GZ  = CACHE / "title.basics.tsv.gz"
TTL_HOURS  = 24  # refresh daily at most

def _fresh(p: pathlib.Path, ttl_h: int) -> bool:
    try:
        return p.exists() and (time.time() - p.stat().st_mtime) < ttl_h * 3600
    except Exception:
        return False

def _dl(url: str, dest: pathlib.Path):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)

def _ensure_latest():
    if not _fresh(RATINGS_GZ, TTL_HOURS):
        print("[IMDb TSV] GET", f"{IMDB_BASE}/title.ratings.tsv.gz")
        _dl(f"{IMDB_BASE}/title.ratings.tsv.gz", RATINGS_GZ)
    if not _fresh(BASICS_GZ, TTL_HOURS):
        print("[IMDb TSV] GET", f"{IMDB_BASE}/title.basics.tsv.gz")
        _dl(f"{IMDB_BASE}/title.basics.tsv.gz", BASICS_GZ)

_ratings: dict[str, float] | None = None
_basics: dict[str, dict] | None = None

def _open_tsv_gz(path: pathlib.Path) -> csv.DictReader:
    data = gzip.decompress(path.read_bytes())
    return csv.DictReader(io.StringIO(data.decode("utf-8", errors="ignore")), delimiter="\t")

def load():
    global _ratings, _basics
    if _ratings is not None and _basics is not None:
        return
    _ensure_latest()

    # ratings
    _ratings = {}
    rdr = _open_tsv_gz(RATINGS_GZ)
    for r in rdr:
        tid = r.get("tconst") or ""
        try:
            _ratings[tid] = float(r.get("averageRating") or 0.0)
        except Exception:
            _ratings[tid] = 0.0
    print("[IMDb TSV] ratings loaded:", f"{len(_ratings):,}")

    # basics (language is not explicit; we’ll use originalTitle language heuristic if present,
    # but mostly we’ll lean on TMDB original_language == 'en' in catalog_builder)
    _basics = {}
    rdr = _open_tsv_gz(BASICS_GZ)
    for r in rdr:
        tid = r.get("tconst") or ""
        genres = (r.get("genres") or "").strip()
        genres_list = [] if genres in ("", "\\N") else [g.strip().lower() for g in genres.split(",") if g and g != "\\N"]
        # primaryTitle/originalTitle exist; no language code here. Keep year for cross-checks if needed.
        start_year = r.get("startYear") or ""
        y = int(start_year) if start_year.isdigit() else None
        _basics[tid] = {"genres": genres_list, "year": y}
    print("[IMDb TSV] basics loaded:", f"{len(_basics):,}")

def get_rating(imdb_id: str) -> float:
    load()
    return float((_ratings or {}).get(imdb_id, 0.0))

def get_genres(imdb_id: str) -> list[str]:
    load()
    return list(((_basics or {}).get(imdb_id, {}) or {}).get("genres", []))

def get_year(imdb_id: str):
    load()
    return ((_basics or {}).get(imdb_id, {}) or {}).get("year")