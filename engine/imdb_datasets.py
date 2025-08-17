# engine/imdb_datasets.py
from __future__ import annotations
import csv, gzip, io, os, time, pathlib, typing, hashlib, requests
from typing import Dict, Optional, Tuple, List
from rich import print as rprint

# IMDb official weekly dumps (no key needed)
_BASICS_URL  = "https://datasets.imdbws.com/title.basics.tsv.gz"
_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"

CACHE_DIR = pathlib.Path("data/cache/imdb_datasets")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _path(name: str) -> pathlib.Path:
    return CACHE_DIR / name

def _fresh(path: pathlib.Path, ttl_days: int) -> bool:
    if not path.exists(): return False
    age = (time.time() - path.stat().st_mtime) / 86400.0
    return age <= float(ttl_days)

def _download(url: str, name: str, ttl_days: int) -> bytes:
    """
    Download a gzip file (if stale), persist raw .gz alongside a .etag stamp.
    Returns the raw bytes (still gzipped).
    """
    gz_path = _path(name)
    if _fresh(gz_path, ttl_days):
        return gz_path.read_bytes()

    rprint(f"[cyan][IMDb TSV] GET {url}[/cyan]")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    gz_path.write_bytes(r.content)
    return r.content

def _open_tsv_gz(raw_gz: bytes) -> csv.DictReader:
    buf = io.BytesIO(raw_gz)
    with gzip.GzipFile(fileobj=buf, mode="rb") as gz:
        text = gz.read().decode("utf-8", errors="ignore")
    return csv.DictReader(io.StringIO(text), delimiter="\t")

class IMDbIndex:
    """
    Lightweight in-memory indices backed by the two TSVs:
      - ratings: imdb_id -> (rating_0_1, num_votes)
      - basics:  imdb_id -> (start_year:int?, titleType, genres: List[str], primaryTitle:str)
    We also expose a (title_norm, year)->imdb_id map as a fallback if a TMDB item
    somehow lacks an imdb_id (rare, but nice to have).
    """

    def __init__(self,
                 ttl_days: int = 7,
                 include_adult: bool = True) -> None:
        self.ttl_days = ttl_days
        self.include_adult = include_adult
        self._ratings: Dict[str, Tuple[float, int]] = {}
        self._basics: Dict[str, Tuple[Optional[int], str, List[str], str]] = {}
        self._title_year_to_id: Dict[Tuple[str, Optional[int]], str] = {}

        self._load()

    @staticmethod
    def _norm_title(s: str) -> str:
        import re, unicodedata
        s = unicodedata.normalize("NFKD", (s or "")).encode("ascii","ignore").decode("ascii")
        s = s.lower().strip()
        s = re.sub(r"[\-—–_:;/,.'!?()]", " ", s)
        s = s.replace("&"," and ")
        s = " ".join(s.split())
        if s.startswith("the "): s = s[4:]
        return s

    def _load(self) -> None:
        # ratings
        r_raw = _download(_RATINGS_URL, "title.ratings.tsv.gz", self.ttl_days)
        rdr = _open_tsv_gz(r_raw)
        cnt = 0
        for row in rdr:
            tid = row.get("tconst","").strip()
            try:
                rating = float(row.get("averageRating") or 0.0) / 10.0
            except Exception:
                rating = 0.0
            try:
                votes = int(row.get("numVotes") or 0)
            except Exception:
                votes = 0
            if tid and rating >= 0.0:
                self._ratings[tid] = (rating, votes)
                cnt += 1
        rprint(f"[green][IMDb TSV] ratings loaded[/green]: {cnt:,}")

        # basics
        b_raw = _download(_BASICS_URL, "title.basics.tsv.gz", self.ttl_days)
        bdr = _open_tsv_gz(b_raw)
        cnt = 0
        for row in bdr:
            tid = row.get("tconst","").strip()
            if not tid: continue
            # Optional adult filtering
            is_adult = (row.get("isAdult") or "0").strip() == "1"
            if (not self.include_adult) and is_adult:
                continue
            tt = (row.get("titleType") or "").strip().lower()
            year_raw = (row.get("startYear") or "").strip()
            try:
                sy = int(year_raw) if year_raw.isdigit() else None
            except Exception:
                sy = None
            genres = [g.strip().lower() for g in (row.get("genres") or "").split(",") if g and g.strip() and g.strip() != r"\N"]
            ptitle = (row.get("primaryTitle") or "").strip()
            self._basics[tid] = (sy, tt, genres, ptitle)
            if ptitle:
                key = (self._norm_title(ptitle), sy)
                # Prefer first seen for that (title,year)
                if key not in self._title_year_to_id:
                    self._title_year_to_id[key] = tid
            cnt += 1
        rprint(f"[green][IMDb TSV] basics loaded[/green]: {cnt:,}")

    # ---------- public lookups ----------

    def rating_for(self, imdb_id: str) -> Tuple[float, int]:
        """
        Returns (rating_0_1, num_votes). Missing -> (0.0, 0)
        """
        return self._ratings.get(imdb_id, (0.0, 0))

    def basics_for(self, imdb_id: str) -> Tuple[Optional[int], str, List[str], str]:
        """
        Returns (start_year, titleType, genres[], primaryTitle). Missing -> (None,"",[], "")
        """
        return self._basics.get(imdb_id, (None, "", [], ""))

    def find_id_by_title_year(self, title: str, year: Optional[int]) -> Optional[str]:
        return self._title_year_to_id.get((self._norm_title(title), year))

class IMDbEnricher:
    """
    Simple façade you can use from catalog_builder.
    """
    def __init__(self, ttl_days: int = 7) -> None:
        self.idx = IMDbIndex(ttl_days=ttl_days)

    def enrich(self, title: str, year: int, media_type: str, imdb_id: str = "") -> dict:
        """
        Audience from IMDb TSV (0..1). Genres from basics TSV (fallback to []).
        We do not have language here (TMDB already gives that); return blank.
        """
        iid = imdb_id.strip()
        if not iid and title:
            iid = self.idx.find_id_by_title_year(title, year) or ""
        aud, votes = self.idx.rating_for(iid) if iid else (0.0, 0)
        sy, ttype, genres, ptitle = self.idx.basics_for(iid) if iid else (None, "", [], "")

        return {
            "imdb_id": iid,
            "audience": float(aud),
            "audience_votes": int(votes),
            "genres": genres or [],
            "language_primary": "",  # keep language from TMDB if you have it upstream
        }