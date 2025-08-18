# engine/imdb_tsv.py
from __future__ import annotations
import gzip, io, json, os, time
from pathlib import Path
from typing import Dict, Any, List, Tuple
import requests

IMDB_HOST = "https://datasets.imdbws.com"
TMDB_API = "https://api.themoviedb.org/3"

def download(name: str, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    dest = cache_dir / name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    r = requests.get(f"{IMDB_HOST}/{name}", timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest

def iter_tsv_gz(path: Path):
    with gzip.open(path, "rb") as gz:
        data = gz.read()
    text = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", errors="replace")
    it = iter(text)
    header = next(it).rstrip("\n").split("\t")
    for line in it:
        row = line.rstrip("\n").split("\t")
        yield dict(zip(header, row))

def _tmdb_headers() -> Dict[str, str]:
    bearer = os.getenv("TMDB_BEARER") or os.getenv("TMDB_ACCESS_TOKEN") or os.getenv("TMDB_V4_TOKEN")
    h = {"Accept": "application/json"}
    if bearer:
        h["Authorization"] = f"Bearer {bearer}"
    return h

def _tmdb_params() -> Dict[str, str]:
    p: Dict[str, str] = {}
    if os.getenv("TMDB_API_KEY"):
        p["api_key"] = os.getenv("TMDB_API_KEY")
    return p

def hydrate_imdb_ids_to_tmdb(imdb_ids: List[str], limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    session = requests.Session()
    headers = _tmdb_headers()
    params = _tmdb_params()
    mapped = 0
    for tid in imdb_ids:
        if mapped >= limit:
            break
        try:
            data = session.get(
                f"{TMDB_API}/find/{tid}",
                params={**params, "external_source": "imdb_id", "language": "en-US"},
                headers=headers, timeout=20
            ).json()
        except Exception:
            continue
        def norm_movie(r):  # minimal
            return {
                "media_type": "movie", "tmdb_id": r.get("id"),
                "title": r.get("title") or r.get("original_title"),
                "release_date": r.get("release_date"), "tmdb_vote": r.get("vote_average"),
                "popularity": r.get("popularity"), "original_language": r.get("original_language"),
                "year": (r.get("release_date") or "")[:4] if r.get("release_date") else None,
            }
        def norm_tv(r):
            return {
                "media_type": "tv", "tmdb_id": r.get("id"),
                "name": r.get("name") or r.get("original_name"), "title": r.get("name") or r.get("original_name"),
                "first_air_date": r.get("first_air_date"), "last_air_date": r.get("last_air_date"),
                "tmdb_vote": r.get("vote_average"), "popularity": r.get("popularity"),
                "original_language": r.get("original_language"),
                "year": (r.get("first_air_date") or "")[:4] if r.get("first_air_date") else None,
            }
        found = False
        for r in data.get("movie_results") or []:
            out.append(norm_movie(r)); found = True; mapped += 1
        for r in data.get("tv_results") or []:
            out.append(norm_tv(r)); found = True; mapped += 1
        time.sleep(0.06)
    return out