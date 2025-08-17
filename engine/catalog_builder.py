# FILE: engine/catalog_builder.py
from __future__ import annotations
import os, time, json, gzip, io, pathlib, hashlib
from typing import Dict, List, Tuple, Optional
import requests
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

TMDB = "https://api.themoviedb.org/3"
IMDB_BASE = "https://datasets.imdbws.com"
ROOT = pathlib.Path(__file__).resolve().parents[1]
CACHE_ROOT = ROOT / "data" / "cache"
TMDB_CACHE = CACHE_ROOT / "tmdb"
IMDB_CACHE = CACHE_ROOT / "imdb"
for p in (TMDB_CACHE, IMDB_CACHE):
    p.mkdir(parents=True, exist_ok=True)

def _tmdb_auth():
    """
    Returns (use_v3_key: bool, v3_key: str, headers: dict)
    Use TMDB_V4_TOKEN if present (Authorization: Bearer ...); else TMDB_API_KEY as v3 query param.
    """
    v4 = os.environ.get("TMDB_V4_TOKEN", "").strip()
    if v4:
        return (False, "", {"Authorization": f"Bearer {v4}"})
    v3 = os.environ.get("TMDB_API_KEY", "").strip()
    if v3:
        return (True, v3, {})
    return (False, "", {})  # no auth → TMDB will 401

def _with_api_key(url: str, key: str) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q["api_key"] = key
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _cache_path(url: str, hours: int, bucket: pathlib.Path) -> pathlib.Path:
    p = bucket / f"{_h(url)}.json"
    if p.exists():
        age = time.time() - p.stat().st_mtime
        if age < hours * 3600:
            return p
    return p

def _get_json_cached(url: str, hours: int, bucket: pathlib.Path, headers=None):
    path = _cache_path(url, hours, bucket)
    if path.exists():
        try:
            return json.load(open(path, "r", encoding="utf-8"))
        except Exception:
            pass
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        print(f" {r.status_code} for {url} (no body cached)")
        return None
    data = r.json()
    json.dump(data, open(path, "w", encoding="utf-8"))
    return data

def _tmdb_get(url: str, cache_hours: int):
    use_v3, key, headers = _tmdb_auth()
    final_url = _with_api_key(url, key) if use_v3 and key else url
    return _get_json_cached(final_url, cache_hours, TMDB_CACHE, headers=headers)

def _collect_tmdb_ids(media_type: str, pages: int) -> List[int]:
    ids = []
    for page in range(1, pages + 1):
        url = f"{TMDB}/discover/{media_type}?page={page}&sort_by=popularity.desc"
        data = _tmdb_get(url, 6)
        for r in (data.get("results") or []) if data else []:
            try:
                ids.append(int(r.get("id")))
            except Exception:
                pass
    return ids

def _tmdb_details(media_type: str, tmdb_id: int) -> Optional[dict]:
    url = f"{TMDB}/{media_type}/{tmdb_id}?append_to_response=external_ids,watch/providers"
    return _tmdb_get(url, 48)

def _providers_from_tmdb(d: dict, region="US") -> List[str]:
    out = set()
    prov_data = ((d or {}).get("watch/providers") or {}).get("results") or {}
    rd = prov_data.get(region, {}) if isinstance(prov_data, dict) else {}
    for bucket in ("flatrate", "ads", "free"):
        for p in rd.get(bucket, []) or []:
            name = p.get("provider_name") or ""
            if name:
                out.add(name)
    return sorted(out)

# ---- IMDb TSV cache/load ----

def _download_gz(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def _ensure_gz(path: pathlib.Path, url: str, max_age_hours: int):
    if path.exists():
        age = time.time() - path.stat().st_mtime
        if age < max_age_hours * 3600:
            return
    content = _download_gz(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)

def ensure_imdb_cache():
    basics = IMDB_CACHE / "title.basics.tsv.gz"
    ratings = IMDB_CACHE / "title.ratings.tsv.gz"
    _ensure_gz(basics, f"{IMDB_BASE}/title.basics.tsv.gz", 72)
    _ensure_gz(ratings, f"{IMDB_BASE}/title.ratings.tsv.gz", 72)
    print("[IMDb TSV] basics+ratings cached →", IMDB_CACHE)

def _read_tsv_gz(path: pathlib.Path):
    import gzip, io
    with gzip.open(path, "rb") as f:
        text = io.TextIOWrapper(f, encoding="utf-8", errors="ignore")
        header = None
        for line in text:
            line = line.rstrip("\n")
            cells = line.split("\t")
            if header is None:
                header = cells
                continue
            yield dict(zip(header, cells))

def _imdb_maps() -> Tuple[Dict[str, float], Dict[str, Tuple[str, int]]]:
    ratings_map: Dict[str, float] = {}
    basics_map: Dict[str, Tuple[str, int]] = {}
    rpath = IMDB_CACHE / "title.ratings.tsv.gz"
    bpath = IMDB_CACHE / "title.basics.tsv.gz"
    print("[IMDb TSV] loading ratings…")
    for r in _read_tsv_gz(rpath):
        tconst = r.get("tconst", "")
        try:
            ratings_map[tconst] = float(r.get("averageRating", "0") or "0")
        except Exception:
            pass
    print("[IMDb TSV] loading basics…")
    for b in _read_tsv_gz(bpath):
        tconst = b.get("tconst", "")
        title = b.get("primaryTitle", "") or b.get("originalTitle", "")
        y = b.get("startYear", "")
        try:
            year = int(y) if y.isdigit() else 0
        except Exception:
            year = 0
        basics_map[tconst] = (title, year)
    return ratings_map, basics_map

def _to_item(media_type: str, d: dict,
             imdb_ratings: Dict[str, float],
             imdb_basics: Dict[str, Tuple[str, int]]) -> Optional[Dict]:
    if not d:
        return None
    title = d.get("title") or d.get("name") or ""
    date = (d.get("release_date") or d.get("first_air_date") or "")[:4]
    year = int(date) if (date and date.isdigit()) else 0
    seasons = int(d.get("number_of_seasons") or 1) if media_type == "tv" else 1
    typ = "movie" if media_type == "movie" else ("tvMiniSeries" if seasons == 1 else "tvSeries")
    imdb_id = ((d.get("external_ids") or {}).get("imdb_id") or "").strip()
    imdb_rating = 0.0
    if imdb_id and imdb_id in imdb_ratings:
        imdb_rating = float(imdb_ratings.get(imdb_id, 0.0))
        if year == 0:
            by = imdb_basics.get(imdb_id)
            if by:
                year = by[1] or year
    providers = _providers_from_tmdb(d, region=os.environ.get("REGION", "US").strip() or "US")
    tmdb_vote = float(d.get("vote_average") or 0.0)
    return {
        "tmdb_id": int(d.get("id") or 0),
        "imdb_id": imdb_id,
        "title": title,
        "year": year,
        "type": typ,
        "seasons": seasons,
        "tmdb_vote": tmdb_vote,
        "providers": providers,
        "critic": 0.0,
        "audience": 0.0,
        "imdb_rating": imdb_rating,
    }

def build_catalog() -> List[Dict]:
    pages_movie = int(os.environ.get("TMDB_PAGES_MOVIE", "12"))
    pages_tv    = int(os.environ.get("TMDB_PAGES_TV", "12"))
    include_tv  = (os.environ.get("INCLUDE_TV_SEASONS", "true").lower() in ("1", "true", "yes"))
    hard_cap    = int(os.environ.get("MAX_CATALOG", "6000"))

    imdb_ratings, imdb_basics = _imdb_maps()

    movie_ids = _collect_tmdb_ids("movie", pages_movie)
    tv_ids    = _collect_tmdb_ids("tv", pages_tv) if include_tv else []
    # FIXED: removed extra trailing ')'
    ids = [("movie", i) for i in movie_ids] + [("tv", i) for i in tv_ids]

    out: List[Dict] = []
    for media_type, tid in ids:
        d = _tmdb_details(media_type, tid)
        it = _to_item(media_type, d, imdb_ratings, imdb_basics)
        if it:
            out.append(it)
            if len(out) >= hard_cap:
                break
    return out