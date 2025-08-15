# engine/catalog_builder.py
import os, time, requests
from typing import Dict, List, Tuple, Any
from rich import print as rprint
from .cache import get_fresh, set as cache_set

TMDB_API = "https://api.themoviedb.org/3"
OMDB_API = "http://www.omdbapi.com/"

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}

def _tmdb_key() -> str:
    k = os.environ.get("TMDB_API_KEY","").strip()
    if not k:
        raise RuntimeError("TMDB_API_KEY not set")
    return k

def _omdb_key() -> str:
    k = os.environ.get("OMDB_API_KEY","").strip()
    if not k:
        raise RuntimeError("OMDB_API_KEY not set")
    return k

def _tmdb_get(path: str, params: Dict[str,Any]) -> Dict:
    params = dict(params or {})
    headers = {"Authorization": f"Bearer {_tmdb_key()}"} if len(_tmdb_key()) > 40 else {}
    # support v3 key as fallback (query param)
    if not headers:
        params["api_key"] = _tmdb_key()
    key = f"tmdb:{path}:{sorted(params.items())}"
    cached = get_fresh(key, ttl_days=2)
    if cached is not None:
        return cached
    url = f"{TMDB_API}{path}"
    r = requests.get(url, params=params, headers=headers or UA, timeout=30)
    if r.status_code == 401:
        raise RuntimeError("TMDB 401 Unauthorized — check TMDB_API_KEY (Bearer v4 token or v3 key).")
    if r.status_code >= 500:
        time.sleep(1.0)
    r.raise_for_status()
    data = r.json()
    cache_set(key, data)
    return data

def _omdb_get(params: Dict[str,Any]) -> Dict:
    params = dict(params or {})
    params["apikey"] = _omdb_key()
    key = f"omdb:{sorted(params.items())}"
    cached = get_fresh(key, ttl_days=7)
    if cached is not None:
        return cached
    r = requests.get(OMDB_API, params=params, headers=UA, timeout=30)
    if r.status_code >= 500:
        time.sleep(1.0)
    r.raise_for_status()
    data = r.json()
    cache_set(key, data)
    return data

def _discover(kind: str, pages: int) -> List[Dict]:
    assert kind in ("movie", "tv")
    out: List[Dict] = []
    for p in range(1, pages+1):
        params = {
            "with_original_language": "en",   # English originals (US/UK/AU/CA/etc)
            "include_adult": "false",
            "page": p,
            "sort_by": "popularity.desc",
            "language": "en-US",              # response language (does not filter)
        }
        data = _tmdb_get(f"/discover/{kind}", params)
        results = data.get("results", []) or []
        rprint(f"[tmdb] discover {kind} page={p} results={len(results)}")
        out.extend(results)
        time.sleep(0.2)
    return out

def _external_ids(kind: str, tmdb_id: int) -> Dict:
    return _tmdb_get(f"/{kind}/{tmdb_id}/external_ids", {})

def _tv_details(tmdb_id: int) -> Dict:
    return _tmdb_get(f"/tv/{tmdb_id}", {"language": "en-US"})

def _ratings_from_omdb(imdb_id: str) -> Tuple[float, float]:
    """returns (critic, audience) in [0,1], tolerant to missing fields."""
    if not imdb_id:
        return (0.0, 0.0)
    try:
        j = _omdb_get({"i": imdb_id, "tomatoes": "true"})
    except Exception as e:
        rprint(f"[yellow][omdb] {imdb_id} failed: {e}[/yellow]")
        return (0.0, 0.0)

    # Audience: IMDb rating if present
    try:
        aud = float(j.get("imdbRating","0") or 0) / 10.0
    except:
        aud = 0.0

    # Critic: RottenTomatoes (Tomatometer) if present; else Metascore
    critic = 0.0
    ratings = j.get("Ratings") or []
    for r in ratings:
        if r.get("Source") == "Rotten Tomatoes":
            v = r.get("Value","").strip().replace("%","")
            if v.isdigit():
                critic = max(critic, float(v)/100.0)
    if critic == 0.0:
        try:
            ms = float(j.get("Metascore","0") or 0)
            critic = ms/100.0
        except:
            critic = 0.0
    return (critic, aud)

def build_catalog(pages_movie: int, pages_tv: int, include_tv_seasons: bool=True) -> List[Dict]:
    # 1) discover lists
    movies = _discover("movie", max(1, pages_movie))
    tvs    = _discover("tv",    max(1, pages_tv))
    rprint(f"[catalog] discovered movies={len(movies)} tv={len(tvs)} (pre-enrich)")

    if len(movies) + len(tvs) == 0:
        raise RuntimeError("TMDB discover returned 0 results — likely API key/permission problem.")

    # 2) enrich with imdb_id (+ seasons for TV) and ratings
    out: List[Dict] = []
    # movies
    for m in movies:
        tid = m.get("id")
        title = m.get("title") or m.get("original_title") or ""
        year = 0
        if m.get("release_date"):
            year = int((m["release_date"] or "0000")[:4])
        ext = _external_ids("movie", tid)
        imdb_id = (ext.get("imdb_id") or "").strip()
        critic, audience = _ratings_from_omdb(imdb_id)
        out.append({
            "imdb_id": imdb_id,
            "title": title,
            "year": year,
            "type": "movie",
            "critic": round(critic, 3),
            "audience": round(audience, 3),
        })
        time.sleep(0.15)

    # tv
    for t in tvs:
        tid = t.get("id")
        title = t.get("name") or t.get("original_name") or ""
        first_air_year = 0
        if t.get("first_air_date"):
            first_air_year = int((t["first_air_date"] or "0000")[:4])
        ext = _external_ids("tv", tid)
        imdb_id = (ext.get("imdb_id") or "").strip()
        seasons = 1
        if include_tv_seasons:
            det = _tv_details(tid)
            seasons = int(det.get("number_of_seasons") or 1)
        critic, audience = _ratings_from_omdb(imdb_id)
        out.append({
            "imdb_id": imdb_id,
            "title": title,
            "year": first_air_year,
            "type": "tvSeries",
            "seasons": seasons,
            "critic": round(critic, 3),
            "audience": round(audience, 3),
        })
        time.sleep(0.15)

    # 3) keep entries that at least have a title (ratings can be 0.0)
    clean = [x for x in out if (x.get("title") or "").strip()]
    rprint(f"[catalog] enriched total={len(clean)} (movies={len(movies)} tv={len(tvs)})")
    return clean