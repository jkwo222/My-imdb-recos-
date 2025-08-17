# engine/catalog_builder.py
import os, json, time, hashlib, pathlib, requests
from typing import Dict, List, Optional, Tuple

TMDB_V3 = "https://api.themoviedb.org/3"
OMDB    = "http://www.omdbapi.com/"

CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
OMDB_CACHE = CACHE_ROOT / "omdb"
for p in (TMDB_CACHE, OMDB_CACHE):
    p.mkdir(parents=True, exist_ok=True)

def _env_i(name: str, default: int) -> int:
    try:
        return int((os.environ.get(name, "") or "").strip() or default)
    except Exception:
        return default

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _tmdb_auth() -> Dict[str, str]:
    """
    Support BOTH:
      - TMDB v4 token (starts with 'eyJ'): send as Bearer
      - TMDB v3 key (hex-ish): do NOT send as Bearer; we will append as ?api_key=...
    """
    key = (os.environ.get("TMDB_API_KEY") or "").strip()
    if not key:
        return {}
    if key.startswith("eyJ"):  # JWT-ish v4 token
        return {"mode": "v4", "token": key}
    return {"mode": "v3", "token": key}

def _http_get_json(url: str, headers=None, timeout=30) -> Tuple[Optional[dict], int]:
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout)
        if r.status_code == 200:
            return r.json(), r.status_code
        return None, r.status_code
    except Exception:
        return None, -1

def _cached_get_json(url: str, headers=None, ttl_hours=24, cache_dir: pathlib.Path = TMDB_CACHE):
    cache_file = cache_dir / f"{_h(url)}.json"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < 3600 * ttl_hours:
            try:
                return json.load(open(cache_file, "r", encoding="utf-8"))
            except Exception:
                pass
    data, code = _http_get_json(url, headers=headers)
    if data is not None:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        json.dump(data, open(cache_file, "w", encoding="utf-8"))
    return data

def _tmdb_headers_and_url(path_qs: str) -> Tuple[str, Dict[str, str]]:
    """
    Build a full TMDB v3 URL and appropriate headers, appending ?api_key= for v3 keys.
    path_qs: e.g. "/discover/movie?page=1&sort_by=popularity.desc"
    """
    auth = _tmdb_auth()
    headers = {}
    url = f"{TMDB_V3}{path_qs}"
    if auth.get("mode") == "v4":
        headers["Authorization"] = f"Bearer {auth['token']}"
    elif auth.get("mode") == "v3":
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api_key={auth['token']}"
    return url, headers

PROV_MAP = {
    "Netflix":"netflix","Amazon Prime Video":"prime_video","Prime Video":"prime_video",
    "Hulu":"hulu","Max":"max","HBO Max":"max","Disney Plus":"disney_plus","Disney+":"disney_plus",
    "Apple TV Plus":"apple_tv_plus","Apple TV+":"apple_tv_plus","Peacock":"peacock",
    "Paramount Plus":"paramount_plus","Paramount+":"paramount_plus",
}

def _providers_tmdb(media_type: str, tmdb_id: int, region="US"):
    path = f"/{media_type}/{tmdb_id}/watch/providers"
    url, headers = _tmdb_headers_and_url(path)
    data = _cached_get_json(url, headers=headers, ttl_hours=24)
    out = set()
    if not data:
        return []
    res = (data.get("results") or {}).get(region) or {}
    for bucket in ("flatrate","ads","free"):
        for p in res.get(bucket, []) or []:
            name = PROV_MAP.get(p.get("provider_name"))
            if name:
                out.add(name)
    return sorted(out)

def _omdb_enrich(title: str, year: int, media_type: str, imdb_id: str = "") -> Dict:
    key = (os.environ.get("OMDB_API_KEY") or "").strip()
    if not key:
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}
    if imdb_id:
        url = f"{OMDB}?apikey={key}&i={imdb_id}&plot=short&r=json"
    else:
        t = (title or "").replace(" ","+")
        url = f"{OMDB}?apikey={key}&t={t}&y={year or ''}&type={'series' if media_type=='tv' else 'movie'}&plot=short&r=json"
    data = _cached_get_json(url, ttl_hours=168, cache_dir=OMDB_CACHE)
    if not data or data.get("Response") == "False":
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}

    imdb_id_out = data.get("imdbID") or imdb_id or ""
    # audience = IMDb 0..10 → 0..1
    try:
        aud = float(data.get("imdbRating") or 0.0) / 10.0
    except Exception:
        aud = 0.0
    # critic = RT %
    rt = 0.0
    for r in data.get("Ratings") or []:
        if (r.get("Source") or "").lower() == "rotten tomatoes":
            try:
                rt = float((r.get("Value") or "0%").rstrip("%"))/100.0
            except Exception:
                rt = 0.0
            break
    lang_primary = (data.get("Language") or "").split(",")[0].strip().lower()
    genres = [g.strip().lower() for g in (data.get("Genre") or "").split(",") if g.strip()]
    return {"critic":rt,"audience":aud,"language_primary":lang_primary,"imdb_id":imdb_id_out,"genres":genres}

def _collect_tmdb_ids(media_type: str, pages: int) -> List[int]:
    ids: List[int] = []
    pages = max(1, min(20, pages))
    for page in range(1, pages+1):
        path_qs = f"/discover/{media_type}?page={page}&sort_by=popularity.desc"
        url, headers = _tmdb_headers_and_url(path_qs)
        data = _cached_get_json(url, headers=headers, ttl_hours=3)
        if not data or not isinstance(data, dict):
            continue
        for r in (data.get("results") or []):
            tid = r.get("id")
            if isinstance(tid, int):
                ids.append(tid)
    return ids

def _tmdb_details(media_type: str, tmdb_id: int) -> Optional[dict]:
    path_qs = f"/{media_type}/{tmdb_id}?append_to_response=external_ids"
    url, headers = _tmdb_headers_and_url(path_qs)
    return _cached_get_json(url, headers=headers, ttl_hours=168)

def _to_item_from_tmdb(media_type: str, d: dict) -> Optional[dict]:
    if not d:
        return None
    title = d.get("title") or d.get("name") or ""
    date = (d.get("release_date") or d.get("first_air_date") or "")[:4]
    try:
        year = int(date) if date and date.isdigit() else 0
    except Exception:
        year = 0
    imdb_id = ((d.get("external_ids") or {}).get("imdb_id") or "") if isinstance(d.get("external_ids"), dict) else ""
    seasons = int(d.get("number_of_seasons") or 1) if media_type == "tv" else 1
    typ = "movie" if media_type == "movie" else ("tvMiniSeries" if seasons == 1 else "tvSeries")
    return {
        "tmdb_id": int(d.get("id") or 0),
        "imdb_id": imdb_id or "",
        "title": title,
        "year": year,
        "type": typ,
        "seasons": seasons,
        "tmdb_vote": float(d.get("vote_average") or 0.0),
        "popularity": float(d.get("popularity") or 0.0),
        "kind": "tv" if media_type == "tv" else "movie",
    }

def build_catalog() -> List[Dict]:
    pages_movie = _env_i("TMDB_PAGES_MOVIE", 4)   # smaller by default
    pages_tv    = _env_i("TMDB_PAGES_TV", 4)
    region      = (os.environ.get("REGION") or "US").strip() or "US"
    include_tv  = (os.environ.get("INCLUDE_TV_SEASONS","true").lower() in ("1","true","yes"))
    hard_cap    = _env_i("MAX_CATALOG", 4000)
    require_english = (os.environ.get("ENGLISH_ONLY","false").lower() in ("1","true","yes"))

    movie_ids = _collect_tmdb_ids("movie", pages_movie)
    tv_ids    = _collect_tmdb_ids("tv", pages_tv) if include_tv else []
    ids = [("movie", i) for i in movie_ids] + [("tv", i) for i in tv_ids]

    out: List[Dict] = []
    for media_type, tid in ids:
        d = _tmdb_details(media_type, tid)
        item = _to_item_from_tmdb(media_type, d)
        if not item:
            continue

        # Providers (subs)
        item["providers"] = _providers_tmdb(media_type, tid, region=region)

        # OMDb enrichment: RT (critic), IMDb (audience), Language, Genres
        enrich = _omdb_enrich(item["title"], item["year"], "tv" if media_type=="tv" else "movie", imdb_id=item.get("imdb_id",""))
        item["imdb_id"] = enrich.get("imdb_id") or item.get("imdb_id") or ""
        item["critic"] = float(enrich.get("critic") or 0.0)      # 0..1
        item["audience"] = float(enrich.get("audience") or 0.0)  # 0..1
        item["language_primary"] = (enrich.get("language_primary") or "").lower()
        item["genres"] = enrich.get("genres") or []

        if require_english:
            if "english" not in (item.get("language_primary") or ""):
                continue

        out.append(item)
        if len(out) >= hard_cap:
            break

    return out