# engine/catalog_builder.py
import os, json, time, hashlib, pathlib, requests

TMDB = "https://api.themoviedb.org/3"
OMDB = "http://www.omdbapi.com/"

CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
OMDB_CACHE = CACHE_ROOT / "omdb"
for p in (TMDB_CACHE, OMDB_CACHE):
    p.mkdir(parents=True, exist_ok=True)

def _env_i(name: str, default: int) -> int:
    try: return int(os.environ.get(name, "").strip() or default)
    except Exception: return default

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _cached_get_json(url: str, headers=None, ttl_hours=48):
    cache_dir = TMDB_CACHE if "themoviedb" in url else OMDB_CACHE
    cache_file = cache_dir / f"{_h(url)}.json"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < 3600 * ttl_hours:
            return json.load(open(cache_file, "r", encoding="utf-8"))
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        return None
    data = r.json()
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(cache_file, "w", encoding="utf-8"))
    return data

def _tmdb_headers():
    key = os.environ.get("TMDB_API_KEY","").strip()
    return {"Authorization": f"Bearer {key}"} if key else {}

PROV_MAP = {
    "Netflix":"netflix","Amazon Prime Video":"prime_video","Prime Video":"prime_video",
    "Hulu":"hulu","Max":"max","HBO Max":"max","Disney Plus":"disney_plus","Disney+":"disney_plus",
    "Apple TV Plus":"apple_tv_plus","Apple TV+":"apple_tv_plus","Peacock":"peacock",
    "Paramount Plus":"paramount_plus","Paramount+":"paramount_plus",
}

def _providers_tmdb(media_type: str, tmdb_id: int, region="US"):
    url = f"{TMDB}/{media_type}/{tmdb_id}/watch/providers"
    data = _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=24)
    out = set()
    if not data: return []
    res = (data.get("results") or {}).get(region) or {}
    for bucket in ("flatrate","ads","free"):
        for p in res.get(bucket, []) or []:
            name = PROV_MAP.get(p.get("provider_name"))
            if name: out.add(name)
    return sorted(out)

def _omdb_enrich(title: str, year: int, media_type: str, imdb_id: str = ""):
    key = os.environ.get("OMDB_API_KEY","").strip()
    if not key:
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}
    if imdb_id:
        url = f"{OMDB}?apikey={key}&i={imdb_id}&plot=short&r=json"
    else:
        t = title.replace(" ","+")
        url = f"{OMDB}?apikey={key}&t={t}&y={year or ''}&type={'series' if media_type=='tv' else 'movie'}&plot=short&r=json"
    data = _cached_get_json(url, ttl_hours=168)  # 7 days
    if not data or data.get("Response") == "False":
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}

    imdb_id_out = data.get("imdbID") or imdb_id or ""
    # audience = IMDb 0..10 → 0..1
    try: aud = float(data.get("imdbRating") or 0.0) / 10.0
    except Exception: aud = 0.0
    # critic = RT %
    rt = 0.0
    for r in data.get("Ratings") or []:
        if (r.get("Source") or "").lower() == "rotten tomatoes":
            try: rt = float((r.get("Value") or "0%").rstrip("%"))/100.0
            except Exception: rt = 0.0
            break
    lang_primary = (data.get("Language") or "").split(",")[0].strip().lower()
    genres = [g.strip().lower() for g in (data.get("Genre") or "").split(",") if g.strip()]
    return {"critic":rt,"audience":aud,"language_primary":lang_primary,"imdb_id":imdb_id_out,"genres":genres}

def _collect_tmdb_ids(media_type: str, pages: int):
    ids = []
    for page in range(1, pages+1):
        url = f"{TMDB}/discover/{media_type}?page={page}&sort_by=popularity.desc"
        data = _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=6)
        for r in (data.get("results") or []) if data else []:
            ids.append(int(r.get("id")))
    return ids

def _tmdb_details(media_type: str, tmdb_id: int):
    url = f"{TMDB}/{media_type}/{tmdb_id}?append_to_response=external_ids"
    return _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=168)

def _to_item_from_tmdb(media_type: str, d: dict):
    if not d: return None
    title = d.get("title") or d.get("name") or ""
    date = (d.get("release_date") or d.get("first_air_date") or "")[:4]
    year = int(date) if (date and date.isdigit()) else 0
    imdb_id = ((d.get("external_ids") or {}).get("imdb_id") or "") if isinstance(d.get("external_ids"), dict) else ""
    seasons = int(d.get("number_of_seasons") or 1) if media_type == "tv" else 1
    typ = "movie" if media_type == "movie" else ("tvMiniSeries" if seasons == 1 else "tvSeries")
    return {"tmdb_id": int(d.get("id") or 0), "imdb_id": imdb_id or "", "title": title, "year": year, "type": typ, "seasons": seasons}

def build_catalog():
    pages_movie = _env_i("TMDB_PAGES_MOVIE", 12)
    pages_tv    = _env_i("TMDB_PAGES_TV", 12)
    region      = os.environ.get("REGION","US").strip() or "US"
    include_tv  = (os.environ.get("INCLUDE_TV_SEASONS","true").lower() in ("1","true","yes"))
    hard_cap    = _env_i("MAX_CATALOG", 6000)

    movie_ids = _collect_tmdb_ids("movie", pages_movie)
    tv_ids    = _collect_tmdb_ids("tv", pages_tv) if include_tv else []
    ids = [("movie", i) for i in movie_ids] + [("tv", i) for i in tv_ids]

    out = []
    for media_type, tid in ids:
        d = _tmdb_details(media_type, tid)
        item = _to_item_from_tmdb(media_type, d)
        if not item: continue

        # Providers (subs)
        item["providers"] = _providers_tmdb(media_type, tid, region=region)

        # OMDb enrichment: RT (critic), IMDb (audience), Language, Genres
        enrich = _omdb_enrich(item["title"], item["year"], "tv" if media_type=="tv" else "movie", imdb_id=item.get("imdb_id",""))
        item["imdb_id"] = enrich.get("imdb_id") or item.get("imdb_id") or ""
        item["critic"] = float(enrich.get("critic") or 0.0)
        item["audience"] = float(enrich.get("audience") or 0.0)
        item["language_primary"] = (enrich.get("language_primary") or "").lower()
        item["genres"] = enrich.get("genres") or []

        out.append(item)
        if len(out) >= hard_cap: break

    # English originals only
    english_only = [x for x in out if ("english" in (x.get("language_primary") or ""))]
    return english_only