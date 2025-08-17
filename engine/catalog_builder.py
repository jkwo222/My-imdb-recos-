# engine/catalog_builder.py
from __future__ import annotations
import os, json, time, hashlib, pathlib, requests

from . import imdb_bulk

TMDB = "https://api.themoviedb.org/3"

CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
for p in (TMDB_CACHE,):
    p.mkdir(parents=True, exist_ok=True)

def _env_i(name: str, default: int) -> int:
    try:
        return int((os.environ.get(name, "") or "").strip() or default)
    except Exception:
        return default

def _h(s: str) -> str: return hashlib.sha1(s.encode("utf-8")).hexdigest()
def _cache_path_for(url: str) -> pathlib.Path:
    return TMDB_CACHE / f"{_h(url)}.json"

def _cached_get_json(url: str, headers=None, ttl_hours=48):
    cf = _cache_path_for(url)
    if cf.exists():
        age = time.time() - cf.stat().st_mtime
        if age < 3600 * ttl_hours:
            try: return json.load(open(cf, "r", encoding="utf-8"))
            except Exception: pass
    try:
        r = requests.get(url, headers=headers or {}, timeout=30)
    except Exception:
        return None
    if r.status_code != 200:
        try: print(f" {r.status_code} for {url.split('?')[0]} (no body cached)")
        except Exception: pass
        return None
    data = r.json()
    cf.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(cf, "w", encoding="utf-8"))
    return data

# --- TMDB auth: support v3 (?api_key=) or v4 (Bearer) ---
def _tmdb_is_v4(key: str) -> bool: return key.strip().startswith("eyJ")  # JWT-look
def _tmdb_headers():
    key = os.environ.get("TMDB_API_KEY","").strip()
    if key and _tmdb_is_v4(key): return {"Authorization": f"Bearer {key}"}
    return {}  # v3 uses querystring
def _tmdb_url(path: str) -> str:
    base = TMDB.rstrip("/")
    if not path.startswith("/"): path = "/" + path
    url = f"{base}{path}"
    key = os.environ.get("TMDB_API_KEY","").strip()
    if key and not _tmdb_is_v4(key):
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api_key={key}"
    return url

# --- Providers ---
PROV_MAP = {
    "Netflix":"netflix","Amazon Prime Video":"prime_video","Prime Video":"prime_video",
    "Hulu":"hulu","Max":"max","HBO Max":"max","Disney Plus":"disney_plus","Disney+":"disney_plus",
    "Apple TV Plus":"apple_tv_plus","Apple TV+":"apple_tv_plus","Peacock":"peacock",
    "Paramount Plus":"paramount_plus","Paramount+":"paramount_plus",
}
def _providers_tmdb(media_type: str, tmdb_id: int, region="US"):
    url = _tmdb_url(f"/{media_type}/{tmdb_id}/watch/providers")
    data = _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=24)
    out = set()
    if not data: return []
    res = (data.get("results") or {}).get(region) or {}
    for bucket in ("flatrate","ads","free"):
        for p in res.get(bucket, []) or []:
            name = PROV_MAP.get(p.get("provider_name"))
            if name: out.add(name)
    return sorted(out)

# --- Discovery/Details ---
def _collect_tmdb_ids(media_type: str, pages: int):
    ids = []
    for page in range(1, pages+1):
        url = _tmdb_url(f"/discover/{media_type}?page={page}&sort_by=popularity.desc")
        data = _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=6)
        for r in (data.get("results") or []) if data else []:
            tid = r.get("id")
            if tid is not None: ids.append(int(tid))
    return ids

def _tmdb_details(media_type: str, tmdb_id: int):
    url = _tmdb_url(f"/{media_type}/{tmdb_id}?append_to_response=external_ids")
    return _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=168)

def _to_item_from_tmdb(media_type: str, d: dict):
    if not d: return None
    title = d.get("title") or d.get("name") or ""
    date = (d.get("release_date") or d.get("first_air_date") or "")[:4]
    year = int(date) if (date and date.isdigit()) else 0
    imdb_id = ((d.get("external_ids") or {}).get("imdb_id") or "") if isinstance(d.get("external_ids"), dict) else ""
    seasons = int(d.get("number_of_seasons") or 1) if media_type == "tv" else 1
    typ = "movie" if media_type == "movie" else ("tvMiniSeries" if seasons == 1 else "tvSeries")

    lang_code = (d.get("original_language") or "").lower()
    language_primary = "english" if lang_code == "en" else lang_code
    genres = []
    try:
        for g in d.get("genres") or []:
            name = (g.get("name") or "").strip().lower()
            if name: genres.append(name)
    except Exception:
        pass

    tmdb_vote = float(d.get("vote_average") or 0.0)

    return {
        "tmdb_id": int(d.get("id") or 0),
        "imdb_id": imdb_id or "",
        "title": title,
        "year": year,
        "type": typ,
        "seasons": seasons,
        "language_primary": language_primary,
        "genres": genres,
        "tmdb_vote": tmdb_vote,
        "popularity": float(d.get("popularity") or 0.0),
    }

# --- Public API ---
def build_catalog():
    """
    Build a catalog of popular movie/TV items with providers and IMDb TSV enrichment (no OMDb).
    Returns English-originals only (based on TMDB original_language == 'en').
    """
    pages_movie = _env_i("TMDB_PAGES_MOVIE", 12)
    pages_tv    = _env_i("TMDB_PAGES_TV", 12)
    region      = (os.environ.get("REGION","US") or "US").strip() or "US"
    include_tv  = (os.environ.get("INCLUDE_TV_SEASONS","true").lower() in ("1","true","yes"))
    hard_cap    = _env_i("MAX_CATALOG", 6000)

    # ensure IMDb TSVs are ready for lookups
    imdb_bulk.load()

    movie_ids = _collect_tmdb_ids("movie", pages_movie)
    tv_ids    = _collect_tmdb_ids("tv", pages_tv) if include_tv else []
    ids = [("movie", i) for i in movie_ids] + [("tv", i) for i in tv_ids]

    out = []
    for media_type, tid in ids:
        d = _tmdb_details(media_type, tid)
        item = _to_item_from_tmdb(media_type, d)
        if not item:
            continue

        # Providers
        item["providers"] = _providers_tmdb(media_type, tid, region=region)

        # IMDb enrichment (ratings + genres) if we have an imdb_id
        iid = item.get("imdb_id","").strip()
        if iid:
            # IMDb average rating is 0..10; we scale to 0..1 for parity with prior pipeline
            imdb_avg = imdb_bulk.get_rating(iid)  # float
            item["audience"] = float(imdb_avg) / 10.0
            # prefer IMDb genres if present (often richer for movies)
            genres_imdb = imdb_bulk.get_genres(iid)
            if genres_imdb:
                item["genres"] = genres_imdb
        else:
            item["audience"] = 0.0

        # No Rotten Tomatoes without OMDb; use TMDB vote as "critic proxy" in downstream scoring
        item["critic"] = 0.0  # leave 0.0; your scorer already backs off to tmdb_vote

        out.append(item)
        if len(out) >= hard_cap:
            break

    # English originals only (TMDB original_language)
    english_only = [x for x in out if ("english" in (x.get("language_primary") or ""))]
    return english_only