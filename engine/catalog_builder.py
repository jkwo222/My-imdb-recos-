# engine/catalog_builder.py
import os, time, datetime, requests
from typing import Dict, Any, List, Tuple

# ---- CONFIG / ENV ---------------------------------------------
REGION = os.environ.get("REGION", "US").upper()
TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
OMDB_KEY = os.environ.get("OMDB_API_KEY", "").strip()

PAGES_MOVIE = int(os.environ.get("TMDB_PAGES_MOVIE", "5"))
PAGES_TV    = int(os.environ.get("TMDB_PAGES_TV", "5"))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "RecoEngine/2.13 (+github-actions)"
})

# ---- TMDB CLIENT ----------------------------------------------
TMDB_BASE = "https://api.themoviedb.org/3"

def _tmdb_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not TMDB_KEY:
        return {}
    url = f"{TMDB_BASE}{path}"
    p = {"api_key": TMDB_KEY, **(params or {})}
    for attempt in range(3):
        r = SESSION.get(url, params=p, timeout=20)
        if r.status_code == 200:
            return r.json()
        time.sleep(0.8 * (attempt + 1))
    return {}

def _tmdb_external_ids(kind: str, tmdb_id: int) -> Dict[str, Any]:
    return _tmdb_get(f"/{kind}/{tmdb_id}/external_ids", {})

def _providers(kind: str, tmdb_id: int) -> List[str]:
    data = _tmdb_get(f"/{kind}/{tmdb_id}/watch/providers", {})
    results = (data or {}).get("results", {})
    r = results.get(REGION) or {}
    out: List[str] = []

    buckets = []
    for k in ("flatrate", "free", "ads"):
        if r.get(k):
            buckets.append(k)
    # if nothing on subs, capture buy/rent (still normalized; runner will filter)
    if not buckets:
        for k in ("buy", "rent"):
            if r.get(k):
                buckets.append(k)

    seen = set()
    for k in buckets:
        for it in (r.get(k) or []):
            name = (it.get("provider_name") or "").lower()
            if not name: continue
            if "netflix" in name: norm = "netflix"
            elif "prime" in name or "amazon" in name: norm = "prime_video"
            elif name == "hulu": norm = "hulu"
            elif "max" in name or "hbo" in name: norm = "max"
            elif "disney" in name: norm = "disney_plus"
            elif "apple tv" in name: norm = "apple_tv_plus"
            elif "peacock" in name: norm = "peacock"
            elif "paramount" in name: norm = "paramount_plus"
            else: norm = name.replace(" ", "_")
            if norm not in seen:
                seen.add(norm); out.append(norm)
    return out

# ---- OMDb ratings (IMDb + RT) ---------------------------------
def _omdb_ratings(imdb_id: str, title: str, year: int) -> Tuple[float, float]:
    """
    Returns (critic, audience) in 0..1
    critic -> RottenTomatoes Tomatometer if present, else Metascore/100, else 0
    audience -> IMDb rating/10
    """
    if not OMDB_KEY:
        return (0.0, 0.0)
    params = {"apikey": OMDB_KEY, "tomatoes": "true"}
    if imdb_id:
        params["i"] = imdb_id
    else:
        params["t"] = title
        if year: params["y"] = str(year)

    for attempt in range(2):
        r = SESSION.get("http://www.omdbapi.com/", params=params, timeout=20)
        if r.status_code != 200:
            time.sleep(0.6); continue
        j = r.json()
        if not j or j.get("Response") != "True":
            return (0.0, 0.0)
        # IMDb rating
        aud = 0.0
        try:
            ir = j.get("imdbRating")
            if ir and ir != "N/A":
                aud = max(0.0, min(1.0, float(ir) / 10.0))
        except:  # noqa
            pass
        # RT critic (tomatometer) or Metascore
        crit = 0.0
        try:
            # Ratings array sometimes includes Rotten Tomatoes entry
            for entry in (j.get("Ratings") or []):
                if entry.get("Source") == "Rotten Tomatoes":
                    v = entry.get("Value","").strip().rstrip("%")
                    crit = max(crit, min(1.0, float(v)/100.0))
            if crit == 0.0:
                ms = j.get("Metascore")
                if ms and ms != "N/A":
                    crit = max(0.0, min(1.0, float(ms)/100.0))
        except:  # noqa
            pass
        return (crit, aud)
    return (0.0, 0.0)

# ---- DISCOVER helpers ------------------------------------------
def _discover(kind: str, pages: int) -> List[Dict[str, Any]]:
    """
    kind: 'movie' or 'tv'
    """
    out: List[Dict[str, Any]] = []
    today = datetime.date.today().isoformat()
    for page in range(1, max(1, pages) + 1):
        params: Dict[str, Any] = {
            "with_original_language": "en",          # ORIGINAL language English
            "watch_region": REGION,
            "with_watch_monetization_types": "flatrate|free|ads",
            "page": page,
            "sort_by": "popularity.desc"
        }
        if kind == "movie":
            params["primary_release_date.lte"] = today
            params["vote_count.gte"] = 200
            j = _tmdb_get("/discover/movie", params)
        else:
            params["first_air_date.lte"] = today
            params["vote_count.gte"] = 100
            j = _tmdb_get("/discover/tv", params)
        results = (j or {}).get("results") or []
        for r in results:
            tmdb_id = int(r.get("id") or 0)
            if not tmdb_id: continue
            title = r.get("title") or r.get("name") or ""
            date_key = "release_date" if kind == "movie" else "first_air_date"
            year = 0
            if r.get(date_key):
                try: year = int((r[date_key])[:4])
                except: year = 0
            seasons = int(r.get("number_of_seasons") or 1) if kind == "tv" else 0

            # providers
            provs = _providers(kind, tmdb_id)

            # external ids -> imdb
            imdb_id = ""
            ext = _tmdb_external_ids(kind, tmdb_id)
            imdb_id = (ext or {}).get("imdb_id") or ""

            # ratings
            critic, audience = _omdb_ratings(imdb_id, title, year)

            out.append({
                "imdb_id": imdb_id,
                "title": title,
                "year": year,
                "type": "movie" if kind == "movie" else "tvSeries",
                "seasons": seasons or 1,
                "providers": provs,
                "critic": round(critic, 4),
                "audience": round(audience, 4),
            })
        # be nice to APIs
        time.sleep(0.4)
    return out

def build_catalog() -> List[Dict[str, Any]]:
    movies = _discover("movie", PAGES_MOVIE)
    tv     = _discover("tv",    PAGES_TV)
    return movies + tv