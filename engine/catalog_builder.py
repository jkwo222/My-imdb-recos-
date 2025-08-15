import os, math, requests, time
from typing import Dict, List, Any
from rich import print as rprint
from .cache import get_fresh, set as cache_set

TMDB_API = "https://api.themoviedb.org/3"
UA = {"User-Agent": "RecoEngine/1.0 (+github actions)"}

def _tmdb_get(path: str, params: Dict[str, Any]) -> Dict:
    headers = {"Authorization": f"Bearer {os.environ.get('TMDB_API_KEY','')}", **UA}
    url = f"{TMDB_API}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        rprint(f"[red][TMDB] {path} → {r.status_code} {r.text[:120]}[/red]")
        return {}
    return r.json()

def _tmdb_external_ids(kind: str, tmdb_id: int) -> Dict:
    key = f"tmdb_ext_{kind}_{tmdb_id}"
    cached = get_fresh(key, ttl_days=14)
    if cached is not None: return cached
    j = _tmdb_get(f"/{kind}/{tmdb_id}/external_ids", {})
    imdb_id = j.get("imdb_id") if j else None
    cache_set(key, {"imdb_id": imdb_id})
    return {"imdb_id": imdb_id}

def _tmdb_tv_details(tmdb_id: int) -> Dict:
    if os.environ.get("INCLUDE_TV_SEASONS","true").lower() != "true":
        return {}
    key = f"tmdb_tv_{tmdb_id}_details"
    cached = get_fresh(key, ttl_days=14)
    if cached is not None: return cached
    j = _tmdb_get(f"/tv/{tmdb_id}", {})
    out = {"number_of_seasons": j.get("number_of_seasons") if j else None}
    cache_set(key, out)
    return out

def _omdb_get_by_imdb(imdb_id: str) -> Dict:
    if not imdb_id: return {}
    key = f"omdb_{imdb_id}"
    cached = get_fresh(key, ttl_days=7)
    if cached is not None: return cached
    omdb_key = os.environ.get("OMDB_API_KEY","")
    params = {"apikey": omdb_key, "i": imdb_id, "tomatoes": "true"}
    r = requests.get("http://www.omdbapi.com/", params=params, timeout=30)
    try:
        j = r.json()
    except Exception:
        j = {}
    cache_set(key, j)
    return j

def _ratings_from_omdb(omdb: Dict) -> Dict[str, float]:
    # Normalize critic/audience to 0..1 (continuous; no minimum thresholds)
    critic, audience = None, None
    if not omdb or omdb.get("Response") == "False":
        return {"critic": None, "audience": None}
    # IMDb audience
    try:
        ir = float(omdb.get("imdbRating", "0") or 0.0) / 10.0
        if ir > 0: audience = max(audience or 0, ir)
    except: pass
    # Metascore (critic)
    try:
        ms = float(omdb.get("Metascore", "0") or 0.0) / 100.0
        if ms > 0: critic = max(critic or 0, ms)
    except: pass
    # Ratings array (may include Rotten Tomatoes)
    for ent in (omdb.get("Ratings") or []):
        src = (ent.get("Source") or "").lower()
        val = (ent.get("Value") or "")
        if "%" in val:
            try:
                pct = float(val.strip().replace("%",""))/100.0
            except:
                pct = None
        elif "/" in val:
            try:
                num, den = val.split("/",1)
                pct = float(num)/float(den)
            except:
                pct = None
        else:
            pct = None
        if pct is None: 
            continue
        if "rotten" in src:  # Tomatometer (critic) most likely
            critic = max(critic or 0, pct)
        if "audience" in src:  # sometimes Audience score shows
            audience = max(audience or 0, pct)
    return {"critic": critic, "audience": audience}

def _discover(kind: str, pages: int) -> List[Dict]:
    out: List[Dict] = []
    for page in range(1, pages+1):
        j = _tmdb_get(f"/discover/{kind}", {
            "include_adult": "false",
            "sort_by": "popularity.desc",
            "page": page,
            "with_original_language": "en",
        })
        results = (j or {}).get("results") or []
        for r in results:
            # Filter again by original_language to be safe
            if r.get("original_language") != "en": 
                continue
            item = {
                "tmdb_id": r.get("id"),
                "title": r.get("title") or r.get("name"),
                "year": (r.get("release_date") or r.get("first_air_date") or "")[:4],
                "type": "movie" if kind == "movie" else "tvSeries"
            }
            out.append(item)
        time.sleep(0.15)
    return out

def build_or_update_master() -> List[Dict]:
    pages_movie = int(os.environ.get("TMDB_PAGES_MOVIE","5"))
    pages_tv    = int(os.environ.get("TMDB_PAGES_TV","5"))
    cache_key = f"master_catalog_en_v2_m{pages_movie}_t{pages_tv}"
    cached = get_fresh(cache_key, ttl_days=3)
    if cached: 
        rprint(f"[cache] using cached master catalog: {len(cached)} items")
        return cached

    movies = _discover("movie", pages_movie)
    tvs    = _discover("tv", pages_tv)

    # Attach IMDB IDs (external_ids), seasons (for TV), and OMDb ratings
    final: List[Dict] = []
    for item in (movies + tvs):
        kind = "movie" if item["type"] == "movie" else "tv"
        ext = _tmdb_external_ids(kind, item["tmdb_id"])
        imdb_id = ext.get("imdb_id")
        seasons = None
        if item["type"] == "tvSeries":
            det = _tmdb_tv_details(item["tmdb_id"])
            seasons = det.get("number_of_seasons")
        omdb = _omdb_get_by_imdb(imdb_id) if imdb_id else {}
        ratings = _ratings_from_omdb(omdb)
        final.append({
            "imdb_id": imdb_id or "",
            "title": item["title"],
            "year": int(item["year"] or 0),
            "type": item["type"],
            "seasons": seasons if seasons is not None else (1 if item["type"]!="tvSeries" else None),
            "critic": ratings["critic"],
            "audience": ratings["audience"]
        })

    cache_set(cache_key, final)
    # also write a working snapshot area for transparency
    os.makedirs("data/catalog", exist_ok=True)
    import json, datetime
    json.dump(final, open(f"data/catalog/master_en_{pages_movie}_{pages_tv}.json","w"), indent=2)
    return final

def write_working_snapshot(master: List[Dict]) -> List[Dict]:
    # In this version, working == master (you could apply extra filters here if needed)
    os.makedirs("data/work", exist_ok=True)
    import json
    json.dump(master, open("data/work/working.json","w"), indent=2)
    return master