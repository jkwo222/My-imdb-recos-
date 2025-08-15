import os, json, time, requests, hashlib
from typing import List, Dict
from rich import print as rprint

def _tmdb_cache_dir():
    p = "data/cache/tmdb"; os.makedirs(p, exist_ok=True); return p

def _tmdb_cache(path: str, params: Dict) -> str:
    key = path + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return os.path.join(_tmdb_cache_dir(), f"{h}.json")

def _tmdb_get(api_key: str, path: str, params: Dict) -> Dict:
    params = dict(params); params["api_key"] = api_key
    cp = _tmdb_cache(path, params)
    if os.path.exists(cp):
        try:
            return json.load(open(cp,"r",encoding="utf-8"))
        except Exception:
            pass
    url = f"https://api.themoviedb.org/3{path}"
    for _ in range(3):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            data = r.json()
            json.dump(data, open(cp,"w"), indent=2)
            return data
        time.sleep(0.6)
    return {}

def _map_movie(x: Dict) -> Dict:
    title = x.get("title") or x.get("original_title") or ""
    year = (x.get("release_date") or "0000")[:4]
    return {
        "type": "movie",
        "tmdb_id": x.get("id"),
        "title": title,
        "year": int(year) if year.isdigit() else 0,
        "original_language": x.get("original_language"),
        "tmdb_vote": float(x.get("vote_average") or 0.0),
        "tmdb_votes": int(x.get("vote_count") or 0),
    }

def _map_tv(x: Dict) -> Dict:
    title = x.get("name") or x.get("original_name") or ""
    year = (x.get("first_air_date") or "0000")[:4]
    return {
        "type": "tvSeries",
        "tmdb_id": x.get("id"),
        "title": title,
        "year": int(year) if year.isdigit() else 0,
        "original_language": x.get("original_language"),
        "tmdb_vote": float(x.get("vote_average") or 0.0),
        "tmdb_votes": int(x.get("vote_count") or 0),
        "seasons": 1,  # will try to enrich later
    }

def _external_ids(api_key: str, typ: str, tmdb_id: int) -> Dict:
    path = f"/{typ}/{tmdb_id}/external_ids"
    return _tmdb_get(api_key, path, {})

def _tv_details(api_key: str, tmdb_id: int) -> Dict:
    path = f"/tv/{tmdb_id}"
    return _tmdb_get(api_key, path, {})

def build_catalog(
    tmdb_key: str,
    pages_movie: int,
    pages_tv: int,
    original_langs: List[str],
    include_tv_seasons: bool,
    max_catalog: int
) -> List[Dict]:
    out: List[Dict] = []

    langs = ",".join(original_langs) if original_langs else ""
    # Movies: discover with original language filter
    for p in range(1, max(1, pages_movie)+1):
        data = _tmdb_get(tmdb_key, "/discover/movie", {
            "with_original_language": langs or "en",
            "sort_by": "popularity.desc",
            "page": str(p),
            "include_adult": "false",
        })
        for x in data.get("results", []):
            out.append(_map_movie(x))

    # TV: discover with original language filter
    for p in range(1, max(1, pages_tv)+1):
        data = _tmdb_get(tmdb_key, "/discover/tv", {
            "with_original_language": langs or "en",
            "sort_by": "popularity.desc",
            "page": str(p),
            "include_adult": "false",
        })
        for x in data.get("results", []):
            out.append(_map_tv(x))

    rprint(f"[cyan]TMDB pulled base items:[/cyan] {len(out)}")

    # Enrich a slice (IMDB ids + TV seasons) with caching
    enrich_cap = min(len(out), 800)
    for i, rec in enumerate(out[:enrich_cap]):
        typ = "movie" if rec["type"] == "movie" else "tv"
        ext = _external_ids(tmdb_key, "movie" if typ=="movie" else "tv", rec["tmdb_id"])
        imdb_id = ext.get("imdb_id") or ""
        if imdb_id: rec["imdb_id"] = imdb_id
        if rec["type"] == "tvSeries":
            det = _tv_details(tmdb_key, rec["tmdb_id"])
            if isinstance(det, dict):
                rec["seasons"] = int(det.get("number_of_seasons") or 1)

    # Hard cap
    if max_catalog and len(out) > max_catalog:
        out = out[:max_catalog]

    return out