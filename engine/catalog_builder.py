# engine/catalog_builder.py
from __future__ import annotations
import hashlib, json, os, pathlib, time, urllib.request
from typing import Dict, List, Tuple, Optional

TMDB_ROOT = "https://api.themoviedb.org/3"
CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
TMDB_CACHE.mkdir(parents=True, exist_ok=True)

def _tmdb_headers() -> Dict[str, str]:
    key = (os.environ.get("TMDB_API_KEY") or "").strip()
    # TMDB v4 bearer token OR v3 key in query. We support bearer only here.
    return {"Authorization": f"Bearer {key}"} if key else {}

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _cached_get_json(url: str, ttl_hours: int = 24) -> Optional[dict]:
    fp = TMDB_CACHE / f"{_h(url)}.json"
    if fp.exists():
        age = time.time() - fp.stat().st_mtime
        if age < ttl_hours * 3600:
            try:
                return json.load(open(fp, "r", encoding="utf-8"))
            except Exception:
                pass
    req = urllib.request.Request(url, headers=_tmdb_headers())
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            raw = r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  [http] error for {url} → {e}")
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    fp.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(fp, "w", encoding="utf-8"))
    return data

def _discover_ids(media_type: str, pages: int) -> List[int]:
    out: List[int] = []
    for p in range(1, pages + 1):
        url = f"{TMDB_ROOT}/discover/{media_type}?page={p}&sort_by=popularity.desc"
        d = _cached_get_json(url, ttl_hours=6)
        for r in (d.get("results") or []) if d else []:
            try:
                out.append(int(r.get("id")))
            except Exception:
                pass
    return out

PROV_MAP = {
    "Netflix":"netflix","Amazon Prime Video":"prime_video","Prime Video":"prime_video",
    "Hulu":"hulu","Max":"max","HBO Max":"max","Disney Plus":"disney_plus","Disney+":"disney_plus",
    "Apple TV Plus":"apple_tv_plus","Apple TV+":"apple_tv_plus","Peacock":"peacock",
    "Paramount Plus":"paramount_plus","Paramount+":"paramount_plus",
}

def _providers(media: str, tmdb_id: int, region: str) -> List[str]:
    url = f"{TMDB_ROOT}/{media}/{tmdb_id}/watch/providers"
    d = _cached_get_json(url, ttl_hours=24) or {}
    res = (d.get("results") or {}).get(region.upper()) or {}
    out = set()
    for bucket in ("flatrate","ads","free"):
        for e in res.get(bucket, []) or []:
            name = PROV_MAP.get(e.get("provider_name"))
            if name: out.add(name)
    return sorted(out)

def _details(media: str, tmdb_id: int) -> Optional[dict]:
    url = f"{TMDB_ROOT}/{media}/{tmdb_id}?append_to_response=external_ids"
    return _cached_get_json(url, ttl_hours=168)

def _to_item(media: str, d: dict) -> Optional[dict]:
    if not d: return None
    title = d.get("title") or d.get("name") or ""
    date = (d.get("release_date") or d.get("first_air_date") or "")[:4]
    year = int(date) if date.isdigit() else 0
    imdb_id = ((d.get("external_ids") or {}).get("imdb_id") or "") if isinstance(d.get("external_ids"), dict) else ""
    seasons = int(d.get("number_of_seasons") or 1) if media == "tv" else 1
    typ = "movie" if media == "movie" else ("tvMiniSeries" if seasons == 1 else "tvSeries")
    tmdb_vote = float(d.get("vote_average") or 0.0)
    pop = float(d.get("popularity") or 0.0)
    return {
        "tmdb_id": int(d.get("id") or 0),
        "imdb_id": imdb_id or "",
        "title": title,
        "year": year,
        "type": typ,
        "seasons": seasons,
        "tmdb_vote": tmdb_vote,
        "popularity": pop,
    }

def build_catalog(basics_map: Dict[str, Tuple[str,int,str]], ratings_map: Dict[str, Tuple[float,int]]) -> List[Dict]:
    pages_movie = int((os.environ.get("TMDB_PAGES_MOVIE") or "12").strip())
    pages_tv    = int((os.environ.get("TMDB_PAGES_TV") or "12").strip())
    include_tv  = (os.environ.get("INCLUDE_TV_SEASONS","true").lower() in ("1","true","yes"))
    region      = (os.environ.get("REGION") or "US").strip() or "US"
    hard_cap    = int((os.environ.get("MAX_CATALOG") or "6000").strip())

    movie_ids = _discover_ids("movie", pages_movie)
    tv_ids    = _discover_ids("tv", pages_tv) if include_tv else []

    ids = [("movie", i) for i in movie_ids] + [("tv", i) for i in tv_ids]

    out: List[Dict] = []
    for media, tid in ids:
        d = _details(media, tid)
        it = _to_item(media, d)
        if not it: continue
        it["providers"] = _providers(media, tid, region)

        # Attach IMDb aggregates (no OMDb)
        iid = it.get("imdb_id") or ""
        if iid and iid in ratings_map:
            r, votes = ratings_map[iid]
            it["imdb_rating"] = float(r)
            it["imdb_votes"]  = int(votes)
        else:
            it["imdb_rating"] = 0.0
            it["imdb_votes"]  = 0

        # A light critic proxy from TMDB vote when IMDb missing
        it["critic"]   = float(it.get("tmdb_vote") or 0.0) / 10.0
        it["audience"] = float(it.get("imdb_rating") or 0.0) / 10.0 if it.get("imdb_rating") else float(it.get("tmdb_vote") or 0.0)/10.0

        out.append(it)
        if len(out) >= hard_cap: break

    # English originals only, if ORIGINAL_LANGS provided (defaults to 'en')
    langs = (os.environ.get("ORIGINAL_LANGS") or "en").lower().split(",")
    langs = [s.strip() for s in langs if s.strip()]
    if langs:
        # Need language… fetch from details payload if present
        filtered = []
        for it in out:
            # try original_language from same details cache
            media = "movie" if it["type"]=="movie" else "tv"
            d = _details(media, it["tmdb_id"]) or {}
            ol = (d.get("original_language") or "").lower().strip()
            if not ol or ol in langs:
                filtered.append(it)
        out = filtered

    return out