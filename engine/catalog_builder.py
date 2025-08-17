# engine/catalog_builder.py
from __future__ import annotations
import os, json, time, hashlib, pathlib, requests
from typing import Dict, List

TMDB = "https://api.themoviedb.org/3"

CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
for p in (TMDB_CACHE,):
    p.mkdir(parents=True, exist_ok=True)

from rich import print as rprint
from .imdb_datasets import IMDbEnricher

def _env_i(name: str, default: int) -> int:
    try: return int(os.environ.get(name, "").strip() or default)
    except Exception: return default

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _cached_get_json(url: str, headers=None, ttl_hours=48):
    cache_dir = TMDB_CACHE
    cache_file = cache_dir / f"{_h(url)}.json"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < 3600 * ttl_hours:
            try:
                return json.load(open(cache_file, "r", encoding="utf-8"))
            except Exception:
                pass
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        rprint(f"[yellow][http] {r.status_code} for {url} (no body cached)[/yellow]")
        return None
    data = r.json()
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(cache_file, "w", encoding="utf-8"))
    return data

def _tmdb_headers():
    # Accept either v4 Bearer or legacy api_key on URL; prefer Bearer
    bearer = os.environ.get("TMDB_API_KEY","").strip()
    if bearer and len(bearer) > 40:
        return {"Authorization": f"Bearer {bearer}"}
    # If it looks like a short v3 key, we’ll tack it onto URLs instead
    return {}

# Map a subset of TMDB provider names -> our slugs
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

def _collect_tmdb_ids(media_type: str, pages: int, region="US", langs="en", with_watch_region=True):
    ids = []
    apikey = os.environ.get("TMDB_API_KEY","").strip()
    has_bearer = (len(apikey) > 40)
    for page in range(1, pages+1):
        base = f"{TMDB}/discover/{media_type}?page={page}&sort_by=popularity.desc"
        extras = f"&watch_region={region}" if with_watch_region else ""
        ol = f"&with_original_language={langs}" if langs else ""
        if not has_bearer and apikey:  # v3
            base += f"&api_key={apikey}"
        url = base + extras + ol
        data = _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=6)
        for r in (data.get("results") or []) if data else []:
            ids.append(int(r.get("id")))
    return ids

def _tmdb_details(media_type: str, tmdb_id: int):
    apikey = os.environ.get("TMDB_API_KEY","").strip()
    has_bearer = (len(apikey) > 40)
    url = f"{TMDB}/{media_type}/{tmdb_id}?append_to_response=external_ids"
    if not has_bearer and apikey:
        url += f"&api_key={apikey}"
    return _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=168)

def _to_item_from_tmdb(media_type: str, d: dict):
    if not d: return None
    title = d.get("title") or d.get("name") or ""
    date = (d.get("release_date") or d.get("first_air_date") or "")[:4]
    year = int(date) if (date and date.isdigit()) else 0
    imdb_id = ((d.get("external_ids") or {}).get("imdb_id") or "") if isinstance(d.get("external_ids"), dict) else ""
    seasons = int(d.get("number_of_seasons") or 1) if media_type == "tv" else 1
    typ = "movie" if media_type == "movie" else ("tvMiniSeries" if seasons == 1 else "tvSeries")
    tmdb_vote = float(d.get("vote_average") or 0.0)
    return {
        "tmdb_id": int(d.get("id") or 0),
        "imdb_id": imdb_id or "",
        "title": title,
        "year": year,
        "type": typ,
        "seasons": seasons,
        "tmdb_vote": tmdb_vote,  # critic proxy (0..10)
        "language_primary": (d.get("original_language") or "").strip().lower(),
        "genres": [g.get("name","").strip().lower() for g in (d.get("genres") or []) if g.get("name")],
    }

def build_catalog():
    pages_movie = _env_i("TMDB_PAGES_MOVIE", 12)
    pages_tv    = _env_i("TMDB_PAGES_TV", 12)
    region      = os.environ.get("REGION","US").strip() or "US"
    langs       = os.environ.get("ORIGINAL_LANGS","en").strip()
    include_tv  = (os.environ.get("INCLUDE_TV_SEASONS","true").lower() in ("1","true","yes"))
    hard_cap    = _env_i("MAX_CATALOG", 6000)

    # 1) collect ids
    movie_ids = _collect_tmdb_ids("movie", pages_movie, region=region, langs=langs)
    tv_ids    = _collect_tmdb_ids("tv",    pages_tv,    region=region, langs=langs) if include_tv else []
    ids = [("movie", i) for i in movie_ids] + [("tv", i) for i in tv_ids]

    enricher = IMDbEnricher(ttl_days=7)

    # 2) hydrate and enrich
    out: List[Dict] = []
    for media_type, tid in ids:
        d = _tmdb_details(media_type, tid)
        item = _to_item_from_tmdb(media_type, d)
        if not item: 
            continue

        # Providers (subs)
        item["providers"] = _providers_tmdb(media_type, tid, region=region)

        # IMDb enrichment: real audience rating + genres from datasets (if present)
        e = enricher.enrich(item["title"], item["year"], "tv" if media_type=="tv" else "movie", imdb_id=item.get("imdb_id",""))
        if e.get("imdb_id"):    item["imdb_id"] = e["imdb_id"]
        if e.get("genres"):     item["genres"]  = e["genres"]
        if e.get("audience",0) > 0: item["audience"] = float(e["audience"])
        else: item["audience"] = 0.0  # default if missing
        # critic proxy from TMDB vote (0..10 -> 0..1)
        item["critic"] = float(item.get("tmdb_vote",0.0)) / 10.0

        out.append(item)
        if len(out) >= hard_cap: break

    # English (or requested) originals only — already filtered by discover param.
    # Keep the simple additional guard to be safe:
    target_langs = {x.strip().lower() for x in langs.split(",") if x.strip()}
    english_only = [x for x in out if (not target_langs) or (x.get("language_primary","") in target_langs)]
    rprint(f"[magenta]catalog built[/magenta] → {len(english_only)} items")
    return english_only