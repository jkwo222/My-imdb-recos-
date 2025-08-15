# tools/tmdb_client.py
import os, time, json, pathlib, requests
from typing import Dict, List, Any, Tuple

TMDB_API = "https://api.themoviedb.org/3"
UA = {"User-Agent":"RecoEngine/2.13 (+github actions)"}
CACHE_DIR = pathlib.Path("data/cache/tmdb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _key() -> str:
    k = os.environ.get("TMDB_API_KEY","").strip()
    if not k:
        raise RuntimeError("TMDB_API_KEY is missing")
    return k

def _cache_path(name: str) -> pathlib.Path:
    return CACHE_DIR / name

def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(params or {})
    params["api_key"] = _key()
    r = requests.get(url, params=params, headers=UA, timeout=30)
    if r.status_code == 429:
        time.sleep(1.0)
        r = requests.get(url, params=params, headers=UA, timeout=30)
    if r.status_code != 200:
        return {"__error__": f"{r.status_code} {r.text[:200]}"}
    return r.json()

def _cached_json(name: str, fetcher) -> Dict[str, Any]:
    p = _cache_path(name)
    if p.exists():
        try:
            return json.load(p.open("r"))
        except Exception:
            pass
    data = fetcher()
    try:
        json.dump(data, p.open("w"))
    except Exception:
        pass
    return data

def _discover(kind: str, page: int, region: str, original_lang: str|None) -> Dict[str, Any]:
    url = f"{TMDB_API}/discover/{'movie' if kind=='movie' else 'tv'}"
    params = {
        "page": page,
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "watch_region": region or "US"
    }
    if original_lang:
        params["with_original_language"] = original_lang  # ISO-639-1, e.g., 'en'
    return _get(url, params)

def fetch_catalog(region: str, pages_movie: int, pages_tv: int, original_langs: List[str]) -> Tuple[List[Dict], Dict]:
    diag = {
        "movie_pages": pages_movie,
        "tv_pages": pages_tv,
        "langs": original_langs or [],
        "counts": {"movie":0, "tv":0},
        "errors": []
    }
    items: List[Dict[str,Any]] = []

    langs = original_langs or [None]
    for kind, pages in (("movie", pages_movie), ("tv", pages_tv)):
        for lang in langs:
            for page in range(1, max(1, pages)+1):
                cache_name = f"discover_{kind}_{lang or 'any'}_{region}_p{page}.json"
                data = _cached_json(cache_name, lambda k=kind, p=page, l=lang: _discover(k, p, region, l))
                if "__error__" in data:
                    diag["errors"].append({ "where": cache_name, "error": data["__error__"] })
                    continue
                results = data.get("results") or []
                for r in results:
                    r["_kind"] = kind
                    items.append(r)
                diag["counts"][kind] += len(results)
                time.sleep(0.12)

    seen = set()
    deduped = []
    for r in items:
        key = (r["_kind"], r.get("id"))
        if key in seen: continue
        seen.add(key); deduped.append(r)

    diag["after_dedupe"] = len(deduped)
    return deduped, diag

def fetch_providers(kind: str, tmdb_id: int, region: str) -> List[str]:
    name = f"providers_{kind}_{tmdb_id}_{region}.json"
    data = _cached_json(name, lambda: _get(f"{TMDB_API}/{kind}/{tmdb_id}/watch/providers", {}))
    if "__error__" in data: 
        return []
    results = (data.get("results") or {}).get(region, {})
    providers = set()
    for bucket in ("flatrate","ads","free","rent","buy"):
        for p in results.get(bucket, []) or []:
            providers.add((p.get("provider_name") or "").strip().lower().replace(" ","_"))
    return sorted(providers)