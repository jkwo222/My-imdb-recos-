# engine/catalog_builder.py  (only the TMDB/HTTP parts changed)
import os, json, time, hashlib, pathlib, requests

TMDB = "https://api.themoviedb.org/3"
OMDB = "http://www.omdbapi.com/"

CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
OMDB_CACHE = CACHE_ROOT / "omdb"
for p in (TMDB_CACHE, OMDB_CACHE):
    p.mkdir(parents=True, exist_ok=True)

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _cached_get_json(url: str, headers=None, ttl_hours=48):
    cache_dir = TMDB_CACHE if "themoviedb" in url else OMDB_CACHE
    cache_file = cache_dir / f"{_h(url)}.json"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < 3600 * ttl_hours:
            try:
                return json.load(open(cache_file, "r", encoding="utf-8"))
            except Exception:
                pass
    try:
        r = requests.get(url, headers=headers or {}, timeout=30)
    except Exception:
        return None
    if r.status_code != 200:
        # leave a tiny breadcrumb to help diagnose in Actions logs
        try:
            print(f" {r.status_code} for {url.split('?')[0]} (no body cached)")
        except Exception:
            pass
        return None
    data = r.json()
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(cache_file, "w", encoding="utf-8"))
    return data

def _tmdb_is_v4(key: str) -> bool:
    return key.strip().startswith("eyJ")  # JWT-ish

def _tmdb_headers():
    key = os.environ.get("TMDB_API_KEY","").strip()
    if key and _tmdb_is_v4(key):
        return {"Authorization": f"Bearer {key}"}
    return {}  # v3 uses query param, not header

def _tmdb_url(path: str) -> str:
    """
    Build a TMDB URL that works for either key type.
    For v4, no query string needed; for v3, append ?api_key=...
    """
    base = TMDB.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    url = f"{base}{path}"
    key = os.environ.get("TMDB_API_KEY","").strip()
    if key and not _tmdb_is_v4(key):
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api_key={key}"
    return url

# ---- provider name map unchanged ----
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
    if not data:
        return []
    res = (data.get("results") or {}).get(region) or {}
    for bucket in ("flatrate","ads","free"):
        for p in res.get(bucket, []) or []:
            name = PROV_MAP.get(p.get("provider_name"))
            if name: out.add(name)
    return sorted(out)

def _collect_tmdb_ids(media_type: str, pages: int):
    ids = []
    for page in range(1, pages+1):
        url = _tmdb_url(f"/discover/{media_type}?page={page}&sort_by=popularity.desc")
        data = _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=6)
        for r in (data.get("results") or []) if data else []:
            ids.append(int(r.get("id")))
    return ids

def _tmdb_details(media_type: str, tmdb_id: int):
    url = _tmdb_url(f"/{media_type}/{tmdb_id}?append_to_response=external_ids")
    return _cached_get_json(url, headers=_tmdb_headers(), ttl_hours=168)