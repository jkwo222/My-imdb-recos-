# engine/catalog_builder.py
import os, json, time, hashlib, pathlib, requests

TMDB = "https://api.themoviedb.org/3"
# Use HTTPS (explicit). Some hosts redirect http->https, but be explicit here.
OMDB = "https://www.omdbapi.com/"

CACHE_ROOT = pathlib.Path("data/cache")
TMDB_CACHE = CACHE_ROOT / "tmdb"
OMDB_CACHE = CACHE_ROOT / "omdb"
for p in (TMDB_CACHE, OMDB_CACHE):
    p.mkdir(parents=True, exist_ok=True)

# --- NEW: small global guard to stop hammering OMDb after a hard failure.
_OMDB_DISABLED = False
_OMDB_FIRST_ERROR_LOGGED = False

def _env_i(name: str, default: int) -> int:
    try: return int(os.environ.get(name, "").strip() or default)
    except Exception: return default

def _h(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _cached_get_json(url: str, headers=None, ttl_hours=48):
    """
    Simple file cache; on non-200s print a single-line diagnostic with a short body preview.
    """
    cache_dir = TMDB_CACHE if "themoviedb" in url else OMDB_CACHE
    cache_file = cache_dir / f"{_h(url)}.json"
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < 3600 * ttl_hours:
            return json.load(open(cache_file, "r", encoding="utf-8"))

    try:
        r = requests.get(url, headers=headers, timeout=30)
    except Exception as e:
        print(f"[http] EXC for {url.split('?')[0]} → {e}")
        return None

    if r.status_code != 200:
        # Print a small preview of the body so we can see "Invalid API key" vs "Request limit reached"
        body_preview = (r.text or "").strip().replace("\n", " ")
        if len(body_preview) > 160:
            body_preview = body_preview[:160] + "…"
        print(f"[http] {r.status_code} for {url.split('?')[0]} → {body_preview or 'no body'}")
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
    global _OMDB_DISABLED, _OMDB_FIRST_ERROR_LOGGED

    if _OMDB_DISABLED:
        # Short-circuit after a confirmed hard failure (reduces noisy logs)
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}

    key = os.environ.get("OMDB_API_KEY","").strip()
    if not key:
        if not _OMDB_FIRST_ERROR_LOGGED:
            print("[omdb] OMDB_API_KEY is empty/missing in environment — skipping enrich.")
            _OMDB_FIRST_ERROR_LOGGED = True
        _OMDB_DISABLED = True
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}

    if imdb_id:
        url = f"{OMDB}?apikey={key}&i={imdb_id}&plot=short&r=json"
    else:
        t = (title or "").replace(" ","+")
        typ = "series" if media_type=="tv" else "movie"
        url = f"{OMDB}?apikey={key}&t={t}&y={year or ''}&type={typ}&plot=short&r=json"

    data = _cached_get_json(url, ttl_hours=168)  # 7 days
    if not data:
        # _cached_get_json already printed the HTTP code + preview
        # If this was likely a hard auth/rate failure, disable for the rest of the run
        _OMDB_DISABLED = True
        return {"critic":0.0,"audience":0.0,"language_primary":"","imdb_id":imdb_id or "","genres":[]}

    if str(data.get("Response","")).lower() == "false":
        # OMDb sent a JSON error (still 200) — show once, then carry on
        msg = (data.get("Error") or "").strip()
        if msg and not _OMDB_FIRST_ERROR_LOGGED:
            print(f"[omdb] Response=false: {msg}")
            _OMDB_FIRST_ERROR_LOGGED = True
            # If it's auth/rate related, disable further calls
            if "invalid api key" in msg.lower() or "request limit" in msg.lower():
                _OMDB_DISABLED = True
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

# (rest of the file unchanged)