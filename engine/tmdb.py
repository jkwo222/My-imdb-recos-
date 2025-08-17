from __future__ import annotations
import hashlib, json, os, time
from typing import Dict, Iterable, List, Optional, Tuple
import requests

_TMDB_API_BASE = "https://api.themoviedb.org/3"
_CACHE_DIR = os.path.join("data", "cache", "tmdb")
os.makedirs(_CACHE_DIR, exist_ok=True)

_DEFAULT_TIMEOUT = (5, 20)
_DEFAULT_UA = "my-imdb-recos/1.0 (+github actions)"

def _api_key() -> str:
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise RuntimeError("TMDB_API_KEY not set")
    return key

def _hash_key(kind: str, url: str, params: Dict) -> str:
    blob = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    return f"{kind}_{hashlib.sha1(blob.encode('utf-8')).hexdigest()}.json"

def _cache_path(kind: str, url: str, params: Dict) -> str:
    return os.path.join(_CACHE_DIR, _hash_key(kind, url, params))

def _load_cache(path: str) -> Optional[Dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _save_cache(path: str, data: Dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)

def _get_json(kind: str, url: str, params: Dict) -> Dict:
    path = _cache_path(kind, url, params)
    cached = _load_cache(path)
    if cached is not None:
        return cached

    headers = {
        "Authorization": f"Bearer {_api_key()}",
        "Accept": "application/json",
        "User-Agent": _DEFAULT_UA,
    }
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=_DEFAULT_TIMEOUT)
            if r.status_code == 429:
                sleep_s = float(r.headers.get("Retry-After", "1"))
                time.sleep(min(max(sleep_s, 1.0), 5.0))
                continue
            r.raise_for_status()
            data = r.json()
            _save_cache(path, data)
            return data
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)
    raise RuntimeError("TMDB request failed and retries exhausted")

# --- Provider helpers ---------------------------------------------------------

def _norm_slug(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())

def providers_from_env(subs_csv_or_list, region: str = "US") -> List[int]:
    if isinstance(subs_csv_or_list, str):
        wanted = [s.strip().lower() for s in subs_csv_or_list.split(",") if s.strip()]
    elif isinstance(subs_csv_or_list, Iterable):
        wanted = [str(s).strip().lower() for s in subs_csv_or_list if str(s).strip()]
    else:
        wanted = []
    # mapping: env slugs -> TMDB IDs (US)
    mapping = {
        "netflix": 8,
        "prime_video": 9,
        "amazonprimevideo": 9,
        "primevideo": 9,
        "hulu": 15,
        "max": 384,
        "hbomax": 384,
        "disney_plus": 337,
        "disneyplus": 337,
        "apple_tv_plus": 350,
        "appletvplus": 350,
        "peacock": 386,
        "paramount_plus": 531,
        "paramountplus": 531,
    }
    ids: List[int] = []
    seen = set()
    for slug in wanted:
        base = _norm_slug(slug)
        pid = mapping.get(slug) or mapping.get(base)
        if pid and pid not in seen:
            ids.append(pid); seen.add(pid)
    return ids

def parse_provider_names(obj: Dict, region: str = "US") -> List[str]:
    out: List[str] = []
    if not obj:
        return out
    results = obj.get("results") or {}
    entry = results.get(region.upper()) or {}
    for bucket in ("flatrate","free","ads"):
        for it in entry.get(bucket, []) or []:
            name = it.get("provider_name")
            if name and name not in out:
                out.append(name)
    return out

# --- Discover / details -------------------------------------------------------

def _normalize_langs(original_langs: Optional[Iterable[str] | str]) -> Optional[str]:
    if not original_langs:
        return None
    if isinstance(original_langs, str):
        return original_langs.split(",")[0].strip() or None
    it = list(original_langs)
    return (str(it[0]).strip() or None) if it else None

def _discover(kind: str, page: int, *, region: str, provider_ids: List[int], original_langs: Optional[Iterable[str] | str]) -> Dict:
    url = f"{_TMDB_API_BASE}/discover/{kind}"
    with_lang = _normalize_langs(original_langs)
    params = {
        "include_adult": "false",
        "language": "en-US",
        "page": str(page),
        "sort_by": "popularity.desc",
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)
        params["watch_region"] = region.upper()
        params["with_watch_monetization_types"] = "flatrate|free|ads"
    if with_lang:
        params["with_original_language"] = with_lang
    return _get_json(f"discover_{kind}", url, params)

def discover_movie_page(page: int, *, region: str = "US", provider_ids: Optional[List[int]] = None, original_langs: Optional[Iterable[str] | str] = None) -> Tuple[List[Dict], int]:
    data = _discover("movie", page, region=region, provider_ids=provider_ids or [], original_langs=original_langs)
    return (data.get("results") or []), int(data.get("page") or page)

def discover_tv_page(page: int, *, region: str = "US", provider_ids: Optional[List[int]] = None, original_langs: Optional[Iterable[str] | str] = None) -> Tuple[List[Dict], int]:
    data = _discover("tv", page, region=region, provider_ids=provider_ids or [], original_langs=original_langs)
    return (data.get("results") or []), int(data.get("page") or page)

def external_ids(media_type: str, tmdb_id: int) -> Dict:
    url = f"{_TMDB_API_BASE}/{media_type}/{tmdb_id}/external_ids"
    params = {"language":"en-US"}
    return _get_json(f"external_ids_{media_type}", url, params)

def details(media_type: str, tmdb_id: int) -> Dict:
    url = f"{_TMDB_API_BASE}/{media_type}/{tmdb_id}"
    params = {"language":"en-US"}
    return _get_json(f"details_{media_type}", url, params)

def find_by_imdb_id(tconst: str) -> Dict:
    url = f"{_TMDB_API_BASE}/find/{tconst}"
    params = {"external_source": "imdb_id", "language": "en-US"}
    return _get_json("find_imdb", url, params)

def search_title_year(title: str, year: int | None, kind: str) -> Dict:
    kind = "movie" if kind == "movie" else "tv"
    url = f"{_TMDB_API_BASE}/search/{kind}"
    params = {"query": title, "language": "en-US", "include_adult": "false", "page": "1"}
    if year and kind == "movie":
        params["year"] = str(year)
    if year and kind == "tv":
        params["first_air_date_year"] = str(year)
    return _get_json(f"search_{kind}", url, params)

def watch_providers(media_type: str, tmdb_id: int) -> Dict:
    url = f"{_TMDB_API_BASE}/{media_type}/{tmdb_id}/watch/providers"
    params = {"language": "en-US"}
    return _get_json(f"providers_{media_type}", url, params)