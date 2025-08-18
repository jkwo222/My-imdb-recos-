# engine/tmdb.py
from __future__ import annotations
import os, json, time, hashlib, pathlib
from typing import Any, Dict, List, Optional, Tuple

import requests

_TMDb_V3 = "https://api.themoviedb.org/3"
_CACHE_DIR = pathlib.Path("data/cache/tmdb")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_API_KEY = (os.getenv("TMDB_API_KEY") or "").strip()
_BEARER  = (os.getenv("TMDB_BEARER") or os.getenv("TMDB_ACCESS_TOKEN") or "").strip()

def _headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if _BEARER:
        h["Authorization"] = f"Bearer {_BEARER}"
    return h

def _with_key(params: Dict[str, Any]) -> Dict[str, Any]:
    if _API_KEY and "api_key" not in params:
        params = {**params, "api_key": _API_KEY}
    return params

def _cache_path(url: str, params: Dict[str, Any]) -> pathlib.Path:
    sig = json.dumps({"u": url, "p": params}, sort_keys=True, ensure_ascii=False)
    key = hashlib.sha256(sig.encode("utf-8")).hexdigest()[:32]
    return _CACHE_DIR / f"{key}.json"

def _get_json(url: str, params: Dict[str, Any], *, ttl_s: int = 3600, timeout: int = 16) -> Dict[str, Any]:
    params = _with_key(params or {})
    cp = _cache_path(url, params)
    if cp.exists():
        try:
            if (time.time() - cp.stat().st_mtime) <= ttl_s:
                return json.loads(cp.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            pass
    try:
        r = requests.get(url, headers=_headers(), params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        cp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return data
    except Exception:
        if cp.exists():
            try:
                return json.loads(cp.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                pass
        return {}

# ---------- Normalizers ----------
def _norm_company_names(companies: Any) -> List[str]:
    out: List[str] = []
    if isinstance(companies, list):
        for c in companies:
            if isinstance(c, dict) and c.get("name"):
                out.append(str(c["name"]).strip())
            elif isinstance(c, str):
                out.append(c.strip())
    return [x for x in out if x]

def _norm_genre_names(genres: Any) -> List[str]:
    out: List[str] = []
    if isinstance(genres, list):
        for g in genres:
            if isinstance(g, dict) and g.get("name"):
                out.append(str(g["name"]).strip())
            elif isinstance(g, str):
                out.append(g.strip())
    return [x for x in out if x]

def _norm_networks(nets: Any) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if isinstance(nets, list):
        for n in nets:
            name = None
            if isinstance(n, dict):
                name = (n.get("name") or n.get("abbr") or n.get("network") or "").strip()
            elif isinstance(n, str):
                name = n.strip()
            if name:
                out.append({"name": name})
    return out

def _pick_title(kind: str, d: Dict[str, Any]) -> Tuple[str, Optional[int]]:
    if kind == "movie":
        title = d.get("title") or d.get("original_title") or d.get("name") or d.get("original_name") or "Untitled"
        year = None
        rd = (d.get("release_date") or "").strip()
        if len(rd) >= 4 and rd[:4].isdigit(): year = int(rd[:4])
        return title, year
    else:
        title = d.get("name") or d.get("original_name") or d.get("title") or d.get("original_title") or "Untitled"
        year = None
        fd = (d.get("first_air_date") or "").strip()
        if len(fd) >= 4 and fd[:4].isdigit(): year = int(fd[:4])
        return title, year

def _basic_from_result(kind: str, r: Dict[str, Any]) -> Dict[str, Any]:
    tid = r.get("id")
    title, year = _pick_title(kind, r)
    return {
        "tmdb_id": tid,
        "id": tid,
        "media_type": kind,
        "title": title,
        "name": r.get("name") or r.get("title") or title,
        "year": year,
        "popularity": r.get("popularity"),
        "release_date": r.get("release_date"),
        "first_air_date": r.get("first_air_date"),
        "original_language": r.get("original_language"),
        "media_type_raw": r.get("media_type") or kind,
    }

# ---------- Provider mapping ----------
_PROVIDER_MAP = {
    "netflix": "netflix",
    "max": "max", "hbo max": "max", "hbo": "max", "hbomax": "max",
    "paramount+": "paramount_plus", "paramount plus": "paramount_plus",
    "disney+": "disney_plus", "disney plus": "disney_plus",
    "apple tv+": "apple_tv_plus", "apple tv plus": "apple_tv_plus",
    "peacock": "peacock",
    "hulu": "hulu",
    "prime video": "prime_video", "amazon prime video": "prime_video", "amazon video": "prime_video",
    "starz": "starz", "showtime": "showtime",
}

def _provider_slug(name: str) -> Optional[str]:
    if not name:
        return None
    return _PROVIDER_MAP.get(name.strip().lower())

def _extract_provider_slugs(result_block: Dict[str, Any]) -> List[str]:
    slugs: List[str] = []
    for k in ("flatrate", "ads"):
        for item in (result_block.get(k) or []):
            s = _provider_slug(item.get("provider_name") or "")
            if s and s not in slugs:
                slugs.append(s)
    return slugs

# ---------- Discovery lists ----------
def _page_params(page: int) -> Dict[str, Any]:
    p = int(page or 1)
    return {"page": max(1, min(1000, p))}

def discover_movie(*, page: int = 1, region: str = "US", langs: List[str] | None = None) -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/discover/movie", {"region": region, **_page_params(page), **({"with_original_language": langs[0]} if langs and len(langs)==1 else {})}, ttl_s=8*3600)
    return [_basic_from_result("movie", r) for r in (data.get("results") or [])]

def discover_tv(*, page: int = 1, region: str = "US", langs: List[str] | None = None) -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/discover/tv", {"region": region, **_page_params(page), **({"with_original_language": langs[0]} if langs and len(langs)==1 else {})}, ttl_s=8*3600)
    return [_basic_from_result("tv", r) for r in (data.get("results") or [])]

def popular_movie(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/movie/popular", {"region": region, **_page_params(page)}, ttl_s=8*3600)
    return [_basic_from_result("movie", r) for r in (data.get("results") or [])]

def top_rated_movie(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/movie/top_rated", {"region": region, **_page_params(page)}, ttl_s=8*3600)
    return [_basic_from_result("movie", r) for r in (data.get("results") or [])]

def now_playing_movie(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/movie/now_playing", {"region": region, **_page_params(page)}, ttl_s=4*3600)
    return [_basic_from_result("movie", r) for r in (data.get("results") or [])]

def upcoming_movie(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/movie/upcoming", {"region": region, **_page_params(page)}, ttl_s=8*3600)
    return [_basic_from_result("movie", r) for r in (data.get("results") or [])]

def trending_movie(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/trending/movie/day", _page_params(page), ttl_s=4*3600)
    return [_basic_from_result("movie", r) for r in (data.get("results") or [])]

def popular_tv(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/tv/popular", {"region": region, **_page_params(page)}, ttl_s=8*3600)
    return [_basic_from_result("tv", r) for r in (data.get("results") or [])]

def top_rated_tv(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/tv/top_rated", {"region": region, **_page_params(page)}, ttl_s=8*3600)
    return [_basic_from_result("tv", r) for r in (data.get("results") or [])]

def airing_today_tv(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/tv/airing_today", {"region": region, **_page_params(page)}, ttl_s=4*3600)
    return [_basic_from_result("tv", r) for r in (data.get("results") or [])]

def on_the_air_tv(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/tv/on_the_air", {"region": region, **_page_params(page)}, ttl_s=4*3600)
    return [_basic_from_result("tv", r) for r in (data.get("results") or [])]

def trending_tv(*, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    data = _get_json(f"{_TMDb_V3}/trending/tv/day", _page_params(page), ttl_s=4*3600)
    return [_basic_from_result("tv", r) for r in (data.get("results") or [])]

# ---------- Per-title ----------
def get_details(kind: str, tmdb_id: int) -> Dict[str, Any]:
    if kind not in {"movie","tv"}: return {}
    data = _get_json(f"{_TMDb_V3}/{kind}/{tmdb_id}", {"append_to_response": "content_ratings,release_dates"}, ttl_s=14*24*3600)
    out: Dict[str, Any] = {}
    if kind == "movie":
        out["runtime"] = data.get("runtime")
        out["release_date"] = data.get("release_date")
        out["title"] = data.get("title") or data.get("original_title")
    else:
        out["number_of_seasons"] = data.get("number_of_seasons")
        out["first_air_date"] = data.get("first_air_date")
        out["last_air_date"] = data.get("last_air_date")
        out["episode_run_time"] = data.get("episode_run_time") or []
        out["name"] = data.get("name") or data.get("original_name")
        out["networks"] = _norm_networks(data.get("networks"))
    out["original_language"] = data.get("original_language")
    out["genres"] = _norm_genre_names(data.get("genres"))
    out["production_companies"] = _norm_company_names(data.get("production_companies"))
    try:
        va = float(data.get("vote_average") or 0.0)
        out["audience"] = max(0.0, min(100.0, va * 10.0))
    except Exception:
        pass
    title, year = _pick_title(kind, data)
    out["title"] = out.get("title") or title
    out["year"] = year
    return out

def get_credits(kind: str, tmdb_id: int) -> Dict[str, Any]:
    if kind not in {"movie","tv"}: return {}
    data = _get_json(f"{_TMDb_V3}/{kind}/{tmdb_id}/credits", {}, ttl_s=14*24*3600)
    out: Dict[str, Any] = {"directors": [], "writers": [], "cast": []}
    crew = data.get("crew") or []; cast = data.get("cast") or []

    dirs=[]
    for c in crew:
        dept=(c.get("department") or "").lower(); job=(c.get("job") or "").lower()
        if dept=="directing" and ("director" in job or job=="directing"):
            name=c.get("name"); 
            if name and name not in dirs: dirs.append(name)
    out["directors"]=dirs[:4]

    wrs=[]
    for c in crew:
        dept=(c.get("department") or "").lower(); job=(c.get("job") or "").lower()
        if dept=="writing" and any(k in job for k in ("writer","screenplay","teleplay","story")):
            nm=c.get("name"); 
            if nm and nm not in wrs: wrs.append(nm)
    out["writers"]=wrs[:6]

    cast_sorted = sorted(cast, key=lambda x: int(x.get("order") or 9999))
    names=[]
    for c in cast_sorted[:12]:
        nm=c.get("name")
        if nm and nm not in names: names.append(nm)
    out["cast"]=names[:8]
    return out

def get_keywords(kind: str, tmdb_id: int) -> List[str]:
    if kind not in {"movie","tv"}: return []
    if kind=="movie":
        data = _get_json(f"{_TMDb_V3}/movie/{tmdb_id}/keywords", {}, ttl_s=21*24*3600)
        ks = data.get("keywords") or []
    else:
        data = _get_json(f"{_TMDb_V3}/tv/{tmdb_id}/keywords", {}, ttl_s=21*24*3600)
        ks = data.get("results") or []
    out: List[str] = []
    for k in ks:
        name = k.get("name") if isinstance(k, dict) else None
        if name: out.append(str(name).strip())
    seen=set(); dedup=[]
    for k in out:
        if k not in seen:
            seen.add(k); dedup.append(k)
    return dedup[:60]

def get_external_ids(kind: str, tmdb_id: int) -> Dict[str, Any]:
    if kind not in {"movie","tv"}: return {}
    data = _get_json(f"{_TMDb_V3}/{kind}/{tmdb_id}/external_ids", {}, ttl_s=60*24*3600)
    out = {}
    imdb_id = data.get("imdb_id")
    if imdb_id: out["imdb_id"] = imdb_id
    return out

def get_title_watch_providers(kind: str, tmdb_id: int, region: str = "US") -> List[str]:
    if kind not in {"movie","tv"}: return []
    data = _get_json(f"{_TMDb_V3}/{kind}/{tmdb_id}/watch/providers", {}, ttl_s=2*24*3600)
    res = (data.get("results") or {}).get(region.upper()) or {}
    slugs = _extract_provider_slugs(res)
    seen=set(); out=[]
    for s in slugs:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

# ---------- Multi-search (used by enrichment fallback) ----------
def search_multi(query: str, *, page: int = 1, region: str = "US") -> List[Dict[str, Any]]:
    """
    Lightweight multi search across movie/tv/person. We return only movie/tv hits,
    normalized to the same basic schema used by discovery.
    """
    if not (query or "").strip():
        return []
    data = _get_json(f"{_TMDb_V3}/search/multi", {"query": query, **_page_params(page), "region": region}, ttl_s=2*3600)
    out: List[Dict[str, Any]] = []
    for r in data.get("results") or []:
        mt = (r.get("media_type") or "").lower()
        if mt not in {"movie","tv"}:
            continue
        out.append(_basic_from_result(mt, r))
    return out