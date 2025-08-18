# engine/tmdb.py
from __future__ import annotations
import os
from typing import Dict, List, Tuple, Any, Optional

import requests

# Accept multiple env names for the TMDB v4 token; keep v3 key support
TMDB_KEY = os.getenv("TMDB_API_KEY")
TMDB_BEARER = (
    os.getenv("TMDB_BEARER")
    or os.getenv("TMDB_ACCESS_TOKEN")
    or os.getenv("TMDB_V4_TOKEN")
)

_API_BASE = "https://api.themoviedb.org/3"
_TIMEOUT = 25

def _tmdb_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET TMDB v3/v4 with either API key (v3) or Bearer (v4)."""
    url = path if path.startswith("http") else f"{_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers: Dict[str, str] = {}
    if TMDB_BEARER:
        headers["Authorization"] = f"Bearer {TMDB_BEARER}"
    q = dict(params or {})
    if TMDB_KEY and "api_key" not in q and not TMDB_BEARER:
        q["api_key"] = TMDB_KEY
    r = requests.get(url, headers=headers, params=q, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()

# ---------- provider name <-> slug helpers ----------
def _slugify_provider_name(name: str) -> str:
    n = (name or "").strip().lower()
    if not n:
        return ""
    if "apple tv+" in n or n == "apple tv plus":
        return "apple_tv_plus"
    if "netflix" in n:
        return "netflix"
    if n in {"hbo", "hbo max", "hbomax", "max"}:
        return "max"
    if "paramount+" in n:
        return "paramount_plus"
    if "disney+" in n:
        return "disney_plus"
    if "peacock" in n:
        return "peacock"
    if "hulu" in n:
        return "hulu"
    if "prime video" in n or "amazon" in n:
        return "prime_video"
    if "starz" in n:
        return "starz"
    if "showtime" in n:
        return "showtime"
    if "amc+" in n:
        return "amc_plus"
    if "criterion" in n:
        return "criterion_channel"
    if "mubi" in n:
        return "mubi"
    return n.replace(" ", "_")

def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    if s in {"hbo", "hbo_max", "hbomax"}:
        return "max"
    return s

# ---------- provider directory ----------
def _fetch_provider_directory(region: str) -> Dict[str, int]:
    region = (region or "US").upper()
    out: Dict[str, int] = {}
    for kind in ("watch/providers/movie", "watch/providers/tv"):
        data = _tmdb_get(kind, {"watch_region": region})
        for rec in (data or {}).get("results", []) or []:
            slug = _slugify_provider_name(rec.get("provider_name", ""))
            pid = rec.get("provider_id")
            if slug and isinstance(pid, int):
                out.setdefault(slug, pid)
    return out

def providers_from_env(subs: List[str], region: str) -> Tuple[List[int], Dict[str, Optional[int]]]:
    subs = [_normalize_slug(s) for s in (subs or [])]
    directory = _fetch_provider_directory(region)
    used_map: Dict[str, Optional[int]] = {}
    ids: List[int] = []
    for s in subs:
        pid = directory.get(s)
        used_map[s] = pid if isinstance(pid, int) else None
        if isinstance(pid, int):
            ids.append(pid)
    ids = sorted({i for i in ids if isinstance(i, int)})
    return ids, used_map

# ---------- discovery (subscription-only) ----------
def _common_discover_params(region: str, langs: List[str], provider_ids: List[int]) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "watch_region": (region or "US").upper(),
        "with_watch_monetization_types": "flatrate,ads",
        "sort_by": "popularity.desc",
        "include_adult": "false",
    }
    if langs:
        params["with_original_language"] = ",".join(langs)
    if provider_ids:
        params["with_watch_providers"] = ",".join(str(i) for i in provider_ids)
    return params

def _to_year(date_s: str) -> Optional[int]:
    if not date_s:
        return None
    try:
        return int((date_s or "")[:4])
    except Exception:
        return None

def _shape_movie_result(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "media_type": "movie",
        "tmdb_id": rec.get("id"),
        "title": rec.get("title") or rec.get("original_title"),
        "year": _to_year(rec.get("release_date")),
        "tmdb_vote": rec.get("vote_average"),
        "popularity": rec.get("popularity"),
        "original_language": (rec.get("original_language") or "").lower(),
    }

def _shape_tv_result(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "media_type": "tv",
        "tmdb_id": rec.get("id"),
        "name": rec.get("name") or rec.get("original_name"),
        "title": rec.get("name") or rec.get("original_name"),
        "year": _to_year(rec.get("first_air_date")),
        "tmdb_vote": rec.get("vote_average"),
        "popularity": rec.get("popularity"),
        "original_language": (rec.get("original_language") or "").lower(),
    }

def discover_movie_page(page: int, region: str, langs: List[str], provider_ids: List[int], slot: int = 0) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = _common_discover_params(region, langs, provider_ids)
    params.update({"page": max(1, int(page)), "vote_count.gte": 50 if (page % 3) else 100})
    data = _tmdb_get("discover/movie", params)
    results = [_shape_movie_result(r) for r in (data or {}).get("results", [])]
    diag = {"page": page, "total_pages": (data or {}).get("total_pages"), "count": len(results)}
    return results, diag

def discover_tv_page(page: int, region: str, langs: List[str], provider_ids: List[int], slot: int = 0) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = _common_discover_params(region, langs, provider_ids)
    params.update({"page": max(1, int(page)), "vote_count.gte": 50 if (page % 2) else 100})
    data = _tmdb_get("discover/tv", params)
    results = [_shape_tv_result(r) for r in (data or {}).get("results", [])]
    diag = {"page": page, "total_pages": (data or {}).get("total_pages"), "count": len(results)}
    return results, diag

# ---------- trending ----------
def trending(kind: str, period: str = "day") -> List[Dict[str, Any]]:
    kind = (kind or "movie").lower()
    if kind not in ("movie", "tv"):
        kind = "movie"
    period = "week" if period == "week" else "day"
    data = _tmdb_get(f"trending/{kind}/{period}")
    shaped: List[Dict[str, Any]] = []
    for r in (data or {}).get("results", []) or []:
        shaped.append(_shape_movie_result(r) if kind == "movie" else _shape_tv_result(r))
    return shaped

# ---------- external IDs / mapping ----------
def get_external_ids(kind: str, tmdb_id: int) -> Dict[str, Any]:
    kind = (kind or "").lower()
    if kind not in ("movie", "tv") or not tmdb_id:
        return {}
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/external_ids")
    return {"imdb_id": data.get("imdb_id")}

def find_by_imdb(imdb_id: str) -> Dict[str, Any]:
    """
    Map an IMDb tconst (tt...) to TMDB id and media_type via /find.
    Returns: {"media_type": "movie"|"tv"|None, "tmdb_id": int|None}
    """
    imdb_id = (imdb_id or "").strip()
    if not imdb_id:
        return {}
    data = _tmdb_get(f"find/{imdb_id}", {"external_source": "imdb_id"})
    for bucket, kind in (("movie_results", "movie"), ("tv_results", "tv")):
        arr = (data or {}).get(bucket) or []
        if arr:
            rid = arr[0].get("id")
            return {"media_type": kind, "tmdb_id": rid}
    return {}

# ---------- details / credits / keywords ----------
def get_details(kind: str, tmdb_id: int) -> Dict[str, Any]:
    kind = (kind or "").lower()
    if kind not in ("movie", "tv") or not tmdb_id:
        return {}
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}")
    out: Dict[str, Any] = {
        "original_language": (data.get("original_language") or "").lower(),
        "production_countries": [c.get("iso_3166_1") for c in (data.get("production_countries") or []) if isinstance(c, dict) and c.get("iso_3166_1")],
        "production_companies": [c.get("name") for c in (data.get("production_companies") or []) if isinstance(c, dict) and c.get("name")],
    }
    if kind == "movie":
        out["runtime"] = data.get("runtime")  # minutes
        out["belongs_to_collection"] = (data.get("belongs_to_collection") or {}).get("name")
    else:
        out["episode_run_time"] = data.get("episode_run_time") or []
        out["networks"] = [n.get("name") for n in (data.get("networks") or []) if isinstance(n, dict) and n.get("name")]
        out["number_of_seasons"] = data.get("number_of_seasons")
        out["number_of_episodes"] = data.get("number_of_episodes")
    return out

def get_credits(kind: str, tmdb_id: int) -> Dict[str, Any]:
    kind = (kind or "").lower()
    if kind not in ("movie", "tv") or not tmdb_id:
        return {}
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/credits")
    directors = []
    writers = []
    try:
        for c in (data.get("crew") or []):
            if not isinstance(c, dict): continue
            job = (c.get("job") or "").lower()
            name = c.get("name")
            if not name: continue
            if job == "director":
                directors.append(name)
            elif job in {"writer", "screenplay", "story"}:
                writers.append(name)
    except Exception:
        pass
    cast = []
    try:
        for c in (data.get("cast") or [])[:8]:
            if isinstance(c, dict) and c.get("name"):
                cast.append(c.get("name"))
    except Exception:
        pass
    return {"directors": directors, "writers": writers, "cast": cast}

def get_keywords(kind: str, tmdb_id: int) -> List[str]:
    kind = (kind or "").lower()
    if kind not in ("movie", "tv") or not tmdb_id:
        return []
    path = f"{kind}/{int(tmdb_id)}/keywords"
    data = _tmdb_get(path)
    raw = data.get("keywords") if kind == "movie" else data.get("results")
    kws = []
    for kw in raw or []:
        name = kw.get("name")
        if isinstance(name, str) and name.strip():
            kws.append(name.strip().lower())
    return list(dict.fromkeys(kws))

# ---------- per-title watch providers (subscription-only) ----------
def get_title_watch_providers(kind: str, tmdb_id: int, region: str = "US") -> List[str]:
    kind = (kind or "").lower()
    if kind not in ("movie", "tv") or not tmdb_id:
        return []
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/watch/providers")
    by_region = (data or {}).get("results", {}).get((region or "US").upper()) or {}
    slugs = set()
    for bucket in ("flatrate", "ads"):
        for offer in by_region.get(bucket, []) or []:
            slug = _slugify_provider_name(offer.get("provider_name", ""))
            if slug:
                slugs.add(slug)
    return sorted(slugs)