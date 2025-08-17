# engine/tmdb.py
from __future__ import annotations
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

TMDB_BASE = "https://api.themoviedb.org/3"
CACHE_DIR = Path("data/cache/tmdb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

UA = {"User-Agent": "RecoEngine/2.14 (+github actions)"}


# ---------- auth / http helpers ----------

def _auth_headers_and_params() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (headers, params) for TMDB auth. Prefers v4 bearer token if present.
    - If TMDB_BEARER is set: uses Authorization: Bearer <token>
    - Else requires TMDB_API_KEY param (?api_key=...)
    """
    bearer = os.getenv("TMDB_BEARER", "").strip()
    if bearer:
        headers = {"Authorization": f"Bearer {bearer}", **UA}
        params: Dict[str, str] = {}
        return headers, params

    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("TMDB_API_KEY or TMDB_BEARER is required")
    headers = dict(UA)
    params = {"api_key": api_key}
    return headers, params


def _cache_key(path: str, params: Dict[str, Any]) -> str:
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    raw = f"{path}?{items}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _cache_path(group: str, key: str) -> Path:
    g = CACHE_DIR / group
    g.mkdir(parents=True, exist_ok=True)
    return g / f"{key}.json"


def _http_get_json(path: str, params: Dict[str, Any], group: Optional[str] = None,
                   ttl_min: int = 60) -> Dict[str, Any]:
    headers, base_params = _auth_headers_and_params()
    full_params = {**base_params, **params}
    key = _cache_key(path, full_params)
    if group:
        cp = _cache_path(group, key)
        if cp.exists():
            try:
                st = cp.stat()
                age_min = (time.time() - st.st_mtime) / 60.0
                if age_min <= ttl_min:
                    with cp.open("r", encoding="utf-8") as f:
                        return json.load(f)
            except Exception:
                pass

    url = f"{TMDB_BASE}{path}"
    backoff = 0.8
    last_err: Optional[Dict[str, Any]] = None
    for attempt in range(5):
        try:
            r = requests.get(url, params=full_params, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json()
                if group:
                    try:
                        with _cache_path(group, key).open("w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False)
                    except Exception:
                        pass
                return data
            else:
                last_err = {"status_code": r.status_code, "body": r.text}
        except Exception as e:
            last_err = {"exception": repr(e)}
        time.sleep(backoff)
        backoff *= 1.7
    return {"__error__": last_err or {"error": "unknown"}}


# Exposed for tmdb_detail.py
def _get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    return _http_get_json(path, params, group="raw", ttl_min=60)


# ---------- provider helpers ----------

def _provider_slug(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "_")


def _providers_catalog(kind: str, region: str, ttl_min: int = 8 * 60) -> List[Dict[str, Any]]:
    path = f"/watch/providers/{'movie' if kind=='movie' else 'tv'}"
    data = _http_get_json(path, {"watch_region": region}, group="providers", ttl_min=ttl_min)
    return data.get("results") or []


def providers_from_env(subs: List[str], region: str) -> List[int]:
    subs_norm = {_provider_slug(s) for s in (subs or []) if s}
    if not subs_norm:
        return []
    movie_provs = _providers_catalog("movie", region)
    tv_provs = _providers_catalog("tv", region)
    id_by_slug: Dict[str, int] = {}
    for entry in movie_provs + tv_provs:
        nm = _provider_slug(entry.get("provider_name"))
        pid = int(entry.get("provider_id") or 0)
        if nm and pid:
            id_by_slug[nm] = pid

    out: List[int] = []
    seen = set()
    for s in subs_norm:
        pid = id_by_slug.get(s)
        if pid and pid not in seen:
            out.append(pid)
            seen.add(pid)
    return out


# ---------- discovery ----------

def _normalize_items(kind: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for r in results or []:
        tmdb_id = int(r.get("id") or 0)
        if not tmdb_id:
            continue
        title = r.get("title") if kind == "movie" else r.get("name")
        date = r.get("release_date") if kind == "movie" else r.get("first_air_date")
        year = int((date or "0000")[:4]) if date else None
        genres = r.get("genre_ids") or []
        vote = r.get("vote_average") or 0.0
        items.append({
            "media_type": kind,
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "genres": genres,
            "tmdb_vote": vote,
        })
    return items


def _discover(kind: str, page: int, region: str, langs: List[str], provider_ids: List[int],
              slot: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    with_providers = "|".join(str(x) for x in provider_ids) if provider_ids else None
    with_langs = "|".join(langs) if langs else None
    params: Dict[str, Any] = {
        "page": page,
        "include_adult": "false",
        "sort_by": "popularity.desc",
        "watch_region": region,
    }
    if with_providers:
        params["with_watch_providers"] = with_providers
        params["with_watch_monetization_types"] = "flatrate|free|ads|rent|buy"
    if with_langs:
        params["with_original_language"] = with_langs
    params["cb"] = slot  # small cache-buster partitioning

    data = _http_get_json(f"/discover/{'movie' if kind=='movie' else 'tv'}",
                          params, group=f"discover_{kind}", ttl_min=30)
    results = data.get("results") or []
    items = _normalize_items(kind, results)
    diag = {
        "page": int(page),
        "total_pages": int(data.get("total_pages") or 1),
        "total_results": int(data.get("total_results") or 0),
        "returned": len(items),
    }
    return items, diag


def discover_movie_page(page: int, region: str, langs: List[str],
                        provider_ids: List[int], slot: int = 0
                        ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _discover("movie", page, region, langs, provider_ids, slot)


def discover_tv_page(page: int, region: str, langs: List[str],
                     provider_ids: List[int], slot: int = 0
                     ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _discover("tv", page, region, langs, provider_ids, slot)


# ---------- detail / providers ----------

def _merge_watch_providers(detail: Dict[str, Any], region: str) -> Dict[str, Any]:
    results = (detail.get("watch/providers", {}) or {}).get("results", {})
    region_blob = results.get(region, {}) if isinstance(results, dict) else {}
    providers = set()
    for bucket in ("flatrate", "ads", "free", "rent", "buy"):
        for p in region_blob.get(bucket, []) or []:
            nm = _provider_slug(p.get("provider_name"))
            if nm:
                providers.add(nm)
    return {"providers": sorted(providers)}


def get_title_watch_providers(kind: str, tmdb_id: int, region: str) -> List[str]:
    k = "movie" if kind == "movie" else "tv"
    data = _http_get_json(f"/{k}/{int(tmdb_id)}/watch/providers", {}, group="title_providers", ttl_min=180)
    results = (data.get("results") or {}).get(region, {})
    out = set()
    for bucket in ("flatrate", "ads", "free", "rent", "buy"):
        for p in results.get(bucket, []) or []:
            nm = _provider_slug(p.get("provider_name"))
            if nm:
                out.add(nm)
    return sorted(out)