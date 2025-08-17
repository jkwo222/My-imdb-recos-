# engine/tmdb_detail.py
from __future__ import annotations
from typing import Dict, Any, Optional, List
from pathlib import Path
import datetime as dt

from .tmdb import _get_json, TMDB_BASE

def _safe_year(datestr: str) -> Optional[int]:
    if not datestr:
        return None
    try:
        return int(datestr[:4])
    except Exception:
        return None

def _merge_watch_providers(d: Dict[str, Any], region: str) -> Dict[str, Any]:
    wp = d.get("watch/providers", {})
    res = wp.get("results", {})
    reg = res.get(region, {}) if isinstance(res, dict) else {}
    # Compose a simple list of names/types present
    avail: List[str] = []
    for k in ("flatrate", "ads", "free"):
        for entry in reg.get(k, []) or []:
            name = entry.get("provider_name")
            if name:
                avail.append(name)
    return {"watch_available": sorted(set(avail))}

def get_movie_details(tmdb_id: int, region: str) -> Dict[str, Any]:
    url = f"{TMDB_BASE}/movie/{tmdb_id}"
    params = {
        "append_to_response": "external_ids,watch/providers,release_dates,credits",
        "language": "en-US",
    }
    d = _get_json("detail_movie", url, params, ttl_seconds=24*3600)
    imdb_id = (d.get("external_ids") or {}).get("imdb_id")
    year = _safe_year(d.get("release_date") or "")
    genres = [g.get("name") for g in (d.get("genres") or []) if g.get("name")]
    rating = d.get("vote_average")
    title = d.get("title") or d.get("original_title")
    info = {
        "media_type": "movie",
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "title": title,
        "year": year,
        "genres": genres,
        "tmdb_vote": rating,
        "runtime": d.get("runtime"),
        "overview": d.get("overview"),
        "directors": [c["name"] for c in (d.get("credits", {}).get("crew") or []) if c.get("job") == "Director"],
    }
    info.update(_merge_watch_providers(d, region))
    return info

def get_tv_details(tmdb_id: int, region: str) -> Dict[str, Any]:
    url = f"{TMDB_BASE}/tv/{tmdb_id}"
    params = {
        "append_to_response": "external_ids,watch/providers,content_ratings,aggregate_credits",
        "language": "en-US",
    }
    d = _get_json("detail_tv", url, params, ttl_seconds=24*3600)
    imdb_id = (d.get("external_ids") or {}).get("imdb_id")
    year = _safe_year(d.get("first_air_date") or "")
    genres = [g.get("name") for g in (d.get("genres") or []) if g.get("name")]
    rating = d.get("vote_average")
    title = d.get("name") or d.get("original_name")
    # Directors don't map 1:1 for series; list main directors if present in agg credits
    directors = []
    for x in (d.get("aggregate_credits", {}).get("crew") or []):
        if x.get("job") == "Director" or "Director" in (x.get("jobs") or []):
            nm = x.get("name")
            if nm:
                directors.append(nm)
    info = {
        "media_type": "tv",
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "title": title,
        "year": year,
        "genres": genres,
        "tmdb_vote": rating,
        "overview": d.get("overview"),
        "directors": sorted(set(directors)),
    }
    info.update(_merge_watch_providers(d, region))
    return info

def enrich_item(item: Dict[str, Any], region: str) -> Dict[str, Any]:
    if item.get("media_type") == "movie":
        return get_movie_details(int(item["tmdb_id"]), region)
    else:
        return get_tv_details(int(item["tmdb_id"]), region)