# engine/catalog.py
from __future__ import annotations

import os
import time
import math
import random
from typing import Dict, Any, List, Tuple
import requests

TMDB_API = "https://api.themoviedb.org/3"

# Map your provider slugs -> TMDB provider IDs
_PROVIDER_IDS = {
    "netflix": 8,
    "prime_video": 9,        # Amazon Prime Video
    "hulu": 15,
    "max": 384,              # HBO Max / Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

def _env_csv(name: str, default: str = "") -> List[str]:
    v = os.environ.get(name, default).strip()
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]

def _now_slot(minutes: int) -> int:
    # stable seed for each <minutes> window
    m = max(1, int(os.environ.get("ROTATE_MINUTES", minutes)))
    tmin = int(time.time() // 60)
    return (tmin // m) * m

def _rng_for_slot(slot: int, salt: str) -> random.Random:
    seed = f"{slot}:{salt}"
    h = hash(seed) & 0xFFFFFFFF
    return random.Random(h)

def _tmdb_request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = os.environ.get("TMDB_API_KEY")
    if not key:
        return {"page": 1, "total_pages": 1, "results": []}
    headers = {"Accept": "application/json"}
    params = dict(params or {})
    params["api_key"] = key
    # cache-bust parameter bound to rotation slot so we never reuse stale results
    params["cb"] = os.environ.get("TMDB_CB", str(_now_slot(int(os.environ.get("ROTATE_MINUTES", "15")))))
    try:
        r = requests.get(f"{TMDB_API}{path}", params=params, headers=headers, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {"page": 1, "total_pages": 1, "results": []}

def _build_discover_params(kind: str, page: int, provider_ids: List[int], region: str, langs: List[str]) -> Dict[str, Any]:
    # TMDB discover params shared across movie/tv
    params = {
        "watch_region": region or "US",
        "with_watch_monetization_types": "flatrate",
        "with_watch_providers": "|".join(str(p) for p in provider_ids) if provider_ids else None,
        "with_original_language": ",".join(langs) if langs else "en",
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "include_null_first_air_dates": "false" if kind == "tv" else None,
        "page": page,
    }
    # remove None values
    return {k: v for k, v in params.items() if v is not None}

def _total_pages_for(kind: str, params: Dict[str, Any]) -> int:
    path = "/discover/movie" if kind == "movie" else "/discover/tv"
    first = _tmdb_request(path, dict(params, page=1))
    total = int(first.get("total_pages") or 1)
    # TMDB caps at 500
    return max(1, min(total, 500))

def _choose_pages(total_pages: int, want_pages: int, rng: random.Random) -> List[int]:
    if total_pages <= 1:
        return [1]
    want = max(1, min(want_pages, total_pages))
    # pick a random start and odd step to cover the space without repeating quickly
    start = rng.randrange(1, total_pages + 1)
    # choose an odd step in [3..17] (bounded by total_pages)
    step_candidates = [s for s in range(3, 18, 2) if s < total_pages]
    if not step_candidates:
        step_candidates = [1]
    step = rng.choice(step_candidates)
    pages = []
    cur = start
    seen = set()
    while len(pages) < want:
        if cur not in seen:
            pages.append(cur)
            seen.add(cur)
        cur = ((cur - 1 + step) % total_pages) + 1
    return pages

def _coerce_year(datestr: str | None) -> int | None:
    if not datestr or len(datestr) < 4:
        return None
    try:
        return int(datestr[:4])
    except Exception:
        return None

def _collect(kind: str, page_nums: List[int], base_params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    path = "/discover/movie" if kind == "movie" else "/discover/tv"
    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for p in page_nums:
        data = _tmdb_request(path, dict(base_params, page=p))
        for r in data.get("results", []):
            tmdb_id = r.get("id")
            if not tmdb_id or (tmdb_id, kind) in seen_ids:
                continue
            title = r.get("title") if kind == "movie" else r.get("name")
            date = r.get("release_date") if kind == "movie" else r.get("first_air_date")
            year = _coerce_year(date)
            item = {
                "id": f"tmdb:{kind}:{tmdb_id}",
                "tmdb_id": tmdb_id,
                "type": kind,
                "title": title,
                "name": title,
                "year": year,
                "vote_average": float(r.get("vote_average") or 0.0),
                "vote_count": int(r.get("vote_count") or 0),
                "popularity": float(r.get("popularity") or 0.0),
                "genre_ids": list(r.get("genre_ids") or []),
                "origin_country": list(r.get("origin_country") or []),
                "original_language": r.get("original_language"),
            }
            seen_ids.add((tmdb_id, kind))
            items.append(item)
    return items, len(items)

def build_pool() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    region = os.environ.get("REGION", "US")
    langs = _env_csv("ORIGINAL_LANGS", "en")
    subs_include = _env_csv("SUBS_INCLUDE", "")
    provider_ids = [_PROVIDER_IDS[s] for s in subs_include if s in _PROVIDER_IDS]

    movie_pages = max(1, int(os.environ.get("TMDB_PAGES_MOVIE", "40")))
    tv_pages = max(1, int(os.environ.get("TMDB_PAGES_TV", "40")))
    rotate_minutes = max(1, int(os.environ.get("ROTATE_MINUTES", "15")))
    slot = _now_slot(rotate_minutes)

    # base params for discover
    base_movie = _build_discover_params("movie", 1, provider_ids, region, langs)
    base_tv = _build_discover_params("tv", 1, provider_ids, region, langs)

    # figure out total pages and pick page lists with a slot-tied RNG
    rng_movie = _rng_for_slot(slot, f"movie:{movie_pages}:{','.join(subs_include)}")
    rng_tv = _rng_for_slot(slot, f"tv:{tv_pages}:{','.join(subs_include)}")

    total_pages_movie = _total_pages_for("movie", base_movie)
    total_pages_tv = _total_pages_for("tv", base_tv)

    movie_page_list = _choose_pages(total_pages_movie, movie_pages, rng_movie)
    tv_page_list = _choose_pages(total_pages_tv, tv_pages, rng_tv)

    # collect
    movie_items, _ = _collect("movie", movie_page_list, base_movie)
    tv_items, _ = _collect("tv", tv_page_list, base_tv)

    pool = movie_items + tv_items

    meta = {
        "movie_pages": movie_pages,
        "tv_pages": tv_pages,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
        "total_pages_movie": total_pages_movie,
        "total_pages_tv": total_pages_tv,
        "movie_pages_used": movie_page_list,
        "tv_pages_used": tv_page_list,
        "provider_names": subs_include,
        "language": f"{region and 'en-US' or 'en-US'}",
        "with_original_language": ",".join(langs) if langs else "en",
        "watch_region": region or "US",
        "pool_counts": {
            "movie": len(movie_items),
            "tv": len(tv_items),
        },
        "total_pages": [total_pages_movie, total_pages_tv],
    }
    return pool, meta