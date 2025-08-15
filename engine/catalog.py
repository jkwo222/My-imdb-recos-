# engine/catalog.py
from __future__ import annotations
import hashlib
import json
import math
import os
import random
import time
from typing import Dict, List, Optional, Tuple

import requests

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()
BASE = "https://api.themoviedb.org/3"
CACHE_DIR = os.environ.get("CACHE_DIR", "data/cache")
USER_PROVIDERS_JSON = os.path.join(CACHE_DIR, "user_providers.json")
PROVIDER_MAP_JSON = os.path.join(CACHE_DIR, "provider_map.json")

# language/region defaults (English only)
LANG = os.environ.get("LANGUAGE", "en-US")
WITH_ORIG_LANG = os.environ.get("WITH_ORIGINAL_LANGUAGE", "en")
WATCH_REGION = os.environ.get("WATCH_REGION", "US")

# page counts & rotation
MOVIE_PAGES = int(os.environ.get("MOVIE_PAGES", "48"))
TV_PAGES = int(os.environ.get("TV_PAGES", "48"))
ROTATE_MIN = int(os.environ.get("ROTATE_MINUTES", "15"))  # per your spec

_LAST_META: Dict = {}

def _ensure_dirs():
    os.makedirs(CACHE_DIR, exist_ok=True)

def _hb(msg: str):
    print(f"[cat] {msg}", flush=True)

def _get(url: str, params: Dict) -> Dict:
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY is not set")
    headers = {"Accept": "application/json"}
    params = dict(params or {})
    params["api_key"] = TMDB_API_KEY
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()

def _slot_and_salt() -> Tuple[int, int]:
    # 15-min rotation slot (configurable)
    slot = int(time.time() // (ROTATE_MIN * 60))
    # persistent salt for this environment
    _ensure_dirs()
    salt_path = os.path.join(CACHE_DIR, "tmdb_page_salt.txt")
    if os.path.exists(salt_path):
        with open(salt_path, "r") as f:
            salt = int(f.read().strip() or "0")
    else:
        salt = random.randint(10_000, 9_999_999)
        with open(salt_path, "w") as f:
            f.write(str(salt))
    return slot, salt

def _permute(total_pages: int, desired: int, slot: int, salt: int, label: str) -> List[int]:
    """
    Choose 'desired' distinct pages in [1..total_pages], rotating each slot.
    We use a coprime step and a hash-mixed start so pages change frequently.
    """
    desired = max(1, min(desired, total_pages))
    # derive a pseudo-random start and step from salt+label
    h = int(hashlib.blake2s(f"{salt}:{slot}:{label}".encode(), digest_size=8).hexdigest(), 16)
    start = (h % total_pages)
    # pick an odd step coprime-ish to total_pages
    step = (2 * (h % (total_pages // 2)) + 1) or 1
    pages = []
    seen = set()
    cur = start
    while len(pages) < desired:
        p = (cur % total_pages) + 1
        if p not in seen:
            pages.append(p)
            seen.add(p)
        cur += step
    return pages

def _load_user_provider_names() -> List[str]:
    """
    Your services only. Sources (in order):
      1) ENV MY_STREAMING_PROVIDERS (CSV of names)
      2) /mnt/data/providers.json  (["Netflix","Max",..."])
      3) cached snapshot data/cache/user_providers.json
    """
    # 1) env
    envv = os.environ.get("MY_STREAMING_PROVIDERS", "").strip()
    if envv:
        names = [x.strip() for x in envv.split(",") if x.strip()]
        _ensure_dirs()
        with open(USER_PROVIDERS_JSON, "w", encoding="utf-8") as f:
            json.dump(names, f, ensure_ascii=False, indent=2)
        return names

    # 2) file
    try:
        path = "/mnt/data/providers.json"
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f)
            if isinstance(arr, list):
                names = [str(x).strip() for x in arr if str(x).strip()]
                _ensure_dirs()
                with open(USER_PROVIDERS_JSON, "w", encoding="utf-8") as f:
                    json.dump(names, f, ensure_ascii=False, indent=2)
                return names
    except Exception:
        pass

    # 3) cached
    if os.path.exists(USER_PROVIDERS_JSON):
        try:
            with open(USER_PROVIDERS_JSON, "r", encoding="utf-8") as f:
                arr = json.load(f)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass

    # Last resort: no names (we will NOT fail—pool would be too fragile)
    return []

def _provider_map(kind: str) -> Dict[str, int]:
    """
    Map provider names -> IDs for a given kind ("movie" or "tv") in WATCH_REGION.
    Cached to avoid spamming TMDB.
    """
    _ensure_dirs()
    cache_key = f"{PROVIDER_MAP_JSON}"
    data = {}
    if os.path.exists(cache_key):
        try:
            with open(cache_key, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    if kind in (data.get("movie") or {}) and kind in (data.get("tv") or {}):
        pass

    if kind not in data:
        data[kind] = {}

    url = f"{BASE}/watch/providers/{'movie' if kind=='movie' else 'tv'}"
    res = _get(url, {"watch_region": WATCH_REGION})
    mp: Dict[str, int] = {}
    for entry in res.get("results", []):
        name = (entry.get("provider_name") or "").strip()
        pid = entry.get("provider_id")
        if name and isinstance(pid, int):
            mp[name.lower()] = pid
    data[kind] = mp
    with open(cache_key, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return mp

def _names_to_ids(names: List[str], kind: str) -> List[int]:
    mp = _provider_map(kind)
    ids = []
    for n in names:
        k = n.strip().lower()
        if k in mp:
            ids.append(mp[k])
    # de-dup preserve order
    out, seen = [], set()
    for x in ids:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

def _discover(kind: str, pages: List[int], provider_ids: List[int]) -> List[Dict]:
    """
    Fetch discover results for kind in those pages. English only.
    """
    items: List[Dict] = []
    with_prov = "|".join(str(i) for i in provider_ids) if provider_ids else None
    sorts = ["popularity.desc", "vote_count.desc", "primary_release_date.desc" if kind=="movie" else "first_air_date.desc"]
    for i, pg in enumerate(pages):
        sort_by = sorts[i % len(sorts)]
        params = {
            "language": LANG,
            "with_original_language": WITH_ORIG_LANG,
            "page": pg,
            "sort_by": sort_by,
            "watch_region": WATCH_REGION,
            "with_watch_monetization_types": "flatrate,ads,free"
        }
        if with_prov := with_prov:
            params["with_watch_providers"] = with_prov
        url = f"{BASE}/discover/{'movie' if kind=='movie' else 'tv'}"
        data = _get(url, params)
        for r in data.get("results", []):
            if kind == "movie":
                items.append({
                    "type": "movie",
                    "id": r.get("id"),
                    "tmdb_id": r.get("id"),
                    "title": r.get("title") or r.get("original_title"),
                    "original_title": r.get("original_title"),
                    "release_date": r.get("release_date"),
                    "year": (int(r["release_date"][:4]) if r.get("release_date", "")[:4].isdigit() else None),
                    "vote_average": r.get("vote_average"),
                    "vote_count": r.get("vote_count"),
                })
            else:
                items.append({
                    "type": "tvSeries",
                    "id": r.get("id"),
                    "tmdb_id": r.get("id"),
                    "name": r.get("name") or r.get("original_name"),
                    "original_name": r.get("original_name"),
                    "first_air_date": r.get("first_air_date"),
                    "year": (int(r["first_air_date"][:4]) if r.get("first_air_date", "")[:4].isdigit() else None),
                    "vote_average": r.get("vote_average"),
                    "vote_count": r.get("vote_count"),
                })
    return items

def _total_pages(kind: str, provider_ids: List[int]) -> int:
    params = {
        "language": LANG,
        "with_original_language": WITH_ORIG_LANG,
        "page": 1,
        "watch_region": WATCH_REGION,
        "with_watch_monetization_types": "flatrate,ads,free"
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)
    url = f"{BASE}/discover/{'movie' if kind=='movie' else 'tv'}"
    data = _get(url, params)
    return int(data.get("total_pages") or 1)

def _plan_pages() -> Dict[str, List[int]]:
    slot, salt = _slot_and_salt()
    # Provider names -> IDs
    names = _load_user_provider_names()
    movie_ids = _names_to_ids(names, "movie")
    tv_ids = _names_to_ids(names, "tv")

    # Get total pages once to bound the permutation
    try:
        total_m = _total_pages("movie", movie_ids)
    except Exception:
        total_m = 1
    try:
        total_t = _total_pages("tv", tv_ids)
    except Exception:
        total_t = 1

    # Plan rotating pages
    pages_m = _permute(total_m, MOVIE_PAGES, slot, salt, "movie")
    pages_t = _permute(total_t, TV_PAGES, slot, salt, "tv")

    meta = {
        "movie_pages": len(pages_m),
        "tv_pages": len(pages_t),
        "rotate_minutes": ROTATE_MIN,
        "slot": slot,
        "total_pages": {"movie": total_m, "tv": total_t},
        "provider_names": names,
        "provider_ids": {"movie": movie_ids, "tv": tv_ids},
        "language": LANG,
        "with_original_language": WITH_ORIG_LANG,
        "watch_region": WATCH_REGION
    }
    return {"movie": pages_m, "tv": pages_t, "meta": meta}

def build_pool() -> List[Dict]:
    """
    Returns a combined list of movie+tv dicts honoring your provider & language constraints.
    Rotation ensures pages change every ROTATE_MIN minutes.
    """
    global _LAST_META
    _ensure_dirs()
    plan = _plan_pages()
    _LAST_META = plan["meta"]
    prov_ids_m = plan["meta"]["provider_ids"]["movie"]
    prov_ids_t = plan["meta"]["provider_ids"]["tv"]

    _hb(f"plan: movies={len(plan['movie'])} tv={len(plan['tv'])} slot={_LAST_META['slot']} "
        f"prov_m={len(prov_ids_m)} prov_t={len(prov_ids_t)}")

    movies = _discover("movie", plan["movie"], prov_ids_m)
    tv = _discover("tv", plan["tv"], prov_ids_t)

    pool = movies + tv
    _LAST_META["pool_counts"] = {"movie": len(movies), "tv": len(tv), "total": len(pool)}
    return pool

def last_meta() -> Dict:
    return dict(_LAST_META)