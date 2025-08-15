# engine/catalog.py
from __future__ import annotations
import hashlib
import json
import os
import random
import re
import time
from typing import Dict, List, Optional, Tuple

import requests

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()
BASE = "https://api.themoviedb.org/3"
CACHE_DIR = os.environ.get("CACHE_DIR", "data/cache")
USER_PROVIDERS_JSON = os.path.join(CACHE_DIR, "user_providers.json")
PROVIDER_MAP_JSON = os.path.join(CACHE_DIR, "provider_map.json")

# ---- Language/region (accept both old & new env names)
LANG = os.environ.get("LANGUAGE", "en-US")
WATCH_REGION = os.environ.get("WATCH_REGION") or os.environ.get("REGION") or "US"

# We pin to English per your instruction, but accept ORIGINAL_LANGS/WITH_ORIGINAL_LANGUAGE if present.
_orig_lang_env = os.environ.get("WITH_ORIGINAL_LANGUAGE") or os.environ.get("ORIGINAL_LANGS") or "en"
WITH_ORIG_LANG = "en"  # force English only

# ---- Page counts & rotation (accept legacy envs; ensure "substantially increased")
def _int_env(*names: str, default: int) -> int:
    for n in names:
        v = os.environ.get(n)
        if v and str(v).strip().isdigit():
            try:
                return int(v)
            except Exception:
                pass
    return default

# Minimum 48 each unless you explicitly set higher
MOVIE_PAGES = max(48, _int_env("MOVIE_PAGES", "TMDB_PAGES_MOVIE", default=48))
TV_PAGES    = max(48, _int_env("TV_PAGES", "TMDB_PAGES_TV", default=48))
ROTATE_MIN  = _int_env("ROTATE_MINUTES", default=15)  # 15-minute rotation

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
    slot = int(time.time() // (ROTATE_MIN * 60))
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
    desired = max(1, min(desired, max(1, total_pages)))
    h = int(hashlib.blake2s(f"{salt}:{slot}:{label}".encode(), digest_size=8).hexdigest(), 16)
    start = (h % max(1, total_pages))
    step = (2 * (h % max(1, total_pages // 2)) + 1) or 1  # odd step
    pages, seen, cur = [], set(), start
    while len(pages) < desired:
        p = (cur % max(1, total_pages)) + 1
        if p not in seen:
            pages.append(p); seen.add(p)
        cur += step
    return pages

# ---- Provider name handling

_NONALNUM = re.compile(r"[^a-z0-9]+")

def _norm(s: str) -> str:
    return _NONALNUM.sub("", (s or "").lower())

def _load_user_provider_names() -> List[str]:
    """
    Your services only. Sources (in priority order):
      1) SUBS_INCLUDE (csv)
      2) MY_STREAMING_PROVIDERS (csv)
      3) /mnt/data/providers.json  (["Netflix","Max",..."])
      4) cached snapshot (data/cache/user_providers.json)
    """
    # 1) SUBS_INCLUDE (used in your workflow)
    env_subs = os.environ.get("SUBS_INCLUDE", "")
    if env_subs.strip():
        names = [x.strip() for x in env_subs.split(",") if x.strip()]
        _ensure_dirs()
        with open(USER_PROVIDERS_JSON, "w", encoding="utf-8") as f:
            json.dump(names, f, ensure_ascii=False, indent=2)
        return names

    # 2) MY_STREAMING_PROVIDERS
    env_my = os.environ.get("MY_STREAMING_PROVIDERS", "")
    if env_my.strip():
        names = [x.strip() for x in env_my.split(",") if x.strip()]
        _ensure_dirs()
        with open(USER_PROVIDERS_JSON, "w", encoding="utf-8") as f:
            json.dump(names, f, ensure_ascii=False, indent=2)
        return names

    # 3) file on disk
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

    # 4) cached
    if os.path.exists(USER_PROVIDERS_JSON):
        try:
            with open(USER_PROVIDERS_JSON, "r", encoding="utf-8") as f:
                arr = json.load(f)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass

    return []

def _provider_map(kind: str) -> Dict[str, int]:
    """
    Map normalized provider name -> id (for the given kind) in WATCH_REGION.
    Cached for both movie & tv.
    """
    _ensure_dirs()
    data = {}
    if os.path.exists(PROVIDER_MAP_JSON):
        try:
            with open(PROVIDER_MAP_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

    if kind not in data:
        data[kind] = {}

    url = f"{BASE}/watch/providers/{'movie' if kind=='movie' else 'tv'}"
    res = _get(url, {"watch_region": WATCH_REGION})
    mp: Dict[str, int] = {}
    for entry in res.get("results", []):
        name = (entry.get("provider_name") or "").strip()
        pid = entry.get("provider_id")
        if name and isinstance(pid, int):
            mp[_norm(name)] = pid
    data[kind] = mp

    with open(PROVIDER_MAP_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return mp

# fuzzy-ish synonyms
_SYNONYMS = {
    "amazon": "amazonprimevideo",
    "primevideo": "amazonprimevideo",
    "prime_video": "amazonprimevideo",
    "amazonprime": "amazonprimevideo",
    "hbomax": "max",
    "hbomax": "max",
    "disneyplus": "disneyplus",
    "disney+": "disneyplus",
    "apple tv+": "appletvplus",
    "apple_tv_plus": "appletvplus",
    "appletv+": "appletvplus",
    "paramountplus": "paramountplus",
    "paramount+": "paramountplus",
}

def _normalize_user_name(n: str) -> str:
    s = n.strip().lower().replace("_", " ").replace("+", " plus")
    s = re.sub(r"\s+", " ", s).strip()
    key = _norm(s)
    key = _SYNONYMS.get(key, key)
    return key

def _names_to_ids(user_names: List[str], kind: str) -> List[int]:
    mp = _provider_map(kind)  # normalized TMDB names -> id
    ids: List[int] = []
    keys = list(mp.keys())

    for raw in user_names:
        ukey = _normalize_user_name(raw)
        # exact match
        if ukey in mp:
            ids.append(mp[ukey])
            continue
        # substring or close match
        matched = None
        for k in keys:
            if ukey in k or k in ukey:
                matched = k
                break
        if matched:
            ids.append(mp[matched])

    # de-dup, preserve order
    out, seen = [], set()
    for i in ids:
        if i not in seen:
            out.append(i); seen.add(i)
    return out

# ---- Discover helpers

def _discover(kind: str, pages: List[int], provider_ids: List[int]) -> List[Dict]:
    items: List[Dict] = []
    with_prov = "|".join(str(i) for i in provider_ids) if provider_ids else None
    sorts = [
        "popularity.desc",
        "vote_count.desc",
        "primary_release_date.desc" if kind == "movie" else "first_air_date.desc",
    ]
    for i, pg in enumerate(pages):
        sort_by = sorts[i % len(sorts)]
        params = {
            "language": LANG,
            "with_original_language": WITH_ORIG_LANG,
            "page": pg,
            "sort_by": sort_by,
            "watch_region": WATCH_REGION,
            "with_watch_monetization_types": "flatrate,ads,free",
        }
        if with_prov:
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
        "with_watch_monetization_types": "flatrate,ads,free",
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)
    url = f"{BASE}/discover/{'movie' if kind=='movie' else 'tv'}"
    data = _get(url, params)
    return int(data.get("total_pages") or 1)

def _plan_pages() -> Dict[str, List[int]]:
    slot, salt = _slot_and_salt()

    # Your provider names → ids (honor only your list)
    names = _load_user_provider_names()
    movie_ids = _names_to_ids(names, "movie")
    tv_ids = _names_to_ids(names, "tv")

    # Bound permutations by the actual total pages for current filters
    try:
        total_m = _total_pages("movie", movie_ids)
    except Exception:
        total_m = 1
    try:
        total_t = _total_pages("tv", tv_ids)
    except Exception:
        total_t = 1

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
        "watch_region": WATCH_REGION,
    }
    return {"movie": pages_m, "tv": pages_t, "meta": meta}

def build_pool() -> List[Dict]:
    """
    Combined movie+tv honoring your providers & English-only constraints.
    Pages rotate every ROTATE_MIN minutes and are different across slots.
    """
    global _LAST_META
    _ensure_dirs()
    plan = _plan_pages()
    _LAST_META = plan["meta"]
    prov_ids_m = _LAST_META["provider_ids"]["movie"]
    prov_ids_t = _LAST_META["provider_ids"]["tv"]

    _hb(f"plan: movies={len(plan['movie'])} tv={len(plan['tv'])} slot={_LAST_META['slot']} "
        f"prov_m={len(prov_ids_m)} prov_t={len(prov_ids_t)}")

    movies = _discover("movie", plan["movie"], prov_ids_m)
    tv = _discover("tv", plan["tv"], prov_ids_t)

    pool = movies + tv
    _LAST_META["pool_counts"] = {"movie": len(movies), "tv": len(tv), "total": len(pool)}
    return pool

def last_meta() -> Dict:
    return dict(_LAST_META)