# engine/catalog.py
from __future__ import annotations
import hashlib, math, os, random, time
from typing import Dict, List, Tuple, Any
import requests

TMDB_BASE = "https://api.themoviedb.org/3"
API_KEY = os.environ.get("TMDB_API_KEY")
REGION = os.environ.get("REGION", "US")
LANG = os.environ.get("LANGUAGE", "en-US")
ORIG_LANGS = os.environ.get("ORIGINAL_LANGS", "en")
INCLUDE_ADULT = os.environ.get("INCLUDE_ADULT", "false").lower() == "true"
INCLUDE_SEASONS = os.environ.get("INCLUDE_TV_SEASONS", "false").lower() == "true"

# Defaults increased; env can override.
MOVIE_PAGES = int(os.environ.get("TMDB_PAGES_MOVIE", "24"))
TV_PAGES    = int(os.environ.get("TMDB_PAGES_TV", "24"))

ROTATE_MIN  = int(os.environ.get("ROTATE_MINUTES", os.environ.get("ROTATE_EVERY_MINUTES", "15")))
MAX_CATALOG = int(os.environ.get("MAX_CATALOG", "6000"))

# Your services only (US provider IDs)
PROVIDER_MAP = {
    "netflix": 8,
    "prime_video": 9,      # Amazon Prime Video
    "hulu": 15,
    "max": 384,            # Max (HBO Max)
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}
SUBS_INCLUDE = os.environ.get("SUBS_INCLUDE", "")
PROVIDER_SLUGS = [s.strip() for s in SUBS_INCLUDE.split(",") if s.strip()]
PROVIDER_IDS = [PROVIDER_MAP[s] for s in PROVIDER_SLUGS if s in PROVIDER_MAP]
PROVIDER_NAMES = PROVIDER_SLUGS[:]  # echo back

def _q(params: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(params)
    params["api_key"] = API_KEY
    return params

def _discover(kind: str, page: int) -> Dict[str, Any]:
    assert kind in ("movie", "tv")
    url = f"{TMDB_BASE}/discover/{kind}"
    q = {
        "language": LANG,
        "watch_region": REGION,
        "with_original_language": ORIG_LANGS,
        "include_adult": str(INCLUDE_ADULT).lower(),
        "sort_by": "popularity.desc",
        "page": page,
    }
    if PROVIDER_IDS:
        q["with_watch_providers"] = ",".join(str(x) for x in PROVIDER_IDS)
        q["with_watch_monetization_types"] = "flatrate"
    r = requests.get(url, params=_q(q), timeout=15)
    r.raise_for_status()
    return r.json()

def _safe_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return int(str(date_str)[:4])
    except:
        return None

def _rot_seed() -> str:
    # 15-min “slot” + per-run jitter so pages differ frequently
    slot = int(time.time() // (ROTATE_MIN * 60) if ROTATE_MIN > 0 else time.time())
    jitter = os.environ.get("GITHUB_RUN_NUMBER") or os.environ.get("GITHUB_RUN_ID")
    if not jitter:
        jitter = hashlib.md5(os.urandom(16)).hexdigest()[:8]
    return f"{slot}:{jitter}:{','.join(PROVIDER_SLUGS)}:{ORIG_LANGS}:{REGION}"

def _choose_pages(kind: str, want_pages: int) -> Tuple[List[int], int]:
    """
    Probe TMDB for total pages; then select a rotating, per-run subset.
    Safe for tiny totals (1, 2, …).
    """
    probe = _discover(kind, 1)
    total_pages = max(1, int(probe.get("total_pages") or 1))
    total_pages = min(total_pages, 500)  # TMDB hard cap
    want = max(1, min(want_pages, total_pages))

    # If there’s only one page, return it.
    if total_pages == 1:
        return [1], 1

    seed = int(hashlib.sha256((kind + _rot_seed()).encode()).hexdigest(), 16)
    rng = random.Random(seed)

    start = rng.randrange(1, total_pages + 1)

    # Choose a step that is coprime with total_pages to walk the space.
    # When total_pages == 2, the only valid step is 1.
    step = 1 if total_pages == 2 else rng.randrange(1, total_pages)
    # ensure gcd(step, total_pages) == 1
    tries = 0
    while math.gcd(step, total_pages) != 1:
        step = rng.randrange(1, total_pages)
        tries += 1
        if tries > 32:  # extremely defensive fallback
            step = 1
            break

    pages: List[int] = []
    seen = set()
    curr = start
    while len(pages) < want:
        if curr not in seen:
            pages.append(curr)
            seen.add(curr)
        curr = ((curr + step - 1) % total_pages) + 1
    return pages, total_pages

def _shape_item(kind: str, x: Dict[str, Any]) -> Dict[str, Any]:
    if kind == "movie":
        title = x.get("title") or x.get("original_title")
        year = _safe_year(x.get("release_date"))
        typ = "movie"
    else:
        title = x.get("name") or x.get("original_name")
        year = _safe_year(x.get("first_air_date"))
        typ = "tvSeries"
    return {
        "type": typ,
        "tmdb_id": x.get("id"),
        "title": title,
        "name": title,
        "year": year,
        "tmdb_vote": float(x.get("vote_average") or 0.0),   # 0–10
        "tmdb_votes": int(x.get("vote_count") or 0),
        "pop": float(x.get("popularity") or 0.0),
    }

def _collect_kind(kind: str, want_pages: int, budget_left: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    pages, total_pages = _choose_pages(kind, want_pages)
    out: List[Dict[str, Any]] = []
    for p in pages:
        if budget_left <= 0:
            break
        data = _discover(kind, p)
        for x in data.get("results", []):
            out.append(_shape_item(kind, x))
            budget_left -= 1
            if budget_left <= 0:
                break
    meta = {
        "kind": kind, "pages": pages, "total_pages": total_pages,
        "used": len(out)
    }
    return out, meta

def build_pool() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not API_KEY:
        raise RuntimeError("TMDB_API_KEY missing")
    movie_pages = MOVIE_PAGES
    tv_pages = TV_PAGES
    budget = MAX_CATALOG

    movie_items, m_meta = _collect_kind("movie", movie_pages, budget_left=budget)
    budget -= len(movie_items)
    tv_items, t_meta = _collect_kind("tv", tv_pages, budget_left=budget)

    pool = movie_items + tv_items
    meta = {
        "movie_pages": movie_pages,
        "tv_pages": tv_pages,
        "rotate_minutes": ROTATE_MIN,
        "slot": int(time.time() // (ROTATE_MIN * 60) if ROTATE_MIN > 0 else time.time()),
        "total_pages_movie": m_meta["total_pages"],
        "total_pages_tv": t_meta["total_pages"],
        "movie_pages_used": m_meta["pages"],
        "tv_pages_used": t_meta["pages"],
        "provider_names": PROVIDER_NAMES,
        "language": LANG,
        "with_original_language": ORIG_LANGS,
        "watch_region": REGION,
        "pool_counts": {
            "movie": len(movie_items),
            "tv": len(tv_items),
        },
        "total_pages": (m_meta["total_pages"], t_meta["total_pages"]),
    }
    return pool, meta

def last_meta() -> Dict[str, Any]:
    return {}