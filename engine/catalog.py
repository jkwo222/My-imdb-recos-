import os
import time
import math
import json
import hashlib
import datetime as dt
from typing import Dict, List, Tuple, Any
import requests

# ---- Provider ID map (TMDB watch providers; US region) ----
# Override with env PROVIDER_IDS, e.g.:
# "netflix:8,prime_video:119,hulu:15,max:384,disney_plus:337,apple_tv_plus:350,peacock:386,paramount_plus:531"
PROVIDER_ID_MAP_US = {
    "netflix": 8,
    "prime_video": 119,
    "hulu": 15,
    "max": 384,           # HBO Max/Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

def _now_utc() -> int:
    return int(time.time())

def _env_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v is not None and str(v).strip() != "" else default

def _env_int(key: str, default: int) -> int:
    try:
        return int(_env_str(key, str(default)))
    except Exception:
        return default

def _env_bool(key: str, default: bool) -> bool:
    v = _env_str(key, "")
    if v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _hash32(s: str) -> int:
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:8], 16)

def _dedupe(items: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for it in items:
        k = (it.get("media_type"), it.get("id"))
        if k not in seen:
            seen.add(k)
            out.append(it)
    return out

class TmdbClient:
    def __init__(self, api_key: str, language: str, watch_region: str, cb: str, throttle_ms: int = 0):
        self.api_key = api_key
        self.language = language
        self.watch_region = watch_region
        self.cb = cb
        self.base = "https://api.themoviedb.org/3"
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json"})
        self.throttle_ms = max(0, int(throttle_ms))
        self.request_count = 0

    def _params(self, extra: Dict[str, Any]) -> Dict[str, Any]:
        p = {
            "api_key": self.api_key,
            "language": self.language,
            "watch_region": self.watch_region,
            "include_adult": "false",
            "cb": self.cb,
        }
        p.update(extra or {})
        return p

    def get(self, path: str, params: Dict[str, Any]) -> dict:
        if self.throttle_ms:
            time.sleep(self.throttle_ms / 1000.0)
        url = f"{self.base}{path}"
        r = self.s.get(url, params=self._params(params), timeout=25)
        self.request_count += 1
        r.raise_for_status()
        return r.json()

def _resolve_provider_ids() -> Tuple[List[int], Dict[str, int], List[str]]:
    names = [x.strip().lower() for x in _env_str("SUBS_INCLUDE", "").split(",") if x.strip()]
    custom = _env_str("PROVIDER_IDS", "")
    id_map = PROVIDER_ID_MAP_US.copy()
    if custom:
        for token in custom.split(","):
            if ":" in token:
                nm, idstr = token.split(":", 1)
                nm = nm.strip().lower()
                try:
                    id_map[nm] = int(idstr.strip())
                except Exception:
                    pass
    ids, unknown = [], []
    for nm in names:
        pid = id_map.get(nm)
        if isinstance(pid, int):
            ids.append(pid)
        else:
            unknown.append(nm)
    return ids, id_map, unknown

def _choose_pages(total_pages: int, want_pages: int, seed: int) -> List[int]:
    total_pages = max(1, int(total_pages))
    want_pages = max(1, int(want_pages))
    if total_pages == 1:
        return [1]
    want = min(want_pages, total_pages)

    rng_seed = (seed ^ _hash32(f"tp={total_pages},wp={want_pages}")) & 0xFFFFFFFF
    odd_candidates = [x for x in range(1, total_pages) if x % 2 == 1]
    step = odd_candidates[_hash32(f"step:{rng_seed}") % len(odd_candidates)] if odd_candidates else 1
    start = (_hash32(f"start:{rng_seed}") % total_pages) + 1

    pages, used, cur = [], set(), start
    for _ in range(total_pages):
        if cur not in used:
            used.add(cur)
            pages.append(cur)
            if len(pages) >= want:
                break
        cur += step
        if cur > total_pages:
            cur -= total_pages
    return pages

def _rating_safe(val) -> float:
    try:
        return float(val or 0.0)
    except Exception:
        return 0.0

def _extract_item(kind: str, r: dict) -> dict:
    title = r.get("title") if kind == "movie" else r.get("name")
    date_key = "release_date" if kind == "movie" else "first_air_date"
    year = None
    d = r.get(date_key) or ""
    if len(d) >= 4:
        try:
            year = int(d[:4])
        except Exception:
            year = None
    return {
        "id": r.get("id"),
        "media_type": "movie" if kind == "movie" else "tvSeries",
        "title": title,
        "year": year,
        "popularity": _rating_safe(r.get("popularity")),
        "tmdb_vote_average": _rating_safe(r.get("vote_average")),
        "tmdb_vote_count": int(r.get("vote_count") or 0),
        "origin_language": r.get("original_language"),
    }

def _discover_all_pages(
    tmdb: TmdbClient,
    kind: str,
    prov_param: str,
    with_original_language: str,
    base_filters: Dict[str, Any],
    cap_pages: int,
) -> Tuple[List[dict], Dict[str, Any]]:
    """Fetch page=1..cap_pages (or total_pages), sequentially."""
    first_params = dict(
        sort_by="popularity.desc",
        page=1,
        with_original_language=with_original_language,
        with_watch_monetization_types="flatrate",
    )
    if prov_param:
        first_params["with_watch_providers"] = prov_param
    first_params.update(base_filters or {})

    first = tmdb.get(f"/discover/{kind}", first_params)
    total_pages = max(1, int(first.get("total_pages") or 1))
    cap = max(1, min(total_pages, int(cap_pages)))
    items = [_extract_item(kind, r) for r in first.get("results", [])]

    for p in range(2, cap + 1):
        params = dict(first_params)
        params["page"] = p
        try:
            data = tmdb.get(f"/discover/{kind}", params)
            items.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
        except Exception:
            continue

    meta = {
        "total_pages": total_pages,
        "pages_used": list(range(1, cap + 1)),
        "want_pages": cap,
        "fetch_all": True,
    }
    return _dedupe(items), meta

def _discover_sampled_pages(
    tmdb: TmdbClient,
    kind: str,
    prov_param: str,
    with_original_language: str,
    base_filters: Dict[str, Any],
    want_pages_env: int,
    rotate_minutes: int,
) -> Tuple[List[dict], Dict[str, Any]]:
    """Pick many pages (rotating) – bigger defaults for larger pools."""
    # Fetch first to learn total_pages
    first_params = dict(
        sort_by="popularity.desc",
        page=1,
        with_original_language=with_original_language,
        with_watch_monetization_types="flatrate",
    )
    if prov_param:
        first_params["with_watch_providers"] = prov_param
    first_params.update(base_filters or {})
    first = tmdb.get(f"/discover/{kind}", first_params)
    total_pages = max(1, int(first.get("total_pages") or 1))

    slot = math.floor(_now_utc() / (rotate_minutes * 60))
    seed_basis = f"{kind}|slot={slot}|cb={_env_str('TMDB_CB', str(os.getenv('GITHUB_RUN_NUMBER') or slot))}"
    seed = _hash32(seed_basis)

    want_env = max(1, int(want_pages_env))
    pages = _choose_pages(total_pages, want_env, seed)

    items = [_extract_item(kind, r) for r in first.get("results", [])]
    for p in pages:
        if p == 1:
            continue
        params = dict(first_params)
        params["page"] = p
        try:
            data = tmdb.get(f"/discover/{kind}", params)
            items.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
        except Exception:
            continue

    # Optional auto-expand if small
    min_pool = _env_int("MIN_POOL_PER_KIND", 1200)  # raise target per kind
    if len(items) < min_pool and total_pages > len(pages):
        extra_need = min(total_pages, len(pages) * 3)
        extra_pages = _choose_pages(total_pages, extra_need, seed ^ 0xA5A5A5A5)
        for p in extra_pages:
            if p in pages or p == 1:
                continue
            params = dict(first_params)
            params["page"] = p
            try:
                data = tmdb.get(f"/discover/{kind}", params)
                items.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
            except Exception:
                continue

    meta = {
        "total_pages": total_pages,
        "pages_used": sorted(set([1] + pages)),
        "want_pages": want_env,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
        "fetch_all": False,
    }
    return _dedupe(items), meta

def _first_of_month_utc() -> str:
    today = dt.datetime.utcnow().date()
    return today.replace(day=1).isoformat()

def _curated_feeds(
    tmdb: TmdbClient,
    provider_ids: List[int],
    with_original_language: str,
) -> List[dict]:
    """Trending + new-this-month + best-of (per provider) without swamping the pool."""
    out: List[dict] = []
    # Trending (global)
    for kind in ("movie", "tv"):
        for page in (1, 2, 3):
            try:
                tr = tmdb.get(f"/trending/{kind}/week", {"page": page})
                out.extend(_extract_item(kind, r) for r in tr.get("results", []) or [])
            except Exception:
                break

    # New to service this month
    date_gte = _first_of_month_utc()
    for pid in provider_ids:
        for kind in ("movie", "tv"):
            params = {
                "with_watch_providers": str(pid),
                "with_watch_monetization_types": "flatrate",
                "with_original_language": with_original_language,
                "sort_by": "popularity.desc",
                "page": 1,
            }
            if kind == "movie":
                params["primary_release_date.gte"] = date_gte
            else:
                params["first_air_date.gte"] = date_gte
            try:
                data = tmdb.get(f"/discover/{kind}", params)
                out.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
            except Exception:
                continue

    # Best-of on provider
    for pid in provider_ids:
        for kind in ("movie", "tv"):
            params = {
                "with_watch_providers": str(pid),
                "with_watch_monetization_types": "flatrate",
                "with_original_language": with_original_language,
                "sort_by": "vote_average.desc",
                "vote_count.gte": 1000,
                "page": 1,
            }
            try:
                data = tmdb.get(f"/discover/{kind}", params)
                out.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
            except Exception:
                continue

    # Cap curated so main discover dominates
    by_type = {"movie": [], "tvSeries": []}
    for it in out:
        by_type[it["media_type"]].append(it)
    curated = []
    for k in by_type:
        cur = sorted(by_type[k], key=lambda x: (-x["tmdb_vote_count"], -x["popularity"]))[:400]
        curated.extend(cur)
    return _dedupe(curated)

def _load_seen_csv(path: str) -> Dict[str, int]:
    seen = {}
    if not path or not os.path.exists(path):
        return seen
    try:
        import csv
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                t = (row.get("const") or row.get("tconst") or "").strip()
                if t:
                    seen[f"imdb:{t}"] = 1
                nm = (row.get("Title") or row.get("title") or "").strip().lower()
                yr = (row.get("Year") or row.get("year") or "").strip()
                if nm:
                    seen[f"title:{nm}|{yr}"] = 1
    except Exception:
        pass
    return seen

def _discover_kind(
    tmdb: TmdbClient,
    kind: str,
    provider_ids: List[int],
    with_original_language: str,
    base_filters: Dict[str, Any],
) -> Tuple[List[dict], Dict[str, Any]]:
    prov_param = "|".join(str(p) for p in provider_ids) if provider_ids else None
    rotate_minutes = _env_int("ROTATE_MINUTES", 15)

    # Big defaults; can be overridden
    want_pages_env = _env_int("TMDB_PAGES_MOVIE" if kind == "movie" else "TMDB_PAGES_TV", 120)
    fetch_all = _env_bool("TMDB_FETCH_ALL_MOVIE" if kind == "movie" else "TMDB_FETCH_ALL_TV", False)
    cap_pages = _env_int("TMDB_MAX_PAGES_MOVIE" if kind == "movie" else "TMDB_MAX_PAGES_TV", 1000)

    if fetch_all:
        items, meta = _discover_all_pages(
            tmdb, kind, prov_param, with_original_language, base_filters, cap_pages
        )
    else:
        items, meta = _discover_sampled_pages(
            tmdb, kind, prov_param, with_original_language, base_filters, want_pages_env, rotate_minutes
        )
    return items, meta

def build_pool() -> Tuple[List[dict], Dict[str, Any]]:
    api_key = _env_str("TMDB_API_KEY", "")
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required")

    language = _env_str("LANGUAGE", _env_str("LANG", "en-US"))
    watch_region = _env_str("REGION", "US")
    with_original_language = _env_str("ORIGINAL_LANGS", "en").split(",")[0].strip() or "en"
    cb = _env_str("TMDB_CB", str(os.getenv("GITHUB_RUN_NUMBER") or math.floor(_now_utc() / (15*60))))
    throttle_ms = _env_int("TMDB_THROTTLE_MS", 0)

    provider_ids, id_map, unknown = _resolve_provider_ids()
    if unknown:
        print(f"[hb] WARN: unknown providers={unknown} (using known IDs only)")
    if not provider_ids:
        print("[hb] WARN: no provider IDs resolved — queries will NOT filter by provider.")

    tmdb = TmdbClient(api_key, language, watch_region, cb, throttle_ms=throttle_ms)
    base_filters: Dict[str, Any] = {}

    # Core discover (movie + tv)
    movie_items, m_meta = _discover_kind(
        tmdb, "movie", provider_ids, with_original_language, base_filters
    )
    tv_items, t_meta = _discover_kind(
        tmdb, "tv", provider_ids, with_original_language, base_filters
    )

    # Curated extras
    curated = _curated_feeds(tmdb, provider_ids, with_original_language)

    pool = _dedupe(movie_items + tv_items + curated)

    page_plan = {
        "movie_pages": m_meta.get("want_pages"),
        "tv_pages": t_meta.get("want_pages"),
        "rotate_minutes": _env_int("ROTATE_MINUTES", 15),
        "slot": m_meta.get("slot", None),
        "total_pages_movie": m_meta.get("total_pages", 1),
        "total_pages_tv": t_meta.get("total_pages", 1),
        "movie_pages_used": m_meta.get("pages_used", [1]),
        "tv_pages_used": t_meta.get("pages_used", [1]),
        "provider_names": [x.strip() for x in _env_str("SUBS_INCLUDE", "").split(",") if x.strip()],
        "language": language,
        "with_original_language": with_original_language,
        "watch_region": watch_region,
        "pool_counts": {
            "movie": sum(1 for x in pool if x["media_type"] == "movie"),
            "tv": sum(1 for x in pool if x["media_type"] == "tvSeries"),
        },
        "total_pages": [m_meta.get("total_pages", 1), t_meta.get("total_pages", 1)],
        "fetch_all_movie": bool(m_meta.get("fetch_all", False)),
        "fetch_all_tv": bool(t_meta.get("fetch_all", False)),
        "requests_made": tmdb.request_count,
        "throttle_ms": throttle_ms,
    }

    meta = {
        "page_plan": page_plan,
        "provider_id_map": id_map,
        "provider_ids_used": provider_ids,
        "unknown_providers": unknown,
    }
    return pool, meta