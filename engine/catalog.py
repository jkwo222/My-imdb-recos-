import os
import time
import math
import json
import hashlib
import datetime as dt
from typing import Dict, List, Tuple, Any
import requests

# ---- Provider ID map (TMDB watch providers; US region) ----
# If TMDB changes these, you can override with env PROVIDER_IDS,
# e.g. "netflix:8,prime_video:119,hulu:15,max:384,disney_plus:337,apple_tv_plus:350,peacock:386,paramount_plus:531"
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
    def __init__(self, api_key: str, language: str, watch_region: str, cb: str):
        self.api_key = api_key
        self.language = language
        self.watch_region = watch_region
        self.cb = cb
        self.base = "https://api.themoviedb.org/3"
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json"})

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
        url = f"{self.base}{path}"
        r = self.s.get(url, params=self._params(params), timeout=25)
        r.raise_for_status()
        return r.json()

def _resolve_provider_ids() -> Tuple[List[int], Dict[str, int], List[str]]:
    """Read SUBS_INCLUDE and map to TMDB provider IDs. Warn for unknown names."""
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
    """Evenly-spaced pseudo-random page selection; resilient for small totals."""
    total_pages = max(1, int(total_pages))
    want_pages = max(1, int(want_pages))
    if total_pages == 1:
        return [1]
    want = min(want_pages, total_pages)

    rng_seed = (seed ^ _hash32(f"tp={total_pages},wp={want_pages}")) & 0xFFFFFFFF
    # pick an odd step in [1, total_pages-1]; if that range collapses, step=1
    odd_candidates = [x for x in range(1, total_pages) if x % 2 == 1]
    if not odd_candidates:
        step = 1
    else:
        step = odd_candidates[_hash32(f"step:{rng_seed}") % len(odd_candidates)]

    start = (_hash32(f"start:{rng_seed}") % total_pages) + 1
    pages = []
    cur = start
    used = set()
    for _ in range(total_pages):  # full cycle at most
        if cur not in used:
            used.add(cur)
            pages.append(cur)
            if len(pages) >= want:
                break
        cur += step
        if cur > total_pages:
            cur -= total_pages
    # Always sorted for reproducibility in logs (but selection was random)
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

def _discover_pages(
    tmdb: TmdbClient,
    kind: str,
    provider_ids: List[int],
    with_original_language: str,
    base_filters: Dict[str, Any],
    total_pages_hint: int = 0,
) -> Tuple[List[dict], Dict[str, Any]]:
    """Discover items for kind ('movie' or 'tv'), choosing pages with rotation."""
    assert kind in ("movie", "tv")
    monetization = "flatrate"
    prov_param = "|".join(str(p) for p in provider_ids) if provider_ids else None

    # First fetch to learn total_pages
    first_params = dict(
        sort_by="popularity.desc",
        page=1,
        with_original_language=with_original_language,
        with_watch_monetization_types=monetization,
    )
    if prov_param:
        first_params["with_watch_providers"] = prov_param

    first_params.update(base_filters or {})
    first = tmdb.get(f"/discover/{kind}", first_params)
    total_pages = int(first.get("total_pages") or 1)
    total_pages = max(1, total_pages)
    if total_pages_hint and total_pages_hint > 0:
        total_pages = min(total_pages, int(total_pages_hint))  # optional clamp

    # How many pages do we want?
    want_pages_env = _env_int("TMDB_PAGES_MOVIE" if kind == "movie" else "TMDB_PAGES_TV", 40)
    rotate_minutes = _env_int("ROTATE_MINUTES", 15)
    slot = math.floor(_now_utc() / (rotate_minutes * 60))
    seed_basis = f"{kind}|slot={slot}|cb={_env_str('TMDB_CB', str(os.getenv('GITHUB_RUN_NUMBER') or slot))}"
    seed = _hash32(seed_basis)

    pages = _choose_pages(total_pages, want_pages_env, seed)
    items = [_extract_item(kind, r) for r in first.get("results", [])]
    # Fetch the rest of the selected pages
    for p in pages:
        if p == 1:
            continue
        params = dict(first_params)
        params["page"] = p
        try:
            data = tmdb.get(f"/discover/{kind}", params)
            results = data.get("results", []) or []
            items.extend(_extract_item(kind, r) for r in results)
        except Exception:
            # Best effort; keep going
            continue

    # Auto-expand if pool too small
    min_pool = 600  # target per kind
    if len(items) < min_pool and total_pages > len(pages):
        extra_need = min(total_pages, len(pages) * 2)
        extra_pages = _choose_pages(total_pages, extra_need, seed ^ 0xA5A5A5A5)
        for p in extra_pages:
            if p in pages:
                continue
            params = dict(first_params)
            params["page"] = p
            try:
                data = tmdb.get(f"/discover/{kind}", params)
                results = data.get("results", []) or []
                items.extend(_extract_item(kind, r) for r in results)
            except Exception:
                continue

    meta = {
        "total_pages": total_pages,
        "pages_used": sorted(set([1] + pages)),
        "want_pages": want_pages_env,
        "rotate_minutes": rotate_minutes,
        "slot": slot,
    }
    return _dedupe(items), meta

def _first_of_month_utc() -> str:
    today = dt.datetime.utcnow().date()
    first = today.replace(day=1)
    return first.isoformat()

def _curated_feeds(
    tmdb: TmdbClient,
    provider_ids: List[int],
    with_original_language: str,
) -> List[dict]:
    """Add trending + new-this-month + best-of (per provider) without dominating."""
    out: List[dict] = []
    # Trending (global)
    for kind in ("movie", "tv"):
        for page in (1, 2, 3):
            try:
                tr = tmdb.get(f"/trending/{kind}/week", {"page": page})
                out.extend(_extract_item(kind, r) for r in tr.get("results", []) or [])
            except Exception:
                break

    # New to service this month (approx): discover with date_gte=first of month and flatrate
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

    # Best-of on provider: high vote_average with a vote_count floor
    for pid in provider_ids:
        for kind in ("movie", "tv"):
            params = {
                "with_watch_providers": str(pid),
                "with_watch_monetization_types": "flatrate",
                "with_original_language": with_original_language,
                "sort_by": "vote_average.desc",
                "vote_count.gte": 1000,  # avoid low-sample outliers
                "page": 1,
            }
            try:
                data = tmdb.get(f"/discover/{kind}", params)
                out.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
            except Exception:
                continue

    # Limit curated items so they don't swamp the pool
    # Keep top by popularity within each media_type
    by_type = {"movie": [], "tvSeries": []}
    for it in out:
        by_type[it["media_type"]].append(it)
    curated = []
    for k in by_type:
        cur = sorted(by_type[k], key=lambda x: (-x["tmdb_vote_count"], -x["popularity"]))[:300]
        curated.extend(cur)
    return _dedupe(curated)

def _load_seen_csv(path: str) -> Dict[str, int]:
    """Load IMDb ratings CSV and treat tconsts as seen. We also store normalized titles."""
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
                # Optional: title/year heuristics
                nm = (row.get("Title") or row.get("title") or "").strip().lower()
                yr = (row.get("Year") or row.get("year") or "").strip()
                if nm:
                    seen[f"title:{nm}|{yr}"] = 1
    except Exception:
        pass
    return seen

def build_pool() -> Tuple[List[dict], Dict[str, Any]]:
    """Main entry: builds the candidate pool and returns (items, meta)."""
    api_key = _env_str("TMDB_API_KEY", "")
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required")

    language = _env_str("LANGUAGE", _env_str("LANG", "en-US"))
    watch_region = _env_str("REGION", "US")
    with_original_language = _env_str("ORIGINAL_LANGS", "en").split(",")[0].strip() or "en"
    cb = _env_str("TMDB_CB", str(os.getenv("GITHUB_RUN_NUMBER") or math.floor(_now_utc() / (15*60))))

    provider_ids, id_map, unknown = _resolve_provider_ids()
    if unknown:
        print(f"[hb] WARN: unknown providers={unknown} (using known IDs only)")
    if not provider_ids:
        print("[hb] WARN: no provider IDs resolved — queries will NOT filter by provider.")

    tmdb = TmdbClient(api_key, language, watch_region, cb)

    base_filters = {
        # Additional discover filters (none required beyond language/monetization)
    }

    movie_items, m_meta = _discover_pages(
        tmdb,
        "movie",
        provider_ids,
        with_original_language,
        base_filters,
    )
    tv_items, t_meta = _discover_pages(
        tmdb,
        "tv",
        provider_ids,
        with_original_language,
        base_filters,
    )

    curated = _curated_feeds(tmdb, provider_ids, with_original_language)

    pool = _dedupe(movie_items + tv_items + curated)

    # Telemetry/meta
    page_plan = {
        "movie_pages": _env_int("TMDB_PAGES_MOVIE", 40),
        "tv_pages": _env_int("TMDB_PAGES_TV", 40),
        "rotate_minutes": _env_int("ROTATE_MINUTES", 15),
        "slot": m_meta.get("slot"),
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
    }

    meta = {
        "page_plan": page_plan,
        "provider_id_map": id_map,
        "provider_ids_used": provider_ids,
        "unknown_providers": unknown,
    }
    return pool, meta