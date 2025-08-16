import os
import time
import json
import hashlib
from typing import Dict, List, Tuple, Any
import requests

# ---------- Defaults ----------
# (You can override all of these via env; safe defaults provided.)

# TMDB watch-provider IDs for US (override with PROVIDER_IDS, e.g.
# "netflix:8,prime_video:119,hulu:15,max:384,disney_plus:337,apple_tv_plus:350,peacock:386,paramount_plus:531")
PROVIDER_ID_MAP_US = {
    "netflix": 8,
    "prime_video": 119,
    "hulu": 15,
    "max": 384,            # HBO Max/Max
    "disney_plus": 337,
    "apple_tv_plus": 350,
    "peacock": 386,
    "paramount_plus": 531,
}

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
            # explicit cache-buster so GitHub Actions doesn't reuse CDN aggressively
            "cb": self.cb,
        }
        p.update(extra or {})
        return p

    def get(self, path: str, params: Dict[str, Any]) -> dict:
        if self.throttle_ms:
            time.sleep(self.throttle_ms / 1000.0)
        url = f"{self.base}{path}"
        r = self.s.get(url, params=self._params(params), timeout=30)
        self.request_count += 1
        r.raise_for_status()
        return r.json()

# ---------- Provider resolution ----------

def _resolve_provider_ids() -> Tuple[List[int], Dict[str, int], List[str]]:
    names = [x.strip().lower() for x in _env_str("SUBS_INCLUDE", "").split(",") if x.strip()]
    custom = _env_str("PROVIDER_IDS", "")
    id_map = PROVIDER_ID_MAP_US.copy()
    if custom:
        for token in custom.split(","):
            token = token.strip()
            if not token or ":" not in token:
                continue
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

# ---------- Item extraction ----------

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

# ---------- CSV (seen list) helper exposed for runner ----------

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

# ---------- Core: FETCH ALL PAGES for movie and tv ----------

def _discover_all_pages(
    tmdb: TmdbClient,
    kind: str,
    provider_ids: List[int],
    with_original_language: str,
    extra_filters: Dict[str, Any],
    max_pages_cap: int,
) -> Tuple[List[dict], Dict[str, Any]]:
    """
    Fetches page=1..total_pages (capped by max_pages_cap) for /discover/{movie|tv}.
    TMDB caps total_pages to 500 for most discover queries; we walk the full range.
    """
    params_base = dict(
        sort_by="popularity.desc",
        page=1,
        with_watch_monetization_types="flatrate",
        with_original_language=with_original_language,
    )
    if provider_ids:
        params_base["with_watch_providers"] = "|".join(str(p) for p in provider_ids)
    if extra_filters:
        params_base.update(extra_filters)

    # First page: determine total_pages
    first = tmdb.get(f"/discover/{kind}", params_base)
    total_pages = max(1, int(first.get("total_pages") or 1))
    cap = max(1, min(total_pages, int(max_pages_cap)))

    items = [_extract_item(kind, r) for r in first.get("results", [])]
    # page 2..cap
    for p in range(2, cap + 1):
        params = dict(params_base)
        params["page"] = p
        try:
            data = tmdb.get(f"/discover/{kind}", params)
            items.extend(_extract_item(kind, r) for r in data.get("results", []) or [])
        except Exception:
            # Keep going if a single page hiccups (transient API/network)
            continue

    meta = {
        "total_pages": total_pages,
        "pages_used": list(range(1, cap + 1)),
        "want_pages": cap,
        "fetch_all": True,
        "requests_first_kind": tmdb.request_count,
    }
    return _dedupe(items), meta

# ---------- Public: build_pool (always fetch-all) ----------

def build_pool() -> Tuple[List[dict], Dict[str, Any]]:
    api_key = _env_str("TMDB_API_KEY", "")
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required")

    language = _env_str("LANGUAGE", _env_str("LANG", "en-US"))
    watch_region = _env_str("REGION", "US")
    with_original_language = _env_str("ORIGINAL_LANGS", "en").split(",")[0].strip() or "en"

    # Strong cache-buster; defaults to GitHub run number if present
    cb_default = os.getenv("GITHUB_RUN_NUMBER") or str(_hash32(str(time.time())))
    cb = _env_str("TMDB_CB", cb_default)

    throttle_ms = _env_int("TMDB_THROTTLE_MS", 0)  # Optional: 0..100ms if you see 429s

    provider_ids, id_map, unknown = _resolve_provider_ids()
    if unknown:
        print(f"[hb] WARN: unknown providers={unknown} (using known IDs only)")
    if not provider_ids:
        print("[hb] WARN: no provider IDs resolved — queries will NOT filter by provider.")

    tmdb = TmdbClient(api_key, language, watch_region, cb, throttle_ms=throttle_ms)

    # TMDB caps discover to 500 pages; allow an env cap (defaults to 500)
    max_pages_movie = _env_int("TMDB_MAX_PAGES_MOVIE", 500)
    max_pages_tv    = _env_int("TMDB_MAX_PAGES_TV", 500)

    extra_filters: Dict[str, Any] = {}  # hook for future filters if needed

    # ---- ALWAYS FETCH ALL PAGES ----
    movie_items, m_meta = _discover_all_pages(
        tmdb, "movie", provider_ids, with_original_language, extra_filters, max_pages_movie
    )
    tv_items, t_meta = _discover_all_pages(
        tmdb, "tv", provider_ids, with_original_language, extra_filters, max_pages_tv
    )

    pool = _dedupe(movie_items + tv_items)

    page_plan = {
        "movie_pages": m_meta.get("want_pages"),
        "tv_pages": t_meta.get("want_pages"),
        "rotate_minutes": _env_int("ROTATE_MINUTES", 15),   # still reported (harmless here)
        "slot": None,
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
        "fetch_all_movie": True,
        "fetch_all_tv": True,
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