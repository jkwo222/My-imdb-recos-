# FILE: engine/catalog.py
from __future__ import annotations
import hashlib
from typing import Any, Dict, List, Tuple

from .config import Config
from .tmdb import TMDB, normalize_provider_names
from .util.cache import DiskCache
import requests

def _seed_for(key: str) -> int:
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)

def _choose_pages(total_pages: int, want_pages: int, seed: int) -> List[int]:
    import random
    total_pages = max(1, int(total_pages))
    want_pages = max(1, min(int(want_pages), total_pages))
    if total_pages == 1:
        return [1]
    rng = random.Random(seed)
    step = (rng.randrange(1, total_pages) * 2 + 1) % total_pages or 1
    start = rng.randrange(0, total_pages)
    pages, cur = [], start
    for _ in range(want_pages):
        pages.append(cur + 1)
        cur = (cur + step) % total_pages
    return pages

def _omdb_fetch(api_key: str | None, imdb_id: str, title: str, year: int, is_tv: bool) -> Dict[str, Any]:
    if not api_key:
        return {}
    base = "http://www.omdbapi.com/"
    params = {"apikey": api_key, "plot": "short", "r": "json"}
    if imdb_id:
        params["i"] = imdb_id
    else:
        params["t"] = title
        if year:
            params["y"] = str(year)
        params["type"] = "series" if is_tv else "movie"
    r = requests.get(base, params=params, timeout=20)
    if r.status_code != 200:
        return {}
    try:
        d = r.json()
    except Exception:
        return {}
    if d.get("Response") == "False":
        return {}
    # Extract critic/audience/lang/genres
    aud = 0.0
    try:
        aud = float(d.get("imdbRating") or 0.0) / 10.0
    except Exception:
        pass
    rt = 0.0
    for rating in d.get("Ratings") or []:
        if (rating.get("Source") or "").lower() == "rotten tomatoes":
            try:
                rt = float((rating.get("Value") or "0%").rstrip("%")) / 100.0
            except Exception:
                rt = 0.0
            break
    lang_primary = (d.get("Language") or "").split(",")[0].strip().lower()
    genres = [g.strip().lower() for g in (d.get("Genre") or "").split(",") if g.strip()]
    return {
        "imdb_id": d.get("imdbID") or imdb_id or "",
        "audience": aud,
        "critic": rt,
        "language_primary": lang_primary,
        "genres": genres,
    }

def build_pool(cfg: Config) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Discover titles across many TMDB pages, enrich with providers, details (imdb_id, genres, alt titles),
    and OMDb signals. Filter to your subscription list and return (pool, meta).
    """
    tmdb = TMDB(cfg.tmdb_api_key, cache=_cache(cfg))

    # total pages
    total_pages_movie = tmdb.total_pages("movie", cfg.language, cfg.with_original_langs, cfg.region)
    total_pages_tv = tmdb.total_pages("tv", cfg.language, cfg.with_original_langs, cfg.region)

    # choose pages deterministically per config “slot”
    slot_key = f"{cfg.region}|{cfg.language}|{','.join(cfg.with_original_langs)}|{','.join(cfg.subs_include)}"
    pages_movie = _choose_pages(total_pages_movie, max(1, cfg.tmdb_pages_movie), _seed_for("movie|" + slot_key))
    pages_tv    = _choose_pages(total_pages_tv,    max(1, cfg.tmdb_pages_tv),    _seed_for("tv|" + slot_key))

    # collect raw discover
    def _coerce(kind: str, raw: Dict[str, Any]) -> Dict[str, Any]:
        if kind == "movie":
            title = raw.get("title") or raw.get("original_title") or ""
            year = (raw.get("release_date") or "")[:4] or None
        else:
            title = raw.get("name") or raw.get("original_name") or ""
            year = (raw.get("first_air_date") or "")[:4] or None
        return {
            "kind": kind,
            "tmdb_id": int(raw.get("id")),
            "title": title,
            "year": int(year) if (isinstance(year, str) and year.isdigit()) else year,
            "popularity": float(raw.get("popularity") or 0.0),
            "vote_average": float(raw.get("vote_average") or 0.0),
            "original_language": raw.get("original_language"),
        }

    def collect(kind: str, pages: List[int]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for p in pages:
            data = tmdb.discover(kind, p, cfg.language, cfg.with_original_langs, cfg.region)
            for raw in data.get("results", []) or []:
                out.append(_coerce(kind, raw))
        return out

    movie_items = collect("movie", pages_movie)
    tv_items    = collect("tv", pages_tv)

    # enrich + provider filter
    def enrich_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        keep: List[Dict[str, Any]] = []
        for it in items:
            prov_names = tmdb.providers_for_title(it["kind"], it["tmdb_id"], cfg.region)
            prov_slugs = normalize_provider_names(prov_names)
            it["providers"] = prov_slugs
            if not any(s in cfg.subs_include for s in prov_slugs):
                continue

            # details for imdb_id, seasons, genres, alt titles
            det = tmdb.details_with_external_ids(it["kind"], it["tmdb_id"], cfg.language)
            imdb_id = ((det.get("external_ids") or {}).get("imdb_id")) or ""
            seasons = int(det.get("number_of_seasons") or 1) if it["kind"] == "tv" else 1
            genres_tmdb = [g.get("name","").strip().lower() for g in (det.get("genres") or []) if isinstance(g, dict)]
            alt_titles = []
            alts = det.get("alternative_titles", {})
            for k in ("titles", "results"):
                for t in alts.get(k, []) or []:
                    name = (t.get("title") or t.get("name") or "").strip()
                    if name:
                        alt_titles.append(name)

            # OMDb (optional)
            omdb = _omdb_fetch(cfg.omdb_api_key, imdb_id, it["title"], int(it.get("year") or 0) or 0, is_tv=(it["kind"]=="tv"))

            # merge fields
            item = {
                "kind": it["kind"],
                "tmdb_id": it["tmdb_id"],
                "imdb_id": omdb.get("imdb_id") or imdb_id,
                "title": it["title"],
                "alt_titles": alt_titles[:10],  # bounded
                "year": it.get("year"),
                "seasons": seasons,
                "providers": prov_slugs,
                "vote_average": it.get("vote_average", 0.0),
                "genres": (omdb.get("genres") or genres_tmdb),
                "language_primary": omdb.get("language_primary") or (it.get("original_language") or "").lower(),
                "audience": omdb.get("audience", 0.0),  # 0..1
                "critic": omdb.get("critic", 0.0),      # 0..1
            }
            keep.append(item)
        return keep

    movie_items = enrich_and_filter(movie_items)
    tv_items    = enrich_and_filter(tv_items)

    pool = movie_items + tv_items
    pool.sort(key=lambda x: x.get("popularity", 0.0), reverse=True)
    if cfg.max_catalog > 0:
        pool = pool[: cfg.max_catalog]

    meta = {
        "movie_pages": len(pages_movie),
        "tv_pages": len(pages_tv),
        "provider_names": cfg.subs_include,
        "language": cfg.language,
        "with_original_language": ",".join(cfg.with_original_langs),
        "watch_region": cfg.region,
        "pool_counts": {"movie": len([x for x in pool if x["kind"]=="movie"]),
                        "tv": len([x for x in pool if x["kind"]=="tv"])},
        "total_pages": [total_pages_movie, total_pages_tv],
    }
    return pool, meta

# cache singleton
__cache = None
def _cache(cfg: Config):
    global __cache
    if __cache is None:
        __cache = DiskCache(cfg.cache_dir, cfg.cache_ttl_secs)
    return __cache