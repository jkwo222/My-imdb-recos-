from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache" / "tmdb"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TMDB_V3 = "https://api.themoviedb.org/3"

# ---------------------------
# Small cache helper
# ---------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _cache_path(kind: str, key: str) -> Path:
    safe = key.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")
    return CACHE_DIR / kind / f"{safe}.json"

def _read_cache(path: Path) -> Optional[dict]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None

def _write_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _is_fresh(path: Path, ttl_hours: int) -> bool:
    if not path.exists():
        return False
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return (_utcnow() - mtime) <= timedelta(hours=ttl_hours)
    except Exception:
        return False

# ---------------------------
# HTTP
# ---------------------------

def _get_tmdb(api_key: str, url: str, params: Dict[str, Any], ttl_hours: int, kind: str, cache_key: str) -> dict:
    """
    GET with on-disk caching.
    """
    path = _cache_path(kind, cache_key)
    if _is_fresh(path, ttl_hours):
        cached = _read_cache(path)
        if cached is not None:
            return cached

    headers = {"Accept": "application/json"}
    params = dict(params or {})
    params["api_key"] = api_key

    r = requests.get(url, params=params, headers=headers, timeout=(5, 20))
    r.raise_for_status()
    data = r.json()
    _write_cache(path, data)
    return data

# ---------------------------
# Normalizers
# ---------------------------

def _to_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        return int(date_str[:4])
    except Exception:
        return None

def _extract_directors_from_credits(credits: dict, media_type: str) -> List[str]:
    """
    movies: crew with job == 'Director'
    tv:     prefer created_by, else crew entries with 'Director'
    """
    names: List[str] = []

    if media_type == "movie":
        for c in (credits or {}).get("crew", []) or []:
            if (c.get("job") or "").lower() == "director" and c.get("name"):
                names.append(c["name"])
    else:
        # TV
        # 1) creators
        for c in (credits or {}).get("created_by", []) or []:
            if c.get("name"):
                names.append(c["name"])
        # 2) any crew credited as Director in series-level credits (may be sparse)
        for c in (credits or {}).get("crew", []) or []:
            job = (c.get("job") or "").lower()
            if "director" in job and c.get("name"):
                names.append(c["name"])

    # dedupe preserving order
    seen = set()
    out: List[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out

def _extract_providers(watch_providers: dict, region: str) -> List[str]:
    """
    Take flatrate first; if none, include ads/rent/buy names too.
    """
    results = (watch_providers or {}).get("results", {})
    bucket = results.get(region.upper()) or {}
    names: List[str] = []

    for key in ("flatrate", "ads", "rent", "buy"):
        for entry in bucket.get(key, []) or []:
            nm = entry.get("provider_name")
            if nm:
                names.append(nm)

    # dedupe preserving order
    seen = set()
    out: List[str] = []
    for n in names:
        lo = n.lower()
        if lo not in seen:
            seen.add(lo)
            out.append(n)
    return out

def _extract_genres(detail: dict) -> List[str]:
    gens = []
    for g in (detail or {}).get("genres", []) or []:
        nm = g.get("name")
        if nm:
            gens.append(nm)
    return gens

def _extract_imdb_tconst(external_ids: dict) -> Optional[str]:
    iid = (external_ids or {}).get("imdb_id")
    if not iid:
        return None
    # TMDB returns like "tt1234567"
    return iid.strip()

# ---------------------------
# Public: enrich
# ---------------------------

def enrich_items_with_tmdb(
    items: List[Dict[str, Any]],
    *,
    api_key: str,
    region: str = "US",
    ttl_hours: int = 72,
    sleep_sec: float = 0.2,
) -> None:
    """
    Mutates each item in-place, filling:
      - genres (list[str])
      - year (int) if missing
      - directors (list[str]) when available
      - providers (list[str]) for the given region
      - tmdb_vote_average (float) – TMDB's community rating
      - tconst (str) – IMDb id via external_ids, if present

    Caches responses on disk under data/cache/tmdb.
    """
    if not api_key:
        return

    for it in items:
        media_type = it.get("tmdb_media_type") or ("movie" if (it.get("type") == "movie") else "tv")
        tmdb_id = it.get("tmdb_id")
        title = it.get("title") or ""
        if not tmdb_id or media_type not in ("movie", "tv"):
            # can't enrich without a TMDB id; skip
            continue

        # Build a single detail call with append_to_response to cut requests
        url = f"{TMDB_V3}/{media_type}/{tmdb_id}"
        append = "external_ids,watch/providers,credits,release_dates,content_ratings"
        try:
            detail = _get_tmdb(
                api_key,
                url,
                params={"append_to_response": append},
                ttl_hours=ttl_hours,
                kind=f"{media_type}",
                cache_key=f"{media_type}_{tmdb_id}_all",
            )
        except Exception:
            # If detail fails, skip to next; don't break the whole run
            continue

        # Genres
        genres = _extract_genres(detail)
        if genres:
            it["genres"] = genres

        # Year
        year = it.get("year")
        if not isinstance(year, int) or not year:
            if media_type == "movie":
                # prefer main release_date; fallback to release_dates aggregate if needed
                year = _to_year(detail.get("release_date"))
                if not year:
                    # aggregate
                    rd = (detail.get("release_dates") or {}).get("results", [])
                    # try region-first else any
                    picked = None
                    for x in rd:
                        if x.get("iso_3166_1") == region.upper():
                            picked = x
                            break
                    if not picked and rd:
                        picked = rd[0]
                    if picked:
                        # take the earliest theatrical date
                        dates = picked.get("release_dates", [])
                        dates_sorted = sorted(dates, key=lambda d: (d.get("release_date") or "9999"))
                        if dates_sorted:
                            year = _to_year(dates_sorted[0].get("release_date"))
            else:
                year = _to_year(detail.get("first_air_date"))
            if year:
                it["year"] = year

        # Directors
        directors = _extract_directors_from_credits(
            {**(detail.get("credits") or {}), "created_by": detail.get("created_by")}, media_type
        )
        if directors:
            it["directors"] = directors

        # Providers
        providers = _extract_providers(detail.get("watch/providers") or {}, region)
        if providers:
            it["providers"] = providers

        # External IDs → IMDb
        tconst = _extract_imdb_tconst(detail.get("external_ids") or {})
        if tconst and not it.get("tconst"):
            it["tconst"] = tconst

        # TMDB vote avg (keep separate; some pipelines also load IMDb ratings)
        try:
            va = float(detail.get("vote_average") or 0.0)
        except Exception:
            va = 0.0
        if va:
            it["tmdb_vote_average"] = round(va, 1)

        # Be a good API citizen
        if sleep_sec:
            time.sleep(sleep_sec)