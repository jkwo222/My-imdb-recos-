# engine/tmdb_detail.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from .cache import (
    tmdb_find_by_imdb_cached,
    tmdb_details_cached,
    tmdb_providers_cached,
)

def _norm_list(x) -> List[str]:
    if not x:
        return []
    if isinstance(x, (list, tuple)):
        return [str(i) for i in x if i]
    return [str(x)]

def _coerce_media_type(x: str | None, fallback_kind: str) -> str:
    """
    TMDB /find may return 'movie' or 'tv' (sometimes 'person' which we ignore).
    Fall back to item kind if needed.
    """
    x = (x or "").lower()
    if x in ("movie", "tv"):
        return x
    return "tv" if fallback_kind != "movie" else "movie"

def map_imdb_to_tmdb(
    tconst: str,
    *,
    api_key: str,
    fallback_kind: str
) -> Tuple[Optional[int], Optional[str]]:
    """
    Returns (tmdb_id, media_type) or (None, None) if not found.
    """
    if not tconst or not api_key:
        return (None, None)

    data = tmdb_find_by_imdb_cached(tconst, api_key)
    # /find returns 'movie_results' and/or 'tv_results'
    for bucket, mtype in (("movie_results", "movie"), ("tv_results", "tv")):
        results = data.get(bucket) or []
        if results:
            best = results[0]
            tid = best.get("id")
            if isinstance(tid, int):
                return (tid, _coerce_media_type(mtype, fallback_kind))
    # Not found
    return (None, None)

def enrich_one_with_tmdb(
    item: Dict[str, Any],
    *,
    api_key: str,
    region: str
) -> None:
    """
    Mutates 'item' in place. Adds:
      - tmdb_id, tmdb_media_type
      - genres (normalized names, if TMDB has them)
      - providers (names in the given region; flatrate & ads)
    Safe to call repeatedly; respects existing fields.
    """
    if not api_key:
        return

    # 1) ensure tmdb id & media type
    tmdb_id = item.get("tmdb_id")
    mtype = item.get("tmdb_media_type")
    if not tmdb_id:
        tconst = item.get("tconst")
        if tconst:
            tid, mt = map_imdb_to_tmdb(str(tconst), api_key=api_key, fallback_kind=("tv" if item.get("type")!="movie" else "movie"))
            if tid:
                item["tmdb_id"] = tid
                item["tmdb_media_type"] = mt or ("tv" if item.get("type")!="movie" else "movie")
                tmdb_id = tid
                mtype = item.get("tmdb_media_type")
    # If still not mapped, we can't pull detail/providers
    if not tmdb_id:
        return

    # 2) genres from TMDB details
    try:
        det = tmdb_details_cached(int(tmdb_id), api_key, (mtype or "movie"))
        genres = _norm_list(item.get("genres"))
        if isinstance(det, dict):
            glist = det.get("genres") or []
            names = [g.get("name") for g in glist if isinstance(g, dict) and g.get("name")]
            # merge without duplicates
            merged = list(dict.fromkeys([*genres, *names]))
            if merged:
                item["genres"] = merged
            # also carry title/year if missing
            if not item.get("title"):
                item["title"] = det.get("title") or det.get("name") or item.get("title")
    except Exception:
        pass

    # 3) providers (names) for region
    try:
        prov = tmdb_providers_cached(int(tmdb_id), api_key, (mtype or "movie"))
        if prov and "results" in prov:
            pr = prov["results"].get(region.upper()) or {}
            flatrate = [p.get("provider_name") for p in (pr.get("flatrate") or []) if p.get("provider_name")]
            ads = [p.get("provider_name") for p in (pr.get("ads") or []) if p.get("provider_name")]
            names = sorted(set([*(item.get("providers") or []), *flatrate, *ads]))
            item["providers"] = names
    except Exception:
        pass

def enrich_items_with_tmdb(
    items: List[Dict[str, Any]],
    *,
    api_key: str,
    region: str
) -> None:
    """
    Batch enrichment. Mutates in place.
    """
    if not items or not api_key:
        return
    region = (region or "US").upper()
    for it in items:
        enrich_one_with_tmdb(it, api_key=api_key, region=region)
