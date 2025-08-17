from __future__ import annotations
from typing import Dict, List, Any, Tuple
import requests, os, time

TMDB_BASE = "https://api.themoviedb.org/3"
UA = "my-imdb-recos/1.0 (+github actions)"
TIMEOUT = (5, 20)

def _key() -> str:
    k = os.getenv("TMDB_API_KEY", "").strip()
    if not k:
        raise RuntimeError("TMDB_API_KEY missing")
    return k

def _hdrs() -> Dict[str,str]:
    key = _key()
    # Accept either Bearer v4 token or v3; if v3, we add as ?api_key=
    # We’ll always add Authorization; v3 keys won’t break requests, but TMDB ignores the header.
    return {"Authorization": f"Bearer {key}", "Accept": "application/json", "User-Agent": UA}

def _get(url: str, params: Dict[str, Any]) -> Dict:
    # v3 api_key fallback
    key = _key()
    if not key.startswith("eyJ"):  # heuristic: v4 tokens are JWT-ish
        params = {**params, "api_key": key}
        hdr = {"Accept":"application/json","User-Agent":UA}
    else:
        hdr = _hdrs()
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=hdr, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(min(float(r.headers.get("Retry-After","1")), 5.0))
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1+attempt)

def discover(kind: str, page: int, region: str, provider_ids: List[int] | None, with_lang: str | None) -> Dict:
    url = f"{TMDB_BASE}/discover/{'movie' if kind=='movie' else 'tv'}"
    params: Dict[str, Any] = {
        "include_adult": "false",
        "language": "en-US",
        "page": str(page),
        "sort_by": "popularity.desc",
    }
    if with_lang:
        params["with_original_language"] = with_lang
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(i) for i in provider_ids)
        params["watch_region"] = region.upper()
        params["with_watch_monetization_types"] = "flatrate|free|ads"
    return _get(url, params)

def details_with_external_ids(media_type: str, tmdb_id: int) -> Dict:
    url = f"{TMDB_BASE}/{media_type}/{tmdb_id}"
    params = {"language":"en-US", "append_to_response":"external_ids"}
    return _get(url, params)

def watch_providers(media_type: str, tmdb_id: int) -> Dict:
    url = f"{TMDB_BASE}/{media_type}/{tmdb_id}/watch/providers"
    params = {"language":"en-US"}
    return _get(url, params)

def enrich_items_with_tmdb(items: List[Dict[str,Any]], *, api_key: str, region: str) -> None:
    """
    Items may have tmdb_id/media_type (movie|tv). We fill:
      - imdb_id (from external_ids)
      - genres (TMDB names if missing)
      - tmdb_vote (vote_average)
      - providers (human names)
      - seasons (for tv)
    """
    for it in items:
        mtype = it.get("tmdb_media_type") or ("movie" if it.get("type")=="movie" else "tv")
        tid = it.get("tmdb_id")
        if not tid: 
            continue
        det = details_with_external_ids(mtype, int(tid))
        it.setdefault("title", det.get("title") or det.get("name"))
        it.setdefault("year", (int((det.get("release_date") or det.get("first_air_date") or "0000")[:4]) or None))
        it["tmdb_vote"] = det.get("vote_average")
        it["imdb_id"] = (det.get("external_ids", {}) or {}).get("imdb_id") or it.get("imdb_id")
        if not it.get("genres"):
            it["genres"] = [g.get("name") for g in (det.get("genres") or []) if g.get("name")]
        if mtype == "tv":
            it["seasons"] = len(det.get("seasons") or []) or it.get("seasons") or 1
        prov = watch_providers(mtype, int(tid))
        r = (prov or {}).get("results", {}).get(region.upper(), {})
        flatrate = [p.get("provider_name") for p in (r.get("flatrate") or []) if p.get("provider_name")]
        ads = [p.get("provider_name") for p in (r.get("ads") or []) if p.get("provider_name")]
        it["providers"] = sorted(set((it.get("providers") or []) + flatrate + ads))