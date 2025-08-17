# engine/tmdb_detail.py
from __future__ import annotations

import os
import time
from typing import Dict, List, Optional

import requests

TMDB_BASE = "https://api.themoviedb.org/3"
_DEFAULT_TIMEOUT = (5, 20)
_UA = "my-imdb-recos/1.0 (+github actions)"


def _api_key_v3() -> str:
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise RuntimeError("TMDB_API_KEY missing (expecting TMDB v3 API key).")
    return key


def _headers() -> Dict[str, str]:
    return {"Accept": "application/json", "User-Agent": _UA}


def _get(url: str, params: Optional[Dict] = None) -> Dict:
    """requests.get with v3 key via query param + simple retry/backoff."""
    params = dict(params or {})
    params["api_key"] = _api_key_v3()
    for attempt in range(3):
        r = requests.get(url, params=params, headers=_headers(), timeout=_DEFAULT_TIMEOUT)
        if r.status_code == 429:
            # backoff if rate-limited
            sleep_s = float(r.headers.get("Retry-After", "1"))
            time.sleep(min(max(sleep_s, 1.0), 5.0))
            continue
        r.raise_for_status()
        return r.json()
    # should not reach here
    raise RuntimeError(f"TMDB GET failed for {url}")


def fetch_movie_details(tmdb_id: int) -> Dict:
    return _get(f"{TMDB_BASE}/movie/{tmdb_id}", {"language": "en-US"})


def fetch_tv_details(tmdb_id: int) -> Dict:
    # append external_ids so we can pull imdb_id for TV
    return _get(
        f"{TMDB_BASE}/tv/{tmdb_id}",
        {"language": "en-US", "append_to_response": "external_ids"},
    )


def fetch_watch_providers(media_type: str, tmdb_id: int) -> Dict:
    media_type = "movie" if media_type == "movie" else "tv"
    return _get(f"{TMDB_BASE}/{media_type}/{tmdb_id}/watch/providers", {"language": "en-US"})


def enrich_items_with_tmdb(items: List[Dict], *, api_key: str, region: str = "US") -> None:
    """
    Mutates `items` in place. For each item with a tmdb_id:
      - ensures tmdb_media_type
      - populates title (fallback), imdb_id, genres
      - populates providers (human-readable provider names for given region)
    """
    # ensure the provided api_key is present (warn if env doesn’t match)
    env = os.getenv("TMDB_API_KEY", "")
    if not env and api_key:
        os.environ["TMDB_API_KEY"] = api_key

    for it in items:
        mid = it.get("tmdb_id")
        if mid is None:
            continue
        try:
            tmdb_id = int(mid)
        except Exception:
            continue

        mtype = it.get("tmdb_media_type") or ("movie" if it.get("type") == "movie" else "tv")
        it["tmdb_media_type"] = mtype

        try:
            if mtype == "movie":
                d = fetch_movie_details(tmdb_id)
                it.setdefault("title", d.get("title") or d.get("original_title"))
                it["imdb_id"] = it.get("imdb_id") or d.get("imdb_id")
                if not it.get("genres"):
                    it["genres"] = [g.get("name") for g in d.get("genres", []) if g.get("name")]
            else:
                d = fetch_tv_details(tmdb_id)
                it.setdefault("title", d.get("name") or d.get("original_name"))
                ext = d.get("external_ids") or {}
                it["imdb_id"] = it.get("imdb_id") or ext.get("imdb_id")
                if not it.get("genres"):
                    it["genres"] = [g.get("name") for g in d.get("genres", []) if g.get("name")]
        except Exception:
            # best-effort enrichment; continue
            pass

        # Providers per region (names)
        try:
            prov = fetch_watch_providers(mtype, tmdb_id)
            r = (prov or {}).get("results", {}).get(region.upper()) or {}
            names = []
            for grp in ("flatrate", "ads"):
                for p in r.get(grp, []) or []:
                    nm = p.get("provider_name")
                    if nm:
                        names.append(nm)
            if names:
                it["providers"] = sorted(set(names))
        except Exception:
            pass