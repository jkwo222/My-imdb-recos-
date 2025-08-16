# FILE: engine/util/omdb.py
from __future__ import annotations
from typing import Dict, Optional
import requests

from .cache import DiskCache

def _key(title: str, year: Optional[int], media_type: str) -> str:
    return f"omdb:{media_type}:{title.strip().lower()}:{year or ''}"

def fetch_omdb_enrich(
    title: str,
    year: Optional[int],
    media_type: str,       # "movie" or "series"
    api_key: str,
    cache: DiskCache,
) -> Dict[str, str]:
    """
    Minimal OMDb lookup to retrieve imdbID for title-year.
    Cached on disk via DiskCache.
    """
    if not api_key or not title:
        return {}
    url = "http://www.omdbapi.com/"
    params = {
        "apikey": api_key,
        "t": title,
        "type": media_type,
        "plot": "short",
        "r": "json",
    }
    if year:
        params["y"] = str(year)

    # Try cache
    cached = cache.get("omdb_title", url, params)
    if cached is None:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return {}
        data = r.json()
        cache.set("omdb_title", url, params, data)
    else:
        data = cached

    if not isinstance(data, dict) or data.get("Response") == "False":
        return {}
    imdb_id = (data.get("imdbID") or "").lower()
    out: Dict[str, str] = {}
    if imdb_id.startswith("tt"):
        out["imdb_id"] = imdb_id
    return out