# engine/tmdb.py
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# NOTE: keep this import lightweight; caller passes cfg pieces
TMDB_BASE = "https://api.themoviedb.org/3"


# --- util: safe, short cache filenames (fixes OSError: File name too long)
def _cache_key(prefix: str, params: Dict[str, object]) -> str:
    payload = json.dumps(params, sort_keys=True, separators=(",", ":"))
    h = hashlib.sha1((prefix + "|" + payload).encode("utf-8")).hexdigest()
    return f"{prefix}_{h}.json"


def _ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def _get_json(
    cache_dir: str,
    cache_name: str,
    url: str,
    params: Dict[str, object],
    api_key: Optional[str],
    max_age_seconds: int = 6 * 3600,
    retry: int = 2,
    sleep_on_429: float = 2.0,
) -> Dict:
    _ensure_dir(cache_dir)
    cache_file = os.path.join(cache_dir, _cache_key(cache_name, params))

    # fresh cache?
    if os.path.exists(cache_file):
        mtime = os.path.getmtime(cache_file)
        if (time.time() - mtime) < max_age_seconds:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)

    headers = {"Accept": "application/json"}
    q = dict(params)
    if api_key:
        q["api_key"] = api_key

    last_exc: Optional[Exception] = None
    for attempt in range(retry + 1):
        try:
            r = requests.get(url, params=q, headers=headers, timeout=30)
            if r.status_code == 429 and attempt < retry:
                time.sleep(sleep_on_429 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            return data
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if attempt < retry:
                time.sleep(0.5 * (attempt + 1))
                continue
    # last attempt failed
    if last_exc:
        raise last_exc
    return {}


# --- provider helpers

# Common watch providers; TMDB IDs may vary by region but these IDs are stable
# Any missing/unknown names are ignored.
_PROVIDER_ID_MAP = {
    "netflix": 8,
    "prime_video": 9,
    "hulu": 15,
    "disney_plus": 337,
    "hbo_max": 384,  # Max (US)
    "apple_tv": 350,  # Apple TV+
    "peacock": 386,
    "paramount_plus": 531,
    # add more aliases if you use them in SUBS_INCLUDE
    "max": 384,
    "amazon_prime": 9,
}


def providers_from_env(subs_csv: str, region: str) -> List[int]:
    """
    Map a comma-separated list of provider *aliases* to TMDB provider IDs.
    We keep it simple & offline (no network call) so catalog can start immediately.
    """
    out: List[int] = []
    for raw in (subs_csv or "").split(","):
        name = raw.strip().lower()
        if not name:
            continue
        pid = _PROVIDER_ID_MAP.get(name)
        if pid and pid not in out:
            out.append(pid)
    return out


# --- discover endpoints

def _discover(
    kind: str,
    page: int,
    region: str,
    provider_ids: Iterable[int],
    original_langs: Iterable[str],
    monetization_types: str,
    api_key: Optional[str],
    cache_dir: str,
) -> Dict:
    url = f"{TMDB_BASE}/discover/{kind}"
    # Build TMDB query
    params: Dict[str, object] = {
        "sort_by": "popularity.desc",
        "include_adult": False,
        "language": "en-US",
        "page": page,
        "watch_region": region,
        "with_watch_monetization_types": monetization_types,
    }
    if provider_ids:
        params["with_watch_providers"] = "|".join(str(p) for p in provider_ids)
    if original_langs:
        params["with_original_language"] = ",".join(original_langs)

    return _get_json(
        cache_dir=cache_dir,
        cache_name=f"discover_{kind}",
        url=url,
        params=params,
        api_key=api_key,
    )


def discover_movie_page(
    page: int,
    *,
    region: str,
    provider_ids: Iterable[int],
    original_langs: Iterable[str],
    monetization_types: str = "flatrate|free|ads",
    api_key: Optional[str] = None,
    cache_dir: str = "data/cache",
) -> Tuple[List[Dict], int]:
    data = _discover(
        "movie",
        page=page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        monetization_types=monetization_types,
        api_key=api_key,
        cache_dir=cache_dir,
    )
    results = data.get("results", []) or []
    total_pages = int(data.get("total_pages", 1) or 1)
    return results, total_pages


def discover_tv_page(
    page: int,
    *,
    region: str,
    provider_ids: Iterable[int],
    original_langs: Iterable[str],
    monetization_types: str = "flatrate|free|ads",
    api_key: Optional[str] = None,
    cache_dir: str = "data/cache",
    **_kwargs,  # absorb any unknown kwargs (avoids unexpected-kwarg crashes)
) -> Tuple[List[Dict], int]:
    data = _discover(
        "tv",
        page=page,
        region=region,
        provider_ids=provider_ids,
        original_langs=original_langs,
        monetization_types=monetization_types,
        api_key=api_key,
        cache_dir=cache_dir,
    )
    results = data.get("results", []) or []
    total_pages = int(data.get("total_pages", 1) or 1)
    return results, total_pages