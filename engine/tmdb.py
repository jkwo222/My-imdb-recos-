# engine/tmdb.py
from __future__ import annotations
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

TMDB_BASE = "https://api.themoviedb.org/3"
CACHE_DIR = Path("data/cache/tmdb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

UA = {"User-Agent": "RecoEngine/3.0 (+github actions)"}


# ---------- auth / http helpers ----------

def _auth_headers_and_params() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (headers, params) for TMDB v3 endpoints.
    Preference:
      1) TMDB_API_KEY  -> ?api_key=...
      2) TMDB_BEARER   -> Authorization: Bearer <v4 token>
    """
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if api_key:
        return dict(UA), {"api_key": api_key}

    bearer = os.getenv("TMDB_BEARER", "").strip()
    if bearer:
        return {"Authorization": f"Bearer {bearer}", **UA}, {}

    raise RuntimeError("TMDB_API_KEY or TMDB_BEARER is required for TMDB v3 API calls")


def _cache_key(path: str, params: Dict[str, Any]) -> str:
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    raw = f"{path}?{items}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _cache_path(group: str, key: str) -> Path:
    g = CACHE_DIR / group
    g.mkdir(parents=True, exist_ok=True)
    return g / f"{key}.json"


def _http_get_json(path: str, params: Dict[str, Any],
                   group: Optional[str] = None, ttl_min: int = 60) -> Dict[str, Any]:
    headers, base_params = _auth_headers_and_params()
    full_params = {**base_params, **params}
    key = _cache_key(path, full_params)

    if group:
        cp = _cache_path(group, key)
        if cp.exists():
            try:
                st = cp.stat()
                age_min = (time.time() - st.st_mtime) / 60.0
                if age_min <= ttl_min:
                    with cp.open("r", encoding="utf-8") as f:
                        return json.load(f)
            except Exception:
                pass

    url = f"{TMDB_BASE}{path}"
    backoff = 0.7
    last_err: Optional[Dict[str, Any]] = None
    for _ in range(5):
        try:
            r = requests.get(url, params=full_params, headers=headers, timeout=25)
            if r.status_code == 200:
                data = r.json()
                if group:
                    try:
                        with _cache_path(group, key).open("w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False)
                    except Exception:
                        pass
                return data
            else:
                last_err = {"status_code": r.status_code, "text": r.text[:300]}
        except Exception as e:
            last_err = {"exception": repr(e)}
        time.sleep(backoff)
        backoff *= 1.8
    return {"__error__": last_err or {"error": "unknown"}}


# Exposed for detail helpers
def _get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    return _http_get_json(path, params, group="raw", ttl_min=90)


# ---------- provider helpers ----------

def _slugify_provider_name(name: str) -> str:
    """
    Convert TMDB provider display name to a stable slug.
    Examples:
      "Apple TV+" -> "apple_tv_plus"
      "Peacock Premium" -> "peacock_premium"
      "Max" -> "max"
    """
    s = (name or "").strip().lower()
    s = s.replace("&", "and")
    s = s.replace("+", "_plus")
    s = re.sub(r"[^\w\s\-]", "", s)     # drop punctuation except hyphen/underscore
    s = s.replace("-", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s


# Aliases from ENV slugs to one or more candidate TMDB slugs.
# We try candidates in order, then fuzzy fallback.
PROVIDER_ALIASES: Dict[str, List[str]] = {
    # streaming staples
    "netflix": ["netflix"],
    "hulu": ["hulu"],
    "disney_plus": ["disney_plus"],
    "disneyplus": ["disney_plus"],
    "paramount_plus": ["paramount_plus"],
    "paramountplus": ["paramount_plus"],
    "prime_video": ["amazon_prime_video", "amazon_prime"],
    "amazon_prime": ["amazon_prime_video", "amazon_prime"],
    "amazon_prime_video": ["amazon_prime_video"],
    "apple_tv": ["apple