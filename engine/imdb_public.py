# engine/imdb_public.py
from __future__ import annotations
import os
import re
from typing import Iterable, Set, Tuple, Optional
import requests

IMDB_BASE = "https://www.imdb.com"

def _get(url: str, params=None, timeout: int = 20) -> str:
    r = requests.get(url, params=params or {}, timeout=timeout, headers={
        "User-Agent": "Mozilla/5.0 (compatible; imdb-scraper/1.0)"
    })
    r.raise_for_status()
    return r.text

_TCONST_RE = re.compile(r"/title/(tt\d{7,8})/")

def _extract_tconsts(html: str) -> Set[str]:
    return set(m.group(1) for m in _TCONST_RE.finditer(html or ""))

def load_user_ratings_tconsts(user_id: str, max_pages: int = 10) -> Set[str]:
    """
    Scrape public user ratings pages for tconsts (tt…).
    Pages are paginated in steps (100/250 depending on layout); we follow 'start=' pagination until no new ids or max_pages.
    """
    if not user_id:
        return set()
    tconsts: Set[str] = set()
    start = 1
    seen_pages = 0
    while seen_pages < max_pages:
        url = f"{IMDB_BASE}/user/{user_id}/ratings"
        html = _get(url, params={"sort": "ratings_date,desc", "start": str(start)})
        ids = _extract_tconsts(html)
        if not ids:
            break
        prev_len = len(tconsts)
        tconsts.update(ids)
        seen_pages += 1
        # Heuristic: if this page added < 1 id, bail
        if len(tconsts) == prev_len:
            break
        # Advance. Common page sizes are ~100; advancing by 100 is usually safe.
        start += 100
    return tconsts

def load_public_seen_from_env() -> Set[str]:
    """
    Convenience helper that reads IMDB_USER_ID and IMDB_PUBLIC_MAX_PAGES,
    and returns a set of tconst strings.
    """
    user_id = os.getenv("IMDB_USER_ID", "").strip()
    if not user_id:
        return set()
    try:
        max_pages = int(os.getenv("IMDB_PUBLIC_MAX_PAGES", "10"))
    except Exception:
        max_pages = 10
    return load_user_ratings_tconsts(user_id, max_pages=max_pages)