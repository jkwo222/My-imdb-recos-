# engine/imdb_public.py
from __future__ import annotations
import os, re, json, time
from typing import Dict, List, Tuple, Set, Optional
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
UA = os.getenv("IMDB_PUBLIC_UA", "Mozilla/5.0 (compatible; RecoBot/1.0)")
BASE = "https://www.imdb.com"
CACHE_DIR = Path(os.getenv("IMDB_PUBLIC_CACHE_DIR", "data/cache/imdb_user"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
TTL_SECONDS = int(os.getenv("IMDB_PUBLIC_CACHE_TTL_SECONDS", str(7 * 24 * 3600)))  # 7 days

def _cache_path(user_id: str) -> Path:
    return CACHE_DIR / f"{user_id}.json"

def _read_cache(user_id: str) -> Optional[dict]:
    p = _cache_path(user_id)
    if not p.exists(): return None
    try:
        if time.time() - p.stat().st_mtime > TTL_SECONDS:
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_cache(user_id: str, data: dict) -> None:
    try:
        tmp = _cache_path(user_id).with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(_cache_path(user_id))
    except Exception:
        pass

def _norm_title(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").strip().lower()).strip()

def _get(url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/html"}, timeout=20)
            if r.status_code in (429, 503):
                time.sleep(0.6 + 0.6 * attempt)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == 2:
                return None
            time.sleep(0.4 + 0.4 * attempt)
    return None

_TCONST_RX = re.compile(r"/title/(tt\d+)/")
_YEAR_RX   = re.compile(r"\b(19|20)\d{2}\b")

def _parse_page(html: str) -> Tuple[List[Tuple[str, Optional[str], Optional[int]]], Optional[str]]:
    """
    Returns:
      - items: list of (imdb_id, title, year)
      - next_url: absolute "next page" URL if present
    """
    soup = BeautifulSoup(html, "lxml")
    items: List[Tuple[str, Optional[str], Optional[int]]] = []

    # IMDb ratings layout varies; grab tconsts wherever we can find them
    # Then try to pick the nearby title/year text for each block.
    for a in soup.find_all("a", href=True):
        m = _TCONST_RX.search(a["href"])
        if not m: 
            continue
        tconst = m.group(1)
        title  = a.get_text(strip=True) or None

        # Seek year in siblings/parents if the anchor text itself isn't the title/year
        year = None
        ctx = a.find_parent(["div","li","span"])
        if ctx:
            txt = ctx.get_text(" ", strip=True)
            ym = _YEAR_RX.search(txt)
            if ym:
                try: year = int(ym.group(0))
                except Exception: year = None

        items.append((tconst, title, year))

    # Dedup per tconst, prefer first occurrence
    seen = set()
    dedup: List[Tuple[str, Optional[str], Optional[int]]] = []
    for tconst, title, year in items:
        if tconst in seen: 
            continue
        seen.add(tconst)
        dedup.append((tconst, title, year))

    # Find "next" via paginationKey link (href contains 'paginationKey=')
    next_url: Optional[str] = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "ratings" in href and "paginationKey=" in href and ("Next" in (a.get_text() or "") or a.has_attr("aria-label")):
            next_url = href if href.startswith("http") else (BASE + href)
            break

    return dedup, next_url

def _start_url(user_id: str, public_url: Optional[str]) -> str:
    if public_url:
        return public_url
    uid = user_id.strip()
    # default: newest first, detail view
    return f"{BASE}/user/{uid}/ratings?sort=date_added%2Cdesc&mode=detail"

def fetch_user_ratings(user_id: str, *, public_url: Optional[str] = None, max_pages: int = 8, force_refresh: bool = False) -> dict:
    """
    Returns a dict:
      {
        "user_id": "...",
        "fetched_at": "...",
        "pages_fetched": N,
        "imdb_ids": ["tt...", "tt..."],
        "title_year_keys": ["normalized title::YYYY", ...]
      }
    """
    user_id = (user_id or "").strip()
    if not user_id and not public_url:
        return {"user_id": user_id, "pages_fetched": 0, "imdb_ids": [], "title_year_keys": []}

    # Cache read
    if not force_refresh:
        cached = _read_cache(user_id or "url")
        if cached is not None:
            return cached

    url = _start_url(user_id or "", public_url)
    pages = 0
    ids: List[str] = []
    tkeys: List[str] = []
    seen: Set[str] = set()

    while url and pages < max_pages:
        html = _get(url)
        if not html: break
        rows, next_url = _parse_page(html)
        for tconst, title, year in rows:
            if tconst in seen: 
                continue
            seen.add(tconst)
            ids.append(tconst)
            if title and year:
                tkeys.append(f"{_norm_title(title)}::{int(str(year)[:4])}")
        url = next_url
        pages += 1

    data = {
        "user_id": user_id or "(url)",
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pages_fetched": pages,
        "imdb_ids": ids,
        "title_year_keys": list(dict.fromkeys(tkeys)),
    }
    _write_cache(user_id or "url", data)
    return data