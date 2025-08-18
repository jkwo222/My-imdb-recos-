# engine/imdb_scrape.py
from __future__ import annotations
import json, os, re, time
from typing import Dict, Any, Optional, List
import requests
from bs4 import BeautifulSoup

UA = os.getenv("IMDB_SCRAPE_UA", "Mozilla/5.0 (compatible; RecoBot/1.0)")
BASE = "https://m.imdb.com/title"

def _get(url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/html"}, timeout=15)
            if r.status_code in (429, 503):
                time.sleep(0.7 + 0.7*attempt)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == 2:
                return None
            time.sleep(0.5 + 0.5*attempt)
    return None

def _iso_duration_to_minutes(s: str) -> Optional[int]:
    # ISO 8601 duration like "PT2H3M" or "PT58M"
    if not s or not s.startswith("P"): return None
    # Very small parser:
    hours = minutes = 0
    m = re.findall(r"(\d+)([HMS])", s)
    for val, unit in m:
        v = int(val)
        if unit == "H": hours = v
        elif unit == "M": minutes = v
    total = hours*60 + minutes
    return total or None

def _coerce_audience(ld: dict) -> Optional[float]:
    try:
        agg = ld.get("aggregateRating") or {}
        v = float(agg.get("ratingValue"))
        # IMDb ratingValue is 0–10; convert to 0–100
        return max(0.0, min(100.0, v * 10.0))
    except Exception:
        return None

def fetch_title(imdb_id: str) -> Dict[str, Any]:
    """
    Fetches public info from IMDb mobile page JSON-LD.
    Returns a dict with fields that merge cleanly into our items.
    """
    imdb_id = (imdb_id or "").strip()
    if not imdb_id.startswith("tt"): return {}
    html = _get(f"{BASE}/{imdb_id}/")
    if not html:
        return {}
    soup = BeautifulSoup(html, "lxml")
    # Find the JSON-LD blob
    ld = {}
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        txt = tag.string or tag.text or ""
        try:
            data = json.loads(txt)
            # Single object or list of objects
            if isinstance(data, dict) and data.get("@type") in {"Movie", "TVSeries", "TVMiniSeries"}:
                ld = data; break
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict) and d.get("@type") in {"Movie", "TVSeries", "TVMiniSeries"}:
                        ld = d; break
        except Exception:
            continue

    if not ld:
        return {}

    # name, year/date
    title = ld.get("name")
    date_published = ld.get("datePublished")
    year = None
    if isinstance(date_published, str) and len(date_published) >= 4 and date_published[:4].isdigit():
        year = date_published[:4]

    # directors / writers / cast
    def _names(x):
        out=[]
        if isinstance(x, list):
            for it in x:
                nm = (it.get("name") if isinstance(it, dict) else None) or (str(it) if it is not None else None)
                if nm: out.append(str(nm))
        elif isinstance(x, dict):
            nm = x.get("name")
            if nm: out.append(str(nm))
        return list(dict.fromkeys(out))

    directors = _names(ld.get("director"))
    creators  = _names(ld.get("creator"))
    writers   = _names(ld.get("writer") or ld.get("authors") or creators)[:6]
    actors    = _names(ld.get("actor"))[:8]

    # duration -> runtime minutes
    runtime = None
    dur = ld.get("duration")
    if isinstance(dur, str):
        runtime = _iso_duration_to_minutes(dur)

    # genres
    genres=[]
    g = ld.get("genre")
    if isinstance(g, list):
        genres = [str(x).strip().lower() for x in g if str(x).strip()]
    elif isinstance(g, str) and g.strip():
        genres = [g.strip().lower()]

    audience = _coerce_audience(ld)

    return {
        "title": title,
        "year": year,
        "runtime": runtime,
        "genres": genres or None,
        "directors": directors or None,
        "writers": writers or None,
        "cast": actors or None,
        "audience": audience,  # 0–100 scale
        "imdb_augmented": True,
        "imdb_url": f"https://www.imdb.com/title/{imdb_id}/",
    }