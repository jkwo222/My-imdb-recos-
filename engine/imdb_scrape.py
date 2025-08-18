# engine/imdb_scrape.py
from __future__ import annotations
import json, os, re, time
from typing import Dict, Any, Optional, List
import requests
from bs4 import BeautifulSoup

UA = os.getenv("IMDB_SCRAPE_UA", "Mozilla/5.0 (compatible; RecoBot/1.0)")
BASE_MOBILE = "https://m.imdb.com/title"
BASE_DESKTOP = "https://www.imdb.com/title"

def _get(url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = requests.get(
                url,
                headers={"User-Agent": UA, "Accept": "text/html"},
                timeout=15,
            )
            # backoff on transient blocks
            if r.status_code in (429, 503):
                time.sleep(0.7 + 0.7 * attempt)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == 2:
                return None
            time.sleep(0.5 + 0.5 * attempt)
    return None

def _iso_duration_to_minutes(s: str) -> Optional[int]:
    # ISO 8601 "PT2H3M" or "PT58M"
    if not s or not s.startswith("P"): return None
    hours = minutes = 0
    for val, unit in re.findall(r"(\d+)([HMS])", s):
        v = int(val)
        if unit == "H": hours = v
        elif unit == "M": minutes = v
    total = hours * 60 + minutes
    return total or None

def _coerce_audience(ld: dict) -> Optional[float]:
    try:
        agg = ld.get("aggregateRating") or {}
        v = float(agg.get("ratingValue"))
        return max(0.0, min(100.0, v * 10.0))  # 0–10 => 0–100
    except Exception:
        return None

def _names(x) -> List[str]:
    out: List[str] = []
    if isinstance(x, list):
        for it in x:
            nm = (it.get("name") if isinstance(it, dict) else None) or (str(it) if it is not None else None)
            if nm: out.append(str(nm))
    elif isinstance(x, dict):
        nm = x.get("name")
        if nm: out.append(str(nm))
    return list(dict.fromkeys(out))

def fetch_title(imdb_id: str) -> Dict[str, Any]:
    """
    Parse the public JSON-LD from the IMDb mobile title page.
    Returns fields safe to merge into our items.
    """
    imdb_id = (imdb_id or "").strip()
    if not imdb_id.startswith("tt"): return {}
    html = _get(f"{BASE_MOBILE}/{imdb_id}/")
    if not html:
        return {}
    soup = BeautifulSoup(html, "lxml")

    # Locate JSON-LD with @type Movie / TVSeries / TVMiniSeries
    ld = {}
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        txt = tag.string or tag.text or ""
        try:
            data = json.loads(txt)
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

    date_published = ld.get("datePublished")
    year = None
    if isinstance(date_published, str) and len(date_published) >= 4 and date_published[:4].isdigit():
        year = date_published[:4]

    runtime = None
    dur = ld.get("duration")
    if isinstance(dur, str):
        runtime = _iso_duration_to_minutes(dur)

    genres: List[str] = []
    g = ld.get("genre")
    if isinstance(g, list):
        genres = [str(x).strip().lower() for x in g if str(x).strip()]
    elif isinstance(g, str) and g.strip():
        genres = [g.strip().lower()]

    directors = _names(ld.get("director"))
    creators  = _names(ld.get("creator"))
    writers   = _names(ld.get("writer") or ld.get("authors") or creators)[:6]
    actors    = _names(ld.get("actor"))[:8]
    audience  = _coerce_audience(ld)

    return {
        "title": ld.get("name"),
        "year": year,
        "runtime": runtime,
        "genres": genres or None,
        "directors": directors or None,
        "writers": writers or None,
        "cast": actors or None,
        "audience": audience,  # 0–100
        "imdb_augmented": True,
        "imdb_url": f"{BASE_DESKTOP}/{imdb_id}/",
    }

# ---------- Keywords scraping ----------
_KEYWORD_HREF_RX = re.compile(r"/keyword/[^/?#]+|keywords=")

def _extract_keywords_from_html(html: str, limit: int) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    kws: List[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not _KEYWORD_HREF_RX.search(href):
            continue
        text = a.get_text(strip=True)
        if not text:
            continue
        # normalize a bit, strip trailing counts like " (5)"
        text = re.sub(r"\(\d+\)$", "", text).strip().lower()
        text = re.sub(r"\s+", " ", text)
        if len(text) < 2:
            continue
        if text not in seen:
            seen.add(text)
            kws.append(text)
        if len(kws) >= limit:
            break
    return kws

def fetch_keywords(imdb_id: str, limit: int = 30) -> List[str]:
    """
    Fetch keywords from the desktop or mobile keywords subpage.
    Returns a list of lowercased keyword strings (deduped, clipped to 'limit').
    """
    imdb_id = (imdb_id or "").strip()
    if not imdb_id.startswith("tt"): return []
    # Try desktop first (usually richer), then mobile fallback
    html = _get(f"{BASE_DESKTOP}/{imdb_id}/keywords")
    if not html:
        html = _get(f"{BASE_MOBILE}/{imdb_id}/keywords")
        if not html:
            return []
    return _extract_keywords_from_html(html, limit)