# engine/imdb_ingest.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

@dataclass
class IMDbItem:
    title: str
    year: Optional[int]
    imdb_id: str

def scrape_imdb_ratings(url: str, max_pages: int = 20, timeout: int = 20) -> List[IMDbItem]:
    """
    Minimal, best-effort scraper for a public IMDb ratings list URL.
    Strictly optional; caller must tolerate failures.
    This follows 'Next' pagination (if present) but caps pages.
    """
    out: List[IMDbItem] = []
    seen_pages = 0
    next_url = url

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; imdb-recos/1.0)",
        "Accept-Language": "en-US,en;q=0.8",
    })

    while next_url and seen_pages < max_pages:
        seen_pages += 1
        r = session.get(next_url, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Each row often includes a link to the title page with /title/ttXXXXXXX/
        for a in soup.select("a[href*='/title/tt']"):
            href = a.get("href") or ""
            # e.g. /title/tt4154796/?ref_=rt_li_tt
            m = None
            # find the tt id segment
            parts = href.split("/")
            for k in parts:
                if k.startswith("tt") and k[2:].isdigit():
                    m = k
                    break
            if not m:
                continue
            imdb_id = m
            title = (a.get_text() or "").strip()
            year: Optional[int] = None
            # Try to find a nearby year span
            yr = None
            parent = a.find_parent()
            if parent:
                yr_span = parent.find(string=lambda s: s and s.strip().startswith("(") and s.strip()[1:5].isdigit())
                if yr_span:
                    try:
                        yr = int(str(yr_span).strip()[1:5])
                    except Exception:
                        yr = None
            year = yr
            if title:
                out.append(IMDbItem(title=title, year=year, imdb_id=imdb_id))

        # pagination: anchor text contains 'Next' or rel=next
        n = soup.select_one("a[rel='next']") or soup.find("a", string=lambda s: s and "Next" in s)
        next_url = (("https://www.imdb.com" + n.get("href")) if n and n.get("href") else None)

    return out