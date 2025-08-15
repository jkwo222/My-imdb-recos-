# engine/imdb_ingest.py
import re, time, requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from typing import List, Dict
from rich import print as rprint

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}

@dataclass
class RatingItem:
    imdb_id: str; title: str; year: int; type: str; your_rating: float

def scrape_imdb_ratings(public_url: str, max_pages: int = 50) -> List[RatingItem]:
    m = re.search(r"/user/(ur\d+)/ratings", public_url)
    if not m: raise ValueError("Use a URL like https://www.imdb.com/user/ur12345678/ratings")
    user_id = m.group(1); items: List[RatingItem] = []
    base = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    start = 1
    for _ in range(max_pages):
        url = f"{base}&start={start}"
        r = requests.get(url, headers=UA, timeout=30)
        rprint(f"[IMDb] GET start={start} → {r.status_code}")
        if r.status_code != 200: break
        soup = BeautifulSoup(r.text, "lxml")
        blocks = soup.select("div.lister-item.mode-detail")
        if not blocks:
            if start == 1:
                raise ValueError("IMDb returned no items (private or bot protection). Prefer CSV.")
            break
        for b in blocks:
            a = b.select_one("h3.lister-item-header a[href*='/title/tt']")
            if not a: continue
            href = a.get("href",""); m2 = re.search(r"/title/(tt\d+)/", href)
            if not m2: continue
            iid = m2.group(1); title = a.get_text(strip=True)
            ytag = b.select_one("h3 span.lister-item-year"); year = 0
            if ytag:
                my = re.search(r"(\d{4})", ytag.get_text()); year = int(my.group(1)) if my else 0
            yr = b.select_one("div.ipl-rating-widget span.ipl-rating-star__rating")
            your = float(yr.get_text(strip=True)) if yr else 0.0
            t = "movie"
            sub = b.select_one("p.text-muted")
            if sub:
                s = sub.get_text()
                if "TV Mini-Series" in s: t = "tvMiniSeries"
                elif "TV Series" in s: t = "tvSeries"
                elif "TV Movie" in s: t = "tvMovie"
                elif "TV Special" in s: t = "tvSpecial"
                elif "Video Game" in s: t = "game"
            items.append(RatingItem(iid, title, year, t, your))
        nxt = soup.select_one("a.lister-page-next.next-page")
        if not nxt: break
        start += 100; time.sleep(0.8)
    return items

def to_rows(items: List[RatingItem]) -> List[Dict]:
    return [asdict(i) for i in items]