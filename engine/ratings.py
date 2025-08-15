import os, csv, re, requests, time
from typing import List, Dict
from bs4 import BeautifulSoup
from rich import print as rprint

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}

def load_csv(path: str) -> List[Dict]:
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                rows.append({
                    "imdb_id": row.get("Const") or row.get("imdb_id") or "",
                    "title": row.get("Title") or row.get("title") or "",
                    "year": int((row.get("Year") or row.get("year") or "0") or 0),
                    "type": row.get("Title Type") or row.get("type") or "",
                    "your_rating": float((row.get("Your Rating") or row.get("your_rating") or "0") or 0),
                })
            rprint(f"[green]IMDb ingest (CSV):[/green] {path} — {len(rows)} rows")
            return rows
    return []

def scrape_imdb_ratings(public_url: str, max_pages: int = 10) -> List[Dict]:
    # Best-effort scraper; prefer CSV when possible
    m = re.search(r"/user/(ur\d+)/ratings", public_url or "")
    if not m: 
        return []
    user_id = m.group(1)
    base = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    start = 1
    items: List[Dict] = []
    for _ in range(max_pages):
        url = f"{base}&start={start}"
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code != 200: break
        soup = BeautifulSoup(r.text, "lxml")
        blocks = soup.select("div.lister-item.mode-detail")
        if not blocks: break
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
            # type inference
            t = "movie"
            sub = b.select_one("p.text-muted")
            if sub:
                s = sub.get_text()
                if "TV Mini-Series" in s: t = "tvMiniSeries"
                elif "TV Series" in s: t = "tvSeries"
                elif "TV Movie" in s: t = "tvMovie"
                elif "TV Special" in s: t = "tvSpecial"
                elif "Video Game" in s: t = "game"
            items.append({"imdb_id":iid,"title":title,"year":year,"type":t,"your_rating":your})
        nxt = soup.select_one("a.lister-page-next.next-page")
        if not nxt: break
        start += 100; time.sleep(0.7)
    if items:
        rprint(f"[yellow]IMDb ingest (web):[/yellow] {len(items)} rows")
    return items