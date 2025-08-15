import os, io, csv, time, hashlib, requests, re
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Set
from bs4 import BeautifulSoup
from rich import print as rprint
from .cache import get_fresh, set as cache_set

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}

@dataclass
class RatingItem:
    imdb_id: str
    title: str
    year: int
    type: str
    your_rating: float
    date_rated: str = ""

def _map_type(s: str) -> str:
    s = (s or "").strip()
    if s in {"tvSeries","tvMiniSeries","tvMovie","tvSpecial"}: return s
    return "movie" if s == "movie" else (s or "movie")

def _parse_csv_rows(reader: csv.DictReader) -> List[RatingItem]:
    out: List[RatingItem] = []
    for r in reader:
        iid = (r.get("const") or r.get("tconst") or "").strip()
        if not iid: 
            continue
        title = (r.get("Title") or r.get("primaryTitle") or "").strip()
        ys = (r.get("Year") or r.get("startYear") or "").strip()
        y = int(ys) if ys.isdigit() else 0
        t = _map_type(r.get("Title Type") or r.get("titleType") or "")
        try:
            yr = float((r.get("Your Rating") or r.get("userRating") or "0").strip() or "0")
        except:
            yr = 0.0
        dr = (r.get("Date Rated") or r.get("dateRated") or "").strip()
        out.append(RatingItem(iid, title, y, t, yr, dr))
    return out

def load_from_local_csv() -> List[Dict]:
    path = os.environ.get("IMDB_RATINGS_CSV_PATH","data/ratings.csv")
    if not os.path.exists(path): 
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        return [asdict(x) for x in _parse_csv_rows(reader)]

def load_from_csv_url() -> List[Dict]:
    url = os.environ.get("IMDB_RATINGS_CSV_URL","").strip()
    if not url: 
        return []
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    text = r.text
    reader = csv.DictReader(io.StringIO(text))
    return [asdict(x) for x in _parse_csv_rows(reader)]

def _ratings_url_from_env() -> str:
    user_id = os.environ.get("IMDB_USER_ID","").strip()
    ratings_url = os.environ.get("IMDB_RATINGS_URL","").strip()
    if ratings_url:
        return ratings_url
    if user_id:
        return f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    return ""

def _scrape_page(url: str):
    r = requests.get(url, headers=UA, timeout=30)
    status = f"[IMDb] GET {url} → {r.status_code}"
    if r.status_code != 200:
        return ("", [], status)
    soup = BeautifulSoup(r.text, "lxml")
    blocks = soup.select("div.lister-item.mode-detail")
    items: List[Dict] = []
    for b in blocks:
        a = b.select_one("h3.lister-item-header a[href*='/title/tt']")
        if not a: 
            continue
        href = a.get("href","")
        iid = ""
        for part in href.split("/"):
            if part.startswith("tt") and part[2:].isdigit():
                iid = part
                break
        if not iid:
            m2 = re.search(r"/title/(tt\d+)/", href)
            iid = m2.group(1) if m2 else ""
        if not iid:
            continue
        title = a.get_text(strip=True)
        # year
        y = 0
        ytag = b.select_one("h3 span.lister-item-year")
        if ytag:
            my = re.search(r"(\d{4})", ytag.get_text())
            y = int(my.group(1)) if my else 0
        # your rating
        yr = b.select_one("div.ipl-rating-widget span.ipl-rating-star__rating")
        your = float(yr.get_text(strip=True)) if yr else 0.0
        # type
        t = "movie"
        sub = b.select_one("p.text-muted")
        if sub:
            s = sub.get_text()
            if "TV Mini-Series" in s: t = "tvMiniSeries"
            elif "TV Series" in s: t = "tvSeries"
            elif "TV Movie" in s: t = "tvMovie"
            elif "TV Special" in s: t = "tvSpecial"
            elif "Video Game" in s: t = "game"
        items.append(asdict(RatingItem(iid, title, y, t, your)))
    nxt = soup.select_one("a.lister-page-next.next-page")
    next_url = ""
    if nxt and nxt.get("href"):
        href = nxt.get("href")
        next_url = "https://www.imdb.com" + href if href.startswith("/") else href
    return (next_url, items, status)

def _sig_for_csv_ids(csv_ids: Set[str]) -> str:
    h = hashlib.sha256()
    h.update((";".join(sorted(csv_ids))).encode("utf-8"))
    return h.hexdigest()

def _merge_rows(base: List[Dict], additions: List[Dict]) -> List[Dict]:
    by_id: Dict[str, Dict] = {}
    for r in base:
        rid = (r.get("imdb_id") or "").strip()
        if rid: by_id[rid] = r
    for r in additions:
        rid = (r.get("imdb_id") or "").strip()
        if rid and rid not in by_id:
            by_id[rid] = r
    merged = [by_id[(r.get("imdb_id") or "").strip()] for r in base if (r.get("imdb_id") or "").strip() in by_id]
    for r in additions:
        rid = (r.get("imdb_id") or "").strip()
        if rid and rid not in [x.get("imdb_id") for x in merged]:
            merged.append(r)
    return merged

def load_user_ratings_combined():
    rows_csv = load_from_local_csv()
    if not rows_csv:
        try:
            rows_csv = load_from_csv_url()
        except Exception as e:
            rprint(f"[yellow][IMDb CSV] URL failed: {e}[/yellow]")
    csv_ids = { (r.get("imdb_id") or "").strip() for r in rows_csv if (r.get("imdb_id") or "").strip() }
    csv_sig = _sig_for_csv_ids(csv_ids)
    cache_key = "ratings_combined_v1"
    cached = get_fresh(cache_key, ttl_days=1)
    if cached and isinstance(cached, dict) and cached.get("sig") == csv_sig:
        data = cached.get("rows") or []
        rprint(f"[cache] using cached combined ratings: {len(data)} rows (CSV signature matched)")
        return data, {"csv": len(rows_csv), "html_new": 0, "combined": len(data)}

    html_new: List[Dict] = []
    start_url = _ratings_url_from_env()
    if start_url:
        seen_ids = set(csv_ids)
        url = start_url
        pages = 0
        consecutive_known_pages = 0
        while url and pages < 50:
            next_url, items, status = _scrape_page(url)
            rprint(status + f" items={len(items)}")
            pages += 1
            new_on_page = [it for it in items if (it.get("imdb_id") or "") not in seen_ids]
            for it in new_on_page:
                seen_ids.add(it.get("imdb_id"))
            html_new.extend(new_on_page)
            if len(new_on_page) == 0:
                consecutive_known_pages += 1
            else:
                consecutive_known_pages = 0
            if consecutive_known_pages >= 2:
                rprint("[IMDb] No new IDs for 2 pages — stopping incremental scrape.")
                break
            url = next_url
            time.sleep(0.8)
    else:
        rprint("[yellow][IMDb] No IMDB_USER_ID/IMDB_RATINGS_URL set — skipping HTML incremental.[/yellow]")

    merged = _merge_rows(rows_csv, html_new)
    cache_set(cache_key, {"sig": csv_sig, "rows": merged})
    return merged, {"csv": len(rows_csv), "html_new": len(html_new), "combined": len(merged)}