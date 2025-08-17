# engine/imdb_sync.py
from __future__ import annotations

import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# -------- Paths / constants --------

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
USER_DIR = DATA_DIR / "user"
CACHE_DIR = DATA_DIR / "cache"
STATE_DIR = CACHE_DIR / "state"
USER_CACHE_DIR = CACHE_DIR / "user"

USER_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
USER_CACHE_DIR.mkdir(parents=True, exist_ok=True)

RATINGS_CSV = USER_DIR / "ratings.csv"
WEB_CACHE = USER_CACHE_DIR / "ratings_web.json"
PERSONAL_HISTORY = STATE_DIR / "personal_history.json"
PERSONAL_STATE = STATE_DIR / "personal_state.json"

DEFAULT_TTL_DAYS = 2
HTTP_TIMEOUT = (5, 20)
UA = "my-imdb-recos/1.0 (+github actions)"

IMDB_BASE = "https://www.imdb.com"

# -------- Helpers (time, json, parsing) --------

_TCONST_RE = re.compile(r"(tt\d{7,9})")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_iso() -> str:
    # ISO 8601 Zulu, second precision
    return _now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _is_stale(ts_iso: str, ttl_days: int) -> bool:
    """
    Compare a cached-at ISO string (which may be Z or offset-aware/naive)
    against the current UTC time, using timezone-aware datetimes.
    """
    if not ts_iso:
        return True
    try:
        # Accept "....Z" or any ISO offset; normalize to aware UTC
        ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
    except Exception:
        return True
    return _now_utc() - ts > timedelta(days=ttl_days)

def _atomic_write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def _read_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        xf = float(str(x).strip())
        if xf <= 0:
            return None
        return xf
    except Exception:
        return None

def _pick(*vals):
    for v in vals:
        if v not in (None, "", [], {}, "null", "None"):
            return v
    return None

# -------- CSV loader (data/user/ratings.csv) --------

def _parse_tconst_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = _TCONST_RE.search(url)
    return m.group(1) if m else None

def load_ratings_csv() -> List[Dict[str, Any]]:
    """
    Reads data/user/ratings.csv if present. Tolerant to headers:
      - tconst | imdb_id | url | imdb_url | Title Const
      - my_rating | rating | Your Rating
      - rated_at | date | Rated At
    Returns a list of dict rows: {tconst, my_rating, rated_at, source:"csv"}.
    """
    rows: List[Dict[str, Any]] = []
    if not RATINGS_CSV.exists():
        return rows

    with RATINGS_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            tconst = _pick(
                r.get("tconst"),
                r.get("imdb_id"),
                _parse_tconst_from_url(r.get("url") or r.get("imdb_url") or r.get("URL") or ""),
                r.get("Title Const"),
            )
            if not tconst or not _TCONST_RE.fullmatch(tconst):
                continue

            my_rating = _to_float(_pick(r.get("my_rating"), r.get("rating"), r.get("Your Rating")))
            rated_at = _pick(r.get("rated_at"), r.get("date"), r.get("Rated At"))
            rows.append({
                "tconst": tconst,
                "my_rating": my_rating,
                "rated_at": rated_at,
                "source": "csv",
            })
    return rows

# -------- IMDb user ratings (web) --------

def _fetch_page(url: str) -> Optional[str]:
    headers = {"User-Agent": UA, "Accept-Language": "en-US,en"}
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            if r.status_code == 429:
                # simple backoff
                time.sleep(min(5, 1 + attempt))
                continue
            r.raise_for_status()
            return r.text
        except Exception:
            if attempt == 2:
                return None
            time.sleep(1 + attempt)
    return None

def _parse_ratings_list_html(html: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Parses one ratings page (new IMDb UI). Extract tconst + rating + date if visible.
    Returns (rows, next_url) where rows = [{tconst, my_rating, rated_at, source:"web"}]
    """
    out: List[Dict[str, Any]] = []
    if not html:
        return out, None
    soup = BeautifulSoup(html, "lxml")

    # New UI cards often have /title/ttXXXX/ anchors; extract rating stars nearby.
    for a in soup.select('a[href*="/title/tt"]'):
        href = a.get("href", "")
        m = _TCONST_RE.search(href)
        if not m:
            continue
        tconst = m.group(1)

        # Try to find a numeric rating near the link
        rating: Optional[float] = None

        # Common patterns: span with class "ipc-rating-star--rating"
        star = a.find_next("span", class_=re.compile(r"ipc-rating-star--rating"))
        if star and star.text:
            rating = _to_float(star.text.strip())

        # Fallback: look for "Your rating" text nearby
        if rating is None:
            yr = a.find_next(string=re.compile(r"Your rating", re.I))
            if yr:
                # the number may be in a sibling span
                num = None
                try:
                    num = yr.parent.find_next("span").text
                except Exception:
                    pass
                rating = _to_float(num)

        # Try to find date text (often in small metadata spans)
        rated_at: Optional[str] = None
        date_span = a.find_next("span", string=re.compile(r"\d{1,2}\s+\w+\s+\d{4}"))  # e.g., 5 Jan 2024
        if date_span:
            rated_at = date_span.text.strip()

        out.append({
            "tconst": tconst,
            "my_rating": rating,
            "rated_at": rated_at,
            "source": "web",
        })

    # Find next page link
    next_url = None
    nxt = soup.find("a", attrs={"aria-label": re.compile("Next", re.I)}) or soup.find("a", rel="next")
    if nxt and nxt.get("href"):
        href = nxt["href"]
        next_url = href if href.startswith("http") else IMDB_BASE + href

    return out, next_url

def _ratings_url_for_user(user_id: str, page: int = 1) -> str:
    return f"{IMDB_BASE}/user/{user_id}/ratings/"

def fetch_user_ratings_web(user_id: str, ttl_days: int = DEFAULT_TTL_DAYS, max_pages: int = 10) -> List[Dict[str, Any]]:
    """
    Scrapes the user's IMDb *ratings* pages (only), caches to WEB_CACHE.
    If cache is fresh, returns cached.
    """
    cached = _read_json(WEB_CACHE, default=None)
    if cached and not _is_stale(cached.get("cached_at", ""), ttl_days):
        return cached.get("rows", [])

    url = _ratings_url_for_user(user_id)
    all_rows: List[Dict[str, Any]] = []
    seen = set()
    for i in range(max_pages):
        html = _fetch_page(url)
        rows, next_url = _parse_ratings_list_html(html or "")
        new = 0
        for r in rows:
            t = r.get("tconst")
            if not t or t in seen:
                continue
            seen.add(t)
            all_rows.append(r)
            new += 1
        if not next_url or new == 0:
            break
        url = next_url

    payload = {"cached_at": _now_iso(), "rows": all_rows, "source": "web_ratings"}
    _atomic_write_json(WEB_CACHE, payload)
    return all_rows

# -------- Merge + profile --------

def merge_user_sources(local: List[Dict[str, Any]], remote: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge CSV and web rows, preferring CSV rating when both present.
    Return as *flat list* of rows (deduped by tconst).
    """
    by_id: Dict[str, Dict[str, Any]] = {}

    def _ingest(rows: Iterable[Dict[str, Any]]):
        for r in rows:
            t = r.get("tconst")
            if not t:
                continue
            cur = by_id.get(t, {})
            # prefer CSV rating when present; otherwise keep any rating we have
            my_rating = _pick(cur.get("my_rating"), r.get("my_rating"))
            if r.get("source") == "csv" and r.get("my_rating") is not None:
                my_rating = r.get("my_rating")
            rated_at = _pick(cur.get("rated_at"), r.get("rated_at"))
            src = "csv+web" if cur else r.get("source") or "unknown"
            by_id[t] = {
                "tconst": t,
                "my_rating": my_rating,
                "rated_at": rated_at,
                "source": src,
            }

    _ingest(local)
    _ingest(remote)

    merged = list(by_id.values())
    # Write a flat history for debugging/inspection
    history_payload = {
        "cached_at": _now_iso(),
        "rows": merged,
        "counts": {"csv": len(local), "web": len(remote), "merged_unique": len(merged)},
    }
    _atomic_write_json(PERSONAL_HISTORY, history_payload)
    return merged

def to_user_profile(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Convert flat rows -> profile map: {tconst: {my_rating, rated_at}}
    Also writes PERSONAL_STATE with a few flags/telemetry.
    """
    prof: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        t = r.get("tconst")
        if not t:
            continue
        prof[t] = {
            "my_rating": _to_float(r.get("my_rating")),
            "rated_at": r.get("rated_at"),
        }

    state = {
        "cached_at": _now_iso(),
        "user_profile_loaded": bool(prof),
        "profile_size": len(prof),
        "sources": {
            "ratings_csv_present": RATINGS_CSV.exists(),
            "web_cache_present": WEB_CACHE.exists(),
        },
    }
    _atomic_write_json(PERSONAL_STATE, state)
    return prof