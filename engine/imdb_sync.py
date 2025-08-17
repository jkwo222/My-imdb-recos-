from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
USER_DIR = DATA_DIR / "user"
CACHE_DIR = DATA_DIR / "cache"
STATE_DIR = CACHE_DIR / "state"
IMDB_CACHE_DIR = CACHE_DIR / "imdb"
for p in (USER_DIR, CACHE_DIR, STATE_DIR, IMDB_CACHE_DIR):
    p.mkdir(parents=True, exist_ok=True)

RATINGS_CSV = USER_DIR / "ratings.csv"
IMDB_RATINGS_CACHE = IMDB_CACHE_DIR / "user_ratings.json"

# -----------------------
# Helpers: safe datetime
# -----------------------

def _parse_iso_any(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        if "+" in s or s.endswith("Z"):
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        # naive → assume UTC
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _is_stale(cached_at_iso: str, ttl_days: int) -> bool:
    ts = _parse_iso_any(cached_at_iso)
    if not ts:
        return True
    return (_utcnow() - ts) > timedelta(days=ttl_days)

# -----------------------
# Public structures
# -----------------------

@dataclass
class UserProfile:
    # raw evidence
    seen_tconsts: Set[str] = field(default_factory=set)
    seen_titles: Set[Tuple[str, Optional[int]]] = field(default_factory=set)  # (norm_title, year)
    rating_by_tconst: Dict[str, float] = field(default_factory=dict)
    # “DNA”
    genre_counts: Dict[str, float] = field(default_factory=dict)
    director_counts: Dict[str, float] = field(default_factory=dict)
    # misc
    entries: int = 0

def _norm_title(t: str) -> str:
    return (t or "").strip().lower()

# -----------------------
# ratings.csv ingestion
# -----------------------

def load_ratings_csv() -> List[Dict[str, Any]]:
    """
    Expected columns (tolerant): tconst,title,genres,release_date,year,titleType,numVotes,directors,url,date_rated,rating
    """
    rows: List[Dict[str, Any]] = []
    if not RATINGS_CSV.exists():
        return rows
    with RATINGS_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

# -----------------------
# IMDb web fetch (ratings)
# -----------------------

def _imdb_user_ratings_url(user_id: str) -> str:
    return f"https://www.imdb.com/user/{user_id}/ratings"

def fetch_user_ratings_web(user_id: str, ttl_days: int = 3) -> List[Dict[str, Any]]:
    """
    Scrape the user's public IMDb ratings page (first page + pagination if present).
    Very light parsing: title, year (if present), tconst (from link).
    Cached to disk with freshness TTL.
    """
    if not user_id:
        return []

    # Try cache
    if IMDB_RATINGS_CACHE.exists():
        try:
            cached = json.loads(IMDB_RATINGS_CACHE.read_text(encoding="utf-8"))
            if cached and not _is_stale(cached.get("cached_at", ""), ttl_days):
                return cached.get("items", []) or []
        except Exception:
            pass

    url = _imdb_user_ratings_url(user_id)
    headers = {"User-Agent": "my-imdb-recos/1.0 (+github actions)"}
    items: List[Dict[str, Any]] = []

    try:
        r = requests.get(url, headers=headers, timeout=(5, 20))
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Very tolerant—IMDB changes HTML a lot. We look for title links with /title/ttxxxx/
        for a in soup.select('a[href^="/title/tt"]'):
            href = a.get("href", "")
            # format /title/tt0123456/?ref=...
            if "/title/tt" not in href:
                continue
            tt = href.split("/title/")[1].split("/")[0]
            title = a.text.strip()
            if not title:
                continue
            # Try to find a nearby year
            year = None
            yel = a.find_next_sibling("span")
            if yel:
                try:
                    ytxt = "".join(ch for ch in yel.text if ch.isdigit())
                    year = int(ytxt) if ytxt else None
                except Exception:
                    year = None
            items.append({"tconst": tt, "title": title, "year": year})

    except Exception:
        # If scrape fails, keep items empty but write cache to avoid hammering
        pass

    IMDB_RATINGS_CACHE.write_text(
        json.dumps({"cached_at": _utcnow().isoformat().replace("+00:00", "Z"), "items": items}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return items

# -----------------------
# Merge sources → profile
# -----------------------

def merge_user_sources(local_rows: List[Dict[str, Any]], remote_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_tconst: Dict[str, Dict[str, Any]] = {}
    def add(row: Dict[str, Any]) -> None:
        t = (row.get("tconst") or "").strip()
        if not t:
            return
        if t not in by_tconst:
            by_tconst[t] = {}
        by_tconst[t].update(row)
    for r in local_rows:
        add(r)
    for r in remote_rows:
        add(r)
    return list(by_tconst.values())

def _add_weight(bucket: Dict[str, float], key: str, w: float) -> None:
    if not key:
        return
    bucket[key] = bucket.get(key, 0.0) + float(w)

def to_user_profile(rows: List[Dict[str, Any]]) -> UserProfile:
    prof = UserProfile()
    if not rows:
        return prof

    now = _utcnow()

    for r in rows:
        tconst = (r.get("tconst") or "").strip()
        title = (r.get("title") or "").strip()
        year = None
        # year from explicit column or derived from release_date
        if r.get("year"):
            try: year = int(str(r["year"]).strip())
            except Exception: year = None
        if not year and r.get("release_date"):
            try: year = int(r["release_date"][:4])
            except Exception: year = None

        # rating value (if present)
        try:
            rating = float(r.get("rating") or 0.0)
        except Exception:
            rating = 0.0

        # recency from date_rated (optional)
        recency_mult = 1.0
        if r.get("date_rated"):
            dr = _parse_iso_any(r["date_rated"])
            if dr:
                # within 2 years → 1.2x, 2–5 years → 1.1x
                delta_days = (now - dr).days
                if delta_days <= 730:
                    recency_mult = 1.2
                elif delta_days <= 1825:
                    recency_mult = 1.1

        # sentiment weight (center 5 → mild pos/neg)
        sentiment = (rating - 5.0) / 5.0  # [-1, +1]
        base_w = 1.0 + max(0.0, sentiment)  # dislike doesn’t punish genres; like boosts
        w = base_w * recency_mult

        # genres (pipe/comma)
        genres = []
        graw = r.get("genres") or ""
        if graw:
            if "|" in graw:
                genres = [g.strip() for g in graw.split("|") if g.strip()]
            else:
                genres = [g.strip() for g in graw.split(",") if g.strip()]
        for g in genres:
            _add_weight(prof.genre_counts, g, w)

        # directors (comma or pipe)
        d_raw = r.get("directors") or ""
        directors = []
        if d_raw:
            if "|" in d_raw:
                directors = [d.strip() for d in d_raw.split("|") if d.strip()]
            else:
                directors = [d.strip() for d in d_raw.split(",") if d.strip()]
        for d in directors:
            _add_weight(prof.director_counts, d, w * 0.75)

        # seen tracking
        if tconst:
            prof.seen_tconsts.add(tconst)
        if title:
            prof.seen_titles.add((_norm_title(title), year))
        if tconst and rating:
            prof.rating_by_tconst[tconst] = rating

        prof.entries += 1

    # Persist snapshots for inspection
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "entries": prof.entries,
        "seen_tconsts": sorted(list(prof.seen_tconsts)),
        "seen_titles": sorted([f"{t}:{y or ''}" for t, y in prof.seen_titles]),
        "genre_counts": prof.genre_counts,
        "director_counts": prof.director_counts,
        "ratings": prof.rating_by_tconst,
        "saved_at": _utcnow().isoformat().replace("+00:00", "Z"),
    }
    (STATE_DIR / "personal_state.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    # lightweight history append
    hist_path = STATE_DIR / "personal_history.json"
    try:
        hist = json.loads(hist_path.read_text(encoding="utf-8")) if hist_path.exists() else []
    except Exception:
        hist = []
    hist.append({"ts": snapshot["saved_at"], "entries": prof.entries})
    hist_path.write_text(json.dumps(hist[-50:], ensure_ascii=False, indent=2), encoding="utf-8")

    return prof