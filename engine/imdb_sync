# engine/imdb_sync.py
from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import csv, os, re, time
import requests

ROOT = Path(__file__).resolve().parents[1]
USER_DIR = ROOT / "data" / "user"
USER_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------
# Local ratings.csv loader
# ------------------------

def load_ratings_csv(path: Optional[Path] = None) -> List[Dict[str,Any]]:
    """
    Load data/user/ratings.csv if present.
    Accepts flexible headers; must include a tconst (IMDB ID) column and some rating column if available.
    Returns a list of dicts: [{tconst, my_rating?}, ...]
    """
    path = path or (USER_DIR / "ratings.csv")
    out: List[Dict[str,Any]] = []
    if not path.exists():
        return out

    with path.open("r", encoding="utf-8") as f:
        sn = csv.DictReader(f)
        for row in sn:
            tconst = row.get("tconst") or row.get("const") or row.get("imdb_id")
            if not tconst:
                continue
            rating = None
            for k in ("my_rating","user_rating","rating","Your Rating","your_rating"):
                v = row.get(k)
                if v:
                    try:
                        rating = float(v)
                        break
                    except Exception:
                        pass
            out.append({"tconst": str(tconst), "my_rating": rating})
    return out

# -------------------------------------
# Public IMDb signals (best-effort)
# -------------------------------------

_IMDB_HEADERS = {
    "User-Agent": "my-imdb-recos/1.0 (+github actions)"
}

def _fetch_public_list_csv(url: str) -> List[Dict[str,Any]]:
    out: List[Dict[str,Any]] = []
    try:
        r = requests.get(url, headers=_IMDB_HEADERS, timeout=30)
        r.raise_for_status()
        lines = r.text.splitlines()
        sn = csv.DictReader(lines)
        for row in sn:
            tconst = row.get("const") or row.get("tconst") or row.get("IMDb ID")
            if tconst:
                out.append({"tconst": str(tconst)})
    except Exception:
        pass
    return out

def _scrape_tconsts_from_html(url: str) -> List[str]:
    try:
        r = requests.get(url, headers=_IMDB_HEADERS, timeout=30)
        r.raise_for_status()
        return sorted(set(re.findall(r"/title/(tt\d{7,8})/", r.text)))
    except Exception:
        return []

def fetch_public_list(url: Optional[str]) -> List[Dict[str,Any]]:
    """
    If IMDB_PUBLIC_LIST_URL is provided:
      - if it ends with .csv, read it as a CSV export
      - otherwise, scrape HTML for tconsts
    """
    out: List[Dict[str,Any]] = []
    if not url:
        return out
    url = url.strip()
    if not url:
        return out
    if url.endswith(".csv"):
        return _fetch_public_list_csv(url)
    tconsts = _scrape_tconsts_from_html(url)
    return [{"tconst": t} for t in tconsts]

def fetch_user_ratings_web(imdb_user_id: str) -> List[Dict[str,Any]]:
    """
    Best-effort public fetch of *some* items tied to a user id.
    IMDb doesn't expose an unauthenticated full ratings CSV by user-id,
    so we fall back to “ratings” and/or “watchlist” public pages if they exist.
    This is intentionally lenient: treat presence as weak positive signal.
    """
    out: List[Dict[str,Any]] = []
    uid = (imdb_user_id or "").strip()
    if not uid:
        return out

    urls = [
        f"https://www.imdb.com/user/{uid}/ratings",    # ratings page
        f"https://www.imdb.com/user/{uid}/watchlist",  # watchlist page
        f"https://www.imdb.com/user/{uid}/lists",      # lists index
    ]
    seen = set()
    for u in urls:
        for t in _scrape_tconsts_from_html(u):
            if t not in seen:
                out.append({"tconst": t})
                seen.add(t)
        # be polite
        time.sleep(0.3)
    return out

# -------------------------------------
# Merge + normalize to user profile map
# -------------------------------------

def merge_user_sources(local_rows: List[Dict[str,Any]], remote_rows: List[Dict[str,Any]], public_rows: List[Dict[str,Any]] | None = None) -> List[Dict[str,Any]]:
    by_t: Dict[str,Dict[str,Any]] = {}
    for r in (local_rows or []):
        t = r.get("tconst")
        if not t: 
            continue
        by_t[t] = {"tconst": t, "my_rating": r.get("my_rating")}
    for r in (remote_rows or []):
        t = r.get("tconst")
        if not t: 
            continue
        row = by_t.get(t) or {"tconst": t}
        row.setdefault("from_remote_user", True)
        by_t[t] = row
    for r in (public_rows or []):
        t = r.get("tconst")
        if not t: 
            continue
        row = by_t.get(t) or {"tconst": t}
        row.setdefault("from_public_list", True)
        by_t[t] = row
    return list(by_t.values())

def to_user_profile(rows: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    """
    Map: tconst -> {my_rating?, from_public_list?, from_remote_user?}
    """
    prof: Dict[str,Dict[str,Any]] = {}
    for r in rows:
        t = r.get("tconst")
        if not t:
            continue
        prof[t] = {
            "tconst": t,
            **({ "my_rating": float(r["my_rating"]) } if r.get("my_rating") is not None else {}),
            **({ "from_public_list": True } if r.get("from_public_list") else {}),
            **({ "from_remote_user": True } if r.get("from_remote_user") else {}),
        }
    return prof