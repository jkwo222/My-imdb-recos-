# engine/exclusions.py
from __future__ import annotations
import csv, os, re
from pathlib import Path
from typing import Dict, Any, List, Set

from . import imdb_public  # NEW

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm_title(s: str) -> str:
    return _NON_ALNUM.sub(" ", (s or "").strip().lower()).strip()

def _title_year_key(title: str|None, year: Any|None) -> str|None:
    if not title:
        return None
    try:
        yi = int(str(year)[:4]) if year else None
    except Exception:
        yi = None
    if yi is None:
        return None
    return f"{_norm_title(title)}::{yi}"

def load_seen_index(ratings_csv_path: Path) -> Dict[str, Any]:
    """
    Loads seen set from CSV.
    Returns a dict with keys:
      - imdb: set of imdb ids ("tt...")
      - title_year: set of "normtitle::YYYY"
    """
    idx_imdb: Set[str] = set()
    idx_ty:   Set[str] = set()

    if ratings_csv_path.exists():
        with ratings_csv_path.open("r", encoding="utf-8", errors="replace") as fh:
            rd = csv.DictReader(fh)
            for r in rd:
                imdb = (r.get("Const") or r.get("IMDb ID") or r.get("imdb_id") or "").strip()
                t = (r.get("Title") or r.get("Primary Title") or r.get("Original Title") or "").strip()
                year = (r.get("Year") or r.get("Release Year") or r.get("Year Released") or r.get("Original Release Year") or "").strip()
                if imdb.startswith("tt"):
                    idx_imdb.add(imdb)
                key = _title_year_key(t, year)
                if key:
                    idx_ty.add(key)

    return {"imdb": idx_imdb, "title_year": idx_ty}

def merge_with_public(seen_idx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds titles from public ratings page (cached) into the seen index.
    Controlled by:
      IMDB_USER_ID (required unless IMDB_PUBLIC_URL provided)
      IMDB_PUBLIC_URL (optional direct URL)
      IMDB_PUBLIC_MAX_PAGES (default 6)
      IMDB_PUBLIC_FORCE_REFRESH (true/false)
    """
    user_id = os.getenv("IMDB_USER_ID", "").strip()
    public_url = os.getenv("IMDB_PUBLIC_URL", "").strip() or None
    max_pages = int(os.getenv("IMDB_PUBLIC_MAX_PAGES", "6") or "6")
    force = (os.getenv("IMDB_PUBLIC_FORCE_REFRESH", "").strip().lower() in {"1","true","yes","on"})

    if not user_id and not public_url:
        return seen_idx

    try:
        data = imdb_public.fetch_user_ratings(
            user_id or "",
            public_url=public_url,
            max_pages=max_pages,
            force_refresh=force
        )
    except Exception:
        # If IMDb is unreachable, just return what we already had
        return seen_idx

    imdb_ids = set(seen_idx.get("imdb") or set())
    imdb_ids.update([x for x in (data.get("imdb_ids") or []) if isinstance(x, str) and x.startswith("tt")])

    tkeys = set(seen_idx.get("title_year") or set())
    for k in (data.get("title_year_keys") or []):
        if isinstance(k, str) and "::" in k:
            tkeys.add(k)

    return {"imdb": imdb_ids, "title_year": tkeys}

def filter_unseen(items: List[Dict[str, Any]], seen_idx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Strict filter: remove any item whose imdb_id is in seen, OR whose title::year key matches,
    with a tiny tolerance for +/- 1 year in case of metadata slop.
    """
    seen_imdb: Set[str] = set(seen_idx.get("imdb") or [])
    seen_tk:   Set[str] = set(seen_idx.get("title_year") or [])

    out: List[Dict[str, Any]] = []
    for it in items:
        imdb = (it.get("imdb_id") or "").strip()
        title = (it.get("title") or it.get("name") or "").strip()
        year = it.get("year") or it.get("release_year") or it.get("first_air_year")

        is_seen = False

        if imdb and imdb in seen_imdb:
            is_seen = True

        key = _title_year_key(title, year)
        if not is_seen and key and key in seen_tk:
            is_seen = True

        # Tolerance: +/- 1 year
        if not is_seen and title and year:
            try:
                yi = int(str(year)[:4])
                if f"{_norm_title(title)}::{yi-1}" in seen_tk or f"{_norm_title(title)}::{yi+1}" in seen_tk:
                    is_seen = True
            except Exception:
                pass

        if not is_seen:
            out.append(it)

    return out