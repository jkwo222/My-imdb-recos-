# engine/seen_index.py
from __future__ import annotations

import csv
import os
import re
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

try:
    # Optional but better fuzzy match
    from rapidfuzz import fuzz
    def _fuzzy_sim(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return fuzz.token_set_ratio(a, b) / 100.0
except Exception:
    # Fallback: very simple token Jaccard
    def _fuzzy_sim(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        sa = set(a.split())
        sb = set(b.split())
        inter = len(sa & sb)
        union = len(sa | sb) or 1
        return inter / union

# We’ll optionally scrape your public ratings if IMDB_USER_ID / IMDB_RATINGS_URL is set.
# This import is safe even if that module isn’t used (we guard at runtime).
try:
    from .imdb_ingest import scrape_imdb_ratings  # type: ignore
except Exception:
    scrape_imdb_ratings = None  # type: ignore

RATINGS_PATH_ENV = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")
_TCONST_RE = re.compile(r"(tt\d{6,9})", re.IGNORECASE)

__all__ = [
    "load_seen_index",
    "filter_unseen",
    "is_seen",
]

# ------------------------
# Normalization utilities
# ------------------------

def _norm_title(t: str) -> str:
    """ASCII fold, lower, drop punctuation/noise, strip leading articles, collapse whitespace."""
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    # replace punctuation with spaces
    t = re.sub(r"[&]", " and ", t)
    t = re.sub(r"[-—–_:/,.'!?;()]", " ", t)
    # drop leading articles
    t = re.sub(r"^\s*(the|a|an)\s+", "", t)
    # roman numerals to digits for common cases (helps with II/III, etc.)
    roman = {
        " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
        " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
    }
    t = f" {t} "
    for k, v in roman.items():
        t = t.replace(k, v)
    t = " ".join(t.split())
    return t

def _parse_int(x: str) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def _maybe_tconst(cell: str) -> Optional[str]:
    if not cell:
        return None
    m = _TCONST_RE.search(cell)
    return m.group(1).lower() if m else None

# ------------------------
# Load seen signals
# ------------------------

def _read_csv_rows(csv_path: str) -> List[dict]:
    rows: List[dict] = []
    if not os.path.exists(csv_path):
        return rows
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({(k or "").strip(): (v or "").strip() for k, v in (r or {}).items()})
    return rows

def _parse_csv_seen(csv_path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    Returns (id_set, [(title_norm, year), ...]) from a local IMDb ratings CSV.
    Accepts flexible column headers.
    """
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []

    rows = _read_csv_rows(csv_path)
    if not rows:
        return ids, titles

    # Find the best ID column once
    lower_map = {k.lower(): k for k in rows[0].keys()} if rows else {}
    id_key = None
    for cand in ("const", "tconst", "imdb title id", "imdb_id", "id"):
        if cand in lower_map:
            id_key = lower_map[cand]
            break

    for row in rows:
        # ID
        imdb_id = None
        if id_key:
            v = (row.get(id_key) or "").strip()
            if v.startswith("tt"):
                imdb_id = v
        if not imdb_id:
            for v in row.values():
                imdb_id = _maybe_tconst(v)
                if imdb_id:
                    break
        if imdb_id:
            ids.add(imdb_id)

        # Title/year
        title = (
            row.get("Title") or row.get("title") or
            row.get("originalTitle") or row.get("Original Title") or ""
        ).strip()
        y_raw = (
            row.get("Year") or row.get("year") or
            row.get("startYear") or row.get("Release Year") or ""
        ).strip()
        y = _parse_int(y_raw) if y_raw else None
        if title:
            titles.append((_norm_title(title), y))

    return ids, titles

def _scrape_public_seen_from_env() -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    If IMDB_USER_ID or IMDB_RATINGS_URL is set and scraping helper is available,
    pull your public ratings page (newest-first) for incremental seen signals.
    """
    if scrape_imdb_ratings is None:
        return set(), []
    user_id = os.environ.get("IMDB_USER_ID", "").strip()
    ratings_url = os.environ.get("IMDB_RATINGS_URL", "").strip()
    if not (user_id or ratings_url):
        return set(), []

    url = ratings_url or f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    try:
        items = scrape_imdb_ratings(url, max_pages=50)  # respects IMDb politely
    except Exception:
        return set(), []

    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    for i in items:
        iid = getattr(i, "imdb_id", "") or ""
        if iid.startswith("tt"):
            ids.add(iid)
        t = getattr(i, "title", "")
        y = getattr(i, "year", None)
        titles.append((_norm_title(t), int(y) if isinstance(y, int) else None))
    return ids, titles

def _build_index(ids: Iterable[str], titles: List[Tuple[str, Optional[int]]]) -> Dict[str, bool]:
    """
    Back-compat shape: dict of {imdb_id: True} plus a private field with normalized titles.
    """
    idx: Dict[str, bool] = {tid.lower(): True for tid in ids if (tid or "").startswith("tt")}
    # Stash normalized title/year pairs for robust matching
    idx["__titles__"] = True           # marker
    idx["_titles_norm_pairs"] = titles  # type: ignore
    return idx

def load_seen_index(csv_path: Optional[str] = None) -> Dict[str, bool]:
    """
    Primary entry point used by runner.
    Combines local CSV + optional public ratings page into a single index.
    """
    csv_path = (csv_path or "").strip() or RATINGS_PATH_ENV
    ids_csv, titles_csv = _parse_csv_seen(csv_path)
    ids_web, titles_web = _scrape_public_seen_from_env()

    ids = set(ids_csv) | set(ids_web)
    titles = titles_csv + titles_web
    return _build_index(ids, titles)

# ------------------------
# Matching / filtering
# ------------------------

def _matches_seen_by_title(pool_title: str, pool_year: Optional[int], seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    nt = _norm_title(pool_title)
    if not nt:
        return False
    for st, sy in seen_pairs:
        # quick exact after normalization
        if nt == st:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
        # fuzzy fallback
        if _fuzzy_sim(nt, st) >= 0.93:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
    return False

def is_seen(title: str, imdb_id: Optional[str] = None, year: Optional[int] = None, seen_idx: Optional[Dict[str, bool]] = None) -> bool:
    """
    Convenience check usable elsewhere. If no seen_idx passed, we load from disk/env.
    """
    idx = seen_idx or load_seen_index()
    if imdb_id and imdb_id in idx:
        return True
    pairs: List[Tuple[str, Optional[int]]] = idx.get("_titles_norm_pairs", []) if isinstance(idx, dict) else []
    return _matches_seen_by_title(title or "", year, pairs)

def filter_unseen(pool: List[Dict], seen_idx: Dict[str, bool]) -> List[Dict]:
    """
    Drops items that appear to be seen by IMDb id (if available) or by robust title+year match.
    Note: discover items often lack imdb_id; we rely on title/year matching here.
    """
    pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict] = []
    for it in pool:
        title = it.get("title") or it.get("name") or ""
        year = it.get("year")
        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, year, pairs):
            continue
        out.append(it)
    return out