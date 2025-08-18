# engine/exclusions.py
from __future__ import annotations
import csv
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from .imdb_public import load_public_seen_from_env

# Title normalization
_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.I)

def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = _NON_ALNUM.sub(" ", s)
    return " ".join(s.split())

def _title_year_key(title: str, year: int | str | None) -> str:
    y = ""
    if isinstance(year, int):
        y = str(year)
    elif isinstance(year, str) and year.strip().isdigit():
        y = year.strip()
    return f"{_norm_title(title)}::{y}"

def _extract_imdb_from_url(url: str) -> str | None:
    m = re.search(r"/title/(tt\d{7,8})", url or "")
    return m.group(1) if m else None

def load_seen_index(ratings_csv: Path) -> Dict[str, bool]:
    """
    Build a set-like dict of seen IDs and title-year keys from ratings.csv.
    Accepts several common schemas:
      - 'imdb_id' / 'const' / 'IMDb Const'
      - 'URL' with /title/tt.../
      - 'Title' + 'Year' (or 'originalTitle' / 'Year')
    """
    seen: Dict[str, bool] = {}
    if not ratings_csv.exists():
        return seen

    with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            imdb_id = row.get("imdb_id") or row.get("const") or row.get("IMDb Const")
            if not imdb_id and row.get("URL"):
                imdb_id = _extract_imdb_from_url(row.get("URL"))
            if isinstance(imdb_id, str) and imdb_id.startswith("tt"):
                seen[imdb_id] = True

            title = row.get("title") or row.get("Title") or row.get("originalTitle") or row.get("Original Title")
            year = row.get("year") or row.get("Year") or row.get("startYear")
            if title:
                key = _title_year_key(title, year)
                seen[key] = True
    return seen

def merge_with_public(seen: Dict[str, bool]) -> Dict[str, bool]:
    public_ids = load_public_seen_from_env()
    for tconst in public_ids:
        seen[tconst] = True
    return seen

def filter_unseen(items: List[Dict], seen_index: Dict[str, bool]) -> List[Dict]:
    """
    Remove any item whose imdb_id matches seen, OR whose (title,year) normalized matches seen.
    """
    out: List[Dict] = []
    for it in items:
        imdb_id = it.get("imdb_id")
        title = it.get("title") or it.get("name")
        year = it.get("year")
        exclude = False
        if isinstance(imdb_id, str) and imdb_id in seen_index:
            exclude = True
        elif title:
            key = _title_year_key(title, year)
            if key in seen_index:
                exclude = True
        if not exclude:
            out.append(it)
    return out