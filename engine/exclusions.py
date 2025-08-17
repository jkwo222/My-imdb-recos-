from __future__ import annotations

import csv
import os
import re
from typing import Dict, Iterable, Set, Tuple

# -------- title normalization --------

_ARTICLES = ("the", "a", "an")
_SUBS = (
    (r"[‘’´`]", "'"),
    (r"[“”]", '"'),
    (r"&", "and"),
)

_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")

def normalize_title(s: str) -> str:
    """
    Lowercase; normalize quotes/&; strip punctuation; collapse whitespace; drop leading articles.
    """
    if not s:
        return ""
    t = s.strip().lower()
    for pat, rep in _SUBS:
        t = re.sub(pat, rep, t)
    t = _PUNCT_RE.sub(" ", t)
    t = _WS_RE.sub(" ", t).strip()
    # drop leading article (e.g., 'the matrix' -> 'matrix')
    parts = t.split(" ", 1)
    if parts and parts[0] in _ARTICLES and len(parts) > 1:
        t = parts[1]
    return t

def _title_keys(title: str, year: int | None) -> Set[str]:
    """
    Multiple keys we’ll use for matching:
      - normalized title
      - normalized title + year
    """
    n = normalize_title(title)
    keys = set()
    if n:
        keys.add(n)
        if year is not None:
            keys.add(f"{n}|{year}")
    return keys

# -------- CSV loading (robust against different IMDb export headers) --------

def _read_csv_rows(path: str) -> Iterable[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        # autodetect delimiter (commas are standard, but be generous)
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        reader = csv.DictReader(f, dialect=dialect)
        for row in reader:
            yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

def _parse_year(s: str | None) -> int | None:
    if not s:
        return None
    s = s.strip()
    # accept "1999", "1999–", "1999-2001", etc.
    if len(s) >= 4 and s[:4].isdigit():
        try:
            return int(s[:4])
        except Exception:
            return None
    return None

def load_exclusions_from_csv(path: str) -> Tuple[Set[str], Set[str]]:
    """
    Returns (title_keys, imdb_ids)
      title_keys contains normalized title keys and title|year keys
      imdb_ids contains any 'tt...' ids found
    Supports common IMDb exports and custom lists. Looks for columns:
      - title or Title
      - year / Year / Release Year / startYear
      - const / imdb_id / IMDb ID
    """
    title_keys: Set[str] = set()
    imdb_ids: Set[str] = set()

    if not os.path.exists(path):
        return title_keys, imdb_ids

    # Likely column names across IMDb exports & custom sheets:
    TITLE_COLS = ("title", "Title", "originalTitle", "primaryTitle", "Name")
    YEAR_COLS  = ("year", "Year", "Release Year", "startYear")
    ID_COLS    = ("const", "imdb_id", "IMDb ID", "imdbId", "tconst")

    for row in _read_csv_rows(path):
        # imdb id
        for c in ID_COLS:
            v = row.get(c)
            if v and v.startswith("tt"):
                imdb_ids.add(v)
                break

        # title + year
        title = None
        for c in TITLE_COLS:
            v = row.get(c)
            if v:
                title = v
                break
        yr = None
        for c in YEAR_COLS:
            v = row.get(c)
            if v:
                yr = _parse_year(v)
                break

        if title:
            for k in _title_keys(title, yr):
                title_keys.add(k)

            # also add +/- 1 year keys for safety when lists use slightly different years
            base = normalize_title(title)
            if base and yr is not None:
                title_keys.add(f"{base}|{yr-1}")
                title_keys.add(f"{base}|{yr+1}")

    return title_keys, imdb_ids

# -------- filtering utilities --------

def build_exclusion_index(csv_path: str) -> Dict[str, Set[str]]:
    """
    Build a dict with sets we can check quickly:
      {
        "title_keys": {...},
        "imdb_ids": {...},      # we include for future use if items carry imdb ids
      }
    """
    title_keys, imdb_ids = load_exclusions_from_csv(csv_path)
    return {
        "title_keys": title_keys,
        "imdb_ids": imdb_ids,
    }

def is_excluded(item: Dict, idx: Dict[str, Set[str]]) -> bool:
    """
    Decide if an item should be excluded using multiple checks.
    Item format expected from TMDB discovery normalizer:
      { "title": str, "year": int|None, "tmdb_id": int, "type": "movie"/"tvSeries", ... }
    We currently do not have imdb_id on items coming from discover, so we rely on title/year.
    If in the future the item carries 'imdb_id', we’ll use that too.
    """
    title = item.get("title") or ""
    year = item.get("year")
    n = normalize_title(title)

    # 1) strong title+year matches (exact & +/-1 baked into the index)
    keys = _title_keys(title, year)
    if any(k in idx["title_keys"] for k in keys):
        return True

    # 2) fallback: title-only match (when years are missing or differ)
    if n and n in idx["title_keys"]:
        return True

    # 3) imdb id (if present on the item)
    imdb_id = item.get("imdb_id") or item.get("imdbId") or item.get("imdb")
    if imdb_id and imdb_id in idx["imdb_ids"]:
        return True

    return False

def filter_excluded(items: Iterable[Dict], idx: Dict[str, Set[str]]) -> Tuple[list[Dict], int]:
    """
    Return (kept, excluded_count).
    """
    kept = []
    excluded = 0
    for it in items:
        if is_excluded(it, idx):
            excluded += 1
        else:
            kept.append(it)
    return kept, excluded