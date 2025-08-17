from __future__ import annotations

import csv
import os
import re
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Any, Set


def _norm_title(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"\(.*?\)", "", t)              # remove parenthetical years, etc.
    t = re.sub(r"[^a-z0-9]+", " ", t)          # keep alnum, spaces
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _key_title_year(title: str, year: Any) -> str:
    yy = str(year) if year not in (None, "", "0") else ""
    return f"{_norm_title(title)}::{yy}"


def _read_csv_rows(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in r.items()})
    return rows


def _extract_title_year(row: Dict[str, str]) -> Tuple[str, str]:
    # try a handful of common column names (IMDb export, custom lists)
    title = row.get("title") or row.get("Title") or row.get("primaryTitle") or row.get("originalTitle") or ""
    year = row.get("year") or row.get("Year") or row.get("startYear") or row.get("Release Year") or ""
    return title, year


def _extract_imdb(row: Dict[str, str]) -> str:
    return row.get("const") or row.get("imdb_id") or row.get("IMDb ID") or ""


def build_exclusion_index(cfg: Any) -> Dict[str, Any]:
    """
    Build a robust 'seen/exclude' index from the user's CSV list.
    Multiple checks at runtime:
      - imdb id match (if candidate has imdb_id)
      - exact normalized (title, year)
      - fuzzy title-only fallback if year missing (>= 0.92)
    """
    path = getattr(cfg, "exclude_csv", "data/ratings.csv")
    rows = _read_csv_rows(path)

    imdb_ids: Set[str] = set()
    exact_keys: Set[str] = set()
    titles_only: Set[str] = set()

    for r in rows:
        imdb = _extract_imdb(r)
        if imdb:
            imdb_ids.add(imdb.strip())

        title, year = _extract_title_year(r)
        if title:
            exact_keys.add(_key_title_year(title, year))
            titles_only.add(_norm_title(title))

    return {
        "imdb_ids": imdb_ids,
        "exact_keys": exact_keys,
        "titles_only": titles_only,
    }


def is_excluded(item: Dict[str, Any], index: Dict[str, Any]) -> bool:
    # 1) IMDb id check (if present on item)
    imdb_id = (item.get("imdb_id") or "").strip()
    if imdb_id and imdb_id in index["imdb_ids"]:
        return True

    # 2) Exact normalized title+year
    if _key_title_year(item.get("title", ""), item.get("year")) in index["exact_keys"]:
        return True

    # 3) Fuzzy title-only if we lack year or year seems unreliable
    norm = _norm_title(item.get("title", ""))
    if norm in index["titles_only"]:
        return True

    # 3b) fuzzy ratio against titles-only set (cheap heuristic)
    # do a quick early-exit if we find a high similarity
    for t in (item.get("title") or "", item.get("original_title") or "", item.get("name") or ""):
        if not t:
            continue
        cand = _norm_title(t)
        # direct set membership already tested above; use ratio for near-miss
        best = max((SequenceMatcher(None, cand, ex).ratio() for ex in index["titles_only"]), default=0.0)
        if best >= 0.92:
            return True

    return False


def filter_excluded(items: List[Dict[str, Any]], index: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    kept: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    seen_keys: set = set()

    for it in items:
        # de-dupe here as a final guard
        key = (it.get("type"), it.get("tmdb_id"))
        if key in seen_keys:
            removed.append({**it, "_reason": "duplicate"})
            continue
        seen_keys.add(key)

        if is_excluded(it, index):
            removed.append({**it, "_reason": "excluded"})
        else:
            kept.append(it)

    return kept, removed