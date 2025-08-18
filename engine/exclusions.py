# engine/exclusions.py
from __future__ import annotations
import csv
import os
import re
from typing import Dict, Any, List, Set
import requests

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm_title(s: str) -> str:
    return _NON_ALNUM.sub(" ", (s or "").strip().lower()).strip()

def _title_year_key(title: str|None, year: Any|None) -> str|None:
    if not title:
        return None
    try:
        y = int(str(year)[:4]) if year is not None else None
    except Exception:
        y = None
    if y:
        return f"{_norm_title(title)}::{y}"
    return None

def _imdb_id_from_row(row: Dict[str, Any]) -> str|None:
    for k in ("imdb_id","Const","const","ID","Id"):
        v = row.get(k)
        if isinstance(v, str) and v.startswith("tt"):
            return v.strip()
    url = row.get("URL") or row.get("Url") or row.get("url")
    if isinstance(url,str):
        m = re.search(r"/title/(tt\d{7,8})", url)
        if m: return m.group(1)
    return None

def load_seen_index(ratings_csv_path) -> Dict[str, bool]:
    """Return dict-like set with imdb ids and title::year keys from CSV."""
    seen: Dict[str, bool] = {}
    try:
        with open(ratings_csv_path, "r", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                imdb = _imdb_id_from_row(r)
                title = r.get("Title") or r.get("Primary Title") or r.get("Original Title")
                year = r.get("Year")
                if imdb:
                    seen[imdb] = True
                key = _title_year_key(title, year)
                if key:
                    seen[key] = True
                    # also add ±1 year tolerance
                    try:
                        yi = int(str(year)[:4])
                        seen[f"{_norm_title(title)}::{yi-1}"] = True
                        seen[f"{_norm_title(title)}::{yi+1}"] = True
                    except Exception:
                        pass
    except FileNotFoundError:
        pass
    return seen

def _fetch_imdb_list_csv(list_id: str) -> List[str]:
    """
    Fetch public IMDb list CSV: https://www.imdb.com/list/lsXXXXXXXXX/export
    Returns list of tconst ids.
    """
    list_id = list_id.strip()
    if not list_id or not list_id.startswith("ls"):
        return []
    url = f"https://www.imdb.com/list/{list_id}/export"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return []
        ids: List[str] = []
        for i, line in enumerate(r.text.splitlines()):
            if i == 0:
                continue  # header
            cols = line.split(",")
            if not cols:
                continue
            # first col is const for export
            tconst = cols[1] if len(cols) > 1 else cols[0]
            tconst = tconst.strip().strip('"')
            if tconst.startswith("tt"):
                ids.append(tconst)
        return list(dict.fromkeys(ids))
    except Exception:
        return []

def merge_with_public(seen_idx: Dict[str, bool]) -> Dict[str, bool]:
    """
    Merge optional extra public IMDb lists into seen index.
    Configure via IMDB_EXTRA_LIST_IDS='lsXXXXXXXX,lsYYYYYYYY'.
    """
    extras = os.getenv("IMDB_EXTRA_LIST_IDS", "")
    if not extras.strip():
        return seen_idx
    added = 0
    for tok in extras.split(","):
        tok = tok.strip()
        if not tok:
            continue
        ids = _fetch_imdb_list_csv(tok)
        for t in ids:
            if t not in seen_idx:
                seen_idx[t] = True
                added += 1
    # tiny hint in log via caller's diag
    seen_idx["_public_added_count"] = bool(added)  # marker; caller may ignore
    return seen_idx

def filter_unseen(items: List[Dict[str, Any]], seen_idx: Dict[str, bool]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        imdb = it.get("imdb_id")
        title = it.get("title") or it.get("name")
        year = it.get("year")
        key = _title_year_key(title, year)
        is_seen = False
        if isinstance(imdb, str) and imdb in seen_idx:
            is_seen = True
        elif key and key in seen_idx:
            is_seen = True
        # also try ±1 year even if not pre-inserted
        if not is_seen and title and year:
            try:
                yi = int(str(year)[:4])
                if f"{_norm_title(title)}::{yi-1}" in seen_idx or f"{_norm_title(title)}::{yi+1}" in seen_idx:
                    is_seen = True
            except Exception:
                pass
        if not is_seen:
            out.append(it)
    return out