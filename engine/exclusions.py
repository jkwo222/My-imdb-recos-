# engine/exclusions.py
from __future__ import annotations
import csv
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from .imdb_public import load_public_seen_from_env

# --- Normalization helpers ----------------------------------------------------

_STOPWORDS = {"the", "a", "an", "and", "of", "part"}
_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = _NON_ALNUM.sub(" ", s)
    toks = [t for t in s.split() if t and t not in _STOPWORDS]
    return " ".join(toks)

def _tokset(s: str) -> Set[str]:
    return set(_norm_title(s).split())

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

def _int_year(y) -> int | None:
    try:
        yi = int(str(y).strip()[:4])
        if 1880 <= yi <= 2100:
            return yi
    except Exception:
        pass
    return None

def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    union = len(a | b)
    return inter / union

# --- Seen index building ------------------------------------------------------

def load_seen_index(ratings_csv: Path) -> Dict[str, bool]:
    """
    Build a dict-like index of seen items from ratings.csv:
      - imdb_id (tt...)
      - normalized (title, year) pairs  (with variants & tolerance applied at filter time)
    Supports common CSV headers: Const/imdb_id/URL/Title/Original Title/Year/startYear.
    """
    seen: Dict[str, bool] = {}
    if not ratings_csv.exists():
        return seen

    with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # tt id
            imdb_id = None
            for k in ("imdb_id","const","IMDb Const","Const","IMDB_ID","ID"):
                v = row.get(k)
                if isinstance(v, str) and v.startswith("tt"):
                    imdb_id = v.strip()
                    break
            if not imdb_id:
                url = row.get("URL") or row.get("Url") or row.get("url")
                if isinstance(url, str):
                    imdb_id = _extract_imdb_from_url(url)
            if imdb_id:
                seen[imdb_id] = True

            # title + year
            title = row.get("title") or row.get("Title") or row.get("originalTitle") or row.get("Original Title")
            year = row.get("year") or row.get("Year") or row.get("startYear")
            if title:
                key = _title_year_key(title, year)
                seen[key] = True

    return seen

def merge_with_public(seen: Dict[str, bool]) -> Dict[str, bool]:
    """
    Merge your public IMDb user ratings (tconsts) into the seen index.
    """
    public_ids = load_public_seen_from_env()
    for tconst in public_ids:
        seen[tconst] = True
    return seen

# --- Strict filtering ---------------------------------------------------------

def _maybe_years(y) -> List[int]:
    yi = _int_year(y)
    if yi is None:
        return []
    return [yi - 1, yi, yi + 1]  # ±1 tolerance

def _candidate_keys_from_item(it: Dict) -> List[str]:
    title = it.get("title") or it.get("name") or it.get("original_title") or it.get("original_name") or ""
    year = it.get("year")
    keys = []
    for yi in _maybe_years(year) or [None]:
        keys.append(_title_year_key(title, yi))
    return keys

def _fuzzy_seen_by_title(it: Dict, seen_keys: Set[str], thresh: float = 0.90) -> bool:
    """
    Fuzzy fallback: token-set Jaccard on normalized titles, with ±1 year tolerance if present.
    """
    title = it.get("title") or it.get("name") or it.get("original_title") or it.get("original_name")
    if not title:
        return False
    tset = _tokset(title)
    # Try exact year box first
    years = _maybe_years(it.get("year"))
    for key in seen_keys:
        if "::" not in key:
            continue
        t_norm, y = key.split("::", 1)
        # year tolerant check
        y_ok = True
        if y and years:
            try:
                yi = int(y)
                y_ok = yi in years
            except Exception:
                y_ok = True
        # title similarity
        if y_ok and _jaccard(tset, set(t_norm.split())) >= thresh:
            return True
    return False

def filter_unseen(items: List[Dict], seen_index: Dict[str, bool]) -> List[Dict]:
    """
    Remove any item whose imdb_id matches seen, OR whose (title,year) normalized matches seen
    (with ±1 year tolerance), OR whose title fuzzy-matches a seen title (token-set Jaccard).
    """
    id_set: Set[str] = {k for k in seen_index.keys() if isinstance(k, str) and k.startswith("tt")}
    key_set: Set[str] = {k for k in seen_index.keys() if "::" in k}

    out: List[Dict] = []
    for it in items:
        imdb_id = it.get("imdb_id")
        if isinstance(imdb_id, str) and imdb_id in id_set:
            continue

        # exact normalized keys (+- year variants)
        drop_by_key = False
        for key in _candidate_keys_from_item(it):
            if key in key_set:
                drop_by_key = True
                break
        if drop_by_key:
            continue

        # fuzzy safety net
        if _fuzzy_seen_by_title(it, key_set, thresh=0.90):
            continue

        out.append(it)
    return out