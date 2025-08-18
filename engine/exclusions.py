# engine/exclusions.py
from __future__ import annotations
import csv
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any

from .imdb_public import load_public_seen_from_env

_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.I)
ARTICLES = {"the", "a", "an"}
STOPWORDS = ARTICLES | {"and", "of", "in", "on", "at", "to", "for", "with", "part", "season"}

def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = _NON_ALNUM.sub(" ", s)
    s = " ".join(tok for tok in s.split() if tok)
    # drop leading articles once normalized
    toks = s.split()
    if toks and toks[0] in ARTICLES:
        toks = toks[1:]
    return " ".join(toks)

def _token_set(s: str) -> Set[str]:
    return {t for t in _norm_title(s).split() if t and t not in STOPWORDS}

def _title_year_key(title: str, year: Any) -> str:
    y = ""
    if isinstance(year, int):
        y = str(year)
    elif isinstance(year, str) and year.strip().isdigit():
        y = year.strip()
    return f"{_norm_title(title)}::{y}"

def _extract_imdb_from_url(url: str) -> str | None:
    m = re.search(r"/title/(tt\d{7,8})", url or "", flags=re.I)
    return m.group(1) if m else None

class SeenRegistry:
    """
    Registry aggregating multiple match strategies:
      - imdb_ids: exact tt... matches
      - title_year_keys: exact normalized title + year key
      - tokens_index: map normalized title -> token set (for fuzzy)
    """
    def __init__(self) -> None:
        self.imdb_ids: Set[str] = set()
        self.title_year_keys: Set[str] = set()
        self.tokens_index: Dict[str, Set[str]] = {}

    @classmethod
    def from_csv_and_public(cls, ratings_csv: Path | None, include_public: bool = True) -> "SeenRegistry":
        reg = cls()
        # CSV
        if ratings_csv and ratings_csv.exists():
            with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    imdb_id = row.get("imdb_id") or row.get("const") or row.get("IMDb Const")
                    if not imdb_id and row.get("URL"):
                        imdb_id = _extract_imdb_from_url(row.get("URL"))
                    if isinstance(imdb_id, str) and imdb_id.startswith("tt"):
                        reg.imdb_ids.add(imdb_id)

                    title = row.get("title") or row.get("Title") or row.get("originalTitle") or row.get("Original Title")
                    year  = row.get("year")  or row.get("Year")  or row.get("startYear")
                    if title:
                        reg.title_year_keys.add(_title_year_key(title, year))
                        nt = _norm_title(title)
                        if nt:
                            reg.tokens_index.setdefault(nt, _token_set(title))

        # Public IMDb (recent ratings) — IDs only
        if include_public:
            for tconst in load_public_seen_from_env():
                reg.imdb_ids.add(tconst)
        return reg

def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

def _maybe_same_title(a: str, b_norm: str) -> bool:
    A = _token_set(a)
    B = set(b_norm.split())  # b_norm already normalized
    # subset check (handles subtitles, “Part I/II”, etc.) or high Jaccard
    return (A.issubset(B) or B.issubset(A) or _jaccard(A, B) >= 0.88)

def is_seen_by_registry(title: str | None, year: Any, imdb_id: str | None, reg: SeenRegistry, year_tol: int = 1) -> bool:
    # 1) IMDb ID exact
    if isinstance(imdb_id, str) and imdb_id in reg.imdb_ids:
        return True

    if not title:
        return False

    # 2) exact normalized title + year (± year tolerance)
    try:
        y = int(year)
        years = {y}
        for d in range(1, max(1, year_tol) + 1):
            years.add(y - d); years.add(y + d)
    except Exception:
        years = {""}

    nt = _norm_title(title)
    for yy in years:
        key = f"{nt}::{yy}" if yy != "" else f"{nt}::"
        if key in reg.title_year_keys:
            return True

    # 3) fuzzy token match against known titles
    for seen_norm in reg.tokens_index.keys():
        if _maybe_same_title(title, seen_norm):
            return True
    return False

def filter_unseen(items: List[Dict], seen_index: Dict[str, bool]) -> List[Dict]:
    """
    Backwards-compatible filter that accepts a simple dict from the older API.
    Kept for runner compatibility. (New code should use SeenRegistry + filter_unseen_strict.)
    """
    def _is_seen_legacy(it: Dict) -> bool:
        imdb_id = it.get("imdb_id")
        title = it.get("title") or it.get("name")
        year = it.get("year")
        if isinstance(imdb_id, str) and imdb_id in seen_index:
            return True
        if title and f"{_norm_title(title)}::{str(year) if year is not None else ''}" in seen_index:
            return True
        return False
    return [it for it in items if not _is_seen_legacy(it)]

def filter_unseen_strict(items: List[Dict], reg: SeenRegistry, year_tol: int = 1) -> List[Dict]:
    out: List[Dict] = []
    for it in items:
        title = it.get("title") or it.get("name")
        year  = it.get("year")
        imdb  = it.get("imdb_id")
        if not is_seen_by_registry(title, year, imdb, reg, year_tol=year_tol):
            out.append(it)
    return out