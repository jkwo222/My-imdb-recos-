from __future__ import annotations

import csv
import os
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set, Tuple


# ---------- normalization helpers ----------

_ARTICLES = ("the ", "a ", "an ")

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

_punct_re = re.compile(r"[^\w\s]", re.UNICODE)
_ws_re = re.compile(r"\s+")

def _norm_title(raw: str) -> str:
    """
    Normalize titles for fuzzy-but-deterministic equality:
    - lowercase
    - strip accents
    - remove punctuation
    - collapse whitespace
    """
    s = raw or ""
    s = s.lower().strip()
    s = _strip_accents(s)
    s = _punct_re.sub(" ", s)
    s = _ws_re.sub(" ", s).strip()
    return s

def _drop_leading_article(s: str) -> str:
    for a in _ARTICLES:
        if s.startswith(a):
            return s[len(a):]
    return s


# ---------- exclusion index ----------

@dataclass(frozen=True)
class ExclusionIndex:
    imdb_ids: Set[str]
    tmdb_ids: Set[int]
    # exact match: (type, title_lower, year)
    exact_ty: Set[Tuple[str, str, Optional[int]]]
    # normalized variants without/with articles: (type, norm_title, year)
    norm_ty: Set[Tuple[str, str, Optional[int]]]


def _parse_row(row: Dict[str, str]) -> Dict[str, str]:
    # Accept flexible headers: imdb_id, tmdb_id, title, year, type
    # Also accept bare CSVs where first column may be imdb_id or title.
    out = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k is not None}
    # Coerce common alias headers
    if "imdb" in out and "imdb_id" not in out:
        out["imdb_id"] = out["imdb"]
    if "id" in out and out.get("imdb_id", "") == "" and out.get("tmdb_id", "") == "":
        # If it looks like tt1234567, treat as IMDb; else maybe TMDB numeric
        _id = out["id"]
        if _id.startswith("tt"):
            out["imdb_id"] = _id
        elif _id.isdigit():
            out["tmdb_id"] = _id
    return out


def build_exclusion_index(csv_path: str = "data/exclusions.csv") -> ExclusionIndex:
    imdb_ids: Set[str] = set()
    tmdb_ids: Set[int] = set()
    exact_ty: Set[Tuple[str, str, Optional[int]]] = set()
    norm_ty: Set[Tuple[str, str, Optional[int]]] = set()

    if not os.path.exists(csv_path):
        return ExclusionIndex(imdb_ids, tmdb_ids, exact_ty, norm_ty)

    with open(csv_path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)
        # Heuristic: if comma-separated header present, use DictReader;
        # otherwise, treat each line as a single value (imdb_id or title).
        has_header = any(h in sample.lower() for h in ("imdb", "title", "tmdb", "year", "type"))
        reader: Iterable[Dict[str, str]]
        if has_header:
            reader = csv.DictReader(f)
        else:
            reader = ({"imdb_id_or_title": line.strip()} for line in f if line.strip())

        for raw in reader:
            row = _parse_row(raw)

            # 1) IDs
            imdb = (row.get("imdb_id") or row.get("imdb_id_or_title") or "").strip()
            if imdb.startswith("tt"):
                imdb_ids.add(imdb.lower())

            tmdb = row.get("tmdb_id", "").strip()
            if tmdb.isdigit():
                tmdb_ids.add(int(tmdb))

            # 2) Title/type/year
            title = (row.get("title") or "")
            # If no explicit title and the single column isn't an IMDb id, treat it as title
            if not title and row.get("imdb_id_or_title") and not row["imdb_id_or_title"].startswith("tt"):
                title = row["imdb_id_or_title"]

            if title:
                ty = (row.get("type") or "").strip().lower()
                if ty not in ("movie", "tvseries", "tv", "show", ""):
                    # unknown types ignored; default to empty -> match both
                    pass
                # Normalize a couple of common variants
                if ty == "tv": ty = "tvseries"
                if ty == "show": ty = "tvseries"
                if ty == "":
                    # empty means apply to both; we store two variants
                    types = ("movie", "tvseries")
                else:
                    types = (ty,)

                year_s = (row.get("year") or "").strip()
                year = int(year_s) if year_s.isdigit() else None

                title_lc = title.lower().strip()
                title_norm = _norm_title(title_lc)
                title_norm_noart = _drop_leading_article(title_norm)

                for t in types:
                    exact_ty.add((t, title_lc, year))
                    norm_ty.add((t, title_norm, year))
                    norm_ty.add((t, title_norm_noart, year))

    return ExclusionIndex(imdb_ids, tmdb_ids, exact_ty, norm_ty)


# ---------- matching ----------

def is_excluded(item: Dict[str, object], idx: ExclusionIndex) -> bool:
    """
    Multi-pass exclusion check. An item is excluded if ANY of these match:
      1) imdb_id in CSV (if the item has imdb_id)
      2) tmdb_id in CSV
      3) strict (type, lower(title), year)
      4) normalized (type, norm_title/no-article, year)
      5) title-only fallbacks when year is missing/None on either side
    """
    # IDs
    imdb_id = str(item.get("imdb_id") or "").lower()
    if imdb_id and imdb_id in idx.imdb_ids:
        return True

    tmdb_id = item.get("tmdb_id")
    if isinstance(tmdb_id, int) and tmdb_id in idx.tmdb_ids:
        return True

    # Title / year / type
    ty = str(item.get("type") or "").lower()
    if ty == "tv": ty = "tvseries"

    title = str(item.get("title") or "")
    year = item.get("year")
    year_i = int(year) if isinstance(year, int) else (int(year) if isinstance(year, str) and year.isdigit() else None)

    title_lc = title.lower().strip()
    title_norm = _norm_title(title_lc)
    title_norm_noart = _drop_leading_article(title_norm)

    # exact year
    if (ty, title_lc, year_i) in idx.exact_ty:
        return True
    if (ty, title_norm, year_i) in idx.norm_ty:
        return True
    if (ty, title_norm_noart, year_i) in idx.norm_ty:
        return True

    # title-only fallbacks (when year is missing or mismatched in source)
    if (ty, title_lc, None) in idx.exact_ty:
        return True
    if (ty, title_norm, None) in idx.norm_ty:
        return True
    if (ty, title_norm_noart, None) in idx.norm_ty:
        return True

    return False


def filter_excluded(items: Iterable[Dict[str, object]], idx: ExclusionIndex) -> list[Dict[str, object]]:
    return [it for it in items if not is_excluded(it, idx)]