from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
from rapidfuzz import fuzz

# ============================================================
# Title normalization + fuzzy matching
# ============================================================

_ROMAN = {
    " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
    " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
}
_TCONST_RE = re.compile(r"(tt\d+)", re.IGNORECASE)

def _norm_title(s: str) -> str:
    """Normalize titles for robust equality/fuzzy checks."""
    if not s:
        return ""
    s = s.lower().strip()

    # remove text inside parentheses (often contains years, versions)
    out, depth = [], 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    s = "".join(out)

    s = s.replace("&", " and ")
    s = re.sub(r"[-—–_:/,.'!?;]", " ", s)
    s = f" {s} "

    # roman numerals -> digits for common sequels
    for k, v in _ROMAN.items():
        s = s.replace(k, v)

    # drop leading 'the'
    s = re.sub(r"^\s*the\s+", "", s)

    # collapse spaces
    s = " ".join(t for t in s.split() if t)
    return s


def _fuzzy_sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0


# ============================================================
# ratings.csv ingestion (flexible columns + diagnostics)
# ============================================================

# Very common column names we see "in the wild"
POSSIBLE_TCONST_COLUMNS = [
    "const", "tconst", "imdb_id", "IMDbID", "imdbId", "titleId", "TitleId",
    "url", "URL", "IMDb URL",
]
POSSIBLE_TITLE_COLUMNS = ["Title", "title", "originalTitle", "Original Title"]
POSSIBLE_YEAR_COLUMNS  = ["Year", "year", "startYear", "Release Year"]

def _extract_tt_any(s: Any) -> Optional[str]:
    if s is None:
        return None
    m = _TCONST_RE.search(str(s))
    return m.group(1).lower() if m else None


def _to_int_year(x: Any) -> Optional[int]:
    if x is None:
        return None
    sx = str(x).strip()
    return int(sx) if sx.isdigit() else None


def load_seen_index_from_paths(paths: Sequence[Path]) -> Tuple[Set[str], List[Tuple[str, Optional[int]]], Dict[str, Any]]:
    """
    Read 0..N candidate ratings files and return:
      - imdb_ids: set[str] of 'tt...' values
      - title_year_pairs: list of (normalized_title, optional_year)
      - diagnostics: dict for logging/debug artifacts
    Works even if the file only has Title/Year (no IMDb ids), or only an IMDb URL.
    """
    imdb_ids: Set[str] = set()
    title_year_pairs: List[Tuple[str, Optional[int]]] = []
    diags: Dict[str, Any] = {
        "paths_checked": [str(p) for p in paths],
        "found_files": [],
        "errors": [],
        "by_file": {},
        "aggregate": {"ids": 0, "title_pairs": 0},
    }

    for p in paths:
        file_diag: Dict[str, Any] = {"path": str(p), "exists": p.exists(), "read": False}
        if not (p.exists() and p.is_file()):
            diags["by_file"][str(p)] = file_diag
            continue
        try:
            df = pd.read_csv(p)
            file_diag["read"] = True
            file_diag["columns"] = list(df.columns)
        except Exception as ex:
            file_diag["error"] = f"read_error: {ex!r}"
            diags["errors"].append(file_diag["error"])
            diags["by_file"][str(p)] = file_diag
            continue

        # --- IMDb ids ---
        ids_local: Set[str] = set()
        id_col = next((c for c in POSSIBLE_TCONST_COLUMNS if c in df.columns), None)
        if id_col:
            ids_local = set(
                v for v in df[id_col].map(_extract_tt_any).dropna().astype(str) if v.startswith("tt")
            )
        else:
            # fall back: scan all columns for embedded tt-ids
            for c in df.columns:
                cand = df[c].map(_extract_tt_any).dropna().astype(str)
                ids_local.update(v for v in cand if v.startswith("tt"))

        # --- Titles/Years for fuzzy matching ---
        titles_local: List[Tuple[str, Optional[int]]] = []
        t_col = next((c for c in POSSIBLE_TITLE_COLUMNS if c in df.columns), None)
        if t_col:
            t_series = df[t_col].fillna("").astype(str)
            y_col = next((c for c in POSSIBLE_YEAR_COLUMNS if c in df.columns), None)
            if y_col:
                y_series = df[y_col].map(_to_int_year)
            else:
                y_series = pd.Series([None] * len(df))
            titles_local = [(_norm_title(t), (int(y) if y is not None else None)) for t, y in zip(t_series, y_series)]

        # record + merge
        file_diag["ids_count"] = len(ids_local)
        file_diag["titles_count"] = len([1 for t, _ in titles_local if t])
        file_diag["ids_sample"] = sorted(list(ids_local))[:5]
        diags["by_file"][str(p)] = file_diag
        diags["found_files"].append(str(p))

        imdb_ids |= ids_local
        title_year_pairs.extend(titles_local)

    diags["aggregate"]["ids"] = len(imdb_ids)
    diags["aggregate"]["title_pairs"] = len([1 for t, _ in title_year_pairs if t])
    return imdb_ids, title_year_pairs, diags


# ============================================================
# Seen filtering
# ============================================================

def seen_match_by_title(title: str, year: Optional[int], seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    """Return True if (title,year) looks seen based on normalized/fuzzy rules."""
    nt = _norm_title(title)
    if not nt:
        return False
    for st, sy in seen_pairs:
        if not st:
            continue

        # exact match after normalization
        if nt == st:
            if sy is None or year is None or abs(int(year) - int(sy)) <= 1:
                return True

        # fuzzy backup
        if _fuzzy_sim(nt, st) >= 0.93:
            if sy is None or year is None or abs(int(year) - int(sy)) <= 1:
                return True
    return False


def filter_unseen(items: List[Dict[str, Any]], seen_ids: Set[str], seen_pairs: List[Tuple[str, Optional[int]]]) -> List[Dict[str, Any]]:
    """
    Drop items that appear to be seen by:
      1) exact IMDb id match (if present),
      2) robust normalized/fuzzy title+year match.
    Items are returned in original order.
    """
    out: List[Dict[str, Any]] = []
    for it in items:
        iid = str(it.get("imdb_id") or "").strip().lower()
        if iid and iid in seen_ids:
            continue
        title = (it.get("title") or it.get("name") or "").strip()
        year = it.get("year")
        if title and seen_match_by_title(title, year, seen_pairs):
            continue
        out.append(it)
    return out


# ============================================================
# Scoring
# ============================================================

def _coerce_vote(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def add_match_scores(items: List[Dict[str, Any]], tv_penalty_points: float = 2.0) -> List[Dict[str, Any]]:
    """
    Adds a 'match' field in [0,100] to each item (mutates in-place, also returns the list).
    Formula:
      base = (tmdb_vote or vote_average) / 10.0   # audience proxy in [0,1]
      penalty = tv_penalty_points / 100 if TV else 0
      match = max(0, base - penalty) * 100, rounded to 1 decimal
    TV detection: media_type startswith 'tv' OR type == 'tvSeries'
    """
    for it in items:
        raw_vote = (
            it.get("tmdb_vote", None)
            if it.get("tmdb_vote", None) is not None
            else it.get("vote_average", 0.0)
        )
        aud = max(0.0, min(1.0, _coerce_vote(raw_vote) / 10.0))

        mtype = (it.get("media_type") or it.get("kind") or "").lower()
        is_tv = (isinstance(mtype, str) and mtype.startswith("tv")) or (it.get("type") == "tvSeries")
        penalty = (tv_penalty_points / 100.0) if is_tv else 0.0

        match = round(100.0 * max(0.0, aud - penalty), 1)
        it["match"] = match
    return items


def apply_match_cut(items: List[Dict[str, Any]], min_cut: float) -> List[Dict[str, Any]]:
    """Return only items with match >= min_cut (non-destructive)."""
    cut = float(min_cut)
    return [it for it in items if float(it.get("match", 0.0)) >= cut]


# ============================================================
# Optional: small helpers to produce summary/debug payloads
# ============================================================

def summarize_selection(items: List[Dict[str, Any]], min_cut: float) -> Dict[str, Any]:
    """
    Produce a tiny summary you can embed in diagnostics.
    Assumes 'match' has already been added to items.
    """
    above = [it for it in items if float(it.get("match", 0.0)) >= float(min_cut)]
    return {
        "count_total": len(items),
        "min_match_cut": float(min_cut),
        "count_above_cut": len(above),
        "sample_above_cut": [
            {
                "title": it.get("title") or it.get("name"),
                "year": it.get("year"),
                "match": it.get("match"),
                "media_type": it.get("media_type") or it.get("type") or it.get("kind"),
                "providers": it.get("watch_available") or it.get("providers"),
            }
            for it in above[:10]
        ],
    }