# engine/scoring.py
from __future__ import annotations

import csv
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rapidfuzz import fuzz

# Optional: used only when IMDB_USER_ID is set (safe if absent at runtime)
try:
    from .imdb_ingest import scrape_imdb_ratings  # noqa: F401
except Exception:  # pragma: no cover
    scrape_imdb_ratings = None  # type: ignore


__all__ = [
    "load_seen_index",
    "filter_unseen",
    "score_items",
]


# --------------------------
# Title normalization helpers
# --------------------------

_ROMAN = {
    " i ": " 1 ",
    " ii ": " 2 ",
    " iii ": " 3 ",
    " iv ": " 4 ",
    " v ": " 5 ",
    " vi ": " 6 ",
    " vii ": " 7 ",
    " viii ": " 8 ",
    " ix ": " 9 ",
    " x ": " 10 ",
}


def _norm_title(s: str) -> str:
    """Lowercase, strip punctuation, normalize articles/roman numerals for robust matching."""
    if not s:
        return ""
    s = s.lower().strip()

    # remove parenthetical suffixes like "(2024)", "(extended cut)"
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
    s = re.sub(r"[-—–_:/,.'\"!?;]", " ", s)
    s = f" {s} "

    for k, v in _ROMAN.items():
        s = s.replace(k, v)

    # drop leading "the "
    s = re.sub(r"^\s*the\s+", "", s)
    s = " ".join(t for t in s.split() if t)
    return s


def _fuzzy_sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0


# --------------------------
# Seen data (CSV + optional IMDb public ratings)
# --------------------------

_ID_HEADER_CANDS = ("const", "tconst", "imdb title id", "imdb_id", "id")
_TITLE_CANDS = ("title", "originalTitle", "Original Title", "Title", "name")
_YEAR_CANDS = ("year", "startYear", "Release Year", "Year")


def _pick_header(fieldnames: Optional[List[str]], candidates: Iterable[str]) -> Optional[str]:
    if not fieldnames:
        return None
    lower = [h.lower().strip() for h in fieldnames]
    for cand in candidates:
        if cand.lower() in lower:
            return fieldnames[lower.index(cand.lower())]
    return None


def _parse_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    return int(s) if s.isdigit() else None


def _parse_csv_seen(csv_path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    Read a ratings/seen CSV and return (imdb_ids, [(normalized_title, year_or_None), ...])
    Works with multiple possible header spellings.
    """
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []

    if not csv_path or not os.path.exists(csv_path):
        return ids, titles

    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        id_key = _pick_header(rdr.fieldnames, _ID_HEADER_CANDS)
        title_key = _pick_header(rdr.fieldnames, _TITLE_CANDS)
        year_key = _pick_header(rdr.fieldnames, _YEAR_CANDS)

        for row in rdr:
            # IMDb id path
            if id_key:
                iid = (row.get(id_key) or "").strip()
                if iid.startswith("tt"):
                    ids.add(iid)

            # title/year path
            title = (row.get(title_key or "", "") or "").strip()
            year = _parse_int(row.get(year_key or "", "") or "") if year_key else None
            if title:
                titles.append((_norm_title(title), year))

    return ids, titles


def _scrape_public_seen_from_env() -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    Optionally scrape public IMDb ratings if IMDB_USER_ID is provided.
    Safe no-op if imdb_ingest is not available.
    """
    user_id = os.environ.get("IMDB_USER_ID", "").strip()
    if not user_id or not scrape_imdb_ratings:
        return set(), []

    url = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    try:
        items = scrape_imdb_ratings(url, max_pages=50)  # type: ignore[misc]
    except Exception:
        return set(), []

    ids: set[str] = set()
    title_pairs: List[Tuple[str, Optional[int]]] = []

    for it in items:
        iid = getattr(it, "imdb_id", "") or ""
        if iid.startswith("tt"):
            ids.add(iid)
        title = getattr(it, "title", "") or ""
        year = getattr(it, "year", None)
        title_pairs.append((_norm_title(title), year if isinstance(year, int) else None))

    return ids, title_pairs


def load_seen_index(csv_path: str) -> Dict[str, Any]:
    """
    Return a dict-like index for fast "seen" tests.
    Keys:
      - imdb_id -> True
      - "_titles_norm_pairs" -> List[(normalized_title, Optional[int])]
    """
    ids_csv, titles_csv = _parse_csv_seen(csv_path)
    ids_web, titles_web = _scrape_public_seen_from_env()

    ids = set(ids_csv) | set(ids_web)
    titles = titles_csv + titles_web

    idx: Dict[str, Any] = {iid: True for iid in ids}
    idx["_titles_norm_pairs"] = titles
    return idx


# --------------------------
# Filtering “seen” from pool
# --------------------------

def _matches_seen_by_title(pool_title: str, pool_year: Optional[int],
                           seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    nt = _norm_title(pool_title)
    for st, sy in seen_pairs:
        if not st:
            continue
        # exact normalized match
        if nt == st:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
        # fuzzy fallback
        if _fuzzy_sim(nt, st) >= 0.93:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
    return False


def filter_unseen(pool: List[Dict[str, Any]], seen_idx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Remove items already seen (by IMDb id when present, or robust by title+year).
    Accepts items from TMDB discover or enriched structures.
    """
    seen_pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict[str, Any]] = []

    for it in pool:
        iid = (it.get("imdb_id") or "").strip()
        title = (it.get("title") or it.get("name") or "").strip()
        # handle various year fields
        year = it.get("year") or it.get("release_year") or it.get("first_air_year")

        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, year if isinstance(year, int) else None, seen_pairs):
            continue

        out.append(it)

    return out


# --------------------------
# Scoring
# --------------------------

def _audience_proxy(it: Dict[str, Any]) -> float:
    """
    Normalize TMDB vote to 0..1.
    Accepts either 'vote_average' (TMDB discover/enrich) or 'tmdb_vote' (your printed sample).
    """
    v = it.get("vote_average", None)
    if v is None:
        v = it.get("tmdb_vote", None)
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return max(0.0, min(1.0, v / 10.0))


def _kind(it: Dict[str, Any]) -> str:
    """
    Return 'tv' or 'movie' for penalty logic and output type.
    """
    k = (it.get("kind") or it.get("media_type") or "").lower()
    if not k:
        # infer from TMDB structures
        if "first_air_date" in it or it.get("type") in ("tv", "tvSeries", "series"):
            return "tv"
        return "movie"
    return "tv" if "tv" in k else "movie"


def score_items(cfg: Any, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compute match score from an audience proxy + (optional) critic placeholder
    and a light TV penalty scaled by cfg.commitment_cost_scale.

    cfg is expected to have (with sane defaults if missing):
      - audience_weight (default 1.0)
      - critic_weight (default 0.0)
      - commitment_cost_scale (default 1.0)
    """
    aw = float(getattr(cfg, "audience_weight", 1.0))
    cw = float(getattr(cfg, "critic_weight", 0.0))
    cc = float(getattr(cfg, "commitment_cost_scale", 1.0))

    ranked: List[Dict[str, Any]] = []
    for it in items:
        aud = _audience_proxy(it)
        critic = 0.0  # reserved for future critic data (OMDb/Metacritic etc.)

        base = aw * aud + cw * critic

        penalty = 0.0
        if _kind(it) == "tv":
            penalty = 0.02 * cc  # tiny nudge against long commitment

        match = round(100.0 * max(0.0, base - penalty), 1)

        # normalize providers field for output
        providers = (it.get("providers") or
                     it.get("watch_available") or
                     [])

        # normalize year
        year = it.get("year") or it.get("release_year") or it.get("first_air_year")

        ranked.append({
            "title": it.get("title") or it.get("name"),
            "year": year,
            "type": "tvSeries" if _kind(it) == "tv" else "movie",
            "audience": round(aud * 100, 1),
            "critic": round(critic * 100, 1),
            "match": match,
            "providers": providers,
            "imdb_id": it.get("imdb_id"),
            "tmdb_id": it.get("id") or it.get("tmdb_id"),
        })

    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked