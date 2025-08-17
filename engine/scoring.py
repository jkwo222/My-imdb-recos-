# engine/scoring.py
from __future__ import annotations
import csv
import os
import re
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rapidfuzz import fuzz

# Optional IMDb scraper (safe to fail)
try:
    from .imdb_ingest import scrape_imdb_ratings  # noqa: F401
except Exception:  # pragma: no cover
    scrape_imdb_ratings = None  # type: ignore

# -------------------------
# Title normalization / fuzzy
# -------------------------

_ROMAN = {
    " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
    " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
}

def _norm_title(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    # drop text in (...) (years, versions)
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
    for k, v in _ROMAN.items():
        s = s.replace(k, v)
    s = re.sub(r"^\s*the\s+", "", s)
    s = " ".join(t for t in s.split() if t)
    return s

def _fuzzy_sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

# -------------------------
# Seen / ratings ingest
# -------------------------

def _parse_csv_seen(csv_path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]], List[Tuple[str, Optional[int], Optional[float]]]]:
    """
    Returns (imdb_ids, [(title_norm, year)], [(title_norm, year, rating)])
    The third list carries numeric ratings if present, for preference learning.
    """
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    rated: List[Tuple[str, Optional[int], Optional[float]]] = []
    if not os.path.exists(csv_path):
        return ids, titles, rated

    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        fieldnames = rdr.fieldnames or []
        lower = [h.lower().strip() for h in fieldnames]

        def _col(*cands: str) -> Optional[str]:
            for c in cands:
                if c in lower:
                    return fieldnames[lower.index(c)]
            return None

        id_key = _col("const", "tconst", "imdb title id", "imdb_id", "id")
        title_key = _col("title", "originaltitle", "original title", "primarytitle", "name")
        year_key = _col("year", "startyear", "release year", "date rated", "release_date")
        rating_key = _col("your rating", "rating", "user_rating", "score")

        for row in rdr:
            # ids
            if id_key:
                v = (row.get(id_key) or "").strip()
                if v.startswith("tt"):
                    ids.add(v)

            # title/year
            t = (row.get(title_key) or "").strip() if title_key else ""
            y_raw = (row.get(year_key) or "").strip() if year_key else ""
            y = int(y_raw) if y_raw.isdigit() else None
            nt = _norm_title(t) if t else ""
            if nt:
                titles.append((nt, y))

            # numeric rating if present
            r_val: Optional[float] = None
            if rating_key:
                r_raw = (row.get(rating_key) or "").strip()
                try:
                    r_val = float(r_raw)
                except Exception:
                    r_val = None
            if nt:
                rated.append((nt, y, r_val))

    return ids, titles, rated


def _scrape_public_seen_from_env() -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    If IMDB_USER_ID is set, scrape the user's public ratings page for seen titles.
    Fault tolerant: on any error returns empty sets.
    """
    user_id = os.environ.get("IMDB_USER_ID", "").strip()
    if not user_id:
        return set(), []
    if scrape_imdb_ratings is None:
        return set(), []
    try:
        url = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
        items = scrape_imdb_ratings(url, max_pages=50)
    except Exception:
        return set(), []

    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    for it in items:
        iid = getattr(it, "imdb_id", "") or ""
        if iid.startswith("tt"):
            ids.add(iid)
        t = getattr(it, "title", "") or ""
        y = getattr(it, "year", None)
        titles.append((_norm_title(t), int(y) if str(y).isdigit() else None))
    return ids, titles


def load_seen_index(csv_path: str) -> Dict[str, Any]:
    """
    Return a dict with:
      - imdb ids as keys { 'tt123...': True, ... }
      - _titles_norm_pairs: List[(title_norm, year)]
      - _rated_norm: List[(title_norm, year, rating or None)]
    """
    ids_csv, titles_csv, rated_csv = _parse_csv_seen(csv_path)
    ids_web, titles_web = _scrape_public_seen_from_env()

    ids = set(ids_csv) | set(ids_web)
    titles = titles_csv + titles_web

    idx: Dict[str, Any] = {tid: True for tid in ids}
    idx["_titles_norm_pairs"] = titles
    idx["_rated_norm"] = rated_csv
    return idx

# -------------------------
# Unseen filter using fuzzy title+year
# -------------------------

def _matches_seen_by_title(pool_title: str, pool_year: Optional[int], seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    nt = _norm_title(pool_title)
    for st, sy in seen_pairs:
        if nt == st:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
        if _fuzzy_sim(nt, st) >= 0.93:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
    return False


def filter_unseen(pool: List[Dict[str, Any]], seen_idx: Dict[str, Any]) -> List[Dict[str, Any]]:
    seen_pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict[str, Any]] = []
    for it in pool:
        title = it.get("title") or it.get("name") or ""
        year: Optional[int] = None
        # try derive year from known fields
        y = it.get("year")
        if isinstance(y, int):
            year = y
        else:
            rd = (it.get("release_date") or it.get("first_air_date") or "").strip()
            if len(rd) >= 4 and rd[:4].isdigit():
                year = int(rd[:4])
        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, year, seen_pairs):
            continue
        out.append(it)
    return out

# -------------------------
# Preference learning from ratings.csv
# -------------------------

# Common TMDB genre id -> label map (stable enough for our use)
_TMDB_GENRES = {
    # Movies
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
    99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
    27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
    10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
    # TV (overlaps + TV-specific)
    10759: "Action & Adventure", 10762: "Kids", 10763: "News", 10764: "Reality",
    10765: "Sci-Fi & Fantasy", 10766: "Soap", 10767: "Talk", 10768: "War & Politics",
}

def _genre_weight_model(rated_norm: List[Tuple[str, Optional[int], Optional[float]]]) -> Dict[str, float]:
    """
    Very simple preference learner: compute an average centered rating per title text cluster.
    We don't have historical TMDB genres for the CSV rows, so we learn a broad taste “bias”
    vector from the numeric ratings themselves (if present) and then apply it as a weak prior.
    Practically: we compute the user's overall mean rating and a scaling factor used later.
    """
    vals: List[float] = [r for _, __, r in rated_norm if isinstance(r, (int, float))]
    if not vals:
        return {"_mean": 7.0, "_stdev": 1.5}
    mean = sum(vals) / len(vals)
    # robust-ish stdev
    var = sum((v - mean) ** 2 for v in vals) / max(1, len(vals) - 1)
    stdev = math.sqrt(var) if var > 0 else 1.0
    return {"_mean": float(mean), "_stdev": float(max(0.5, min(3.0, stdev)))}

def _genre_names(genre_ids: Iterable[int]) -> List[str]:
    names: List[str] = []
    for gid in genre_ids or []:
        nm = _TMDB_GENRES.get(int(gid))
        if nm and nm not in names:
            names.append(nm)
    return names

# -------------------------
# Scoring
# -------------------------

@dataclass
class ScoreCfg:
    audience_weight: float = 0.85
    critic_weight: float = 0.15  # placeholder for future critic source
    commitment_cost_scale: float = 1.0  # light penalty on TV
    recency_bonus: float = 0.08         # **bonus only** (NOT a filter)
    taste_bonus: float = 0.07           # weak taste alignment bonus

def _year_of(item: Dict[str, Any]) -> Optional[int]:
    y = item.get("year")
    if isinstance(y, int):
        return y
    rd = (item.get("release_date") or item.get("first_air_date") or "").strip()
    if len(rd) >= 4 and rd[:4].isdigit():
        return int(rd[:4])
    return None

def _recency_multiplier(item: Dict[str, Any], now_year: int) -> float:
    """
    Give newer titles a gentle boost; never downrank older ones.
    Curve: 0..+recency_bonus (log shaped) for 0..6 years old.
    """
    y = _year_of(item)
    if not y:
        return 0.0
    age = max(0, now_year - y)
    if age >= 7:
        return 0.0
    # younger -> closer to 1; map age 0..6 to 1..~0.45 then scale
    base = 1.0 / (1.0 + 0.35 * age)
    return base - 1.0  # produce 0..positive small; scaled later

def _taste_alignment(item: Dict[str, Any], taste_stats: Dict[str, float]) -> float:
    """
    Apply a very small alignment bonus using user's overall mean/stdev
    against the item's audience proxy; acts like a weak prior.
    """
    aud = float(item.get("vote_average") or 0.0)
    mean = float(taste_stats.get("_mean", 7.0))
    st = float(taste_stats.get("_stdev", 1.5))
    z = (aud - mean) / max(0.5, st)
    # squash to [-1, +1] then scale to small positive if above average
    return max(0.0, 0.5 * math.tanh(0.7 * z))  # never negative (only bonus)

def score_items(env: Any, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Combine:
      - Audience proxy (TMDB vote_average) as the main signal
      - Small TV commitment penalty
      - **Recency bonus** (no filtering)
      - Weak taste prior from ratings.csv
    Produces 0..100 'match' values.
    """
    cfg = ScoreCfg()

    # taste model from ratings.csv (supplied via load_seen_index)
    # runner passes seen_idx separately; we don't have it here, so load directly if present.
    rated_norm: List[Tuple[str, Optional[int], Optional[float]]] = []
    try:
        _, __, rated_norm = _parse_csv_seen(os.path.join("data", "ratings.csv"))
    except Exception:
        pass
    taste_stats = _genre_weight_model(rated_norm)

    # decide current year for recency curve
    import datetime as _dt
    now_year = _dt.datetime.utcnow().year

    ranked: List[Dict[str, Any]] = []
    for it in items:
        aud = max(0.0, min(1.0, (float(it.get("vote_average") or 0.0) / 10.0)))
        cri = 0.0  # placeholder
        base = cfg.audience_weight * aud + cfg.critic_weight * cri

        # commitment penalty for TV (tiny)
        penalty = 0.0
        kind = it.get("media_type") or it.get("kind")
        if (kind or "").lower() in ("tv", "tv_series", "tvseries", "series"):
            penalty = 0.02 * cfg.commitment_cost_scale

        # Recency bonus (never negative)
        rb = _recency_multiplier(it, now_year)
        # Taste bonus (never negative)
        tb = _taste_alignment(it, taste_stats)

        score = max(0.0, base - penalty + cfg.recency_bonus * rb + cfg.taste_bonus * tb)
        match = round(100.0 * score, 1)

        # derive a friendly year and genres for downstream
        yr = _year_of(it)
        genres = _genre_names(it.get("genre_ids", []))

        ranked.append({
            "title": it.get("title") or it.get("name"),
            "year": yr,
            "type": "tvSeries" if (kind or "").lower() in ("tv", "tv_series", "tvseries", "series") else "movie",
            "audience": round(aud * 100.0, 1),
            "critic": round(cri * 100.0, 1),
            "match": match,
            "genres": genres,
            "providers": it.get("providers", []),
        })

    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked