from __future__ import annotations
import csv
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore

# --------- helpers ---------

def _clamp01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x

def _try_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

_ROMAN = {
    " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
    " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
}

def _normalize_title(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
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
    if fuzz is None:
        return 1.0 if a == b else 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

def _parse_year_from_item(it: Dict[str, Any]) -> Optional[int]:
    y = it.get("year")
    if isinstance(y, int):
        return y
    for key in ("release_date", "first_air_date"):
        d = it.get(key) or ""
        if isinstance(d, str) and len(d) >= 4 and d[:4].isdigit():
            return int(d[:4])
    return None

def _now_year() -> int:
    try:
        return datetime.utcnow().year
    except Exception:
        return 2025

# --------- seen index ---------

@dataclass
class SeenIndex:
    by_id: Dict[str, bool]
    title_year_pairs: List[Tuple[str, Optional[int]]]

def _parse_csv_seen(csv_path: str) -> SeenIndex:
    ids: Dict[str, bool] = {}
    pairs: List[Tuple[str, Optional[int]]] = []
    if not os.path.exists(csv_path):
        return SeenIndex(ids, pairs)
    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if rdr.fieldnames is None:
            return SeenIndex(ids, pairs)
        lower = [h.lower().strip() for h in rdr.fieldnames]
        id_key: Optional[str] = None
        for cand in ("const", "tconst", "imdb title id", "imdb_id", "id"):
            if cand in lower:
                id_key = rdr.fieldnames[lower.index(cand)]
                break
        title_keys = [k for k in rdr.fieldnames if k.lower().strip() in ("title", "originaltitle", "original title", "primarytitle")]
        year_keys = [k for k in rdr.fieldnames if k.lower().strip() in ("year", "release year", "startyear")]
        for row in rdr:
            if id_key:
                v = (row.get(id_key) or "").strip()
                if v.startswith("tt"):
                    ids[v] = True
            t = ""
            for tk in title_keys:
                t = (row.get(tk) or "").strip()
                if t:
                    break
            y_val: Optional[int] = None
            for yk in year_keys:
                yraw = (row.get(yk) or "").strip()
                if yraw and yraw.isdigit():
                    y_val = int(yraw)
                    break
            if t:
                pairs.append((_normalize_title(t), y_val))
    return SeenIndex(ids, pairs)

def _scrape_seen_from_imdb_env() -> SeenIndex:
    user_id = os.getenv("IMDB_USER_ID", "").strip()
    if not user_id:
        return SeenIndex({}, [])
    try:
        from .imdb_ingest import scrape_imdb_ratings  # optional
    except Exception:
        return SeenIndex({}, [])
    url = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    try:
        items = scrape_imdb_ratings(url, max_pages=50)  # pragma: no cover
    except Exception:
        return SeenIndex({}, [])
    ids: Dict[str, bool] = {}
    pairs: List[Tuple[str, Optional[int]]] = []
    for i in items:
        iid = getattr(i, "imdb_id", "") or ""
        if iid.startswith("tt"):
            ids[iid] = True
        t = getattr(i, "title", "") or ""
        y = getattr(i, "year", None)
        pairs.append((_normalize_title(t), int(y) if isinstance(y, int) else None))
    return SeenIndex(ids, pairs)

def load_seen_index(primary_csv: Optional[str] = None, fallback_csv: Optional[str] = None) -> Dict[str, Any]:
    if not primary_csv:
        primary_csv = os.path.join("data", "user", "ratings.csv")
    if not fallback_csv:
        fallback_csv = os.path.join("data", "ratings.csv")
    s1 = _parse_csv_seen(primary_csv)
    s2 = SeenIndex({}, [])
    if not s1.by_id and not s1.title_year_pairs and os.path.exists(fallback_csv):
        s2 = _parse_csv_seen(fallback_csv)
    s3 = _scrape_seen_from_imdb_env()
    ids = {**s1.by_id, **s2.by_id, **s3.by_id}
    pairs = list(s1.title_year_pairs) + list(s2.title_year_pairs) + list(s3.title_year_pairs)
    out: Dict[str, Any] = {k: True for k in ids.keys()}
    out["_titles_norm_pairs"] = pairs
    out["__has_csv_primary__"] = os.path.exists(primary_csv)
    out["__has_csv_fallback__"] = os.path.exists(fallback_csv)
    out["__has_imdb_public__"] = bool(os.getenv("IMDB_USER_ID", "").strip())
    return out

# --------- filtering ---------

def _matches_seen_by_title(pool_title: str, pool_year: Optional[int], seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    nt = _normalize_title(pool_title)
    for st, sy in seen_pairs:
        if nt == st:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
        if _fuzzy_sim(nt, st) >= 0.93:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
    return False

def filter_unseen(pool: List[Dict[str, Any]], seen_idx: Dict[str, Any]) -> List[Dict[str, Any]]:
    pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict[str, Any]] = []
    for it in pool:
        title = (it.get("title") or it.get("name") or "").strip()
        year = _parse_year_from_item(it)
        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, year, pairs):
            continue
        out.append(it)
    return out

# --------- scoring ---------

def _audience_base_0_1(it: Dict[str, Any]) -> float:
    va = _try_float(it.get("vote_average"), None)
    if va is None:
        return 0.6
    return _clamp01(va / 10.0)

def _authority_bonus_pts(it: Dict[str, Any]) -> float:
    nv = _try_float(it.get("numVotes"), None)
    if nv is None:
        nv = _try_float(it.get("vote_count"), None)
    if nv is None:
        # rough proxy from TMDB popularity
        pop = _try_float(it.get("popularity"), 0.0) or 0.0
        # map popularity 0..300 to ~0..4.5
        return min(4.5, max(0.0, math.log10(1.0 + pop) * 2.0))
    return min(6.0, math.log10(1.0 + max(0.0, nv))) * 0.8

def _recency_bonus_pts(it: Dict[str, Any]) -> float:
    y = _parse_year_from_item(it)
    if not y:
        return 0.0
    age = max(0, _now_year() - y)
    # 0 yrs -> +6, 1yr -> +5, 2->+4, 3->+3, 4->+2, 5->+1, 6+ -> taper to 0
    if age <= 5:
        return float(6 - age)
    # soft tail
    return max(0.0, 6.0 * math.exp(-0.35 * (age - 5)))

def _commitment_penalty_pts(it: Dict[str, Any]) -> float:
    if (it.get("media_type") or it.get("kind")) in ("tv", "tvSeries", "series"):
        return 2.0
    return 0.0

def _why_bits(it: Dict[str, Any]) -> List[str]:
    bits: List[str] = []
    imdb = _try_float(it.get("imdb_rating"), None)
    if imdb is not None:
        bits.append(f"IMDb {imdb:g}")
    tmdb = _try_float(it.get("vote_average"), None)
    if tmdb is not None:
        bits.append(f"TMDB {tmdb:g}")
    y = _parse_year_from_item(it)
    if y:
        bits.append(str(y))
    return bits

def score_items(env: Any, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for it in items:
        base = _audience_base_0_1(it)  # 0..1
        score = base * 100.0
        score += _authority_bonus_pts(it)
        score += _recency_bonus_pts(it)  # bonus, not a filter
        score -= _commitment_penalty_pts(it)
        score = max(0.0, min(100.0, score))
        out = dict(it)
        out["match"] = round(score, 1)
        out["audience"] = round(base * 100.0, 1)
        out["critic"] = 0.0
        if "why" not in out:
            out["why"] = "; ".join(_why_bits(it))
        ranked.append(out)
    ranked.sort(key=lambda r: r.get("match", 0.0), reverse=True)
    return ranked