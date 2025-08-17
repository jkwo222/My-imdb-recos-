from __future__ import annotations
import os, re
from typing import Any, Dict, Iterable, List, Optional, Tuple
from rapidfuzz import fuzz
from datetime import date
from .util import normalize_title, parse_year, parse_date, clamp01

def _fuzzy(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

def _extract_titles(row: Dict[str, str]) -> Tuple[str, Optional[int]]:
    t = (row.get("Title") or row.get("title") or row.get("originalTitle") or row.get("Original Title") or "").strip()
    y_raw = (row.get("Year") or row.get("year") or row.get("startYear") or row.get("Release Year") or "").strip()
    y = parse_year(y_raw)
    return normalize_title(t), y

def _extract_imdb_id(row: Dict[str, str]) -> Optional[str]:
    lower_keys = {k.lower(): k for k in row.keys()}
    for cand in ("const", "tconst", "imdb title id", "imdb_id", "id"):
        if cand in lower_keys:
            v = (row.get(lower_keys[cand]) or "").strip()
            if v.startswith("tt") and v[2:].isdigit():
                return v
    url = (row.get("URL") or row.get("url") or "").strip()
    m = re.search(r"(tt\d+)", url)
    if m:
        return m.group(1)
    return None

def _read_ratings_csv(path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    try:
        import csv
        import io
        with open(path, "r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                iid = _extract_imdb_id(row)
                if iid:
                    ids.add(iid)
                t_norm, y = _extract_titles(row)
                if t_norm:
                    titles.append((t_norm, y))
    except UnicodeDecodeError:
        import csv
        with open(path, "r", encoding="latin-1") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                iid = _extract_imdb_id(row)
                if iid:
                    ids.add(iid)
                t_norm, y = _extract_titles(row)
                if t_norm:
                    titles.append((t_norm, y))
    except Exception:
        pass
    return ids, titles

def _scrape_public_seen_from_env() -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    try:
        from .imdb_ingest import scrape_imdb_ratings  # optional module
    except Exception:
        return set(), []
    user_id = os.getenv("IMDB_USER_ID", "").strip()
    if not user_id:
        return set(), []
    url = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    try:
        items = scrape_imdb_ratings(url, max_pages=50)
    except Exception:
        return set(), []
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    for i in items:
        iid = getattr(i, "imdb_id", "")
        if isinstance(iid, str) and iid.startswith("tt"):
            ids.add(iid)
        t = getattr(i, "title", "")
        y = getattr(i, "year", None)
        titles.append((normalize_title(t), y if isinstance(y, int) else None))
    return ids, titles

def load_seen_index(csv_path: str) -> Dict[str, Any]:
    ids_csv: set[str] = set()
    titles_csv: List[Tuple[str, Optional[int]]] = []
    if csv_path and os.path.exists(csv_path):
        ids_csv, titles_csv = _read_ratings_csv(csv_path)
    ids_web, titles_web = _scrape_public_seen_from_env()
    ids = set(ids_csv) | set(ids_web)
    titles = titles_csv + titles_web
    idx: Dict[str, Any] = {tid: True for tid in ids}
    idx["_titles_norm_pairs"] = titles
    return idx

def _matches_seen_by_title(pool_title: str, pool_year: Optional[int], seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    nt = normalize_title(pool_title)
    for st, sy in seen_pairs:
        if nt == st:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
        if _fuzzy(nt, st) >= 0.93:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
    return False

def filter_unseen(pool: List[Dict[str, Any]], seen_idx: Dict[str, Any]) -> List[Dict[str, Any]]:
    seen_pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict[str, Any]] = []
    for it in pool:
        title = it.get("title") or it.get("name") or ""
        if not title:
            out.append(it)
            continue
        year = None
        d = it.get("release_date") or it.get("first_air_date") or ""
        if d:
            y = parse_year(d[:4])
            if y:
                year = y
        iid = (it.get("imdb_id") or "").strip() if isinstance(it.get("imdb_id"), str) else ""
        if iid and iid in seen_idx:
            continue
        if _matches_seen_by_title(title, year, seen_pairs):
            continue
        out.append(it)
    return out

def _recency_bonus(date_str: str) -> float:
    if not date_str:
        return 0.0
    d = parse_date(date_str)
    if not d:
        return 0.0
    days = (date.today() - d).days
    if days <= 14:
        return 8.0
    if days <= 30:
        return 6.0
    if days <= 90:
        return 4.0
    if days <= 180:
        return 2.0
    return 0.0

def _year_of(it: Dict[str, Any]) -> Optional[int]:
    s = it.get("release_date") or it.get("first_air_date") or ""
    return parse_year(s[:4]) if s else None

def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for it in items:
        va = it.get("vote_average", 0.0) or 0.0
        aud = clamp01(float(va) / 10.0)
        cri = 0.0
        base = aud * 100.0
        penalty = 2.0 if (it.get("media_type") == "tv") else 0.0
        pre_hint = 0.0
        try:
            pre_hint = float(it.get("pre_match_hint", 0.0) or 0.0)
        except Exception:
            pre_hint = 0.0
        bonus = _recency_bonus(it.get("release_date") or it.get("first_air_date") or "")
        match = max(0.0, min(100.0, base - penalty + bonus + pre_hint))
        ranked.append({
            "media_type": "tv" if it.get("media_type") == "tv" else "movie",
            "tmdb_id": it.get("tmdb_id"),
            "title": it.get("title"),
            "year": _year_of(it),
            "audience": round(aud * 100.0, 1),
            "critic": round(cri * 100.0, 1),
            "match": round(match, 1),
            "vote_average": it.get("vote_average"),
            "release_date": it.get("release_date"),
            "first_air_date": it.get("first_air_date"),
            "providers": it.get("providers", []),
            "genres": it.get("genres", []),
        })
    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked