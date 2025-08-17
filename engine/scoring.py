# engine/scoring.py
from __future__ import annotations
import csv, os, re, math, datetime
from typing import Any, Dict, Iterable, List, Tuple, Optional
from rapidfuzz import fuzz

try:
    from .imdb_ingest import scrape_imdb_ratings  # optional
except Exception:  # pragma: no cover
    scrape_imdb_ratings = None  # type: ignore

_ROMAN = {
    " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
    " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
}

def _norm_title(s: str) -> str:
    if not s: return ""
    s = s.lower().strip()
    out, depth = [], 0
    for ch in s:
        if ch == '(': depth += 1
        elif ch == ')': depth = max(0, depth-1)
        elif depth == 0: out.append(ch)
    s = ''.join(out)
    s = s.replace("&", " and ")
    s = re.sub(r"[-—–_:/,.'!?;]", " ", s)
    s = f" {s} "
    for k, v in _ROMAN.items():
        s = s.replace(k, v)
    s = re.sub(r"^\s*the\s+", "", s)
    s = " ".join(t for t in s.split() if t)
    return s

def _fuzzy_sim(a: str, b: str) -> float:
    if not a or not b: return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

def _best_path_for_ratings() -> Optional[str]:
    for p in ("data/user/ratings.csv", "data/ratings.csv"):
        if os.path.exists(p): return p
    return None

def _parse_csv_seen(csv_path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]], Dict[str, float]]:
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    rated_norm: Dict[str, float] = {}
    if not os.path.exists(csv_path):
        return ids, titles, rated_norm
    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        lower = [h.lower().strip() for h in (rdr.fieldnames or [])]
        id_key = None
        for cand in ("const","tconst","imdb title id","imdb_id","id"):
            if cand in lower:
                id_key = (rdr.fieldnames or [])[lower.index(cand)]
                break
        title_keys = [k for k in (rdr.fieldnames or []) if k and k.lower() in {"title","originaltitle","original title","primarytitle"}]
        year_keys = [k for k in (rdr.fieldnames or []) if k and k.lower() in {"year","startyear","release year"}]
        rating_keys = [k for k in (rdr.fieldnames or []) if k and k.lower() in {"your rating","rating","user rating"}]
        for row in rdr:
            if id_key:
                v = (row.get(id_key) or "").strip()
                if v.startswith("tt"): ids.add(v)
            t = ""
            for k in title_keys:
                if (row.get(k) or "").strip():
                    t = (row.get(k) or "").strip()
                    break
            y_raw = ""
            for k in year_keys:
                if (row.get(k) or "").strip():
                    y_raw = (row.get(k) or "").strip()
                    break
            y = int(y_raw) if (y_raw.isdigit()) else None
            if t:
                titles.append((_norm_title(t), y))
            if t and rating_keys:
                try:
                    r = float((row.get(rating_keys[0]) or "").strip())
                    if r > 0:
                        rated_norm[_norm_title(t)] = max(0.0, min(1.0, r / 10.0))
                except Exception:
                    pass
    return ids, titles, rated_norm

def _scrape_public_seen_from_env() -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    user_id = os.environ.get("IMDB_USER_ID","").strip()
    if not user_id or not scrape_imdb_ratings:
        return set(), []
    url = f"https://www.imdb.com/user/{user_id}/ratings?sort=ratings_date:desc&mode=detail"
    try:
        items = scrape_imdb_ratings(url, max_pages=50)  # type: ignore[misc]
    except Exception:
        return set(), []
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    for i in items:
        iid = getattr(i, "imdb_id", "")
        if iid: ids.add(iid)
        t = getattr(i, "title", "") or ""
        y = getattr(i, "year", None)
        titles.append((_norm_title(t), y if isinstance(y, int) else None))
    return ids, titles

def load_seen_index(csv_path: Optional[str] = None) -> Dict[str, Any]:
    if csv_path is None:
        csv_path = _best_path_for_ratings() or ""
    ids_csv, titles_csv, rated_norm = _parse_csv_seen(csv_path) if csv_path else (set(), [], {})
    ids_web, titles_web = _scrape_public_seen_from_env()
    ids = set(ids_csv) | set(ids_web)
    titles = titles_csv + titles_web
    idx: Dict[str, Any] = {tid: True for tid in ids}
    idx["_titles_norm_pairs"] = titles
    idx["_ratings_norm"] = rated_norm
    return idx

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
        year = it.get("year")
        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, year, seen_pairs):
            continue
        out.append(it)
    return out

def _parse_year(d: Dict[str, Any]) -> Optional[int]:
    for k in ("release_date", "first_air_date"):
        v = (d.get(k) or "").strip()
        if len(v) >= 4 and v[:4].isdigit():
            try: return int(v[:4])
            except Exception: pass
    y = d.get("year")
    try: return int(y) if y is not None else None
    except Exception: return None

def score_items(cfg: Any, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cw = float(getattr(cfg, "critic_weight", 0.25) or 0.25)
    aw = float(getattr(cfg, "audience_weight", 0.75) or 0.75)
    cc = float(getattr(cfg, "commitment_cost_scale", 1.0) or 1.0)

    csv_path = _best_path_for_ratings()
    _, title_pairs, rated_norm = _parse_csv_seen(csv_path) if csv_path else (set(), [], {})
    liked_titles = {t for (t, _y) in title_pairs if rated_norm.get(t, 0) >= 0.8}

    today = datetime.date.today()
    ranked: List[Dict[str, Any]] = []
    for it in items:
        aud = max(0.0, min(1.0, (it.get("vote_average", 0.0) or 0.0) / 10.0))
        cri = 0.0
        base = aw * aud + cw * cri

        penalty = 0.0
        if it.get("kind") == "tv":
            penalty = 0.02 * cc

        year = _parse_year(it)
        recency_bonus = 0.0
        if year:
            age = max(0, today.year - int(year))
            recency_bonus = max(0.0, (5.0 - min(5.0, age)) * 0.005)  # up to +2.5 pts

        t = (it.get("title") or it.get("name") or "").strip()
        taste_bonus = 0.0
        if t and liked_titles:
            nt = _norm_title(t)
            sim = max((_fuzzy_sim(nt, lt) for lt in liked_titles), default=0.0)
            taste_bonus = 0.05 * sim  # up to +5 pts

        match = round(100.0 * max(0.0, base - penalty + recency_bonus + taste_bonus), 1)

        ranked.append({
            "title": it.get("title") or it.get("name"),
            "year": year,
            "type": "tvSeries" if it.get("kind") == "tv" else "movie",
            "audience": round(aud * 100, 1),
            "critic": round(cri * 100, 1),
            "match": match,
            "providers": it.get("providers", []),
        })
    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked