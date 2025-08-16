from __future__ import annotations
import csv, os, re
from typing import Dict, Iterable, List, Tuple, Optional

from rapidfuzz import fuzz
from .imdb_ingest import scrape_imdb_ratings  # uses IMDB_USER_ID public list if set

_ROMAN = {
    " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
    " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
}

def _norm_title(s: str) -> str:
    if not s: return ""
    s = s.lower().strip()
    # strip anything in (...), e.g. years, versions
    out, depth = [], 0
    for ch in s:
        if ch == '(': depth += 1
        elif ch == ')': depth = max(0, depth-1)
        elif depth == 0: out.append(ch)
    s = ''.join(out)
    s = s.replace("&", " and ")
    s = re.sub(r"[-—–_:/,.'!?;]", " ", s)
    s = f" {s} "
    # roman numerals → digits
    for k, v in _ROMAN.items():
        s = s.replace(k, v)
    # drop leading article "the "
    s = re.sub(r"^\s*the\s+", "", s)
    s = " ".join(t for t in s.split() if t)
    return s

def _fuzzy_sim(a: str, b: str) -> float:
    if not a or not b: return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

def _parse_csv_seen(csv_path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    Returns (imdb_ids, [(title_norm, year), ...])
    """
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    if not os.path.exists(csv_path):
        return ids, titles
    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        # common id headers
        lower = [h.lower().strip() for h in (rdr.fieldnames or [])]
        id_key = None
        for cand in ("const","tconst","imdb title id","imdb_id","id"):
            if cand in lower:
                id_key = (rdr.fieldnames or [])[lower.index(cand)]
                break
        for row in rdr:
            if id_key:
                v = (row.get(id_key) or "").strip()
                if v.startswith("tt"): ids.add(v)
            t = (row.get("Title") or row.get("title") or row.get("originalTitle") or row.get("Original Title") or "").strip()
            y_raw = (row.get("Year") or row.get("year") or row.get("startYear") or row.get("Release Year") or "").strip()
            y = int(y_raw) if (y_raw.isdigit()) else None
            if t:
                titles.append((_norm_title(t), y))
    return ids, titles

def _scrape_public_seen_from_env() -> Tuple[set[str], List[Tuple[str, Optional[int]]]]:
    """
    If IMDB_USER_ID is set, scrape the user's public ratings page.
    """
    user_id = os.environ.get("IMDB_USER_ID","").strip()
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
        if getattr(i, "imdb_id", ""):
            ids.add(i.imdb_id)
        t = getattr(i, "title", "")
        y = getattr(i, "year", None)
        titles.append((_norm_title(t), y if isinstance(y, int) else None))
    return ids, titles

def load_seen_index(csv_path: str) -> Dict[str, bool]:
    """
    Backward-compatible return type for runner: dict {imdb_id: True}
    But we also internally keep title/year pairs for fuzzy matching.
    We store them under a private key on the dict.
    """
    ids_csv, titles_csv = _parse_csv_seen(csv_path)
    ids_web, titles_web = _scrape_public_seen_from_env()

    # merge
    ids = set(ids_csv) | set(ids_web)
    titles = titles_csv + titles_web

    idx: Dict[str, bool] = {tid: True for tid in ids}
    # Stash normalized titles in a hidden slot
    idx["__titles__"] = True  # marker
    idx["_titles_norm_pairs"] = titles  # type: ignore
    return idx

def _matches_seen_by_title(pool_title: str, pool_year: Optional[int], seen_pairs: List[Tuple[str, Optional[int]]]) -> bool:
    nt = _norm_title(pool_title)
    for st, sy in seen_pairs:
        # quick exact after norm
        if nt == st:
            # accept if year matches loosely (+/- 1) or missing
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
        # fuzzy fallback
        if _fuzzy_sim(nt, st) >= 0.93:
            if sy is None or pool_year is None or abs(int(pool_year) - int(sy)) <= 1:
                return True
    return False

def filter_unseen(pool: List[Dict], seen_idx: Dict[str, bool]) -> List[Dict]:
    """
    Drops items that appear to be seen by IMDb id (if available later) or by robust title+year match.
    Note: discover items typically lack imdb_id; we rely on title/year matching here.
    """
    seen_pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict] = []
    for it in pool:
        title = it.get("title") or it.get("name") or ""
        year = it.get("year")
        # imdb_id path (if you later attach it to items)
        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, year, seen_pairs):
            continue
        out.append(it)
    return out

def score_items(cfg, items: List[Dict]) -> List[Dict]:
    """
    Combine TMDB audience proxy (vote_average) with optional external critic (placeholder),
    light penalty for TV commitment, and leave room for future taste boosts.
    """
    cw = cfg.critic_weight
    aw = cfg.audience_weight
    cc = cfg.commitment_cost_scale

    ranked = []
    for it in items:
        aud = max(0.0, min(1.0, (it.get("vote_average", 0.0) or 0.0) / 10.0))
        cri = 0.0  # placeholder; if OMDb enrich is added upstream, pass through here
        base = aw * aud + cw * cri
        penalty = 0.0
        if it.get("kind") == "tv":
            penalty = 0.02 * cc  # ~2 points after scaling
        match = round(100.0 * max(0.0, base - penalty), 1)
        ranked.append({
            "title": it.get("title"),
            "year": it.get("year"),
            "type": "tvSeries" if it.get("kind") == "tv" else "movie",
            "audience": round(aud * 100, 1),
            "critic": round(cri * 100, 1),
            "match": match,
            "providers": it.get("providers", []),
        })
    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked