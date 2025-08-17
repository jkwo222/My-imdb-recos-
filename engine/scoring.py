# engine/scoring.py
from __future__ import annotations
import csv, os, re
from datetime import datetime
from typing import Dict, Iterable, List, Tuple, Optional, Any

from rapidfuzz import fuzz

# ----------------------------
# Title normalization / fuzzy
# ----------------------------

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
    for k, v in _ROMAN.items():
        s = s.replace(k, v)
    s = re.sub(r"^\s*the\s+", "", s)
    s = " ".join(t for t in s.split() if t)
    return s

def _fuzzy_sim(a: str, b: str) -> float:
    if not a or not b: return 0.0
    return fuzz.token_set_ratio(a, b) / 100.0

# ----------------------------
# Seen index loading
# ----------------------------

def _parse_csv_seen(csv_path: str) -> Tuple[set[str], List[Tuple[str, Optional[int]]], Dict[str, int]]:
    """
    Returns (imdb_ids, [(title_norm, year), ...], stats)
    """
    ids: set[str] = set()
    titles: List[Tuple[str, Optional[int]]] = []
    stats = {"rows": 0, "ids": 0, "titles": 0}

    if not os.path.exists(csv_path):
        return ids, titles, stats

    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        lower = [h.lower().strip() for h in (rdr.fieldnames or [])]
        id_key = None
        for cand in ("const","tconst","imdb title id","imdb_id","id"):
            if cand in lower:
                id_key = (rdr.fieldnames or [])[lower.index(cand)]
                break
        for row in rdr:
            stats["rows"] += 1
            if id_key:
                v = (row.get(id_key) or "").strip()
                if v.startswith("tt"):
                    ids.add(v)
            t = (row.get("Title") or row.get("title") or row.get("originalTitle") or row.get("Original Title") or "").strip()
            y_raw = (row.get("Year") or row.get("year") or row.get("startYear") or row.get("Release Year") or "").strip()
            y = int(y_raw) if (y_raw.isdigit()) else None
            if t:
                titles.append((_norm_title(t), y))
        stats["ids"] = len(ids)
        stats["titles"] = len(titles)

    return ids, titles, stats

def load_seen_index(csv_path: str) -> Dict[str, Any]:
    """
    Returns dict with imdb_id keys (True) AND embeds _titles_norm_pairs + _stats for diagnostics.
    """
    ids_csv, titles_csv, stats_csv = _parse_csv_seen(csv_path)

    idx: Dict[str, Any] = {tid: True for tid in ids_csv}
    idx["_titles_norm_pairs"] = titles_csv
    idx["_stats"] = {
        "csv": stats_csv,
        # Web scrape can be added later; keep shape predictable
        "web": {"rows": 0, "ids": 0, "titles": 0},
    }
    return idx

def seen_index_stats(seen_idx: Dict[str, Any]) -> Tuple[int, int, Dict[str, int], Dict[str, int]]:
    """
    Return (id_count, title_count, csv_stats, web_stats) from the seen index.
    """
    ids = sum(1 for k, v in seen_idx.items() if isinstance(k, str) and k.startswith("tt") and v is True)
    titles = len(seen_idx.get("_titles_norm_pairs", []))
    st = seen_idx.get("_stats", {}) if isinstance(seen_idx, dict) else {}
    return ids, titles, st.get("csv", {}), st.get("web", {})

# ----------------------------
# Exclusion by id and fuzzy title/year
# ----------------------------

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
    """
    Drops items that appear to be seen by IMDb id (if available later) or by robust title+year match.
    Note: discover items typically lack imdb_id; we rely on title/year matching here.
    """
    seen_pairs: List[Tuple[str, Optional[int]]] = seen_idx.get("_titles_norm_pairs", []) if isinstance(seen_idx, dict) else []
    out: List[Dict[str, Any]] = []
    for it in pool:
        title = it.get("title") or it.get("name") or ""
        # Year from either field
        y = None
        if it.get("release_date"):
            try:
                y = int((it["release_date"] or "0000")[:4])
            except Exception:
                y = None
        elif it.get("first_air_date"):
            try:
                y = int((it["first_air_date"] or "0000")[:4])
            except Exception:
                y = None
        elif it.get("year"):
            try:
                y = int(it["year"])
            except Exception:
                y = None

        iid = (it.get("imdb_id") or "").strip()
        if iid and iid in seen_idx:
            continue
        if title and _matches_seen_by_title(title, y, seen_pairs):
            continue
        out.append(it)
    return out

# ----------------------------
# Scoring (recency bonus but NOT a filter)
# ----------------------------

def _year_from_item(it: Dict[str, Any]) -> Optional[int]:
    for k in ("release_date","first_air_date"):
        if it.get(k):
            try:
                return int((it[k] or "0000")[:4])
            except Exception:
                pass
    if it.get("year"):
        try:
            return int(it["year"])
        except Exception:
            pass
    return None

def score_items(cfg: Any, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Combine TMDB audience proxy (vote_average) with a small recency bonus and
    a tiny TV commitment penalty. No genre personalization yet.
    """
    # knobs
    critic_weight = getattr(cfg, "critic_weight", 0.0) if hasattr(cfg, "critic_weight") else 0.0
    audience_weight = getattr(cfg, "audience_weight", 1.0) if hasattr(cfg, "audience_weight") else 1.0
    commitment_cost_scale = getattr(cfg, "commitment_cost_scale", 1.0) if hasattr(cfg, "commitment_cost_scale") else 1.0

    current_year = datetime.utcnow().year

    ranked: List[Dict[str, Any]] = []
    for it in items:
        aud = max(0.0, min(1.0, float(it.get("vote_average") or 0.0) / 10.0))
        cri = 0.0 * critic_weight  # placeholder

        # Recency bonus (soft): up to +0.03 for <= 2 years old, fades by 6 years
        y = _year_from_item(it)
        rec_bonus = 0.0
        if y:
            age = max(0, current_year - y)
            if age <= 2:
                rec_bonus = 0.03
            elif age <= 6:
                # linear fade: 2->6 years => 0.03->0.0
                rec_bonus = 0.03 * (1.0 - (age - 2) / 4.0)

        penalty = 0.0
        kind = it.get("media_type") or it.get("kind")
        if kind == "tv":
            penalty = 0.02 * commitment_cost_scale

        base = audience_weight * aud + critic_weight * cri + rec_bonus
        match = round(100.0 * max(0.0, base - penalty), 1)

        ranked.append({
            "media_type": "tv" if kind == "tv" else "movie",
            "tmdb_id": it.get("tmdb_id"),
            "title": it.get("title"),
            "year": y,
            "type": "tvSeries" if kind == "tv" else "movie",
            "audience": round(aud * 100, 1),
            "critic": round(cri * 100, 1),
            "match": match,
            "providers": it.get("providers", []),
            # If enrichment ran earlier, keep watch_available passthrough
            "watch_available": it.get("watch_available", []),
        })

    ranked.sort(key=lambda r: r["match"], reverse=True)
    return ranked