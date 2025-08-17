from __future__ import annotations
import csv
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from .util import safe_read_csv_dicts, normalize_title, parse_year, parse_date

def _split_genres(s: str) -> List[str]:
    if not s:
        return []
    out = []
    for part in s.replace("|", ",").split(","):
        g = part.strip()
        if g:
            out.append(g)
    return out

def _rating_val(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        v = float(s.strip())
        if 0 <= v <= 10:
            return v
    except Exception:
        return None
    return None

def _recency_weight(date_str: str) -> float:
    if not date_str:
        return 1.0
    d = parse_date(date_str)
    if not d:
        return 1.0
    from datetime import date
    days = (date.today() - d).days
    if days <= 30:
        return 1.3
    if days <= 90:
        return 1.2
    if days <= 365:
        return 1.1
    return 1.0

def _collect_from_csv(path: Path) -> Tuple[Dict[str, float], int]:
    by_genre: Dict[str, float] = {}
    rows = safe_read_csv_dicts(path)
    count = 0
    for r in rows:
        rating = _rating_val(r.get("Your Rating") or r.get("rating") or r.get("userRating") or "")
        if rating is None:
            continue
        gcell = r.get("Genres") or r.get("genres") or ""
        rated_at = r.get("Date Rated") or r.get("ratedAt") or r.get("Timestamp") or ""
        weight = rating / 10.0 * _recency_weight(rated_at)
        for g in _split_genres(gcell):
            by_genre[g] = by_genre.get(g, 0.0) + weight
        count += 1
    return by_genre, count

def _normalize(d: Dict[str, float]) -> Dict[str, float]:
    if not d:
        return {}
    m = max(d.values())
    if m <= 0:
        return {}
    return {k: round(v / m, 4) for k, v in d.items()}

def compute_taste_weights(env: Dict[str, Any], ratings_csv_path: Optional[str] = None) -> Dict[str, Any]:
    paths = []
    if ratings_csv_path:
        paths.append(Path(ratings_csv_path))
    paths += [Path("data/user/ratings.csv"), Path("data/ratings.csv")]
    path = next((p for p in paths if p.exists()), None)

    genre_weights: Dict[str, float] = {}
    n_rows = 0
    if path:
        genre_weights, n_rows = _collect_from_csv(path)
        genre_weights = _normalize(genre_weights)

    return {
        "has_ratings": bool(n_rows),
        "n_rows": n_rows,
        "genre_weights": genre_weights,
    }

# Optional helper if callers want a per-item small bonus
def recency_bonus_for_item(item: Dict[str, Any]) -> float:
    date_str = item.get("release_date") or item.get("first_air_date") or ""
    if not date_str:
        return 0.0
    from datetime import date
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

def genre_affinity_bonus(item: Dict[str, Any], genre_weights: Dict[str, float]) -> float:
    if not genre_weights:
        return 0.0
    # Works if upstream enrichment has string genre names:
    names = item.get("genres") or []
    if not isinstance(names, list):
        return 0.0
    s = 0.0
    for n in names:
        w = genre_weights.get(str(n), 0.0)
        s += w
    # Map sum of normalized weights to 0..5
    return min(5.0, s * 2.5)