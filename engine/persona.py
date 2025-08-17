from __future__ import annotations
import csv, math, pathlib, datetime
from collections import defaultdict
from typing import Dict, List, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
USER_RATINGS = DATA / "ratings.csv"   # your exported IMDb ratings

def _now_year() -> int:
    return datetime.date.today().year

def _read_user_ratings() -> List[dict]:
    """
    Expected minimal columns in ratings.csv:
      - titleId (tconst), yourRating (1..10), title, year, genres (pipe- or comma-separated ok)
    We’ll be lenient (header name variations & missing fields are ok).
    """
    if not USER_RATINGS.exists():
        return []
    rows = []
    with USER_RATINGS.open("r", encoding="utf-8") as f:
        snif = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = snif.sniff(sample) if sample else csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        for r in reader:
            # normalize keys
            r = { (k or "").strip(): (v or "").strip() for k, v in r.items() }
            # tolerate different header names
            r["tconst"] = r.get("tconst") or r.get("titleId") or r.get("Const") or ""
            r["yourRating"] = r.get("yourRating") or r.get("Your Rating") or r.get("rating") or ""
            r["year"] = r.get("year") or r.get("Year") or r.get("releaseYear") or ""
            r["genres"] = r.get("genres") or r.get("Genres") or r.get("genre") or ""
            try:
                r["yourRating"] = float(r["yourRating"]) if r["yourRating"] else None
            except Exception:
                r["yourRating"] = None
            try:
                r["year"] = int(r["year"]) if r["year"] else None
            except Exception:
                r["year"] = None
            rows.append(r)
    return [r for r in rows if r.get("yourRating")]

def _split_genres(g: str) -> List[str]:
    if not g:
        return []
    g = g.replace("|", ",")
    return [x.strip() for x in g.split(",") if x.strip() and x.strip() != "\\N"]

def build_genre_profile() -> Dict[str, float]:
    """
    Returns per-genre preference weights centered ~0:
      positive -> you like it, negative -> you avoid it.
    Combines (a) your average rating for the genre minus your global mean,
             (b) volume/commitment (more watches => more confident),
             (c) mild recency weight (recent watches matter a bit more).
    """
    rows = _read_user_ratings()
    if not rows:
        return {}  # no personalization data

    # global stats
    ratings = [r["yourRating"] for r in rows if r.get("yourRating")]
    if not ratings:
        return {}
    global_mean = sum(ratings) / len(ratings)

    per_g_sum = defaultdict(float)
    per_g_n   = defaultdict(int)
    per_g_wsum = defaultdict(float)

    now_y = _now_year()
    for r in rows:
        y = r.get("yourRating") or 0.0
        year = r.get("year")
        # recency weight: 0.75..1.25 for most titles (gentle)
        rec = 1.0
        if year:
            age = max(0, now_y - int(year))
            rec = 1.25 * math.exp(-age / 12.0) + 0.75  # age=0 → ~2.0, age=10 → ~1.0, age=20 → ~0.9
            rec = max(0.75, min(1.5, rec))
        for g in _split_genres(r.get("genres", "")):
            per_g_sum[g] += (y - global_mean) * rec
            per_g_wsum[g] += rec
            per_g_n[g] += 1

    # convert to normalized weights
    weights: Dict[str, float] = {}
    max_abs = 1e-9
    for g in per_g_sum:
        if per_g_wsum[g] <= 0:
            continue
        # genre delta (how much above/below your mean)
        delta = per_g_sum[g] / per_g_wsum[g]
        # confidence factor (more titles -> closer to 1.0)
        conf = 1 - math.exp(-per_g_n[g] / 6.0)   # ~0.8 by ~10 watches
        w = delta * conf
        weights[g] = w
        max_abs = max(max_abs, abs(w))

    # squash to -1..+1 range for stability
    for g in list(weights.keys()):
        weights[g] = weights[g] / max_abs
    return weights

def genre_alignment_score(item_genres: List[str], genre_weights: Dict[str, float]) -> Tuple[float, List[str]]:
    """
    Returns:
      alignment in 0..1 (0.5 means neutral),
      and the top contributing genres (positive or negative) for explanation.
    """
    if not item_genres or not genre_weights:
        return 0.5, []

    contribs = []
    total = 0.0
    seen = 0
    for g in item_genres:
        if g in genre_weights:
            seen += 1
            w = genre_weights[g]
            total += w
            contribs.append((g, w))
    if seen == 0:
        return 0.5, []
    avg = total / seen  # -1..1
    # map -1..1 to 0..1 with 0.5 neutral
    align = 0.5 + 0.5 * avg
    # strongest 2 contributors for “why”
    contribs.sort(key=lambda x: abs(x[1]), reverse=True)
    top = [f"{g}{'+' if w>0 else '−'}" for g, w in contribs[:2]]
    return max(0.0, min(1.0, align)), top