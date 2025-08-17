from __future__ import annotations

import csv
import math
import os
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .tmdb import search_title_once


def _read_ratings_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in r.items()})
    return rows


def _get_title(row: Dict[str, str]) -> str:
    return row.get("title") or row.get("Title") or row.get("primaryTitle") or row.get("originalTitle") or ""


def _get_year(row: Dict[str, str]) -> Optional[int]:
    y = row.get("year") or row.get("Year") or row.get("startYear") or ""
    try:
        return int(y) if y else None
    except Exception:
        return None


def _get_rating(row: Dict[str, str]) -> Optional[float]:
    r = row.get("Your Rating") or row.get("my_rating") or row.get("rating") or ""
    try:
        return float(r) if r else None
    except Exception:
        return None


def _cap(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _z(x: float, mean: float, std: float) -> float:
    return 0.0 if std <= 1e-9 else (x - mean) / std


def _genre_jaccard_weight(item_genres: Iterable[int], liked: Dict[int, float]) -> float:
    if not item_genres or not liked:
        return 0.0
    # Weighted overlap / sqrt(norms) (cosine-like but on IDs)
    s = sum(liked.get(g, 0.0) for g in item_genres)
    denom = math.sqrt(sum(v * v for v in liked.values())) * math.sqrt(len(list(item_genres)))
    return (s / denom) if denom > 0 else 0.0


def build_profile_from_ratings(cfg: Any) -> Dict[str, Any]:
    """
    Build a taste profile from ratings.csv.
    Optionally enrich a limited number of titles via TMDB search to grab genres for DNA.
    """
    rows = _read_ratings_csv(cfg.ratings_csv)
    # Basic stats
    ratings: List[float] = []
    by_lang = Counter()
    by_year = Counter()
    liked_genres: Dict[int, float] = defaultdict(float)

    # Optional enrichment
    augment = bool(cfg.augment_profile)
    augment_limit = int(cfg.augment_profile_limit or 0)
    lang = cfg.tmdb_language or "en-US"
    timeout = int(cfg.tmdb_read_timeout or 20)
    use_bearer = bool(cfg.tmdb_use_bearer)

    augmented = 0

    for r in rows:
        title = _get_title(r)
        if not title:
            continue
        year = _get_year(r)
        user_rating = _get_rating(r)
        if user_rating is not None:
            ratings.append(user_rating)
        if year:
            by_year[year] += 1

        # Attempt to guess language from title if present in pool later; for now skip.
        # Enrich with TMDB to get genres (capped)
        if augment and augmented < augment_limit:
            try:
                data = search_title_once(title, year, lang, timeout, use_bearer)
                results = data.get("results") or []
                if results:
                    top = results[0]
                    gids = top.get("genre_ids") or []
                    for g in gids:
                        # weight by user's score if present, else modest default
                        w = 0.5 + (user_rating or 7.0) / 10.0
                        liked_genres[int(g)] += w
                    augmented += 1
            except Exception:
                # search errors are non-fatal
                pass

    mean = (sum(ratings) / len(ratings)) if ratings else 7.0
    std = (sum((x - mean) ** 2 for x in ratings) / len(ratings)) ** 0.5 if ratings else 1.5

    # Year preference as weighted mean/stdev
    if by_year:
        ys = []
        for y, c in by_year.items():
            ys += [y] * c
        ymean = sum(ys) / len(ys)
        ystd = (sum((y - ymean) ** 2 for y in ys) / len(ys)) ** 0.5 if len(ys) > 1 else 10.0
    else:
        ymean, ystd = 2016.0, 8.0

    return {
        "rating_mean": mean,
        "rating_std": std,
        "year_mean": ymean,
        "year_std": ystd,
        "liked_genres": dict(liked_genres),
    }


def _year_score(year: Optional[int], ymean: float, ystd: float) -> float:
    if not year:
        return 0.0
    # Penalize very far years mildly; reward closeness to user's center
    z = abs((year - ymean) / max(ystd, 1.0))
    return _cap(1.5 - 0.3 * z, 0.0, 1.5)


def _pop_score(popularity: float) -> float:
    # Saturate at ~100
    return _cap(popularity / 100.0, 0.0, 1.0)


def _vote_score(vote_average: float, vote_count: int) -> float:
    # Bayesian-ish: require some count to fully trust average
    weight = _cap(math.log10(1 + vote_count) / 3.0, 0.0, 1.0)
    return (vote_average / 10.0) * (0.5 + 0.5 * weight)


def rank_pool(
    pool: List[Dict[str, Any]],
    cfg: Any,
    *,
    profile: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Compute a personalized 'match' score for each item.
    """
    if profile is None:
        profile = build_profile_from_ratings(cfg)

    critic_w = float(cfg.critic_weight or 0.6)   # influences vote_average
    audience_w = float(cfg.audience_weight or 0.4)  # influences popularity / vote_count
    genre_w = 0.9
    year_w = 0.6

    ymean = profile["year_mean"]
    ystd = profile["year_std"]
    liked_genres = {int(k): float(v) for k, v in (profile.get("liked_genres") or {}).items()}

    out: List[Dict[str, Any]] = []

    for it in pool:
        va = float(it.get("vote_average") or 0.0)
        vc = int(it.get("vote_count") or 0)
        pop = float(it.get("popularity") or 0.0)
        year = None
        try:
            year = int(it.get("year")) if it.get("year") else None
        except Exception:
            year = None

        gscore = _genre_jaccard_weight(it.get("genre_ids") or [], liked_genres)  # 0..~?
        yscore = _year_score(year, ymean, ystd)  # 0..1.5
        vscore = _vote_score(va, vc)            # 0..1
        pscore = _pop_score(pop)                # 0..1

        # Blend
        score = (genre_w * gscore) + (year_w * yscore) + (critic_w * vscore) + (audience_w * pscore)

        out.append({**it, "match": round(float(score), 4)})

    # Higher is better
    out.sort(key=lambda x: x.get("match", 0.0), reverse=True)
    return out