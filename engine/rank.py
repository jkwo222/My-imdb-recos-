# engine/rank.py
from __future__ import annotations

import csv
import dataclasses
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple, Any, Optional

# ----------------------------
# Public API
# ----------------------------

def rank_candidates(cfg, pool: List[dict], meta: Dict[str, Any]) -> List[dict]:
    """
    Rank a pool of TMDB movie/TV candidates for a single user.

    Inputs
    ------
    cfg: Config-like object (engine.config.Config). Only optional fields are used:
         - ratings_csv (str): default "data/ratings.csv"
         - shortlist_size (int): default 50
         - shown_size (int): default 10
         - audience_weight (float): default 0.30
         - critic_weight (float): default 0.20
         - recency_weight (float): default 0.15
         - popularity_weight (float): default 0.10
         - discovery_weight (float): default 0.25
         - year_now (int): override "current year" if desired
    pool: list of TMDB items (dict) as produced by catalog.build_pool(...)
          expected keys (best-effort): 
          - 'media_type' in {'movie','tv'}
          - 'title' or 'name'
          - 'id'
          - 'genre_ids' (List[int]) or 'genres' (List[dict{id,name}])
          - 'popularity' (float)
          - 'vote_average' (float)  # TMDB score
          - 'vote_count' (int)
          - 'release_date' or 'first_air_date' (YYYY-MM-DD)
          - 'original_language'
          - optional provider info at 'providers' or 'watch/providers'
    meta: dict returned by catalog; may be empty

    Returns
    -------
    ranked: list of dicts, each with:
      - all original TMDB item keys
      - 'score': float (final)
      - 'features': dict of feature contributions
      - 'reasons': List[str] human-readable explanations
    """
    # Load ratings and build the user profile DNA
    ratings_path = getattr(cfg, "ratings_csv", "data/ratings.csv")
    history = _load_imdb_ratings(ratings_path)

    profile = _build_profile(history)

    # Precompute genre id/name lookup from pool (best effort)
    gid_to_name = _infer_genre_lookup(pool)

    # Score every candidate
    scored: List[dict] = []
    weights = _weights_from_cfg(cfg)

    for item in pool:
        features, reasons = _score_item(item, profile, gid_to_name, cfg)
        score = (
            weights["audience"] * features["quality"]
            + weights["critic"] * features["critic_proxy"]
            + weights["recency"] * features["recency"]
            + weights["popularity"] * features["popularity"]
            + weights["discovery"] * features["profile_match"]
        )

        enriched = dict(item)
        enriched["score"] = round(float(score), 6)
        enriched["features"] = features
        enriched["reasons"] = reasons
        scored.append(enriched)

    # Normalize final scores to [0, 100] for nicer display
    _minmax_scale(scored, key="score", out_key="score")

    # Sort by score desc
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


# ----------------------------
# IMDb ratings ingestion
# ----------------------------

@dataclass
class RatingRow:
    title: str
    your_rating: Optional[float]
    genres: Tuple[str, ...]
    year: Optional[int]


def _load_imdb_ratings(path: str) -> List[RatingRow]:
    """
    Read IMDb ratings export CSV. Handles common column names:
    - "Title", "Your Rating", "Genres", "Year" (sometimes "Release Date" or "URL" exist)
    If a column is missing, we degrade gracefully.
    """
    rows: List[RatingRow] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                title = (r.get("Title") or r.get("Original Title") or r.get("Const") or "").strip()
                yr = _coerce_year(r.get("Year") or r.get("Release Date") or "")
                your_rating = _coerce_float(r.get("Your Rating") or r.get("Your rating") or r.get("Rating"))
                genres = _split_genres(r.get("Genres"))
                rows.append(RatingRow(title=title, your_rating=your_rating, genres=genres, year=yr))
    except FileNotFoundError:
        # No history — empty profile (we’ll fall back to global quality signals)
        rows = []
    return rows


def _coerce_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def _coerce_year(v: Any) -> Optional[int]:
    if not v:
        return None
    s = str(v).strip()
    # Accept YYYY or partial date
    for token in (s, s[:4]):
        try:
            year = int(token)
            if 1875 <= year <= 2100:
                return year
        except Exception:
            pass
    return None


def _split_genres(s: Optional[str]) -> Tuple[str, ...]:
    if not s:
        return tuple()
    # IMDb uses comma-separated; sometimes semicolons
    parts = [p.strip().lower() for p in s.replace(";", ",").split(",") if p.strip()]
    return tuple(dict.fromkeys(parts))  # dedupe but keep order


# ----------------------------
# Profile DNA building
# ----------------------------

@dataclass
class Profile:
    # Preference intensity by genre (lowercase name)
    genre_weight: Dict[str, float]
    # Preference by year bucket (decade)
    decade_weight: Dict[int, float]
    # Global stats
    mean_user_rating: float
    std_user_rating: float
    liked_titles: int
    total_rated: int


def _build_profile(history: List[RatingRow]) -> Profile:
    if not history:
        return Profile(genre_weight={}, decade_weight={}, mean_user_rating=0.0, std_user_rating=1.0, liked_titles=0, total_rated=0)

    ratings = [r.your_rating for r in history if r.your_rating is not None]
    mean_r = statistics.fmean(ratings) if ratings else 0.0
    std_r = statistics.pstdev(ratings) if len(ratings) > 1 else 1.0
    if std_r == 0:
        std_r = 1.0

    # Genre weights: sum (rating - mean) across appearances
    g_counter: Dict[str, float] = defaultdict(float)
    for r in history:
        if r.your_rating is None:
            continue
        delta = (r.your_rating - mean_r) / std_r
        for g in r.genres:
            g_counter[g] += delta

    # Decade weights similarly
    d_counter: Dict[int, float] = defaultdict(float)
    for r in history:
        if r.your_rating is None or r.year is None:
            continue
        decade = (r.year // 10) * 10
        delta = (r.your_rating - mean_r) / std_r
        d_counter[decade] += delta

    # Normalize to [-1, 1] by max abs
    def _norm_map(m: Dict[Any, float]) -> Dict[Any, float]:
        if not m:
            return {}
        max_abs = max(abs(v) for v in m.values())
        if max_abs == 0:
            return {k: 0.0 for k in m}
        return {k: max(min(v / max_abs, 1.0), -1.0) for k, v in m.items()}

    return Profile(
        genre_weight=_norm_map(g_counter),
        decade_weight=_norm_map(d_counter),
        mean_user_rating=mean_r,
        std_user_rating=std_r,
        liked_titles=sum(1 for r in history if (r.your_rating or 0) >= 8),
        total_rated=len(history),
    )


# ----------------------------
# Item scoring
# ----------------------------

def _infer_genre_lookup(pool: List[dict]) -> Dict[int, str]:
    """
    Try to infer a mapping from TMDB genre id -> lowercase name.
    Works whether items carry 'genre_ids' or full 'genres':[{'id','name'}].
    """
    res: Dict[int, str] = {}
    for it in pool:
        if "genres" in it and isinstance(it["genres"], list):
            for g in it["genres"]:
                if isinstance(g, dict) and "id" in g and "name" in g:
                    res[g["id"]] = str(g["name"]).strip().lower()
        if "genre_ids" in it and isinstance(it["genre_ids"], list):
            # leave names unknown; we’ll still compute overlap by id if needed
            for gid in it["genre_ids"]:
                res.setdefault(gid, str(gid))
    return res


def _get_year(item: dict) -> Optional[int]:
    date = item.get("release_date") or item.get("first_air_date") or ""
    if not date:
        return None
    try:
        y = int(str(date)[:4])
        if 1875 <= y <= 2100:
            return y
    except Exception:
        pass
    return None


def _score_item(item: dict, profile: Profile, gid_to_name: Dict[int, str], cfg) -> Tuple[Dict[str, float], List[str]]:
    # Features are all in [0,1]
    features: Dict[str, float] = {
        "quality": 0.0,
        "critic_proxy": 0.0,
        "recency": 0.0,
        "popularity": 0.0,
        "profile_match": 0.0,
    }
    reasons: List[str] = []

    # --- Quality (audience proxy): TMDB vote_average × sigmoid(vote_count)
    va = float(item.get("vote_average") or 0.0)
    vc = float(item.get("vote_count") or 0.0)
    quality = (va / 10.0) * _sigmoid(vc / 500.0)  # saturates around 500 votes
    features["quality"] = _clamp01(quality)
    if va >= 7.5 and vc >= 200:
        reasons.append("High audience score with solid vote count")

    # --- Critic proxy: combine popularity + long-tail boost for high VA but low VC
    pop = float(item.get("popularity") or 0.0)
    pop_norm = 1.0 - math.exp(-pop / 50.0)  # maps 0..inf to 0..1
    long_tail = max(0.0, (va - 7.0) / 3.0) * (1.0 - _sigmoid(vc / 200.0))
    critic_proxy = _clamp01(0.8 * pop_norm + 0.2 * long_tail)
    features["critic_proxy"] = critic_proxy

    # --- Recency: decay by years since release
    now_year = getattr(cfg, "year_now", None)
    if not now_year:
        from datetime import datetime
        now_year = datetime.utcnow().year
    year = _get_year(item)
    if year:
        age = max(0, now_year - year)
        recency = math.exp(-age / 12.0)  # 1 at current year, ~0.43 at 10y, ~0.20 at 20y
    else:
        recency = 0.2
    features["recency"] = _clamp01(recency)

    # --- Popularity (explicit feature so weight can be tuned)
    features["popularity"] = _clamp01(pop_norm)

    # --- Profile match: genres + decade
    # Gather item genres (names if possible)
    genre_names: List[str] = []
    if isinstance(item.get("genres"), list) and item["genres"] and isinstance(item["genres"][0], dict):
        genre_names = [str(g.get("name", "")).strip().lower() for g in item["genres"] if g]
    elif isinstance(item.get("genre_ids"), list):
        genre_names = [gid_to_name.get(gid, str(gid)) for gid in item["genre_ids"]]

    # Genre alignment score: average of positive weights for contained genres
    if profile.genre_weight and genre_names:
        vals = [profile.genre_weight.get(g, 0.0) for g in genre_names]
        # Shift [-1,1] to [0,1] with emphasis on positives
        genre_align = sum(max(0.0, v) for v in vals) / (len(vals) or 1)
    else:
        genre_align = 0.0

    # Decade alignment
    if profile.decade_weight:
        if year:
            decade = (year // 10) * 10
            dec_align = max(0.0, profile.decade_weight.get(decade, 0.0))
        else:
            dec_align = 0.0
    else:
        dec_align = 0.0

    # Combine, heavier on genres
    profile_match = _clamp01(0.8 * genre_align + 0.2 * dec_align)
    features["profile_match"] = profile_match

    # Build reasons
    t = item.get("title") or item.get("name") or "This title"
    g_print = ", ".join(sorted(set(genre_names))) if genre_names else None
    if profile_match >= 0.6 and g_print:
        reasons.append(f"Matches your favorite genres: {g_print}")
    if dec_align >= 0.4 and year:
        reasons.append(f"Fits a decade you tend to rate highly ({(year // 10) * 10}s)")
    if recency >= 0.6 and year:
        reasons.append(f"Recent release ({year})")
    if critic_proxy >= 0.6 and pop > 0:
        reasons.append("Critics/zeitgeist signal is strong")
    if quality >= 0.65:
        reasons.append("Audience-loved overall")

    # If we have providers/availability, add a reason if on-subscription
    prov = _extract_provider_names(item)
    if prov:
        reasons.append(f"Available on: {', '.join(sorted(prov))}")

    # Trim repetitive phrasing
    reasons = _uniq_preserve(reasons)
    return features, reasons


def _extract_provider_names(item: dict) -> List[str]:
    prov = []
    # Common shapes:
    # item['providers'] = {'flatrate':[{'provider_name':...}], 'ads':[...] ...}
    # item['watch/providers'] = same idea, or 'providers_flatrate': [...]
    for key in ("providers", "watch/providers", "watch_providers"):
        val = item.get(key)
        if isinstance(val, dict):
            for bucket in ("flatrate", "ads", "free"):
                arr = val.get(bucket)
                if isinstance(arr, list):
                    for p in arr:
                        name = (p.get("provider_name") or p.get("name") or "").strip()
                        if name:
                            prov.append(name)
        elif isinstance(val, list):
            for p in val:
                name = (p.get("provider_name") or p.get("name") or "").strip()
                if name:
                    prov.append(name)
    # legacy keys
    for key in ("providers_flatrate", "providers_ads", "providers_free"):
        arr = item.get(key)
        if isinstance(arr, list):
            for p in arr:
                name = (p.get("provider_name") or p.get("name") or "").strip()
                if name:
                    prov.append(name)
    return _uniq_preserve([p for p in prov if p])


def _weights_from_cfg(cfg) -> Dict[str, float]:
    # Reasonable defaults; feel free to tweak
    w = {
        "audience": getattr(cfg, "audience_weight", 0.30),
        "critic": getattr(cfg, "critic_weight", 0.20),
        "recency": getattr(cfg, "recency_weight", 0.15),
        "popularity": getattr(cfg, "popularity_weight", 0.10),
        "discovery": getattr(cfg, "discovery_weight", 0.25),  # profile DNA
    }
    # Normalize to sum=1.0
    s = sum(w.values()) or 1.0
    for k in list(w.keys()):
        w[k] = float(w[k]) / s
    return w


# ----------------------------
# Utils
# ----------------------------

def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _minmax_scale(items: List[dict], key: str, out_key: Optional[str] = None):
    out_key = out_key or key
    vals = [float(it.get(key, 0.0)) for it in items]
    if not vals:
        return
    mn, mx = min(vals), max(vals)
    if mx <= mn:
        for it in items:
            it[out_key] = 50.0
        return
    for it in items:
        v = float(it.get(key, 0.0))
        it[out_key] = (v - mn) / (mx - mn) * 100.0


def _uniq_preserve(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for s in seq:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out