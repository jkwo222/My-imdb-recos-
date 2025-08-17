# engine/rank.py
from __future__ import annotations

import math
import json
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd


# ----------------------------
# Helpers
# ----------------------------

def _safe(val, default=0.0):
    try:
        if val is None:
            return default
        if isinstance(val, str) and not val.strip():
            return default
        return float(val)
    except Exception:
        return default


def _norm(vec: Dict[str, float]) -> Dict[str, float]:
    s = sum(x * x for x in vec.values())
    if s <= 0:
        return vec
    mag = math.sqrt(s)
    return {k: v / mag for k, v in vec.items()}


def _dot(a: Dict[str, float], b: Dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    # iterate over smaller dict
    if len(a) > len(b):
        a, b = b, a
    return sum(v * b.get(k, 0.0) for k, v in a.items())


def _extract_people(credits: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    cast_names = []
    crew_names = []
    if not isinstance(credits, dict):
        return cast_names, crew_names
    if isinstance(credits.get("cast"), list):
        cast_names = [c.get("name") for c in credits["cast"] if isinstance(c, dict) and c.get("name")]
    if isinstance(credits.get("crew"), list):
        crew_names = [c.get("name") for c in credits["crew"] if isinstance(c, dict) and c.get("name")]
    return cast_names, crew_names


def _get_external_scores(meta: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Pull critic/audience scores from a few common keys.
    Return values in [0, 100] or None.
    """
    critic_keys = ["critic_score", "rt_critic", "metacritic", "metacritic_score"]
    audience_keys = ["audience_score", "rt_audience", "imdb_audience_score"]

    critic = None
    audience = None

    for k in critic_keys:
        if k in meta and meta[k] is not None:
            critic = _safe(meta[k], None)
            break
    for k in audience_keys:
        if k in meta and meta[k] is not None:
            audience = _safe(meta[k], None)
            break

    # clamp to 0..100 if present
    def clamp01(x):
        if x is None:
            return None
        return max(0.0, min(100.0, float(x)))

    return clamp01(critic), clamp01(audience)


def _signed_boost(score_0_100: Optional[float]) -> float:
    """
    Map a 0..100 external score to a signed impact in [-1, +1]
    with a neutral zone around 50..65 => 0.
      - 0 -> -1 (max penalty)
      - 50..65 -> ~0 (neutral)
      - 100 -> +1 (max boost)
    Piecewise-linear for clarity and predictability.
    """
    if score_0_100 is None:
        return 0.0

    s = float(score_0_100)

    # Below 50: linear penalty from -1 at 0 up to 0 at 50
    if s < 50.0:
        return (s / 50.0) - 1.0  # 0 -> -1, 50 -> 0

    # Neutral zone 50..65: exactly 0
    if 50.0 <= s <= 65.0:
        return 0.0

    # Above 65: linear boost from 0 at 65 to +1 at 100
    # 35 points span -> divide by 35
    return (s - 65.0) / 35.0  # 65 -> 0, 100 -> +1


# ----------------------------
# Profile building
# ----------------------------

def build_profile_from_ratings(ratings_csv: str) -> Dict[str, Dict[str, float]]:
    """
    Create a user 'DNA' from the IMDb ratings CSV:
    - boosts genres/directors/writers weighted by (rating - user_mean)
    - small temporal taste via release year
    """
    try:
        df = pd.read_csv(ratings_csv)
    except Exception:
        return {"genre": {}, "cast": {}, "director": {}, "writer": {}, "year": {}}

    col_map = {c.lower(): c for c in df.columns}
    def col(*cands):
        for c in cands:
            if c.lower() in col_map:
                return col_map[c.lower()]
        return None

    c_rating = col("Your Rating", "your rating")
    c_year = col("Year", "year")
    c_genres = col("Genres", "genres")
    c_directors = col("Directors", "director", "Directors")
    c_writers = col("Writers", "writer", "Writers")

    if not c_rating:
        return {"genre": {}, "cast": {}, "director": {}, "writer": {}, "year": {}}

    df = df.dropna(subset=[c_rating])
    df["_rating"] = pd.to_numeric(df[c_rating], errors="coerce")
    df = df.dropna(subset=["_rating"])
    if df.empty:
        return {"genre": {}, "cast": {}, "director": {}, "writer": {}, "year": {}}

    user_mean = df["_rating"].mean()

    genre_weights: Dict[str, float] = defaultdict(float)
    director_weights: Dict[str, float] = defaultdict(float)
    writer_weights: Dict[str, float] = defaultdict(float)
    year_weights: Dict[str, float] = defaultdict(float)

    for _, row in df.iterrows():
        r = float(row["_rating"])
        delta = r - user_mean  # positive if above your norm
        boost = 1.0 + max(delta, -2.0) * 0.5  # stable; above-avg ≈ >1.0

        if c_genres and isinstance(row.get(c_genres), str):
            for g in [x.strip() for x in row[c_genres].split(",") if x.strip()]:
                genre_weights[g] += boost

        if c_directors and isinstance(row.get(c_directors), str):
            for d in [x.strip() for x in row[c_directors].split(",") if x.strip()]:
                director_weights[d] += boost

        if c_writers and isinstance(row.get(c_writers), str):
            for w in [x.strip() for x in row[c_writers].split(",") if x.strip()]:
                writer_weights[w] += boost

        if c_year and pd.notna(row.get(c_year)):
            try:
                year = str(int(row[c_year]))
                year_weights[year] += 0.25 * boost
            except Exception:
                pass

    return {
        "genre": _norm(dict(genre_weights)),
        "director": _norm(dict(director_weights)),
        "writer": _norm(dict(writer_weights)),
        "year": _norm(dict(year_weights)),
        # cast is derived during scoring from candidate credits
        "cast": {},
    }


# ----------------------------
# Scoring
# ----------------------------

def score_candidate(
    item: Dict[str, Any],
    profile: Dict[str, Dict[str, float]],
    w_critic: float = 0.35,
    w_audience: float = 0.35,
) -> float:
    """
    Blend of:
      - profile similarity (genres, people, year signal)
      - public TMDB signals if present (vote_average, vote_count)
      - signed external (critic/audience) boosts/penalties with a 50–65 neutral band
    """
    meta = item or {}
    genres = [g.get("name") for g in (meta.get("genres") or []) if isinstance(g, dict) and g.get("name")]
    genre_vec = {g: 1.0 for g in genres}

    credits = meta.get("credits") or {}
    # cast
    cast_names = []
    if isinstance(credits.get("cast"), list):
        cast_names = [c.get("name") for c in credits["cast"] if isinstance(c, dict) and c.get("name")]
    cast_vec = {n: 1.0 for n in cast_names[:10]}  # cap noise

    # director/writer from crew
    directors = [c.get("name") for c in credits.get("crew", []) if c.get("job") == "Director" and c.get("name")]
    writers = [c.get("name") for c in credits.get("crew", []) if c.get("department") == "Writing" and c.get("name")]
    director_vec = {n: 1.0 for n in directors}
    writer_vec = {n: 1.0 for n in writers}

    year = str(meta.get("release_year") or meta.get("first_air_year") or "")
    year_vec = {year: 1.0} if year else {}

    sim = (
        0.45 * _dot(_norm(genre_vec), profile.get("genre", {})) +
        0.20 * _dot(_norm(cast_vec), profile.get("cast", {})) +
        0.20 * _dot(_norm(director_vec), profile.get("director", {})) +
        0.05 * _dot(_norm(writer_vec), profile.get("writer", {})) +
        0.10 * _dot(_norm(year_vec), profile.get("year", {}))
    )

    # Public signals (TMDB)
    tmdb_rating = _safe(meta.get("vote_average"), 0.0) / 10.0  # 0..1
    tmdb_count = _safe(meta.get("vote_count"), 0.0)
    pop_signal = tmdb_rating * (1.0 + min(tmdb_count, 2000.0) / 2000.0 * 0.25)  # slight boost if many votes

    # Signed critic/audience
    critic_raw, audience_raw = _get_external_scores(meta)
    critic_signed = _signed_boost(critic_raw)     # [-1, +1]
    audience_signed = _signed_boost(audience_raw) # [-1, +1]
    external = w_critic * critic_signed + w_audience * audience_signed

    # slight freshness preference if 'release_year' or 'first_air_year' recent
    try:
        y = int(meta.get("release_year") or meta.get("first_air_year") or 0)
        recency = max(0, y - 2015) / 10.0  # 2016..2025 ~ 0..1
    except Exception:
        recency = 0.0

    # Final blend: make external meaningful but not dominant
    # Range intuition:
    # - sim in [0..1]
    # - pop in [0..~1.25]
    # - external in [-w_sum..+w_sum] (e.g., [-0.7..+0.7] with defaults)
    # - recency in [0..1]
    return 0.55 * sim + 0.20 * pop_signal + 0.20 * external + 0.05 * recency


# ----------------------------
# Ranking entrypoint
# ----------------------------

def rank_pool(
    pool: List[Dict[str, Any]],
    cfg,
    meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Rank the pool using your profile DNA + public signals + signed external scores.
    Returns (ranked_list, rank_meta).
    """
    profile = build_profile_from_ratings(cfg.ratings_csv)

    ranked = []
    for it in pool:
        s = score_candidate(
            it, profile, w_critic=cfg.weight_critic, w_audience=cfg.weight_audience
        )
        rec = dict(it)
        rec["_score"] = round(float(s), 6)
        # Include what we used so you can inspect later
        critic_raw, audience_raw = _get_external_scores(it)
        rec["_external"] = {
            "critic_raw": critic_raw,
            "audience_raw": audience_raw,
            "critic_signed": round(_signed_boost(critic_raw), 4) if critic_raw is not None else None,
            "audience_signed": round(_signed_boost(audience_raw), 4) if audience_raw is not None else None,
        }
        ranked.append(rec)

    ranked.sort(key=lambda x: x.get("_score", 0.0), reverse=True)

    rmeta = {
        "weights": {"critic": cfg.weight_critic, "audience": cfg.weight_audience},
        "profile_dims": {k: len(v or {}) for k, v in profile.items()},
        "top_sample": [
            {
                "title": (x.get("title") or x.get("name") or ""),
                "score": x.get("_score"),
                "tmdb_id": x.get("id"),
                "kind": x.get("media_type") or ("tv" if x.get("first_air_date") else "movie"),
                "external": x.get("_external"),
            }
            for x in ranked[:5]
        ],
    }

    # persist a small debug artifact
    try:
        dbg = {
            "profile": {k: dict(sorted((v or {}).items(), key=lambda kv: -kv[1])[:20]) for k, v in profile.items()},
            "meta": rmeta,
        }
        with open("data/debug/rank_debug.json", "w", encoding="utf-8") as f:
            json.dump(dbg, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    return ranked, rmeta