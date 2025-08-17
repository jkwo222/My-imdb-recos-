# engine/rank.py
from __future__ import annotations

import math
import json
from collections import Counter, defaultdict
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd


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


def build_profile_from_ratings(ratings_csv: str) -> Dict[str, Dict[str, float]]:
    """
    Create a user 'DNA' from the IMDb ratings CSV:
    - boosts genres/directors/cast weighted by (rating - user_mean)
    - slight decay for very old watches (if 'Date Rated' column exists)
    The CSV format is the standard IMDb export.
    """
    try:
        df = pd.read_csv(ratings_csv)
    except Exception:
        return {
            "genre": {},
            "cast": {},
            "director": {},
            "writer": {},
            "year": {},
        }

    # Normalize columns we care about (IMDb export field names can vary)
    # Expect at least: Title, URL, Your Rating, Year, Genres, and optionally Directors, Writers
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
        boost = 1.0 + max(delta, -2.0) * 0.5  # keep stable; above-avg ≈ >1.0

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
                year_weights[year] += 0.25 * boost  # small temporal taste

    return {
        "genre": _norm(dict(genre_weights)),
        "director": _norm(dict(director_weights)),
        "writer": _norm(dict(writer_weights)),
        "year": _norm(dict(year_weights)),
        # cast is derived during scoring from candidate credits
        "cast": {},
    }


def score_candidate(
    item: Dict[str, Any],
    profile: Dict[str, Dict[str, float]],
    w_critic: float = 0.25,
    w_audience: float = 0.25,
) -> float:
    """
    Blend of:
      - profile similarity (genres, people, year signal)
      - public signals if present (tmdb vote_average, vote_count)
      - optional critic/audience scores on the item dict (if your pipeline adds them)
    """
    meta = item or {}
    genres = [g.get("name") for g in (meta.get("genres") or []) if isinstance(g, dict) and g.get("name")]
    genre_vec = {g: 1.0 for g in genres}

    credits = meta.get("credits") or {}
    cast_names, crew_names = _extract_people(credits)
    cast_vec = {n: 1.0 for n in cast_names[:10]}  # cap noise
    # treat director/writer from crew (if roles present)
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

    # Public signals
    tmdb_rating = _safe(meta.get("vote_average"), 0.0) / 10.0  # 0..1
    tmdb_count = _safe(meta.get("vote_count"), 0.0)
    pop_signal = tmdb_rating * (1.0 + min(tmdb_count, 2000.0) / 2000.0 * 0.25)  # slight boost if many votes

    # Optional critic/audience if you later add them in catalog
    critic = _safe(meta.get("critic_score"), 0.0) / 100.0
    audience = _safe(meta.get("audience_score"), 0.0) / 100.0
    external = w_critic * critic + w_audience * audience

    # slight freshness preference if 'release_year' or 'first_air_year' recent
    try:
        y = int(meta.get("release_year") or meta.get("first_air_year") or 0)
        recency = max(0, y - 2015) / 10.0  # 2016..2025 ~ 0..1
    except Exception:
        recency = 0.0

    return 0.60 * sim + 0.25 * pop_signal + 0.10 * external + 0.05 * recency


def rank_pool(
    pool: List[Dict[str, Any]],
    cfg,
    meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Rank the pool using your profile DNA + public signals.
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
            }
            for x in ranked[:5]
        ],
    }

    # persist a small debug artifact
    try:
        dbg = {
            "profile": {k: dict(sorted(v.items(), key=lambda kv: -kv[1])[:20]) for k, v in profile.items()},
            "meta": rmeta,
        }
        with open("data/debug/rank_debug.json", "w", encoding="utf-8") as f:
            json.dump(dbg, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    return ranked, rmeta