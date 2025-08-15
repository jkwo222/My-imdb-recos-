# engine/scoring.py
from __future__ import annotations
from typing import List, Dict, Any
import math
import os
import random
import time

# TMDB genre IDs we treat as "unscripted-ish"
GENRE_REALITY = 10764
GENRE_TALK = 10767

def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return default

def _bayesian_mean(rating_0_to_10: float, votes: int, k: float, prior_0_to_10: float) -> float:
    """
    Bayesian smoothing (a.k.a. additive smoothing) for average rating.
    rating_0_to_10 -> convert to 0..1 inside, return 0..1.
    """
    R = max(0.0, min(10.0, float(rating_0_to_10))) / 10.0
    v = max(0, int(votes))
    k = max(0.0, float(k))
    C = max(0.0, min(10.0, float(prior_0_to_10))) / 10.0
    return (v * R + k * C) / (v + k) if (v + k) > 0 else C

def _popularity_factor(popularity: float) -> float:
    # soft cap and gentle boost; 0..~1
    pop = max(0.0, float(popularity))
    return min(1.0, math.log1p(pop) / math.log1p(200.0))

def _unscripted_penalty(genre_ids: List[int]) -> float:
    # small, configurable penalty; default 0.03 (very mild)
    penalty = _get_env_float("UNSCRIPTED_PENALTY", 0.03)
    if not genre_ids:
        return 0.0
    if (GENRE_REALITY in genre_ids) or (GENRE_TALK in genre_ids):
        return penalty
    return 0.0

def _min_votes_threshold(kind: str) -> int:
    if kind == "movie":
        return int(os.environ.get("VOTE_COUNT_MIN_MOVIE", "500"))
    return int(os.environ.get("VOTE_COUNT_MIN_TV", "300"))

def _normalize_weights(cw: float, aw: float) -> (float, float):
    cw = max(0.0, float(cw))
    aw = max(0.0, float(aw))
    total = cw + aw
    if total <= 0:
        return 0.25, 0.75
    return cw / total, aw / total

def score_and_rank(pool: List[Dict[str, Any]],
                   critic_weight: float = 0.25,
                   audience_weight: float = 0.75,
                   novelty_pressure: float = 0.15,
                   commitment_cost_scale: float = 1.0) -> List[Dict[str, Any]]:

    cw, aw = _normalize_weights(critic_weight, audience_weight)

    # Bayesian priors
    prior_movie = float(os.environ.get("BAYES_PRIOR_MOVIE", "7.2"))  # /10
    prior_tv = float(os.environ.get("BAYES_PRIOR_TV", "7.2"))
    k_movie = float(os.environ.get("BAYES_K_MOVIE", "1500"))
    k_tv = float(os.environ.get("BAYES_K_TV", "800"))

    rng = random.Random(hash(f"seed:{int(time.time())//900}") & 0xFFFFFFFF)  # gentle tie-breaking per 15-min window

    ranked: List[Dict[str, Any]] = []
    for it in pool:
        kind = it.get("type") or "movie"
        year = it.get("year")
        if not year:
            # drop bad metadata
            continue

        votes = int(it.get("vote_count") or 0)
        if votes < _min_votes_threshold(kind):
            # filter out low-signal titles
            continue

        tmdb_avg = float(it.get("vote_average") or 0.0)  # 0..10
        pop = float(it.get("popularity") or 0.0)
        genres = list(it.get("genre_ids") or [])

        # Bayesian-smoothed audience (0..1)
        if kind == "movie":
            aud = _bayesian_mean(tmdb_avg, votes, k_movie, prior_movie)
        else:
            aud = _bayesian_mean(tmdb_avg, votes, k_tv, prior_tv)

        # Critic score: if you don’t have an actual critic signal available here,
        # fall back to a softer version of audience (kept separate for weight).
        # If you later enrich items with RT/Metascore, put it here in 0..1.
        crit_raw = it.get("critic_score_norm")
        if crit_raw is None:
            crit = max(0.0, min(1.0, aud * 0.92))
        else:
            crit = max(0.0, min(1.0, float(crit_raw)))

        # Popularity boost (small), unscripted penalty (small)
        popf = _popularity_factor(pop)
        penalty = _unscripted_penalty(genres)

        # Base score
        score = (aw * aud + cw * crit)

        # small popularity blend (up to +15%)
        score *= (0.85 + 0.15 * popf)

        # Novelty pressure: gentle nudge to newer or simply break ties stochastically
        # We’ll use a very small random jitter bounded by novelty_pressure
        if novelty_pressure:
            score += rng.random() * max(0.0, float(novelty_pressure)) * 0.02

        # Commitment cost left as neutral unless you add runtime/season info later
        # score -= commitment_cost_scale * cost

        # Apply unscripted penalty
        score -= penalty

        it_out = dict(it)
        it_out["match"] = score * 100.0  # keep 0..100-ish for display
        ranked.append(it_out)

    ranked.sort(key=lambda x: x["match"], reverse=True)
    return ranked