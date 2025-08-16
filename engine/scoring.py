import os
import math
import time
from typing import List, Dict, Any, Tuple
import requests

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def _bayes_rating(r: float, v: int, m: int = 150, mu: float = 6.5) -> float:
    """Bayesian average (Wilson-like smoothing for TMDB score)."""
    r = float(r or 0.0)
    v = int(v or 0)
    return (v/(v+m))*r + (m/(v+m))*mu

def _normalize_10_to_100(x: float) -> float:
    return max(0.0, min(100.0, x * 10.0))

def _pull_omdb(ids: List[Tuple[str, int]], omdb_api_key: str) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """Best-effort OMDb enrichment for shortlist; returns map[(media_type,id)] -> scores."""
    out: Dict[Tuple[str, int], Dict[str, Any]] = {}
    if not omdb_api_key:
        return out
    # We don't have imdbIDs yet without another TMDB call; use TMDB vote data for fast pass
    # (If you later add TMDB external_ids lookup, plug it here.)
    return out

def score_items(
    items: List[dict],
    weights: Dict[str, float],
    shortlist_size: int = 250,
) -> Tuple[List[dict], Dict[str, Any]]:
    """Two-stage scoring: (1) TMDB-only pass, (2) optional OMDb enrichment on shortlist."""
    cw = float(weights.get("critic", 0.25))
    aw = float(weights.get("audience", 0.75))
    np = float(weights.get("novelty_pressure", 0.15))
    cc = float(weights.get("commitment_cost_scale", 1.0))

    # Stage 1: TMDB-based proxies
    enriched = []
    for it in items:
        r = float(it.get("tmdb_vote_average", 0.0))
        v = int(it.get("tmdb_vote_count", 0))
        bayes = _bayes_rating(r, v)          # ~ audience-ish proxy
        critic = r                            # weak proxy if no metascore available
        audience = bayes

        # Commitment cost (penalize extremely long engagement if we had data; neutral for now)
        commit_penalty = 0.0

        # Novelty: mild boost if recent (within ~24 months)
        year = it.get("year")
        if isinstance(year, int) and year >= 2000:
            age = max(0, 2025 - year)
            novelty = 1.0 - min(1.0, age / 15.0)  # linear fade over ~15y
        else:
            novelty = 0.2

        score = (cw * critic + aw * audience) * (1.0 - cc*commit_penalty)
        score = score * (1.0 + np * novelty)
        it2 = dict(it)
        it2["__score_stage1"] = score
        it2["__audience"] = audience
        it2["__critic"] = critic
        enriched.append(it2)

    # Shortlist by stage1
    enriched.sort(key=lambda x: (-x["__score_stage1"], -x.get("tmdb_vote_count", 0), -x.get("popularity", 0.0)))
    shortlist = enriched[:max(50, shortlist_size)]

    # Optional Stage 2: OMDb enrichment (disabled by default until external_ids are wired)
    # left in place for future plug-in
    # omdb_key = os.getenv("OMDB_API_KEY", "")
    # omdb_data = _pull_omdb([(it["media_type"], it["id"]) for it in shortlist], omdb_key)

    # Final score (currently same as stage 1; hook OMDb values as needed)
    final = []
    for it in shortlist:
        s = it["__score_stage1"]
        match = max(0.0, min(100.0, s * 10.0))  # scale to 0–100-ish display
        row = dict(it)
        row["match"] = round(match, 1)
        final.append(row)

    final.sort(key=lambda x: (-x["match"], -x.get("tmdb_vote_count", 0), -x.get("popularity", 0.0)))
    meta = {
        "ranked": len(final),
        "weights": {"critic": cw, "audience": aw, "novelty_pressure": np, "commitment_cost_scale": cc},
    }
    return final, meta