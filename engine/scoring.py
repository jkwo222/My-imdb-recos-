from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
from .config import Config
from .ratings import load_seen_ids as _load_seen_ids

@dataclass
class Weights:
    critic: float
    audience: float
    novelty_pressure: float
    commitment_cost_scale: float

def load_seen_index(_path: str | None = None):
    """Back-compat: runner used sc.load_seen_index(path)."""
    return _load_seen_ids(_path or "data/ratings.csv")

def _score_one(item: Dict[str, Any], w: Weights) -> Tuple[float, float, float, float]:
    """
    Returns tuple: (match, critic_component, audience_component, penalties)
    We favor audience > critic per weights; fall back gracefully when missing.
    """
    # Normalized 0..1 critic/audience if present, else None
    c = item.get("critic_score_norm")
    a = item.get("audience_score_norm")

    # Sensible fallbacks if no ratings fetched yet (avoid zeros dominating).
    if c is None and a is None:
        # Popularity proxy (0..1) if present, else mid.
        pop = item.get("_popularity_norm", 0.5)
        c = 0.4 * pop
        a = 0.6 * pop
    else:
        c = 0.0 if c is None else float(c)
        a = 0.0 if a is None else float(a)

    # Penalties: commitment cost (e.g., long runtimes or many seasons)
    penalties = 0.0
    cc = float(item.get("_commitment_cost_norm", 0.0))
    penalties += w.commitment_cost_scale * cc

    match = (w.critic * c) + (w.audience * a) - penalties
    return (match, c, a, penalties)

def rank_items(items: List[Dict[str, Any]], cfg: Config) -> List[Dict[str, Any]]:
    w = Weights(
        critic=cfg.critic_weight,
        audience=cfg.audience_weight,
        novelty_pressure=cfg.novelty_pressure,
        commitment_cost_scale=cfg.commitment_cost_scale,
    )
    # Normalize optional proxies
    if items:
        pops = [float(x.get("popularity", 0.0) or 0.0) for x in items]
        maxp = max(pops) or 1.0
        for x in items:
            x["_popularity_norm"] = float(x.get("popularity", 0.0) or 0.0) / maxp

        # commitment cost proxy (episodes or runtime)
        costs = []
        for x in items:
            if x.get("type") == "tvSeries":
                # more seasons/episodes => higher cost
                seasons = int(x.get("seasons", 1) or 1)
                costs.append(seasons)
            else:
                rt = int(x.get("runtime", 110) or 110)
                costs.append(rt / 60)  # hours
        maxc = max(costs) or 1.0
        for x, c in zip(items, costs):
            x["_commitment_cost_norm"] = c / maxc

    for x in items:
        m, c, a, p = _score_one(x, w)
        x["match"] = round(100.0 * m, 1)  # 0..100
        x["critic"] = round(100.0 * c, 1)
        x["audience"] = round(100.0 * a, 1)

    # Novelty pressure: lightly boost items with more recent release date
    # (this is a placeholder; your previous engine may have a richer model)
    def novelty_boost(item: Dict[str, Any]) -> float:
        year = int(item.get("year", 0) or 0)
        base = item.get("match", 0.0)
        if year >= 2023:
            return base + 0.5 * cfg.novelty_pressure * 100.0
        return base

    items.sort(key=lambda x: (novelty_boost(x), x.get("popularity", 0.0)), reverse=True)
    return items