# engine/weights.py
from __future__ import annotations
import json, os
from typing import Dict, List

WEIGHTS = "data/weights_live.json"

def _default() -> Dict[str, float]:
    # Audience-first by default; novelty is small but present.
    return {
        "critic_weight": 0.30,
        "audience_weight": 0.65,
        "novelty_weight": 0.05,
        "commitment_cost_scale": 1.0,
    }

def load_weights() -> Dict[str, float]:
    if os.path.exists(WEIGHTS):
        try:
            w = json.load(open(WEIGHTS, "r"))
            # backfill any missing keys
            for k, v in _default().items():
                w.setdefault(k, v)
            return w
        except Exception:
            pass
    return _default()

def save_weights(w: Dict[str, float]) -> None:
    os.makedirs("data", exist_ok=True)
    json.dump(w, open(WEIGHTS, "w"), indent=2)

def update_from_ratings(rows: List[dict]) -> Dict[str, float]:
    """
    Nudge critic/audience weights based on your ratings:
    - More 8–10s than 1–5s => slightly increase audience weight.
    - Keep audience > critic by design.
    """
    pos = sum(1 for r in rows if float(r.get("your_rating", 0) or 0) >= 8)
    neg = sum(1 for r in rows if 0 < float(r.get("your_rating", 0) or 0) <= 5)
    total = max(1, len(rows))
    delta = (pos - neg) / total  # -1..+1-ish
    w = load_weights()

    # Start from current / default
    aw = float(w.get("audience_weight", 0.65))
    cw = float(w.get("critic_weight", 0.30))
    nw = float(w.get("novelty_weight", 0.05))

    # Gentle nudge: ±0.04 max per run
    aw = aw + 0.04 * delta
    cw = cw - 0.04 * delta * 0.6  # smaller counter-nudge on critic
    nw = max(0.0, min(0.12, nw))  # clamp novelty

    # Hard constraints: audience stays > critic
    aw = max(0.55, min(0.80, aw))
    cw = max(0.15, min(0.40, cw))

    # Normalize trio
    s = aw + cw + nw
    aw, cw, nw = aw / s, cw / s, nw / s

    w["audience_weight"], w["critic_weight"], w["novelty_weight"] = aw, cw, nw
    # keep commitment scale as-is (user-tunable)
    w["commitment_cost_scale"] = float(w.get("commitment_cost_scale", 1.0))
    save_weights(w)
    return w