# engine/weights.py
import json, os

WEIGHTS = "data/weights_live.json"

def _default():
    # audience-first, and the rest with sensible defaults
    return {
        "critic_weight": 0.35,
        "audience_weight": 0.65,
        "commitment_cost_scale": 1.0,
        "novelty_weight": 0.15,
    }

def load_weights():
    if os.path.exists(WEIGHTS):
        try:
            w = json.load(open(WEIGHTS, "r", encoding="utf-8"))
            # guardrails: always audience >= critic
            aw = float(w.get("audience_weight", 0.65))
            cw = float(w.get("critic_weight", 0.35))
            if aw < cw:
                aw, cw = 0.65, 0.35
            w["audience_weight"] = aw
            w["critic_weight"] = cw
            w.setdefault("commitment_cost_scale", 1.0)
            w.setdefault("novelty_weight", 0.15)
            return w
        except Exception:
            pass
    return _default()

def save_weights(w):
    os.makedirs("data", exist_ok=True)
    json.dump(w, open(WEIGHTS, "w", encoding="utf-8"), indent=2)

def update_from_ratings(rows):
    """
    Nudge weights based on your ratings:
      - count 8–10 as positive, 1–5 as negative, 5.5–6.5 ≈ neutral.
      - keeps audience >= critic.
    """
    pos = sum(1 for r in rows if float(r.get("your_rating", 0)) >= 8)
    neg = sum(1 for r in rows if 0 < float(r.get("your_rating", 0)) <= 5)
    total = max(1, len(rows))
    delta = (pos - neg) / total  # -1..+1 typically small

    w = load_weights()
    # move a little toward audience if you skew higher, away if you skew lower
    aw = float(w.get("audience_weight", 0.65)) + 0.05 * delta
    cw = 1.0 - aw
    # clamp and ensure audience >= critic
    aw = max(0.55, min(0.75, aw))
    cw = round(1.0 - aw, 3)
    w["audience_weight"] = round(aw, 3)
    w["critic_weight"] = cw
    save_weights(w)
    return w