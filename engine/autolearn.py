import json, os
WEIGHTS = "data/weights_live.json"

def _default():
    # critic/audience remain significant, continuous; tags not modeled here
    return {"critic_weight":0.5,"audience_weight":0.5,"commitment_cost_scale":1.0}

def load_weights():
    return json.load(open(WEIGHTS,"r")) if os.path.exists(WEIGHTS) else _default()

def save_weights(w): 
    os.makedirs("data",exist_ok=True)
    json.dump(w, open(WEIGHTS,"w"), indent=2)

def update_from_ratings(rows):
    # light-touch: keep critic & audience meaningful; no minimum thresholds
    pos = sum(1 for r in rows if float(r.get("your_rating",0) or 0)>=8)
    neg = sum(1 for r in rows if 0<float(r.get("your_rating",0) or 0)<=5)
    total = len(rows) or 1
    delta = (pos - neg)/total
    w = load_weights()
    # Nudge critic between 0.3 .. 0.7; audience is complementary
    w["critic_weight"] = float(min(0.7, max(0.3, w.get("critic_weight",0.5)+0.05*delta)))
    w["audience_weight"] = round(1.0 - w["critic_weight"], 3)
    save_weights(w)
    return w