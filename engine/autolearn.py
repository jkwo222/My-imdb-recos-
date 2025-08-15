import json, os
WEIGHTS = "data/weights_live.json"

def _default():
    return {"critic_weight":0.54,"audience_weight":0.46,"commitment_cost_scale":1.0,"novelty_pressure":0.15}

def load_weights():
    return json.load(open(WEIGHTS,"r")) if os.path.exists(WEIGHTS) else _default()

def save_weights(w):
    os.makedirs("data",exist_ok=True)
    json.dump(w, open(WEIGHTS,"w"), indent=2)

def update_from_ratings(rows):
    # simple nudging based on your likes/dislikes
    pos = sum(1 for r in rows if float(r.get("your_rating",0))>=8)
    neg = sum(1 for r in rows if 0 < float(r.get("your_rating",0)) <= 5)
    total = len(rows) or 1
    delta = (pos - neg)/total
    w = load_weights()
    w["critic_weight"] = float(min(0.7, max(0.3, w.get("critic_weight",0.54)+0.05*delta)))
    w["audience_weight"] = round(1.0 - w["critic_weight"], 2)
    save_weights(w); return w