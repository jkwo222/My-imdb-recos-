# engine/feed.py
from __future__ import annotations

import json, os, time, pathlib, random
from typing import Dict, List, Any, Tuple

from .rank import rank_items, DEFAULT_WEIGHTS
from .provider_filter import any_allowed
from .recency import should_skip, mark_shown
from .taste import taste_boost_for

OUT_LATEST = pathlib.Path("data/out/latest/assistant_feed.json")
DEBUG_DIR  = pathlib.Path("data/debug")
DAILY_DIR  = pathlib.Path("data/out/daily")

def _now_slug() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

def _ensure_dirs():
    OUT_LATEST.parent.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    (DAILY_DIR / _now_slug()).mkdir(parents=True, exist_ok=True)

def _parse_subs_env(val: Any) -> List[str]:
    """
    Accept CSV: netflix,prime_video,...
           JSON list: ["netflix","hulu"]
    """
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    s = str(val).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    return [x.strip() for x in s.split(",") if x.strip()]

def _load_weights() -> Dict[str, Any]:
    path = pathlib.Path("data/weights_live.json")
    if path.exists():
        try:
            w = json.load(open(path, "r"))
            return {
                "audience_weight": float(w.get("audience_weight", DEFAULT_WEIGHTS["audience_weight"])),
                "critic_weight":   float(w.get("critic_weight",   DEFAULT_WEIGHTS["critic_weight"])),
                "commitment_cost_scale": float(w.get("commitment_cost_scale", DEFAULT_WEIGHTS["commitment_cost_scale"])),
                "novelty_weight":  float(w.get("novelty_weight", DEFAULT_WEIGHTS["novelty_weight"])),
                "min_match_cut":   float(w.get("min_match_cut", DEFAULT_WEIGHTS["min_match_cut"])),
            }
        except Exception:
            pass
    return dict(DEFAULT_WEIGHTS)

def _debug_write(name: str, payload: Any):
    p = DEBUG_DIR / f"{_now_slug()}_{name}.json"
    json.dump(payload, open(p, "w"), indent=2)

def _apply_provider_filter(pool: List[Dict], subs_keep: List[str]) -> Tuple[List[Dict], Dict[str,int]]:
    hits = []
    kept = []
    for it in pool:
        ok = any_allowed(it.get("providers") or [], subs_keep)
        if ok:
            kept.append(it)
            hits.append(it.get("providers") or [])
    # summarize
    agg: Dict[str,int] = {}
    for row in hits:
        for slug in row:
            agg[slug] = agg.get(slug, 0) + 1
    return kept, dict(sorted(agg.items(), key=lambda kv: (-kv[1], kv[0])))

def _drop_recent(pool: List[Dict]) -> List[Dict]:
    out = []
    for it in pool:
        iid = (it.get("imdb_id") or "").strip()
        if iid and should_skip(iid, days=4):
            continue
        out.append(it)
    return out

def build_feed(catalog: List[Dict],
               seen_filter: callable,
               taste_profile: Dict[str, float]) -> Dict[str, Any]:
    _ensure_dirs()

    subs_env = os.environ.get("SUBS_INCLUDE")
    subs_keep = _parse_subs_env(subs_env)  # [] means “use default allow-list inside any_allowed”
    weights = _load_weights()

    # 1) provider filter (if we have a keep-list OR allow defaults)
    pool0 = catalog or []
    pool1, prov_stats = _apply_provider_filter(pool0, subs_keep or [])
    _debug_write("step1_providers", {"in": len(pool0), "out": len(pool1), "subs_keep": subs_keep, "stats": prov_stats})

    # 2) seen filter (by title/year & imdb id)
    pool2 = seen_filter(pool1)
    _debug_write("step2_unseen", {"in": len(pool1), "out": len(pool2)})

    # 3) recency filter (avoid re-showing for a few days)
    pool3 = _drop_recent(pool2)
    _debug_write("step3_recency", {"in": len(pool2), "out": len(pool3)})

    # 4) rank
    def _taste_for(genres: List[str]) -> float:
        return taste_boost_for(genres, taste_profile)

    ranked = rank_items(pool3, weights, taste_for=_taste_for)
    cut = float(weights.get("min_match_cut", 58.0))
    strong = [r for r in ranked if r.get("match", 0.0) >= cut]

    # Fallbacks to avoid empty feeds
    final: List[Dict]
    if not strong and ranked:
        # keep top 30 anyway
        final = ranked[:30]
    else:
        final = strong[:50]

    # Mark shown for recency shielding next time
    mark_shown([ (r.get("imdb_id") or "").strip() for r in final ])

    out_payload = {
        "generated_at": int(time.time()),
        "count": len(final),
        "items": final,
        "meta": {
            "pool_sizes": {"initial": len(pool0), "providers": len(pool1), "unseen": len(pool2), "post_recency": len(pool3), "ranked": len(ranked)},
            "provider_stats": prov_stats,
            "weights": weights,
            "subs_keep": subs_keep,
        },
    }

    # Write latest + daily
    json.dump(out_payload, open(OUT_LATEST, "w"), indent=2)
    daily_path = DAILY_DIR / _now_slug() / "assistant_feed.json"
    json.dump(out_payload, open(daily_path, "w"), indent=2)

    return out_payload