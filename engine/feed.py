# engine/feed.py
from __future__ import annotations
import os, json, pathlib, time
from typing import List, Dict, Any, Callable, Tuple
from rich import print as rprint

from .provider_filter import any_allowed, normalize_user_whitelist
from .recency import should_skip, mark_shown
from .rank import rank_items, DEFAULT_WEIGHTS
from .taste import taste_boost_for

def _parse_subs_include_env() -> List[str]:
    raw = os.environ.get("SUBS_INCLUDE", "").strip()
    if not raw:
        return []
    # supports JSON list or CSV or Python-ish list
    try:
        import ast
        if raw.startswith("["):
            v = ast.literal_eval(raw)
            if isinstance(v, list):
                return [str(x) for x in v]
    except Exception:
        pass
    if "," in raw:
        return [s.strip() for s in raw.split(",") if s.strip()]
    return [raw]

def _weights_from_env(base: Dict[str, Any]) -> Dict[str, Any]:
    w = dict(base)
    # let caller adjust audience>critic if desired via env
    aw = os.environ.get("AUDIENCE_WEIGHT", "")
    cw = os.environ.get("CRITIC_WEIGHT", "")
    n  = os.environ.get("NOVELTY_WEIGHT", "")
    cc = os.environ.get("COMMITMENT_COST_SCALE", "")
    cut= os.environ.get("MIN_MATCH_CUT", "")
    if aw:
        try: w["audience_weight"] = float(aw)
        except: pass
    if cw:
        try: w["critic_weight"] = float(cw)
        except: pass
    if n:
        try: w["novelty_weight"] = float(n)
        except: pass
    if cc:
        try: w["commitment_cost_scale"] = float(cc)
        except: pass
    if cut:
        try: w["min_match_cut"] = float(cut)
        except: pass
    # normalize: keep sum <= 1 focus (ranker doesn’t require but nice to keep balanced)
    s = w.get("audience_weight", 0.65) + w.get("critic_weight", 0.35)
    if s > 1e-9:
        w["audience_weight"] /= s
        w["critic_weight"]   /= s
    return w

def _ensure_out_paths() -> Tuple[pathlib.Path, pathlib.Path]:
    latest = pathlib.Path("data/out/latest")
    daily = pathlib.Path("data/out/daily") / time.strftime("%Y-%m-%d", time.gmtime())
    latest.mkdir(parents=True, exist_ok=True)
    daily.mkdir(parents=True, exist_ok=True)
    return latest, daily

def _drop_recents(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        iid = (it.get("imdb_id") or "").strip()
        if iid and should_skip(iid, days=4):
            continue
        out.append(it)
    return out

def _provider_filter(items: List[Dict[str, Any]], subs: List[str]) -> List[Dict[str, Any]]:
    if not subs:
        return items
    wl = normalize_user_whitelist(subs)
    out = []
    for it in items:
        providers = it.get("providers") or []
        if any_allowed(providers, list(wl)):
            out.append(it)
    return out

def build_feed(
    catalog: List[Dict[str, Any]],
    seen_filter: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
    taste_profile: Dict[str, float],
) -> Dict[str, Any]:
    # --- initial pool
    initial_n = len(catalog)

    # --- providers
    subs = _parse_subs_include_env()
    kept = _provider_filter(catalog, subs)
    providers_n = len(kept)
    rprint(f"provider-filter keep={subs or '[ALL baseline]'} → {providers_n} items")

    # --- seen
    unseen = seen_filter(kept)
    unseen_n = len(unseen)
    rprint(f"unseen-only → {unseen_n} items")

    # --- recency
    fresh = _drop_recents(unseen)
    fresh_n = len(fresh)

    # --- ranking
    w = _weights_from_env(DEFAULT_WEIGHTS)
    def tboost(genres: List[str]) -> float:
        return taste_boost_for(genres, taste_profile)

    ranked = rank_items(fresh, w, taste_for=tboost)

    # Optional minimum match cut (default in DEFAULT_WEIGHTS)
    min_cut = float(w.get("min_match_cut", 58.0))
    final = [r for r in ranked if float(r.get("match", 0.0)) >= min_cut]

    # If too strict and ends up empty, back off to top 20
    if not final and ranked:
        final = ranked[:20]

    # mark recency “shown”
    mark_shown([ (it.get("imdb_id") or "") for it in final ])

    latest_dir, daily_dir = _ensure_out_paths()
    blob = {
        "generated_at": int(time.time()),
        "count": len(final),
        "items": final,
        "meta": {
            "pool_sizes": {
                "initial": initial_n,
                "providers": providers_n,
                "unseen": unseen_n,
                "fresh": fresh_n,
                "final": len(final),
            },
            "weights": w,
            "subs": subs,
        }
    }
    # write both locations (latest + daily)
    latest_path = latest_dir / "assistant_feed.json"
    daily_path  = daily_dir / "assistant_feed.json"
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(blob, f, indent=2)
    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(blob, f, indent=2)

    rprint(f"[green]feed written[/green] → {latest_path}")
    return blob