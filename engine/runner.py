# engine/runner.py
from __future__ import annotations
import os, sys, time, json, pathlib, datetime
from typing import Dict, List, Any

from rich import print as rprint

from .ratings_ingest import load_user_ratings_combined
from .weights import load_weights, update_from_ratings, save_weights
from .taste import build_taste
from .catalog_builder import build_catalog
from .seen_index import load_seen_index, filter_unseen
from .rank import rank_candidates
from .feed import build_feed
from .store import load_store, save_store, merge_catalog_items

DATA_OUT_DEBUG = pathlib.Path("data/debug")
DATA_OUT_DEBUG.mkdir(parents=True, exist_ok=True)

def _env_list(name: str) -> List[str]:
    raw = os.environ.get(name, "") or ""
    if isinstance(raw, list):
        return raw
    return [x.strip() for x in raw.split(",") if x.strip()]

def _provider_allowed(providers: List[str], allow_slugs: List[str]) -> bool:
    if not providers:
        return False
    s = set(p.strip().lower() for p in providers)
    a = set(x.strip().lower() for x in allow_slugs)
    return len(s & a) > 0

def _apply_provider_filter(items: List[Dict[str, Any]], allow_slugs: List[str]) -> List[Dict[str, Any]]:
    if not allow_slugs:
        return items
    out = []
    for it in items:
        if _provider_allowed(it.get("providers") or [], allow_slugs):
            out.append(it)
    return out

def main():
    rprint("[cyan] | catalog:begin[/cyan]")

    # 1) Ratings → combined (CSV + optional public list), taste, weights
    rows, meta_r = load_user_ratings_combined()
    rprint(f"IMDb ratings loaded → rows={len(rows)} meta={meta_r}")

    # Weight nudging towards audience preference (audience > critic enforced later)
    w_live = load_weights()
    w_live = update_from_ratings(rows) if rows else w_live
    # ensure audience is prioritized
    if w_live.get("critic_weight", 0.4) >= w_live.get("audience_weight", 0.6):
        aw = max(0.55, 1.0 - float(w_live.get("critic_weight", 0.4)))
        w_live["audience_weight"] = round(aw, 2)
        w_live["critic_weight"] = round(1.0 - aw, 2)
        save_weights(w_live)
    rprint(f"weights → {w_live}")

    taste = build_taste(rows)
    rprint(f"taste profile → {len(taste)} genres")

    # 2) Fresh catalog scrape/enrich
    fresh: List[Dict[str, Any]] = build_catalog()
    rprint(f"catalog built → {len(fresh)} items")

    # 3) Provider filter
    subs_keep = _env_list("SUBS_INCLUDE")
    if not subs_keep:
        # default to major US SVODs if not set
        subs_keep = ["netflix","prime_video","hulu","max","disney_plus","apple_tv_plus","peacock","paramount_plus"]
    keep = _apply_provider_filter(fresh, subs_keep)
    rprint(f"provider-filter keep={subs_keep} → {len(keep)} items")

    # 4) Accumulating store
    store = load_store()
    merged = merge_catalog_items(store.get("items", []), keep, max_size=20000)
    store["items"] = merged
    save_store(store)
    rprint(f"accumulated store size → {len(merged)} items")

    # 5) Seen filtering (CSV + optional public page scraping)
    ratings_path = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")
    seen_idx = load_seen_index(ratings_path)
    unseen_only = filter_unseen(merged, seen_idx)
    rprint(f"unseen-only → {len(unseen_only)} items")

    # 6) Rank
    ranked_all = rank_candidates(unseen_only, w_live, taste)

    # 7) Final cut (drop obvious low matches)
    min_cut = float(os.environ.get("MIN_MATCH_CUT", "58.0"))
    final = [x for x in ranked_all if float(x.get("match", 0.0)) >= min_cut]
    rprint(f"final after cut (>= {min_cut}) → {len(final)} items")

    # 8) Telemetry/meta
    meta = {
        "pool_sizes": {
            "initial": len(fresh),
            "providers": len(keep),
            "unseen": len(unseen_only),
            "fresh": len(final),
            "final": len(final)
        },
        "weights": {
            "audience_weight": round(float(w_live.get("audience_weight", 0.65)), 2),
            "critic_weight": round(float(w_live.get("critic_weight", 0.35)), 2),
            "commitment_cost_scale": round(float(w_live.get("commitment_cost_scale", 1.0)), 2),
            "novelty_weight": round(float(w_live.get("novelty_weight", 0.15)), 2),
            "min_match_cut": min_cut
        },
        "subs": subs_keep
    }

    # 9) Outputs (JSON/CSV/Markdown in daily + latest)
    payload = build_feed(final, meta)
    rprint(f"[cyan] | catalog:end pool={len(merged)} feed={len(final)} → data/out/daily/{datetime.date.today().isoformat()}/assistant_feed.json[/cyan]")

    # 10) Debug snapshot (optional)
    debug_snap = DATA_OUT_DEBUG / f"debug_{int(time.time())}.json"
    json.dump({
        "weights": w_live,
        "taste": taste,
        "meta_run": meta,
        "counts": {"fresh": len(fresh), "keep": len(keep), "store": len(merged), "unseen": len(unseen_only), "final": len(final)}
    }, open(debug_snap, "w", encoding="utf-8"), indent=2)

if __name__ == "__main__":
    main()