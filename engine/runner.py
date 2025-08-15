# engine/runner.py
from __future__ import annotations
import json, os, time, shutil
from typing import Tuple, List, Dict, Any

from . import catalog as cat
from .seen_index import load_imdb_ratings_csv_auto, update_seen_from_ratings, load_seen
from .filtering import filter_unseen
from .scoring import score_and_rank

OUT_DIR = os.environ.get("OUT_DIR", "data/out")

def _hb(msg: str) -> None:
    print(f"[hb] {time.strftime('%H:%M:%S')} | {msg}", flush=True)

def _dump_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _weights_from_env() -> Tuple[float, float, float, float]:
    # audience-forward defaults; normalize if overridden
    cw = float(os.environ.get("CRITIC_WEIGHT", os.environ.get("CRITIC_SCORE_WEIGHT", 0.25)))
    aw = float(os.environ.get("AUDIENCE_WEIGHT", os.environ.get("AUDIENCE_SCORE_WEIGHT", 0.75)))
    np = float(os.environ.get("NOVELTY_PRESSURE", 0.15))
    cc = float(os.environ.get("COMMITMENT_COST_SCALE", 1.0))
    total = cw + aw
    if total <= 0:
        cw, aw, total = 0.25, 0.75, 1.0
    return cw/total, aw/total, np, cc

def _write_all_targets(feed: dict, telemetry: dict, top10: list, date_tag: str) -> str:
    # daily folder
    daily_dir = os.path.join(OUT_DIR, "daily", date_tag)
    os.makedirs(daily_dir, exist_ok=True)

    # latest folder
    latest_dir = os.path.join(OUT_DIR, "latest")
    os.makedirs(latest_dir, exist_ok=True)

    # legacy root files (to kill any stale readers)
    root_dir = OUT_DIR

    files = {
        os.path.join(daily_dir, "assistant_feed.json"): feed,
        os.path.join(daily_dir, "telemetry.json"): telemetry,
        os.path.join(daily_dir, "top10.json"): top10,

        os.path.join(latest_dir, "assistant_feed.json"): feed,
        os.path.join(latest_dir, "telemetry.json"): telemetry,
        os.path.join(latest_dir, "top10.json"): top10,

        # overwrite legacy locations so old scripts can’t read stale data
        os.path.join(root_dir, "assistant_feed.json"): feed,
        os.path.join(root_dir, "telemetry.json"): telemetry,
        os.path.join(root_dir, "top10.json"): top10,
    }
    for p, obj in files.items():
        _dump_json(p, obj)
    return daily_dir

def main():
    start = time.time()

    rows, ratings_path = load_imdb_ratings_csv_auto()
    if ratings_path:
        print(f"IMDb ingest: {ratings_path} — {len(rows)} rows")
        seen, added = update_seen_from_ratings(rows)
    else:
        print("IMDb ingest: (no ratings CSV found) — 0 rows")
        seen = load_seen()
        added = 0
    print(f"Seen index: {len(seen.keys)} keys (+{added} new)")

    _hb("catalog:begin")
    pool, meta = cat.build_pool()
    _hb(f"catalog:end pool={len(pool)} movie={meta.get('pool_counts',{}).get('movie',0)} tv={meta.get('pool_counts',{}).get('tv',0)}")

    _hb("filter:unseen")
    pool_unseen = filter_unseen(pool, seen)
    _hb(f"filter:end kept={len(pool_unseen)} dropped={len(pool)-len(pool_unseen)}")

    cw, aw, np, cc = _weights_from_env()
    _hb(f"score:begin cw={cw:.3f} aw={aw:.3f} np={np} cc={cc}")
    ranked = score_and_rank(pool_unseen,
                            critic_weight=cw,
                            audience_weight=aw,
                            novelty_pressure=np,
                            commitment_cost_scale=cc)
    _hb(f"score:end ranked={len(ranked)}")

    shortlist_size = int(os.environ.get("SHORTLIST_SIZE", "50"))
    shown_size = int(os.environ.get("SHOWN_SIZE", "10"))
    shortlist = ranked[:shortlist_size]
    shown = shortlist[:shown_size]

    telemetry = {
        "pool": len(pool),
        "eligible": len(pool_unseen),
        "after_skip": len(pool_unseen),
        "shown": len(shown),
        "weights": {"critic": round(cw, 3), "audience": round(aw, 3)},
        "counts": {
            "tmdb_pool": len(pool),
            "eligible_unseen": len(pool_unseen),
            "shortlist": len(shortlist),
            "shown": len(shown),
        },
        "page_plan": meta,
    }

    date_tag = time.strftime("%Y-%m-%d")

    top10 = []
    for rank, item in enumerate(shown, 1):
        top10.append({
            "rank": rank,
            "match": round(float(item.get("match", item.get("score", 0.0))), 1),
            "title": item.get("title") or item.get("name"),
            "year": item.get("year"),
            "type": item.get("type", "movie"),
        })

    feed = {
        "version": 1,
        "disclaimer": "This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.",
        "weights": {
            "critic": round(cw, 3),
            "audience": round(aw, 3),
            "novelty_pressure": np,
            "commitment_cost_scale": cc,
        },
        "telemetry": telemetry,
        "top10": top10,
    }

    out_dir = _write_all_targets(feed, telemetry, top10, date_tag)

    print(f"Run complete in {int(time.time()-start)}s.")
    print(f"Weights: critic={cw:.2f}, audience={aw:.2f}")
    print(f"Counts: tmdb_pool={len(pool)}, eligible_unseen={len(pool_unseen)}, shortlist={len(shortlist)}, shown={len(shown)}")
    print(f"Page plan: movie_pages={meta.get('movie_pages')} tv_pages={meta.get('tv_pages')} rotate_minutes={meta.get('rotate_minutes')} slot={meta.get('slot')}")
    print(f"Providers: {', '.join(meta.get('provider_names', [])) or '(none)'}")
    print(f"Output: {out_dir}")

if __name__ == "__main__":
    main()