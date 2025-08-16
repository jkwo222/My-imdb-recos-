from __future__ import annotations
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple

from .config import from_env, Config
from . import scoring as sc
from .catalog import build_pool

OUT_LATEST = "data/out/latest"
OUT_DAILY_DIR = "data/out/daily"

def _ensure_dirs():
    os.makedirs("data/debug", exist_ok=True)
    os.makedirs(OUT_LATEST, exist_ok=True)
    os.makedirs(OUT_DAILY_DIR, exist_ok=True)

def _warn_or_info(msg: str):
    print(msg, flush=True)

def main():
    cfg = from_env()
    _ensure_dirs()

    # IMDb ingest (fail-fast if missing or 0 rows, but continue running with a warning)
    seen_idx = sc.load_seen_index(cfg.imdb_ratings_csv_path)
    try:
        row_count = len(seen_idx)
        _warn_or_info(f"IMDb ingest: {cfg.imdb_ratings_csv_path} — {row_count} rows")
        _warn_or_info(f"Seen index: {row_count} keys (+0 new)")
        if row_count == 0:
            _warn_or_info("[warn] ratings.csv missing or unreadable; continuing without seen filtering.")
    except Exception:
        _warn_or_info("[warn] ratings.csv could not be parsed; continuing.")

    print("[hb] | catalog:begin", flush=True)
    pool, meta = build_pool(cfg)
    print(f"[hb] | catalog:end pool={len(pool)} movie={meta['pool_counts']['movie']} tv={meta['pool_counts']['tv']}", flush=True)

    # Unseen filter (no-op until mapping added)
    print("[hb] | filter:unseen", flush=True)
    eligible = sc.filter_unseen(pool, seen_idx)
    print(f"[hb] | filter:end kept={len(eligible)} dropped={len(pool)-len(eligible)}", flush=True)

    # Score
    print(f"[hb] | score:begin cw={cfg.critic_weight:.3f} aw={cfg.audience_weight:.3f} np={cfg.novelty_pressure:.2f} cc={cfg.commitment_cost_scale:.1f}", flush=True)
    ranked = sc.score_items(cfg, eligible)
    print(f"[hb] | score:end ranked={len(ranked)}", flush=True)

    # Format top10
    top_n = 10
    top = ranked[:top_n]

    # Telemetry block
    telemetry = {
        "pool": len(pool),
        "eligible": len(eligible),
        "after_skip": len(eligible),  # reserved
        "shown": len(top),
        "weights": {
            "critic": cfg.critic_weight,
            "audience": cfg.audience_weight,
        },
        "counts": {
            "tmdb_pool": len(pool),
            "eligible_unseen": len(eligible),
            "shortlist": min(50, len(ranked)),
            "shown": len(top),
        },
        "page_plan": meta,
    }

    # Compose assistant_feed.json
    feed = {
        "version": 1,
        "disclaimer": "This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.",
        "weights": {
            "critic": cfg.critic_weight,
            "audience": cfg.audience_weight,
            "novelty_pressure": cfg.novelty_pressure,
            "commitment_cost_scale": cfg.commitment_cost_scale,
        },
        "telemetry": telemetry,
        "top10": [
            {
                "rank": i+1,
                "match": item["match"],
                "title": item["title"],
                "year": item["year"],
                "type": item["type"],
            }
            for i, item in enumerate(top)
        ],
    }

    # Write outputs
    today = datetime.utcnow().strftime("%Y-%m-%d")
    latest_path = os.path.join(OUT_LATEST, "assistant_feed.json")
    daily_dir = os.path.join(OUT_DAILY_DIR, today)
    os.makedirs(daily_dir, exist_ok=True)
    daily_path = os.path.join(daily_dir, "assistant_feed.json")
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    print("Run complete.\n", flush=True)
    print(f"Weights: critic={cfg.critic_weight:.2f}, audience={cfg.audience_weight:.2f}", flush=True)
    print(
        f"Counts: tmdb_pool={telemetry['counts']['tmdb_pool']}, "
        f"eligible_unseen={telemetry['counts']['eligible_unseen']}, "
        f"shortlist={telemetry['counts']['shortlist']}, shown={telemetry['counts']['shown']}",
        flush=True,
    )
    print(
        f"Page plan: movie_pages={meta['movie_pages']} tv_pages={meta['tv_pages']} "
        f"rotate_minutes={meta['rotate_minutes']} slot={meta['slot']}",
        flush=True,
    )
    print(
        "Providers: " + ", ".join(meta["provider_names"]),
        flush=True,
    )
    print(f"Output: {daily_dir}\n", flush=True)

if __name__ == "__main__":
    main()