# engine/runner.py
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from .config import Config
from .catalog import build_pool

OUT_DIR = "data/out"

def _write_feed(ranked: List[Dict], meta: Dict]) -> str:
    os.makedirs(f"{OUT_DIR}/daily", exist_ok=True)
    os.makedirs(f"{OUT_DIR}/latest", exist_ok=True)
    # Simple top10 view
    top10 = []
    for i, item in enumerate(ranked[:10], start=1):
        top10.append({
            "rank": i,
            "title": item.get("title"),
            "year": item.get("year"),
            "type": item.get("media_type"),
            "match": item.get("match"),
        })
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top10": top10,
        "telemetry": meta.get("telemetry", {}),
        "pool_counts": meta.get("pool_counts", {}),
        "added_this_run": meta.get("added_this_run", {}),
    }
    today_dir = os.path.join(OUT_DIR, "daily", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    os.makedirs(today_dir, exist_ok=True)
    daily_path = os.path.join(today_dir, "assistant_feed.json")
    latest_path = os.path.join(OUT_DIR, "latest", "assistant_feed.json")
    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return latest_path

def main():
    cfg = Config()
    ranked, meta = build_pool(cfg)

    # Console summary
    print("Run complete.", flush=True)
    print(f"Weights: critic={cfg.critic_weight}, audience={cfg.audience_weight}", flush=True)
    counts = meta.get("telemetry", {}).get("counts", {})
    print(
        f"Counts: tmdb_pool={counts.get('tmdb_pool', 0)}, "
        f"eligible_unseen={counts.get('eligible_unseen', 0)}, "
        f"shortlist={counts.get('shortlist', 0)}, shown={counts.get('shown', 0)}",
        flush=True
    )
    plan = meta.get("telemetry", {}).get("plan", {})
    print(
        f"Page plan: movie_pages={plan.get('movie_pages', 0)} tv_pages={plan.get('tv_pages', 0)}",
        flush=True
    )
    print(f"Providers: {', '.join(plan.get('providers', []))}", flush=True)
    add = meta.get("added_this_run", {})
    print(f"Catalog store: movie={meta['pool_counts']['movie']} tv={meta['pool_counts']['tv']} "
          f"(added this run m={add.get('movie',0)} t={add.get('tv',0)})", flush=True)
    print(f"Output: data/out/daily/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}", flush=True)

    _write_feed(ranked, meta)

if __name__ == "__main__":
    main()