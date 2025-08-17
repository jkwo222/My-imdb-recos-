# engine/runner.py
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Dict, Any, List

from .config import load_config
from .catalog import build_pool
from .rank import rank_pool
from .feed import build_feed


def _log(msg: str):
    print(f"[hb] | {msg}", flush=True)


def main():
    cfg = load_config()

    _log("catalog:begin")
    pool, catalog_meta = build_pool(cfg)  # must return (List[dict], Dict)
    _log(f"catalog:end pool={len(pool)} movie={catalog_meta.get('movie_count', '?')} tv={catalog_meta.get('tv_count', '?')}")

    # Shortlist before ranking to keep the job fast & API-friendly
    shortlist_size = max(10, int(cfg.shortlist_size))
    shortlist = pool[:shortlist_size]

    ranked, rank_meta = rank_pool(shortlist, cfg, meta=catalog_meta)

    # Build and write the feed artifacts (always write, even if empty)
    items, feed_meta = build_feed(ranked, cfg, catalog_meta, rank_meta)

    # Mirror the earlier "Counts/Weights/Output" style diagnostic
    counts_line = f"Counts: tmdb_pool={len(pool)}, eligible_unseen={len(pool)}, shortlist={len(shortlist)}, shown={len(items)}"
    print(counts_line, flush=True)
    weights_line = f"Weights: critic={cfg.weight_critic}, audience={cfg.weight_audience}"
    print(weights_line, flush=True)
    print(f"Output: data/out/daily/{datetime.utcnow().date().isoformat()}", flush=True)

    # Also drop a run_meta for easy debugging
    run_meta = {
        "counts": {
            "pool": len(pool),
            "shortlist": len(shortlist),
            "shown": len(items),
        },
        "weights": {"critic": cfg.weight_critic, "audience": cfg.weight_audience},
        "timestamps": {"utc": datetime.utcnow().isoformat() + "Z"},
    }
    try:
        with open("data/debug/runner_meta.json", "w", encoding="utf-8") as f:
            json.dump(run_meta, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Ensure CI logs show the error clearly
        print(f"[runner] FATAL: {e}", file=sys.stderr, flush=True)
        raise