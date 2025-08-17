# engine/runner.py
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from .config import Config
from .catalog import build_pool  # returns (pool: List[dict], meta: Dict)

# This runner intentionally does not import feed/rank modules yet.
# It will just produce catalog outputs and a lightweight assistant_feed.json.


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bootstrap_log(msg: str) -> None:
    ts = _utc_now_iso()
    print(f"[bootstrap] {ts} — {msg}")


def _hb_log(msg: str) -> None:
    print(f"[hb] | {msg}")


def _ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def _write_json(path: str, obj) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def main() -> None:
    _bootstrap_log("workflow started")
    cfg = Config.from_env()

    # Build the pool via catalog
    _hb_log("catalog:begin")
    pool, meta = build_pool(cfg)
    _hb_log(
        f"catalog:end pool={len(pool)} "
        f"movie={meta.get('movie_count', 0)} tv={meta.get('tv_count', 0)}"
    )

    # Prepare a minimal "assistant_feed.json" so you can inspect the pool immediately.
    # (No dependency on feed/rank yet.)
    shortlist_n = min(50, len(pool))
    shown_n = min(10, shortlist_n)
    feed = {
        "run_started_at": _utc_now_iso(),
        "config": cfg.to_dict(),
        "meta": meta,
        "counts": {
            "tmdb_pool": len(pool),
            "eligible_unseen": len(pool),  # placeholder until rank/exclusions refine it
            "shortlist": shortlist_n,
            "shown": shown_n,
        },
        "items": pool[:shown_n],  # just the first N items as a stub
    }

    # Write to output layout: data/out/latest + daily folder
    out_dir = cfg.out_dir
    latest_dir = os.path.join(out_dir, "latest")
    daily_dir = os.path.join(out_dir, "daily", datetime.now().strftime("%Y-%m-%d"))

    _ensure_dir(latest_dir)
    _ensure_dir(daily_dir)

    feed_path_latest = os.path.join(latest_dir, "assistant_feed.json")
    _write_json(feed_path_latest, feed)

    feed_path_daily = os.path.join(daily_dir, "assistant_feed.json")
    _write_json(feed_path_daily, feed)

    print(
        f"Counts: tmdb_pool={len(pool)}, "
        f"eligible_unseen={len(pool)}, shortlist={shortlist_n}, shown={shown_n}"
    )
    print(f"Output: {daily_dir}")


if __name__ == "__main__":
    main()