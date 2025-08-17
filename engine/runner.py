from __future__ import annotations

import datetime as _dt
import json
import os
from typing import Any, Dict, List, Tuple

from .config import Config
from .catalog import build_pool
from .exclusions import build_exclusion_index, filter_excluded
from .rank import rank_pool, build_profile_from_ratings
from .feed import build_feed_document, write_feed


def _now_day_utc() -> str:
    return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")


def main() -> None:
    print("[bootstrap] runner — start", flush=True)

    cfg = Config()

    # 1) Build cumulative discovery pool
    pool, meta = build_pool(cfg)
    pool_size = len(pool)

    # 2) Apply exclusions (your CSV list == “never show”)
    ex_index = build_exclusion_index(cfg)
    pool_after, removed = filter_excluded(pool, ex_index)
    unseen_count = len(pool_after)

    # 3) Build user profile DNA and rank
    profile = build_profile_from_ratings(cfg)
    ranked = rank_pool(pool_after, cfg, profile=profile)

    # 4) Build feed + write outputs
    day_stamp = _now_day_utc()
    shortlist_size = int(cfg.shortlist_size or 50)
    shown_count = int(cfg.shown_count or 10)
    feed_doc = build_feed_document(
        ranked,
        shortlist_size=shortlist_size,
        shown_count=shown_count,
        pool_size=pool_size,
        unseen_count=unseen_count,
        day_stamp=day_stamp,
    )
    write_feed(feed_doc, meta)

    # 5) A little telemetry log line
    cw = float(cfg.critic_weight or 0.6)
    aw = float(cfg.audience_weight or 0.4)
    print(f"Weights: critic={cw}, audience={aw}", flush=True)
    print(f"Counts: tmdb_pool={meta.get('counts',{}).get('tmdb_pool',0)}, eligible_unseen={unseen_count}, shortlist={shortlist_size}, shown={shown_count}", flush=True)
    print(f"Output: data/out/daily/{day_stamp.split(' ')[0]}", flush=True)
    print("[bootstrap] runner — done", flush=True)


if __name__ == "__main__":
    main()