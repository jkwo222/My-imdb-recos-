from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from .config import Config
from .catalog import build_pool, _rank
from .exclusions import build_exclusion_index, filter_excluded


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _now_utc_stamp() -> Tuple[str, str]:
    dt = datetime.now(timezone.utc)
    date = dt.strftime("%Y-%m-%d")
    stamp = dt.strftime("%Y-%m-%d %H:%M UTC")
    return date, stamp


def _write_json(path: str, data: Any) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _build_feed(date_stamp: str,
                pool: List[Dict[str, Any]],
                shortlist: List[Dict[str, Any]],
                shown: List[Dict[str, Any]],
                meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": date_stamp,
        "counts": {
            "pool": len(pool),
            "eligible_unseen": meta.get("counts", {}).get("eligible_unseen", len(pool)),
            "shortlist": len(shortlist),
            "shown": len(shown),
        },
        "weights": meta.get("weights", {}),
        "filters": meta.get("filters", {}),
        "cursor_after": meta.get("cursor_after", {}),
        "items": shown,  # the Top N we display
        "shortlist": shortlist,
        "pool_sample": pool[:200],  # optional slice for debugging
    }


def main() -> None:
    print("[bootstrap] workflow started", flush=True)
    cfg = Config()  # values are pulled with sane defaults below

    # 1) Build / refresh pool from TMDB (with on-disk cache inside tmdb.py)
    pool, meta = build_pool(cfg)

    # 2) Apply hard exclusions (CSV)
    excl_csv = _env_str("EXCLUSIONS_CSV", "data/exclusions.csv")
    idx = build_exclusion_index(excl_csv)
    eligible = filter_excluded(pool, idx)

    # Update telemetry
    meta.setdefault("counts", {})
    meta["counts"]["eligible_unseen"] = len(eligible)

    # 3) Ranking
    critic_w = meta.get("weights", {}).get("critic_weight", 0.6)
    audience_w = meta.get("weights", {}).get("audience_weight", 0.4)
    try:
        critic_w = float(critic_w)
        audience_w = float(audience_w)
    except Exception:
        critic_w, audience_w = 0.6, 0.4
    if critic_w == 0 and audience_w == 0:
        critic_w, audience_w = 0.6, 0.4  # guardrail against 0-weights
    ranked = _rank(list(eligible), critic_w, audience_w)

    # 4) Shortlist & shown sizes
    shortlist_size = _env_int("SHORTLIST_SIZE", 50)
    shown_size = _env_int("SHOWN_SIZE", 10)
    shortlist = ranked[:shortlist_size]
    shown = shortlist[:shown_size]

    # 5) Output
    date, stamp = _now_utc_stamp()
    out_root = _env_str("OUT_DIR", "data/out")
    daily_dir = os.path.join(out_root, "daily", date)
    latest_dir = os.path.join(out_root, "latest")

    feed = _build_feed(stamp, pool, shortlist, shown, meta)

    _write_json(os.path.join(latest_dir, "assistant_feed.json"), feed)
    _write_json(os.path.join(daily_dir, "assistant_feed.json"), feed)

    print(
        f"Counts: tmdb_pool={meta.get('counts',{}).get('tmdb_pool',len(pool))}, "
        f"eligible_unseen={len(eligible)}, shortlist={len(shortlist)}, shown={len(shown)}",
        flush=True,
    )
    print(f"Output: {daily_dir}", flush=True)


if __name__ == "__main__":
    main()