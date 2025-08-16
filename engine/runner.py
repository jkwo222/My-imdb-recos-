from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Local imports — keep the same module names you already have in the repo
from .catalog import build_pool  # expected to return (pool: List[dict], meta: Dict)
from .config import Config       # expected to provide Config.from_env()


# ---------- small helpers ----------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------- feed writing ----------

def _write_feed(ranked: List[Dict], meta: Dict) -> str:
    """
    Persist the output feed to:
      - data/out/latest/assistant_feed.json
      - data/out/daily/<YYYY-MM-DD>/assistant_feed.json

    Returns the daily output directory path as a string.
    """
    # Directories
    root = Path("data")
    out_latest = root / "out" / "latest"
    out_daily = root / "out" / "daily" / _today_str()
    _ensure_dir(out_latest)
    _ensure_dir(out_daily)

    # Compute a Top 10 table for summaries (rank, title, year, type, match)
    top10: List[Dict[str, Any]] = []
    for i, item in enumerate(ranked[:10], start=1):
        top10.append({
            "rank": i,
            "title": item.get("title") or item.get("name") or "",
            "year": item.get("year") or item.get("first_air_year") or item.get("release_year") or "",
            "type": item.get("type") or ("tvSeries" if item.get("media_type") == "tv" else item.get("media_type") or ""),
            "match": round(float(item.get("match", 0.0)), 1)
        })

    # Telemetry/Counts — be defensive and fill defaults if missing
    telemetry: Dict[str, Any] = dict(meta.get("telemetry", {}))
    counts = dict(telemetry.get("counts", {}))

    # Fallbacks from meta (in case catalog filled these)
    # Expected keys commonly seen in your logs:
    #   tmdb_pool, eligible_unseen, shortlist, shown
    tmdb_pool = _int(counts.get("tmdb_pool", meta.get("tmdb_pool", 0)))
    eligible_unseen = _int(counts.get("eligible_unseen", meta.get("eligible_unseen", len(ranked))))
    shortlist = _int(counts.get("shortlist", meta.get("shortlist", min(len(ranked), 50))))
    shown = _int(counts.get("shown", meta.get("shown", min(len(top10), 10))))

    # Always set counts in telemetry
    telemetry["counts"] = {
        "tmdb_pool": tmdb_pool,
        "eligible_unseen": eligible_unseen,
        "shortlist": shortlist,
        "shown": shown,
    }

    # Also include simple pool splits (movie/tv) for logging/diagnostics
    movie_count = sum(1 for x in ranked if (x.get("type") == "movie" or x.get("media_type") == "movie"))
    tv_count = sum(1 for x in ranked if (x.get("type") in ("tv", "tvSeries") or x.get("media_type") == "tv"))
    telemetry["pool_counts"] = {
        "total": len(ranked),
        "movie": movie_count,
        "tv": tv_count,
    }

    feed = {
        "generated_at": _utc_now_iso(),
        "top10": top10,
        "telemetry": telemetry,
        # keep the full ranked list available for consumers/debug (optional but handy)
        "ranked": ranked,
        # pass-through meta (non-critical)
        "meta": meta,
    }

    # Write to latest and daily
    latest_path = out_latest / "assistant_feed.json"
    daily_path = out_daily / "assistant_feed.json"

    with latest_path.open("w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    with daily_path.open("w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    return str(out_daily)


# ---------- main ----------

def main() -> None:
    print(f"[bootstrap] {_utc_now_iso()} — workflow started", flush=True)

    # Load env/config
    cfg = Config.from_env()

    # Build catalog pool
    print("[hb] | catalog:begin", flush=True)
    pool, meta = build_pool(cfg)
    # Safe pool counts
    movie_count = sum(1 for x in pool if (x.get("type") == "movie" or x.get("media_type") == "movie"))
    tv_count = sum(1 for x in pool if (x.get("type") in ("tv", "tvSeries") or x.get("media_type") == "tv"))
    print(f"[hb] | catalog:end pool={len(pool)} movie={movie_count} tv={tv_count}", flush=True)

    # Optional progress prints (if your scoring/filtering happens inside build_pool,
    # these may already be printed by other modules)
    if "weights" in meta:
        w = meta["weights"]
        critic_w = w.get("critic", w.get("rt_critic", 0.0))
        audience_w = w.get("audience", w.get("rt_audience", 0.0))
        print(f"Weights: critic={critic_w}, audience={audience_w}", flush=True)

    # Bring through common counters so the summary step can `jq` them
    # If build_pool already provided them, great; otherwise defaults get filled in _write_feed.
    counts = meta.get("telemetry", {}).get("counts", {})
    tmdb_pool = counts.get("tmdb_pool", meta.get("tmdb_pool"))
    eligible_unseen = counts.get("eligible_unseen", meta.get("eligible_unseen"))
    shortlist = counts.get("shortlist", meta.get("shortlist"))
    shown = counts.get("shown", meta.get("shown"))

    if tmdb_pool is not None and eligible_unseen is not None:
        print(f"Counts: tmdb_pool={tmdb_pool}, eligible_unseen={eligible_unseen}, "
              f"shortlist={shortlist or 0}, shown={shown or min(10, len(pool))}", flush=True)

    # Write feed files
    output_dir = _write_feed(pool, meta)
    print(f"Output: {output_dir}", flush=True)


if __name__ == "__main__":
    main()