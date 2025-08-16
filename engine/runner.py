# engine/runner.py
from __future__ import annotations
import json
import os
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

from .catalog import build_pool
from .self_check import run_self_check  # (new) helps produce friendly diagnostics

def _cfg_from_env() -> SimpleNamespace:
    # expose env as attributes; our catalog reads both attrs and env
    return SimpleNamespace(
        TMDB_PAGES_MOVIE=os.getenv("TMDB_PAGES_MOVIE"),
        TMDB_PAGES_TV=os.getenv("TMDB_PAGES_TV"),
        REGION=os.getenv("REGION"),
        ORIGINAL_LANGS=os.getenv("ORIGINAL_LANGS"),
        SUBS_INCLUDE=os.getenv("SUBS_INCLUDE"),
        MAX_CATALOG=os.getenv("MAX_CATALOG"),
        INCLUDE_TV_SEASONS=os.getenv("INCLUDE_TV_SEASONS"),
        SKIP_WINDOW_DAYS=os.getenv("SKIP_WINDOW_DAYS"),
    )

def _ensure_dirs() -> None:
    for d in ("data/out/latest", "data/debug", "data/cache", "data/out/daily"):
        os.makedirs(d, exist_ok=True)

def _pick_top(pool: List[Dict[str, Any]], k: int = 10) -> List[Dict[str, Any]]:
    # Placeholder ranking; your scoring/filtering can replace this
    # Already sorted by popularity in catalog_store
    out: List[Dict[str, Any]] = []
    rank = 1
    for it in pool[:k]:
        out.append({
            "rank": rank,
            "title": it.get("title"),
            "year": it.get("year"),
            "type": it.get("type"),
            "match": round(float(it.get("popularity") or 0.0), 1)
        })
        rank += 1
    return out

def main() -> None:
    _ensure_dirs()
    # Friendly repo validation before we start
    run_self_check()

    cfg = _cfg_from_env()
    pool, meta = build_pool(cfg)

    # Guarantee telemetry keys exist
    meta.setdefault("telemetry", {}).setdefault("counts", {})
    t = meta["telemetry"]["counts"]
    t.setdefault("tmdb_pool", len(pool))
    t.setdefault("eligible_unseen", len(pool))  # placeholder until your unseen filter runs
    t.setdefault("shortlist", min(50, len(pool)))
    t.setdefault("shown", min(10, len(pool)))

    # Produce top-10 feed
    top10 = _pick_top(pool, k=10)

    out = {
        "top10": top10,
        "telemetry": meta.get("telemetry", {"counts": {}}),
        "page_plan": meta.get("page_plan", {}),
        "providers": meta.get("providers", []),
        "store_added": meta.get("store_added", {"movie": 0, "tv": 0}),
        "pool_counts": meta.get("pool_counts", {"movie": 0, "tv": 0}),
    }

    # Write “latest” and a day-stamped copy
    latest_dir = "data/out/latest"
    daily_dir = os.path.join("data/out/daily")
    os.makedirs(daily_dir, exist_ok=True)

    latest_path = os.path.join(latest_dir, "assistant_feed.json")
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # Copy to dated folder (UTC date)
    from datetime import datetime, timezone
    date_str = datetime.now(timezone.utc).date().isoformat()
    day_dir = os.path.join(daily_dir, date_str)
    os.makedirs(day_dir, exist_ok=True)
    with open(os.path.join(day_dir, "assistant_feed.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("Run complete.", flush=True)
    print(f"Counts: tmdb_pool={t['tmdb_pool']}, eligible_unseen={t['eligible_unseen']}, shortlist={t['shortlist']}, shown={t['shown']}", flush=True)
    print(f"Page plan: movie_pages={meta['page_plan']['movie_pages']} tv_pages={meta['page_plan']['tv_pages']} rotate_minutes={meta['page_plan']['rotate_minutes']} slot={meta['page_plan']['slot']}", flush=True)
    print(f"Providers: {', '.join(meta.get('providers', []))}", flush=True)
    print(f"Catalog store: movie={meta['store_added']['movie']} tv={meta['store_added']['tv']} (added this run m={meta['store_added']['movie']} t={meta['store_added']['tv']})", flush=True)
    print(f"Output: {day_dir}", flush=True)

if __name__ == "__main__":
    main()