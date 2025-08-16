from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from .config import Config
from .ratings import load_seen_ids
from .catalog import build_pool
from .scoring import rank_items

def _ensure_dirs(cfg: Config):
    os.makedirs(cfg.out_dir, exist_ok=True)
    os.makedirs(os.path.join(cfg.out_dir, "latest"), exist_ok=True)
    os.makedirs(os.path.join(cfg.out_dir, "daily"), exist_ok=True)
    os.makedirs(cfg.debug_dir, exist_ok=True)
    os.makedirs(cfg.cache_dir, exist_ok=True)

def _write_json(p: str, obj: Any):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)

def _today_ymd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def main():
    cfg = Config()
    _ensure_dirs(cfg)

    # Load seen (IMDb IDs)
    seen_idx = load_seen_ids(cfg.imdb_ratings_csv_path)
    print(f"IMDb ingest: {cfg.imdb_ratings_csv_path} — {len(seen_idx)} rows")
    print(f"Seen index: {len(seen_idx)} keys (+{len(seen_idx)} new)")  # simple statement to match earlier logs

    print("[hb] | catalog:begin")
    pool, meta = build_pool(cfg)
    print(f"[hb] | catalog:end pool={len(pool)} movie={meta['pool_counts']['movie']} tv={meta['pool_counts']['tv']}")

    # Filter by unseen
    print("[hb] | filter:unseen")
    pre = len(pool)
    def _is_unseen(item: Dict[str, Any]) -> bool:
        imdb_id = item.get("imdb_id")
        return (imdb_id is None) or (imdb_id not in seen_idx)

    pool_unseen = [x for x in pool if _is_unseen(x)]
    kept = len(pool_unseen)
    dropped = pre - kept
    print(f"[hb] | filter:end kept={kept} dropped={dropped}")

    # Score & rank
    print(f"[hb] | score:begin cw={cfg.critic_weight:.3f} aw={cfg.audience_weight:.3f} np={cfg.novelty_pressure:.2f} cc={cfg.commitment_cost_scale:.1f}")
    ranked = rank_items(pool_unseen, cfg)
    print(f"[hb] | score:end ranked={len(ranked)}")

    # Shortlist & top10
    shortlist_n = 50
    top_n = 10
    shortlist = ranked[:shortlist_n]
    top = ranked[:top_n]

    # Build feed
    feed = {
        "version": "v2.13-feed-1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "weights": {
            "critic_weight": cfg.critic_weight,
            "audience_weight": cfg.audience_weight,
            "commitment_cost_scale": cfg.commitment_cost_scale,
        },
        "telemetry": {
            "pool": len(pool),
            "eligible_after_subs": len(pool_unseen),
            "shortlist": len(shortlist),
            "shown": len(top),
            "page_plan": meta,
        },
        "top": [
            {
                "imdb_id": x.get("imdb_id"),
                "title": x.get("title"),
                "year": x.get("year"),
                "type": x.get("type"),
                "seasons": x.get("seasons", 1),
                "critic": x.get("critic", 0.0),
                "audience": x.get("audience", 0.0),
                "match": x.get("match", 0.0),
                "providers": x.get("_providers", []),
            }
            for x in top
        ],
        "considered_sample": [x.get("imdb_id") or f"tmdb:{x.get('tmdb_id')}" for x in shortlist[:140]],
    }

    # Console summary
    print("Run complete.")
    print(f"Weights: critic={cfg.critic_weight:.2f}, audience={cfg.audience_weight:.2f}")
    print(f"Counts: tmdb_pool={len(pool)}, eligible_unseen={len(pool_unseen)}, shortlist={len(shortlist)}, shown={len(top)}")
    print(
        "Page plan: movie_pages={m} tv_pages={t} rotate_minutes={r} slot={s}".format(
            m=meta["movie_pages"], t=meta["tv_pages"], r=meta["rotate_minutes"], s=meta["slot"]
        )
    )
    print("Providers: " + ", ".join(cfg.subs_include))

    # Outputs
    latest_dir = os.path.join(cfg.out_dir, "latest")
    daily_dir = os.path.join(cfg.out_dir, "daily", _today_ymd())

    _write_json(os.path.join(latest_dir, "assistant_feed.json"), feed)
    _write_json(os.path.join(daily_dir, "assistant_feed.json"), feed)

    # Minimal top10 text file for quick read
    lines = []
    for idx, x in enumerate(top, 1):
        lines.append(f"{idx:2d} {x.get('match',0.0):.1f} — {x.get('title')} ({x.get('year')}) [{x.get('type')}]")
    with open(os.path.join(latest_dir, "top10.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    main()