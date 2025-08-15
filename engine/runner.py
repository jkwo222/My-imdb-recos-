# engine/runner.py
from __future__ import annotations
import json
import os
import sys
import time
from typing import Dict, List, Tuple

from . import catalog as cat
from . import scoring as sc  # <-- fixed to a relative import
from .seen_index import (
    load_imdb_ratings_csv_auto,
    update_seen_from_ratings,
    SeenIndex,
    load_seen,
)
from .filtering import filter_unseen

OUT_DIR = os.environ.get("OUT_DIR", "data/out")
DAILY_DIR = os.path.join(OUT_DIR, "daily")

def _hb(msg: str) -> None:
    print(f"[hb] {time.strftime('%H:%M:%S')} | {msg}", flush=True)

def _dump_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _weights_from_env() -> Tuple[float, float, float, float]:
    # accept both old/new envs, then enforce audience dominance
    cw = float(os.environ.get("CRITIC_WEIGHT", os.environ.get("CRITIC_SCORE_WEIGHT", 0.2)))
    aw = float(os.environ.get("AUDIENCE_WEIGHT", os.environ.get("AUDIENCE_SCORE_WEIGHT", 0.8)))
    np = float(os.environ.get("NOVELTY_PRESSURE", 0.15))
    cc = float(os.environ.get("COMMITMENT_COST_SCALE", 1.0))

    total = max(1e-9, cw + aw)
    cw, aw = cw / total, aw / total
    if aw < 0.70:  # your requirement: audience significantly higher than critic
        aw, cw = 0.70, 0.30
    return cw, aw, np, cc

def main():
    start = time.time()

    # 1) IMDb ingest -> seen index (auto path, supports IMDB_RATINGS_CSV_PATH and fallbacks)
    rows, ratings_path = load_imdb_ratings_csv_auto()
    if ratings_path:
        print(f"IMDb ingest: {ratings_path} — {len(rows)} rows")
        seen, added = update_seen_from_ratings(rows)
    else:
        print("IMDb ingest: (no ratings CSV found) — 0 rows")
        seen = load_seen()
        added = 0
    print(f"Seen index: {len(seen.keys)} keys (+{added} new)")

    # 2) Build TMDB pool (English-only, your services only, rotating pages)
    _hb("catalog:begin")
    pool = cat.build_pool()
    meta = cat.last_meta()
    _hb(f"catalog:end pool={len(pool)} movie={meta.get('pool_counts',{}).get('movie',0)} tv={meta.get('pool_counts',{}).get('tv',0)}")

    # 3) Seen filter pass
    _hb("filter1:unseen")
    pool_unseen = filter_unseen(pool, seen)
    _hb(f"filter1:end kept={len(pool_unseen)} dropped={len(pool)-len(pool_unseen)}")

    # 4) Scoring with audience dominance
    cw, aw, np, cc = _weights_from_env()
    _hb(f"score:begin cw={cw:.2f} aw={aw:.2f} np={np} cc={cc}")
    ranked = sc.score_and_rank(
        pool_unseen,
        critic_weight=cw,
        audience_weight=aw,
        novelty_pressure=np,
        commitment_cost_scale=cc,
    )
    _hb(f"score:end ranked={len(ranked)}")

    shortlist_size = int(os.environ.get("SHORTLIST_SIZE", "50"))
    shortlist = ranked[:shortlist_size]

    # 5) Second safety pass (paranoid)
    shown_size = int(os.environ.get("SHOWN_SIZE", "10"))
    shortlist2 = filter_unseen(shortlist, seen)
    shown = shortlist2[:shown_size]

    # 6) Telemetry & guardrails
    providers_listed = meta.get("provider_names") or []
    telemetry = {
        "pool": len(pool),
        "eligible": len(pool_unseen),
        "after_skip": len(pool_unseen),
        "shown": len(shown),
        "weights": {"critic": cw, "audience": aw},
        "counts": {
            "tmdb_pool": len(pool),
            "eligible_unseen": len(pool_unseen),
            "shortlist": len(shortlist2),
            "shown": len(shown),
        },
        "page_plan": {
            "movie_pages": meta.get("movie_pages"),
            "tv_pages": meta.get("tv_pages"),
            "rotate_minutes": meta.get("rotate_minutes"),
            "slot": meta.get("slot"),
            "total_pages": meta.get("total_pages"),
            "provider_names": providers_listed,
            "language": meta.get("language"),
            "with_original_language": meta.get("with_original_language"),
            "watch_region": meta.get("watch_region"),
        },
    }

    if len(pool) == 0:
        shown = []
        _hb("pool=0 (check provider list, language filters, or API key)")

    top10 = []
    for rank, item in enumerate(shown, 1):
        top10.append({
            "rank": rank,
            "match": round(float(item.get("match", item.get("score", 0.0))), 1),
            "title": item.get("title") or item.get("name"),
            "year": item.get("year"),
            "type": item.get("type", "movie"),
        })

    # 7) Emit artifacts
    date_tag = time.strftime("%Y-%m-%d")
    daily_dir = os.path.join(DAILY_DIR, date_tag)
    os.makedirs(daily_dir, exist_ok=True)

    feed = {
        "version": 1,
        "disclaimer": "This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.",
        "weights": {
            "critic": round(cw, 2),
            "audience": round(aw, 2),
            "novelty_pressure": np,
            "commitment_cost_scale": cc,
        },
        "telemetry": telemetry,
        "top10": top10,
    }

    def _dump(name: str, obj):
        _dump_json(os.path.join(daily_dir, name), obj)

    _dump("assistant_feed.json", feed)
    _dump("top10.json", top10)
    _dump("telemetry.json", telemetry)

    # 8) Console summary
    print(f"Run complete in {int(time.time()-start)}s.")
    print(f"Weights: critic={cw:.2f}, audience={aw:.2f}")
    print(f"Counts: tmdb_pool={len(pool)}, eligible_unseen={len(pool_unseen)}, shortlist={len(shortlist2)}, shown={len(shown)}")
    print(f"Page plan: movie_pages={meta.get('movie_pages')} tv_pages={meta.get('tv_pages')} rotate_minutes={meta.get('rotate_minutes')} slot={meta.get('slot')}")
    print(f"Providers: {', '.join(providers_listed) if providers_listed else '(none configured)'}")
    print(f"Output: {daily_dir}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[fatal] {type(e).__name__}: {e}", file=sys.stderr)
        raise