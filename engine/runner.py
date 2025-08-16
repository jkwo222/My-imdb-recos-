import os
import json
import csv
import pathlib
from datetime import datetime
from typing import List, Dict, Any

from . import catalog as cat
from . import scoring as sc

OUT_ROOT = pathlib.Path("data/out")
DBG_ROOT = pathlib.Path("data/debug")
LATEST_DIR = OUT_ROOT / "latest"

def _ensure_dirs():
    for p in [OUT_ROOT, DBG_ROOT, LATEST_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def _load_seen() -> Dict[str, int]:
    ratings_path = os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")
    seen = cat._load_seen_csv(ratings_path)
    mylist_path = os.getenv("MYLIST_CSV_PATH", "")
    if mylist_path and os.path.exists(mylist_path):
        try:
            with open(mylist_path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    nm = (row.get("title") or row.get("Title") or "").strip().lower()
                    yr = (row.get("year") or row.get("Year") or "").strip()
                    if nm:
                        seen[f"title:{nm}|{yr}"] = 1
        except Exception:
            pass
    return seen

def _is_seen(it: dict, seen: Dict[str, int]) -> bool:
    nm = (it.get("title") or "").strip().lower()
    yr = str(it.get("year") or "").strip()
    return bool(seen.get(f"title:{nm}|{yr}"))

def _write_json(path: pathlib.Path, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    _ensure_dirs()

    weights = {
        "critic": float(os.getenv("WEIGHT_CRITIC", "0.25")),
        "audience": float(os.getenv("WEIGHT_AUDIENCE", "0.75")),
        "novelty_pressure": float(os.getenv("NOVELTY_PRESSURE", "0.15")),
        "commitment_cost_scale": float(os.getenv("COMMITMENT_COST_SCALE", "1.0")),
    }

    ratings_path = os.getenv("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")
    if os.path.exists(ratings_path):
        try:
            import pandas as _pd
            df = _pd.read_csv(ratings_path)
            print(f"IMDb ingest: {ratings_path} — {len(df)} rows")
        except Exception:
            print(f"IMDb ingest: {ratings_path} — (read error)")

    seen = _load_seen()
    print(f"Seen index: {len(seen)} keys")

    print("[hb] catalog:begin")
    pool, meta = cat.build_pool()
    print(f"[hb] catalog:end pool={len(pool)} movie={sum(1 for x in pool if x['media_type']=='movie')} tv={sum(1 for x in pool if x['media_type']=='tvSeries')}")

    print("[hb] filter:unseen")
    eligible = [x for x in pool if not _is_seen(x, seen)]
    print(f"[hb] filter:end kept={len(eligible)} dropped={len(pool) - len(eligible)}")

    print(f"[hb] score:begin cw={weights['critic']:.3f} aw={weights['audience']:.3f} np={weights['novelty_pressure']:.2f} cc={weights['commitment_cost_scale']:.1f}")
    ranked, score_meta = sc.score_items(eligible, weights, shortlist_size=250)
    print(f"[hb] score:end ranked={len(ranked)}")

    topn = int(os.getenv("TOP_N", "10"))
    picks = ranked[:topn]

    today = datetime.utcnow().date().isoformat()
    daily_dir = OUT_ROOT / "daily" / today
    daily_dir.mkdir(parents=True, exist_ok=True)

    telemetry = {
        "pool": len(pool),
        "eligible": len(eligible),
        "after_skip": len(eligible),
        "shown": len(picks),
        "weights": {"critic": weights["critic"], "audience": weights["audience"]},
        "counts": {
            "tmdb_pool": len(pool),
            "eligible_unseen": len(eligible),
            "shortlist": len(ranked),
            "shown": len(picks),
        },
        "page_plan": meta.get("page_plan", {}),
    }

    feed = {
        "version": 1,
        "disclaimer": "This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.",
        "weights": weights,
        "telemetry": telemetry,
        "top10": [
            {
                "rank": i+1,
                "match": p.get("match", 0.0),
                "title": p.get("title"),
                "year": p.get("year"),
                "type": p.get("media_type"),
            }
            for i, p in enumerate(picks)
        ],
    }

    _write_json(daily_dir / "assistant_feed.json", feed)
    _write_json((OUT_ROOT / "latest") / "assistant_feed.json", feed)
    _write_json(DBG_ROOT / "page_plan.json", meta.get("page_plan", {}))

    print("Run complete.")
    print(f"Weights: critic={weights['critic']:.2f}, audience={weights['audience']:.2f}")
    print(f"Counts: tmdb_pool={len(pool)}, eligible_unseen={len(eligible)}, shortlist={len(ranked)}, shown={len(picks)}")
    pp = meta.get("page_plan", {})
    if pp:
        print(f"Page plan: movie_pages={pp.get('movie_pages')} tv_pages={pp.get('tv_pages')} rotate_minutes={pp.get('rotate_minutes')} slot={pp.get('slot')}")
    providers = (pp.get("provider_names") or [])
    if providers:
        print("Providers: " + ", ".join(providers))
    print(f"Output: {daily_dir}")

if __name__ == "__main__":
    main()