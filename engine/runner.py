# FILE: engine/runner.py
from __future__ import annotations
import os, json, time, pathlib
from typing import Dict, List, Tuple

from .catalog_builder import build_catalog, ensure_imdb_cache
from .feed import filter_by_providers, score_items, top10_by_type, to_markdown
from .store import PersistentPool

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_LATEST = ROOT / "data" / "out" / "latest"
OUT_DAILY_DIR = ROOT / "data" / "out" / "daily"
DEBUG_DIR = ROOT / "data" / "debug"

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _weights_path() -> pathlib.Path:
    return ROOT / "data" / "weights_live.json"

def _default_weights() -> Dict[str, float]:
    # You can tweak these; runner never hard-fails if file missing
    return {"critic_weight": 0.35, "audience_weight": 0.65, "commitment_cost_scale": 1.0, "novelty_weight": 0.15}

def load_weights() -> Dict[str, float]:
    p = _weights_path()
    if p.exists():
        try:
            return json.load(open(p, "r", encoding="utf-8"))
        except Exception:
            pass
    return _default_weights()

def _min_match_cut() -> float:
    try:
        return float(os.environ.get("MIN_MATCH_CUT", "58.0"))
    except Exception:
        return 58.0

def _subs_include() -> List[str]:
    raw = os.environ.get("SUBS_INCLUDE","").strip()
    if not raw: return []
    return [s.strip() for s in raw.split(",") if s.strip()]

def _write_json(path: pathlib.Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(path, "w", encoding="utf-8"), indent=2)

def _write_text(path: pathlib.Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def main():
    print(" | catalog:begin")
    weights = load_weights()

    # Ensure IMDb TSV cache exists/updated (weekly datasets, cached to disk)
    ensure_imdb_cache()

    # Build a fresh batch (TMDB discover + details + providers + IMDb ratings join)
    pool = build_catalog()

    print(f"catalog built → {len(pool)} items")

    allowed = _subs_include()
    kept = filter_by_providers(pool, allowed)
    print(f"provider-filter keep={allowed} → {len(kept)} items")

    # Merge into persistent pool (accumulate over time)
    store = PersistentPool(ROOT / "data" / "cache" / "pool.json")
    merged = store.merge_and_save(kept)
    # merged is the current full memory of all known items
    print(f"persistent-pool size → {len(merged)} items (accumulated)")

    # Score the *current* candidates (today's kept batch) using weights
    scored_today = score_items(kept, weights)

    # Cut by min_match
    mincut = _min_match_cut()
    scored_today = [r for r in scored_today if r.get("match", 0) >= mincut]
    movies, series = top10_by_type(scored_today)

    meta = {
        "pool_sizes": {
            "today_initial": len(pool),
            "today_kept": len(kept),
            "today_scored_kept": len(scored_today),
            "accumulated_total": len(merged),
        },
        "weights": weights,
        "subs": allowed,
    }

    # Export latest dir
    OUT_LATEST.mkdir(parents=True, exist_ok=True)
    feed = {
        "generated_at": int(time.time()),
        "count": len(scored_today),
        "items": scored_today,
        "meta": meta,
    }
    _write_json(OUT_LATEST / "assistant_feed.json", feed)
    _write_json(OUT_LATEST / "top10.json", {"movies": movies, "series": series, "meta": meta})

    md = to_markdown(movies, series, weights, meta)
    _write_text(OUT_LATEST / "summary.md", md)

    # Also export into dated daily folder
    day = time.strftime("%Y-%m-%d", time.gmtime())
    daily_dir = OUT_DAILY_DIR / day
    _write_json(daily_dir / "assistant_feed.json", feed)
    _write_json(daily_dir / "top10.json", {"movies": movies, "series": series, "meta": meta})
    _write_text(daily_dir / "summary.md", md)

    # Optional debug dump
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(DEBUG_DIR / "last_pool.json", pool)

    print(f" | catalog:end kept={len(kept)} scored_cut={len(scored_today)} → {OUT_LATEST / 'assistant_feed.json'}")

if __name__ == "__main__":
    main()