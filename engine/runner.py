# engine/runner.py
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from .catalog import build_pool  # returns (pool: List[dict], meta: Dict)
from .exclusions import build_exclusion_index, filter_excluded


# ---------------------------
# simple env helpers
# ---------------------------

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


# ---------------------------
# ranking / selection
# ---------------------------

def _rank(items: List[Dict], critic_weight: float, audience_weight: float) -> List[Dict]:
    """Lightweight deterministic scorer similar to catalog._rank (kept here to avoid private import)."""
    ranked = []
    for it in items:
        va  = float(it.get("vote_average", 0.0))  # 0..10
        pop = float(it.get("popularity", 0.0))
        score = (critic_weight * va * 10.0) + (audience_weight * min(pop, 100.0) * 0.1)
        j = dict(it)
        j["match"] = round(score, 1)
        ranked.append(j)
    ranked.sort(key=lambda x: x.get("match", 0.0), reverse=True)
    return ranked


def _pick(items: List[Dict], k: int) -> List[Dict]:
    return items[:max(0, min(k, len(items)))]


# ---------------------------
# output helpers
# ---------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _write_json(path: str, data: Dict) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _build_feed(now_utc: datetime,
                pool: List[Dict],
                shortlist: List[Dict],
                shown: List[Dict],
                meta: Dict) -> Dict:
    # Pretty header counts
    counts = meta.get("counts", {})
    weights = meta.get("weights", {})
    critic_w = float(weights.get("critic_weight", 0.6))
    audience_w = float(weights.get("audience_weight", 0.4))

    header = {
        "generated_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "counts": {
            "pool": counts.get("cumulative", len(pool)),
            "eligible_unseen": counts.get("eligible_unseen", len(pool)),
            "shortlist": len(shortlist),
            "shown": len(shown),
        },
        "weights": {
            "critic": critic_w,
            "audience": audience_w,
        },
        "filters": meta.get("filters", {}),
        "cursor_after": meta.get("cursor_after", {}),
        "exclusions": meta.get("exclusions", {}),
    }

    # Normalize items for output
    def norm(it: Dict) -> Dict:
        return {
            "title": it.get("title"),
            "year": it.get("year"),
            "type": it.get("type"),
            "tmdb_id": it.get("tmdb_id"),
            "popularity": it.get("popularity"),
            "vote_average": it.get("vote_average"),
            "match": it.get("match"),
        }

    body = {
        "shortlist": [norm(x) for x in shortlist],
        "shown": [norm(x) for x in shown],
    }

    return {**header, **body}


# ---------------------------
# main
# ---------------------------

def main() -> None:
    # knobs
    shortlist_k = _env_int("SHORTLIST_K", 50)
    shown_k     = _env_int("SHOWN_K", 10)
    out_root    = _env_str("OUT_DIR", os.path.join("data", "out"))
    csv_path    = _env_str("RATINGS_CSV", os.path.join("data", "ratings.csv"))

    # 1) Build/extend the catalog pool (this already does exclusion once)
    pool, meta = build_pool(cfg=None)  # cfg optional; env-backed defaults inside catalog

    # 2) Build exclusion index and re-filter (belt & suspenders)
    ex_idx = build_exclusion_index(csv_path)
    pool_filtered, fresh_excluded_dummy = filter_excluded(pool, ex_idx)  # fresh pass on cumulative

    # Telemetry: exclusions + pool counts
    ex_meta = meta.setdefault("exclusions", {})
    ex_meta.setdefault("csv_path", csv_path)
    # Note: catalog.py already tracked fresh/prev exclusions; here we only report the belt-pass delta
    ex_meta["runner_belt_pass_dropped"] = len(pool) - len(pool_filtered)

    # Compute type counts
    pool_total = len(pool_filtered)
    pool_movie = sum(1 for it in pool_filtered if it.get("type") == "movie")
    pool_tv    = sum(1 for it in pool_filtered if it.get("type") == "tvSeries")

    print(f"Pool counts: total={pool_total} movie={pool_movie} tv={pool_tv}", flush=True)

    # 3) Rank → shortlist → shown
    weights = meta.get("weights", {})
    critic_w   = _env_float("CRITIC_WEIGHT", float(weights.get("critic_weight", 0.6)))
    audience_w = _env_float("AUDIENCE_WEIGHT", float(weights.get("audience_weight", 0.4)))

    ranked = _rank(pool_filtered, critic_w, audience_w)

    # Extra safety: never allow CSV titles into shortlist/shown even if they slipped somehow
    shortlist = _pick(ranked, shortlist_k)
    shortlist, _ = filter_excluded(shortlist, ex_idx)

    shown = _pick(shortlist, shown_k)
    shown, _ = filter_excluded(shown, ex_idx)

    # 4) Write artifacts
    now = datetime.now(timezone.utc)
    daily_dir = os.path.join(out_root, "daily", now.strftime("%Y-%m-%d"))
    latest_dir = os.path.join(out_root, "latest")

    feed = _build_feed(now, pool_filtered, shortlist, shown, meta)

    _write_json(os.path.join(latest_dir, "assistant_feed.json"), feed)
    _write_json(os.path.join(daily_dir, "assistant_feed.json"), feed)

    # 5) Pretty console output (for Actions logs)
    print(f"Weights: critic={critic_w}, audience={audience_w}", flush=True)
    print(f"Output: {daily_dir}", flush=True)

    # Optional: minimal table preview for the top-10
    if shown:
        print("Top 10:", flush=True)
        for i, it in enumerate(shown, 1):
            ti = it.get("title")
            yr = it.get("year")
            ty = it.get("type")
            sc = it.get("match")
            print(f"{i:>2}. {ti} ({yr}) [{ty}] — {sc}", flush=True)


if __name__ == "__main__":
    main()