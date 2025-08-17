# engine/runner.py
from __future__ import annotations

import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, List

from .catalog_builder import build_catalog
from .summarize import write_summary_md
from .personalize import genre_weights_from_profile, apply_personal_score

# Optional imdb helpers
try:
    from .imdb_sync import (
        load_ratings_csv,
        fetch_user_ratings_web,
        merge_user_sources,
        to_user_profile,
    )
    _IMDB_SYNC_OK = True
except Exception:
    _IMDB_SYNC_OK = False

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_user_profile(env: Dict[str, str]) -> Dict[str, Dict]:
    if not _IMDB_SYNC_OK:
        return {}
    local = load_ratings_csv()
    remote = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile


def _rank_items(items: List[Dict[str, Any]], env: Dict[str, str]) -> List[Dict[str, Any]]:
    profile = _load_user_profile(env)
    # Compute genre weights from your ratings on the current item universe
    weights = genre_weights_from_profile(items, profile, imdb_id_field="imdb_id")
    # Save weights for summary/debug
    (OUT_DIR / "genre_weights.json").write_text(
        json.dumps(weights, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Apply score (base: imdb_rating; fallback midpoint 60)
    apply_personal_score(items, genre_weights=weights, base_key="imdb_rating")

    # produce match_score (float) and reason string
    ranked: List[Dict[str, Any]] = []
    for it in items:
        base = it.get("imdb_rating")
        tmdb_vote = it.get("tmdb_vote")
        year = it.get("year")
        why_parts = []
        if base is not None:
            try:
                why_parts.append(f"IMDb {float(base):.1f}")
            except Exception:
                pass
        if tmdb_vote is not None:
            try:
                why_parts.append(f"TMDB {float(tmdb_vote):.1f}")
            except Exception:
                pass
        if year:
            why_parts.append(str(year))
        why = "; ".join(why_parts) if why_parts else ""

        score = it.get("score")
        if score is None:
            # convert imdb rating to 0–100
            base10 = float(base) if base is not None else math.nan
            score = (base10 * 10.0) if not math.isnan(base10) else 60.0

        ranked.append(
            {
                **it,
                "match_score": round(float(score), 2),
                "why": why,
            }
        )

    ranked.sort(key=lambda x: x.get("match_score") or 0.0, reverse=True)
    return ranked


def main() -> None:
    env = dict(os.environ)
    t0 = time.time()

    print(" | catalog:begin")
    items = build_catalog(env)
    print(f" | catalog:end kept={len(items)}")

    # Validate minimal fields
    ids_present = sum(1 for it in items if it.get("imdb_id") or it.get("tconst"))
    genres_present = sum(1 for it in items if it.get("genres"))
    print(f"validation: items={len(items)} ids_present={ids_present} genres_present={genres_present}")

    ranked = _rank_items(items, env)

    # cut by MIN_MATCH_CUT if provided
    cut = float(env.get("MIN_MATCH_CUT") or 0)
    kept = [it for it in ranked if (it.get("match_score") or 0) >= cut]
    print(f"score-cut {cut}: kept {len(kept)} / {len(ranked)}")

    # write outputs
    (OUT_DIR / "assistant_ranked.json").write_text(
        json.dumps({"items": kept}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # summary.md
    write_summary_md(env, genre_weights_path=OUT_DIR / "genre_weights.json")

    # debug status
    elapsed = round(time.time() - t0, 2)
    dbg = {
        "elapsed_sec": elapsed,
        "ranked_total": len(ranked),
        "kept_after_cut": len(kept),
        "min_match_cut": env.get("MIN_MATCH_CUT"),
    }
    (OUT_DIR / "debug_status.json").write_text(
        json.dumps(dbg, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"wrote → {OUT_DIR/'assistant_ranked.json'}")
    print(f"wrote → {OUT_DIR/'summary.md'}")
    print(f"wrote → {OUT_DIR/'debug_status.json'}")


if __name__ == "__main__":
    main()