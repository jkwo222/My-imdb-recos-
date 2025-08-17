# engine/runner.py
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .catalog_builder import build_catalog
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile
from .personalize import genre_weights_from_profile, director_weights_from_profile, apply_personal_score
from .summarize import write_summary_md

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _load_user_profile(env: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    local = load_ratings_csv()
    remote: List[Dict[str, Any]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    # Remote is optional; we only use cache if present (no scraping here).
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

def _rank_items(items: List[Dict[str, Any]], env: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    profile = _load_user_profile(env)

    # Exclusion set: everything you've rated/seen (from CSV + cached web)
    exclude_ids = set(profile.keys())
    before_excl = len(items)
    items = [it for it in items if not it.get("tconst") or str(it["tconst"]) not in exclude_ids]

    # Affinity
    gweights = genre_weights_from_profile(profile)
    dweights = director_weights_from_profile(profile)

    # Score
    apply_personal_score(items, gweights, dweights)

    # Cut
    cut = float(env.get("MIN_MATCH_CUT") or "58")
    kept = [it for it in items if (it.get("match_score") or 0.0) >= cut]

    telemetry = {
        "total_input": before_excl,
        "excluded_already_seen": before_excl - len(items),
        "scored_total": len(items),
        "score_cut": cut,
        "kept": len(kept),
        "subs_include": [s.strip() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()],
        "region": env.get("REGION") or "US",
        "original_langs": env.get("ORIGINAL_LANGS") or "",
        "profile_size": len(profile),
        "affinity": {
            "genres_learned": sorted(gweights.keys()),
            "directors_learned": sorted(dweights.keys()),
        },
    }

    # Write ranked list for inspection
    (OUT_DIR / "assistant_ranked.json").write_text(
        json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (OUT_DIR / "debug_status.json").write_text(
        json.dumps(telemetry, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return kept, telemetry

def main() -> None:
    env = dict(os.environ)
    print(" | catalog:begin")
    items = build_catalog(env)  # pulls IMDb TSVs if present; enriches via TMDB; applies provider filter
    print(f" | catalog:end kept={len(items)}")

    ranked, telemetry = _rank_items(items, env)

    # Summary/email
    write_summary_md(
        env,
        items_ranked=ranked,
        genre_weights=None,   # computed inside summary from debug_status + ranked
        director_weights=None,
        telemetry=telemetry,
    )

    print(f"validation: items={len(ranked)} ids_present={sum(1 for i in ranked if i.get('imdb_id') or i.get('tconst'))} genres_present={sum(1 for i in ranked if i.get('genres'))}")
    print(f"score-cut {telemetry['score_cut']}: kept {telemetry['kept']} / {telemetry['scored_total']}")
    (OUT_DIR / "assistant_ranked.json").write_text(
        json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8"
    )

if __name__ == "__main__":
    main()