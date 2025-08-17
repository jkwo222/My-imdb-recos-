# engine/runner.py
from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict, List

from .catalog_builder import build_catalog
from .personalize import genre_weights_from_profile, apply_personal_score
from .profile import load_user_profile

# Downvote memory hooks (your feedback.py provides these)
try:
    from .feedback import (
        load_downvote_state,
        save_downvote_state,
        collect_downvote_events,
        update_downvote_state,
        compute_penalties,  # returns (title_penalties, hidden_ids, genre_penalties)
    )
    _DOWNVOTE_OK = True
except Exception as e:
    print(f"[feedback] downvote integration unavailable: {e}")
    _DOWNVOTE_OK = False

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
FEED_JSON = OUT_DIR / "assistant_feed.json"


def _apply_penalties_after_scoring(
    items: List[Dict[str, Any]],
    title_pen: Dict[str, float],
    hidden_ids: set[str],
    genre_pen: Dict[str, float],
) -> None:
    """
    Apply hard-hides and subtract penalties from 'score' in-place.
    This keeps your existing personalize.apply_personal_score untouched.
    """
    tpen = { (k or "").lower(): float(v) for k, v in (title_pen or {}).items() }
    gpen = { (k or "").lower(): float(v) for k, v in (genre_pen or {}).items() }
    hidden = set([(x or "").lower() for x in (hidden_ids or set())])

    for it in items:
        tconst = (it.get("tconst") or "").lower()

        # Hard hide first
        if tconst in hidden:
            it["score"] = -1.0
            it["hidden_reason"] = "downvoted"
            continue

        # If item not scored, skip
        if not isinstance(it.get("score"), (int, float)):
            continue

        # Title-level penalty
        p_title = float(tpen.get(tconst, 0.0))

        # Genre-level penalty (average across its genres)
        p_genre = 0.0
        genres = it.get("genres") or []
        if genres:
            acc = 0.0
            for g in genres:
                acc += float(gpen.get((g or "").lower(), 0.0))
            p_genre = acc / len(genres)

        total_pen = p_title + p_genre
        if total_pen > 0:
            it["score"] = max(0.0, float(it["score"]) - total_pen)
            it["penalties"] = {"title": round(p_title, 2), "genre": round(p_genre, 2)}


def main() -> None:
    env = dict(os.environ)

    # 1) Build candidate catalog (handles caching for IMDb/TMDB inside your builder)
    items = build_catalog(env)  # List[Dict]

    # 2) Load user profile (ratings.csv + cached public IMDb list if available)
    user_profile = load_user_profile(env)

    # 3) Derive genre weights using YOUR personalize.py
    genre_weights = genre_weights_from_profile(items, user_profile, imdb_id_field="tconst")

    # 4) Downvote memory: load state, collect new events (e.g., from GH issue), update
    title_penalties: Dict[str, float] = {}
    hidden_ids: set[str] = set()
    genre_penalties: Dict[str, float] = {}

    if _DOWNVOTE_OK:
        dv_state = load_downvote_state()
        events = collect_downvote_events(items)
        if any(events.values()):
            update_downvote_state(dv_state, events)
            save_downvote_state(dv_state)

        # Convert state into current penalties/hide set (handles decay internally)
        title_penalties, hidden_ids, genre_penalties = compute_penalties(items, dv_state)

    # 5) First compute personalized scores using your existing method
    apply_personal_score(items, genre_weights, base_key="imdb_rating")

    # 6) Now subtract penalties and apply hard hides
    if _DOWNVOTE_OK:
        _apply_penalties_after_scoring(items, title_penalties, hidden_ids, genre_penalties)

    # 7) Persist the feed and the summary
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FEED_JSON.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    # 8) Write the Markdown summary
    from .summarize import write_summary_md
    write_summary_md(env, genre_weights=genre_weights, picks_limit=15)


if __name__ == "__main__":
    main()