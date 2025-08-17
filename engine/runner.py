# engine/runner.py
from __future__ import annotations
import os
import json
from pathlib import Path

from .catalog_builder import build_catalog  # assumes your existing builder
from .personalize import apply_personal_score
from .summarize import write_summary_md

# Personal profile utilities (keep your existing implementations/locations)
# If these live elsewhere in your codebase, adjust the import accordingly.
try:
    from .profile import load_user_profile, genre_weights_from_profile
except Exception:
    # Minimal fallbacks if your project structures these differently.
    def load_user_profile(env: dict) -> dict:
        return {}
    def genre_weights_from_profile(items, profile) -> dict[str, float]:
        # neutral 0.5 for all genres seen in the corpus
        seen = set()
        for it in items:
            for g in (it.get("genres") or []):
                seen.add(g)
        return {g: 0.5 for g in seen}

# Downvote memory
from .feedback import (
    load_downvote_state,
    save_downvote_state,
    collect_downvote_events,
    update_downvote_state,
    compute_penalties,
)

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
FEED_JSON = OUT_DIR / "assistant_feed.json"

def main() -> None:
    env = dict(os.environ)

    # 1) Build today's candidate catalog (your existing logic)
    items = build_catalog(env)  # returns List[Dict]

    # 2) Build personalization weights from the user profile (ratings/history)
    profile = load_user_profile(env)
    genre_weights = genre_weights_from_profile(items, profile)

    # 3) Ingest downvote signals from GH issue comments + inbox, update state
    dv_state = load_downvote_state()
    events = collect_downvote_events(items)
    if any(v for v in events.values()):
        update_downvote_state(dv_state, events)
        save_downvote_state(dv_state)

    # 4) Compute time-decayed penalties and hide list
    title_pen, hidden_ids, genre_pen = compute_penalties(items, dv_state)

    # 5) Apply personalization + penalties to get a final score
    apply_personal_score(
        items,
        genre_weights,
        base_key="imdb_rating",
        title_penalties=title_pen,
        genre_penalties=genre_pen,
        hidden_tconsts=hidden_ids,
    )

    # 6) Persist feed and write Markdown summary
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FEED_JSON.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    write_summary_md(env, genre_weights=genre_weights, picks_limit=15)

if __name__ == "__main__":
    main()