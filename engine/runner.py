# engine/runner.py
from __future__ import annotations
import json, os, pathlib, time, datetime
from typing import Any, Dict, List

from rich import print as rprint

from .ratings_ingest import load_user_ratings_combined
from .imdb_ingest import load_imdb_maps
from .catalog_builder import build_catalog
from .seen_index import filter_unseen, load_seen_index
from .taste import build_taste
from .feed import filter_by_providers, score_items, top10_by_type, to_markdown
from .store import load_store, save_store, remember
from .weights import load_weights, update_from_ratings

OUT_ROOT = pathlib.Path("data/out")

def _env_list(name: str, default: str) -> List[str]:
    raw = os.environ.get(name, default)
    return [s.strip() for s in (raw or "").split(",") if s.strip()]

def main():
    rprint(" | catalog:begin")

    # Ratings (local + public scrape)
    rows, meta = load_user_ratings_combined()
    rprint(f"IMDb ratings loaded → rows={len(rows)} meta={meta}")

    # Weights
    w = load_weights()
    # Nudge from ratings
    try:
        w = update_from_ratings(rows)
    except Exception:
        pass
    # keep novelty weight if already present
    if "novelty_weight" not in w:
        w["novelty_weight"] = float(os.environ.get("NOVELTY_WEIGHT","0.15"))
    rprint(f"weights → {w}")

    # IMDb TSV aggregates (cached weekly via Actions cache)
    basics_map, ratings_map = load_imdb_maps(ttl_hours=72)

    # Build pool from TMDB; attach imdb aggregates when ids present
    pool = build_catalog(basics_map, ratings_map)
    rprint(f"catalog built → {len(pool)} items")

    # Provider filter
    subs = _env_list("SUBS_INCLUDE", "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus")
    keep = filter_by_providers(pool, subs)
    rprint(f"provider-filter keep={subs} → {len(keep)} items")

    # Unseen (title/year fuzzy or imdb id)
    seen_idx = load_seen_index(os.environ.get("IMDB_RATINGS_CSV_PATH","data/ratings.csv"))
    unseen = filter_unseen(keep, seen_idx)
    rprint(f"unseen-only → {len(unseen)} items")

    # Taste profile (from ratings)
    taste = build_taste(rows)
    rprint(f"taste profile → {len(taste)} genres")

    # Score, cut, top10s
    scored = score_items(unseen, w)
    min_cut = float(os.environ.get("MIN_MATCH_CUT","58.0"))
    scored = [x for x in scored if float(x.get("match") or 0.0) >= min_cut]
    movies, series = top10_by_type(scored)

    # Persist long-lived store
    store = load_store()
    store = remember(store, pool)          # grows over time
    save_store(store)

    # Output files
    today = datetime.date.today().isoformat()
    day_dir = OUT_ROOT / "daily" / today
    latest_dir = OUT_ROOT / "latest"
    day_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    # Full assistant feed (up to 99 like before)
    feed = {
        "generated_at": int(time.time()),
        "count": len(scored[:99]),
        "items": scored[:99],
        "meta": {
            "pool_sizes": {
                "initial": len(pool),
                "providers": len(keep),
                "unseen": len(unseen),
                "fresh": len(scored),
                "final": len(scored[:99]),
            },
            "weights": {
                "audience_weight": round(w.get("audience_weight",0.5),2),
                "critic_weight": round(w.get("critic_weight",0.5),2),
                "commitment_cost_scale": w.get("commitment_cost_scale",1.0),
                "novelty_weight": w.get("novelty_weight",0.15),
                "min_match_cut": min_cut,
            },
            "subs": subs
        }
    }
    for dest in (day_dir/"assistant_feed.json", latest_dir/"assistant_feed.json"):
        json.dump(feed, open(dest,"w"), indent=2)

    # Top 10s export
    top10 = {
        "generated_at": int(time.time()),
        "movies": movies,
        "series": series,
    }
    for dest in (day_dir/"top10.json", latest_dir/"top10.json"):
        json.dump(top10, open(dest,"w"), indent=2)

    # Markdown summary for GitHub notifications (issues / step summary)
    md = []
    md.append(f"## Daily Recs — {today}")
    md.append("")
    md.append(f"- Pool: initial={len(pool)} | providers={len(keep)} | unseen={len(unseen)} | cut≥{min_cut} → {len(scored)}")
    md.append(f"- Subs: {', '.join(subs)}")
    md.append("")
    md.append("—")
    md.append("")
    md.append(to_markdown(movies, series, taste))
    md.append("")
    md_text = "\n".join(md)
    (day_dir/"summary.md").write_text(md_text, encoding="utf-8")
    (latest_dir/"summary.md").write_text(md_text, encoding="utf-8")

    # Print summary to STDOUT so your workflow can pick it up (and you see it in logs)
    print("\n==== BEGIN SUMMARY ====\n")
    print(md_text)
    print("\n==== END SUMMARY ====\n")

    rprint(f" | catalog:end pool={len(pool)} feed={len(scored[:99])} → {day_dir/'assistant_feed.json'}")

if __name__ == "__main__":
    main()