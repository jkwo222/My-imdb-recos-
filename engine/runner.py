# engine/runner.py
import os, json, datetime, sys
from rich import print as rprint
from engine.ratings_ingest import load_user_ratings_combined
from engine.seen_index import update_seen_from_ratings
from engine.autolearn import update_from_ratings, load_weights
from engine.catalog_builder import build_catalog
from engine.recommender import recommend

def main():
    pages_movie = int(os.environ.get("TMDB_PAGES_MOVIE","5") or 5)
    pages_tv    = int(os.environ.get("TMDB_PAGES_TV","5") or 5)
    include_tv_seasons = (os.environ.get("INCLUDE_TV_SEASONS","true").lower() == "true")

    # 1) load ratings (CSV + incremental HTML when available)
    rows, ingest_stats = load_user_ratings_combined()
    if len(rows) == 0:
        rprint("[red]No ratings loaded (CSV missing/private and IMDb page unreadable).[/red]")
        sys.exit(1)
    rprint(f"[ingest] ratings rows={len(rows)} (csv={ingest_stats['csv']}, html_new={ingest_stats['html_new']})")

    # 2) build seen index
    update_seen_from_ratings(rows)

    # 3) autolearn weights
    weights = update_from_ratings(rows)
    rprint(f"[weights] critic={weights['critic_weight']:.2f} audience={weights['audience_weight']:.2f}")

    # 4) build catalog (English originals only)
    try:
        catalog = build_catalog(pages_movie, pages_tv, include_tv_seasons=include_tv_seasons)
    except Exception as e:
        rprint(f"[red][catalog] failed: {e}[/red]")
        sys.exit(1)

    rprint(f"[catalog] total candidates (pre-seen-filter) = {len(catalog)}")

    # 5) recommend
    recs = recommend(catalog, weights)
    pool = len(catalog)
    shortlist = min(50, len(recs))
    shown = min(10, len(recs))
    rprint(f"[recs] pool={pool} shortlist={shortlist} shown={shown}")

    # 6) persist outputs
    out_dir = f"data/out/daily/{datetime.date.today().isoformat()}"
    os.makedirs(out_dir, exist_ok=True)
    json.dump({"date":str(datetime.date.today()),"recs":recs,"weights":weights},
              open(f"{out_dir}/recs.json","w"), indent=2)
    json.dump({"eligible_unseen":pool,"considered":pool,"shortlist":shortlist,"shown":shown,
               "dedupe":{"pre":0,"post":0,"output":0}}, open(f"{out_dir}/telemetry.json","w"), indent=2)
    rprint("[green]Run complete.[/green] See:", out_dir)

if __name__ == "__main__":
    main()