import os, json, datetime, sys
from rich import print
from engine.ratings_ingest import load_user_ratings_combined
from engine.seen_index import update_seen_from_ratings
from engine.autolearn import update_from_ratings
from engine.catalog_builder import build_or_update_master, write_working_snapshot
from engine.recommender import recommend

def main():
    # Need at least one ratings source
    have_csv_local = os.path.exists(os.environ.get("IMDB_RATINGS_CSV_PATH","data/ratings.csv"))
    have_csv_url = bool(os.environ.get("IMDB_RATINGS_CSV_URL","").strip())
    have_html = bool(os.environ.get("IMDB_USER_ID","").strip() or os.environ.get("IMDB_RATINGS_URL","").strip())
    if not (have_csv_local or have_csv_url or have_html):
        print("[red]Provide at least one source: data/ratings.csv, IMDB_RATINGS_CSV_URL, or IMDB_USER_ID/IMDB_RATINGS_URL.[/red]")
        sys.exit(1)

    # 1) Ratings
    rows, counts = load_user_ratings_combined()
    if not rows:
        print("[red]No ratings loaded (CSV missing/private and IMDb page unreadable).[/red]")
        sys.exit(1)
    print(f"[bold green]Ratings loaded[/bold green]: CSV={counts['csv']} + HTML new={counts['html_new']} → combined={counts['combined']}")
    os.makedirs("data/ingest", exist_ok=True)
    json.dump(rows, open(f"data/ingest/ratings_{datetime.date.today().isoformat()}.json","w"), indent=2)

    # 2) Seen + autolearn
    update_seen_from_ratings(rows)
    weights = update_from_ratings(rows)

    # 3) Catalog (original_language=en) + enrich
    master = build_or_update_master()
    working = write_working_snapshot(master)

    # 4) Recommend
    recs = recommend(working, weights)

    # 5) Output + telemetry
    out_dir = f"data/out/daily/{datetime.date.today().isoformat()}"; os.makedirs(out_dir, exist_ok=True)
    json.dump({"date":str(datetime.date.today()),"recs":recs,"weights":weights}, open(f"{out_dir}/recs.json","w"), indent=2)
    json.dump({"eligible_unseen":len(working),"considered":len(working),"shortlist":min(50,len(recs)),"shown":min(10,len(recs)),
               "dedupe":{"pre":0,"post":0,"output":0}}, open(f"{out_dir}/telemetry.json","w"), indent=2)
    print("[green]Run complete.[/green] See:", out_dir)

if __name__ == "__main__":
    main()