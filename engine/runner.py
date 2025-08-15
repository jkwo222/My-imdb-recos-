# engine/runner.py
import os, json, datetime, sys, pathlib
from rich import print
from engine.seen_index import update_seen_from_ratings
from engine.autolearn import update_from_ratings, load_weights
from engine.catalog_builder import build_catalog

# Try to import the optional IMDb HTML scraper.
# If it's missing (or later removed), we still run using CSV.
try:
    from engine.imdb_ingest import scrape_imdb_ratings, to_rows
    HAVE_IMDB_SCRAPER = True
except Exception:
    scrape_imdb_ratings = None
    to_rows = None
    HAVE_IMDB_SCRAPER = False

def _load_ratings_from_csv(csv_path: str):
    """
    Load IMDb ratings from CSV (preferred for reliability).
    Supports official IMDb export or your curated data/ratings.csv.
    """
    rows = []
    if not csv_path or not os.path.exists(csv_path):
        return rows
    import csv
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            title = (r.get("Title") or r.get("title") or "").strip()
            year = int((r.get("Year") or r.get("year") or "0").strip() or 0)
            imdb_id = (r.get("Const") or r.get("imdb_id") or "").strip()
            your_rating = float((r.get("Your Rating") or r.get("your_rating") or "0").strip() or 0)
            typ_raw = (r.get("Title Type") or r.get("type") or "").strip()
            if typ_raw in ("tvMiniSeries","tvSeries","tvMovie","tvSpecial","movie"):
                typ = typ_raw
            else:
                typ = "movie" if typ_raw == "movie" else ("tvSeries" if "series" in typ_raw else "movie")
            rows.append({"imdb_id": imdb_id, "title": title, "year": year, "type": typ, "your_rating": your_rating})
    return rows

def main():
    # ---- Load ratings (CSV first, then optional web) -------------
    imdb_user = os.environ.get("IMDB_USER_ID","").strip()
    csv_path  = os.environ.get("IMDB_RATINGS_CSV_PATH","").strip()

    rows = []
    if csv_path and os.path.exists(csv_path):
        print(f"[bold]IMDb ingest (CSV):[/bold] {csv_path}")
        rows = _load_ratings_from_csv(csv_path)
    elif imdb_user and HAVE_IMDB_SCRAPER:
        url = f"https://www.imdb.com/user/{imdb_user}/ratings"
        print(f"[bold]IMDb ingest (web):[/bold] {url}")
        try:
            rows = to_rows(scrape_imdb_ratings(url))
        except Exception as e:
            print(f"[yellow]IMDb web ingest failed ({e}). Continuing without update.[/yellow]")
    else:
        if imdb_user and not HAVE_IMDB_SCRAPER:
            print("[yellow]IMDB_USER_ID set but imdb_ingest module not available. Prefer CSV path.[/yellow]")
        else:
            print("[yellow]No ratings source configured; continuing without update.[/yellow]")

    # ---- Save ingest + update seen index + autolearn -------------
    today = datetime.date.today().isoformat()
    os.makedirs("data/ingest", exist_ok=True)
    if rows:
        json.dump(rows, open(f"data/ingest/ratings_{today}.json","w"), indent=2)
        update_seen_from_ratings(rows)
    weights = update_from_ratings(rows) if rows else load_weights()

    # ---- Build catalog (TMDB + OMDb) -----------------------------
    print("[bold]Building catalog …[/bold]")
    catalog = build_catalog()

    # ---- Filter to your subscriptions by default -----------------
    subs_env = os.environ.get(
        "SUBS_INCLUDE",
        "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"
    )
    subs = set([s.strip() for s in subs_env.split(",") if s.strip()])
    def on_subs(item):
        provs = item.get("providers") or []
        return any(p in subs for p in provs)

    eligible = [c for c in catalog if on_subs(c)]

    # ---- Recommend ------------------------------------------------
    from engine.recommender import recommend
    recs = recommend(eligible, weights)

    # ---- Output files --------------------------------------------
    out_dir = pathlib.Path(f"data/out/daily/{today}")
    out_dir.mkdir(parents=True, exist_ok=True)

    json.dump({"date": str(today), "recs": recs, "weights": weights}, open(out_dir/"recs.json","w"), indent=2)
    json.dump({
        "eligible_unseen": len(eligible),
        "considered": len(eligible),
        "shortlist": min(50, len(recs)),
        "shown": min(10, len(recs)),
        "dedupe": {"pre": 0, "post": 0, "output": 0}
    }, open(out_dir/"telemetry.json","w"), indent=2)

    feed = {
        "version": "v2.13-feed-1",
        "generated_at": datetime.datetime.utcnow().isoformat()+"Z",
        "weights": weights,
        "telemetry": {
            "pool": len(catalog),
            "eligible_after_subs": len(eligible),
            "shortlist": min(50, len(recs)),
            "shown": min(10, len(recs))
        },
        "top": [
            {
                "imdb_id": r.get("imdb_id",""),
                "title": r.get("title",""),
                "year": r.get("year",0),
                "type": r.get("type",""),
                "seasons": r.get("seasons",1),
                "critic": r.get("critic",0.0),
                "audience": r.get("audience",0.0),
                "match": r.get("match",0.0),
                "providers": r.get("providers",[])
            } for r in recs[:200]
        ],
        "considered_sample": [c.get("imdb_id","") for c in eligible[:200]]
    }
    open(out_dir/"assistant_feed.json","w",encoding="utf-8").write(json.dumps(feed, indent=2))

    print("[green]Run complete.[/green] See:", out_dir)

if __name__ == "__main__":
    main()