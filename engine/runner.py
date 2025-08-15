import os, json, datetime
from rich import print as rprint
from engine.ratings import load_csv, scrape_imdb_ratings
from engine.seen_index import update_seen_from_ratings
from engine.autolearn import update_from_ratings, load_weights
from engine.catalog import build_catalog
from engine.recommender import recommend
from engine.omdb import fetch_omdb

def _env_bool(name: str, default: bool=False) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    if v in ("1","true","yes","on"): return True
    if v in ("0","false","no","off"): return False
    return default

def main():
    os.makedirs("data/out", exist_ok=True)
    os.makedirs("data/cache", exist_ok=True)

    # --- Config via env
    imdb_user = (os.environ.get("IMDB_USER_ID") or "").strip()
    csv_path  = (os.environ.get("IMDB_RATINGS_CSV_PATH") or "").strip()
    tmdb_key  = (os.environ.get("TMDB_API_KEY") or "").strip()
    omdb_key  = (os.environ.get("OMDB_API_KEY") or "").strip()

    pages_movie = int(os.environ.get("TMDB_PAGES_MOVIE") or 12)
    pages_tv    = int(os.environ.get("TMDB_PAGES_TV") or 12)
    max_catalog = int(os.environ.get("MAX_CATALOG") or 6000)
    original_langs = [s.strip() for s in (os.environ.get("ORIGINAL_LANGS") or "en").split(",") if s.strip()]

    include_tv_seasons = _env_bool("INCLUDE_TV_SEASONS", True)

    # --- Ratings ingest (prefer CSV)
    rows = []
    csv_rows = load_csv(csv_path)
    rows.extend(csv_rows)
    if not rows and imdb_user:
        rows.extend(scrape_imdb_ratings(f"https://www.imdb.com/user/{imdb_user}/ratings"))

    if not rows:
        rprint("[red]No ratings loaded (CSV missing/private and IMDb page unreadable).[/red]")
        # proceed anyway, but without seen-index we might show obvious repeats
    else:
        update_seen_from_ratings(rows)

    # --- Update weights
    weights = update_from_ratings(rows) if rows else load_weights()

    # --- Build catalog from TMDB
    if not tmdb_key:
        rprint("[red]TMDB_API_KEY missing[/red]")
        return

    cat = build_catalog(
        tmdb_key=tmdb_key,
        pages_movie=pages_movie,
        pages_tv=pages_tv,
        original_langs=original_langs,
        include_tv_seasons=include_tv_seasons,
        max_catalog=max_catalog
    )

    pool = len(cat)

    # --- Enrich subset with OMDb (IMDb + RT)
    enrich_cap = min(pool, 600)
    for rec in cat[:enrich_cap]:
        iid = rec.get("imdb_id")
        if iid and omdb_key:
            data = fetch_omdb(iid, omdb_key)
            if data and data.get("Response") == "True":
                try:
                    rec["imdb_rating"] = float(data.get("imdbRating") or 0.0)
                except Exception:
                    pass
                # Rotten Tomatoes score if present
                for ent in data.get("Ratings", []):
                    if ent.get("Source") == "Rotten Tomatoes":
                        try:
                            rec["rt"] = float((ent.get("Value") or "0").replace("%",""))
                        except Exception:
                            pass
                        break

    # --- Recommend
    recs = recommend(cat, weights)

    # --- Telemetry
    telemetry = {
        "pool": pool,
        "eligible_unseen": len(recs),  # approximation after seen filter
        "after_skip": len(recs),       # skip-window not enforced here
        "shown": min(10, len(recs)),
        "notes": {
            "language_filter": original_langs,
            "subs_filter_enforced": False  # no watch-provider API in this version
        }
    }

    # --- Output
    out_dir = f"data/out/daily/{datetime.date.today().isoformat()}"
    os.makedirs(out_dir, exist_ok=True)
    json.dump({"date":str(datetime.date.today()),"recs":recs,"weights":weights}, open(f"{out_dir}/recs.json","w"), indent=2)
    json.dump(telemetry, open(f"{out_dir}/telemetry.json","w"), indent=2)
    rprint(f"[green]Run complete.[/green] See:", out_dir)

if __name__ == "__main__":
    main()