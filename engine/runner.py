# engine/runner.py
import os, json, datetime, pathlib
from rich import print
from engine.seen_index import update_seen_from_ratings
from engine.autolearn import update_from_ratings, load_weights
from engine.catalog_builder import build_catalog

def _load_ratings_from_csv(csv_path: str):
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
    # 1) Ratings ingest (CSV preferred)
    csv_path  = os.environ.get("IMDB_RATINGS_CSV_PATH","").strip()
    rows = _load_ratings_from_csv(csv_path) if csv_path else []
    if rows:
        print(f"[bold]IMDb ingest (CSV):[/bold] {csv_path} — {len(rows)} rows")
        update_seen_from_ratings(rows)
        weights = update_from_ratings(rows)
    else:
        print("[yellow]No ratings CSV found; using last learned weights.[/yellow]")
        weights = load_weights()

    # 2) Build catalog (TMDB + OMDb) with English + providers + ratings
    print("[bold]Building catalog…[/bold]")
    catalog = build_catalog()

    # 3) Subscription filter (default to your services)
    subs_env = os.environ.get(
        "SUBS_INCLUDE",
        "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus"
    )
    subs = set([s.strip() for s in subs_env.split(",") if s.strip()])
    def on_subs(item):
        provs = item.get("providers") or []
        return any(p in subs for p in provs)

    eligible = [c for c in catalog if on_subs(c)]

    # 4) Recommend
    from engine.recommender import recommend
    recs = recommend(eligible, weights)

    # 5) Output
    today = datetime.date.today().isoformat()
    out_dir = pathlib.Path(f"data/out/daily/{today}")
    out_dir.mkdir(parents=True, exist_ok=True)

    json.dump({
        "date": str(today),
        "recs": recs,
        "weights": weights
    }, open(out_dir/"recs.json","w"), indent=2)

    json.dump({
        "pool": len(catalog),
        "eligible_after_subs": len(eligible),
        "considered": len(eligible),
        "shortlist": min(50, len(recs)),
        "shown": min(10, len(recs))
    }, open(out_dir/"telemetry.json","w"), indent=2)

    # Keep an assistant feed the workflow can summarize into the Issue
    top = []
    for r in recs[:200]:
        top.append({
            "title": r.get("title",""),
            "year": r.get("year",0),
            "type": r.get("type",""),
            "seasons": r.get("seasons",1),
            "imdb_id": r.get("imdb_id",""),
            "critic_rt": round(100 * float(r.get("critic",0.0))),
            "audience_imdb": round(10 * float(r.get("audience",0.0)), 1),
            "providers": r.get("providers",[]),
            "match": r.get("match",0.0),
        })

    json.dump({
        "version": "v2.13-sync-omdb-tmdb",
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "weights": weights,
        "telemetry": {
            "pool": len(catalog),
            "eligible_after_subs": len(eligible),
            "shortlist": min(50, len(recs)),
            "shown": min(10, len(recs))
        },
        "top": top
    }, open(out_dir/"assistant_feed.json","w"), indent=2)

    print(f"[green]Run complete.[/green] See: {out_dir}")

if __name__ == "__main__":
    main()