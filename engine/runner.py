# engine/runner.py
import os, json, datetime, sys
from rich import print

from tools.ratings import load_imdb_ratings_csv, enrich_with_omdb, is_english_from_item
from engine.autolearn import update_from_ratings, load_weights
from engine.seen_index import update_seen_from_ratings, is_seen
from engine.recommender import recommend
from engine import catalog as cat  # your existing TMDB catalog builder

def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name, "")
    if not v:
        return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

def main():
    # ------ load your ratings (CSV preferred) ------
    csv_path = os.environ.get("IMDB_RATINGS_CSV_PATH","").strip()
    rows = load_imdb_ratings_csv(csv_path) if csv_path else []
    if not rows:
        print("[yellow]No ratings loaded from CSV — engine will be far less personalized.[/yellow]")

    # update seen index + autolearn weights
    if rows:
        update_seen_from_ratings(rows)
    weights = update_from_ratings(rows)

    # ------ build a wide catalog from TMDB (no language filter here) ------
    pages_mov = int(os.environ.get("TMDB_PAGES_MOVIE", "12") or 12)
    pages_tv  = int(os.environ.get("TMDB_PAGES_TV", "12") or 12)
    include_tv_seasons = _env_bool("INCLUDE_TV_SEASONS", True)
    max_catalog = int(os.environ.get("MAX_CATALOG", "6000") or 6000)
    region = os.environ.get("REGION", "US")
    subs = [s.strip() for s in os.environ.get("SUBS_INCLUDE","").split(",") if s.strip()]  # currently not enforced here

    print("Building catalog…")
    catalog = cat.build_catalog(
        pages_movie=pages_mov,
        pages_tv=pages_tv,
        region=region,
        include_tv_seasons=include_tv_seasons,
        max_catalog=max_catalog,
        subs_include=subs
    )

    # ------ OMDb enrich (gets Language/Country/Genres/Runtime/IMDb/RT) ------
    catalog = enrich_with_omdb(catalog)

    # ------ filter to English-original content (UK/AU/CA/US/etc) ------
    # We honor LANGUAGE, *not* region. If ORIGINAL_LANGS empty -> default to English.
    orig_langs = os.environ.get("ORIGINAL_LANGS","en").strip().lower()
    require_english = (orig_langs == "" or "en" in [p.strip() for p in orig_langs.split(",")])
    eligible = []
    for c in catalog:
        # allow English if OMDb says English OR TMDB original_language=en
        if require_english and not is_english_from_item(c):
            continue
        eligible.append(c)

    # ------ recommend (skip anything 'seen') ------
    def _seen_checker(item):
        return is_seen(item.get("title",""), item.get("imdb_id",""), int(item.get("year") or 0))

    recs = recommend(eligible, weights, _seen_checker)

    # ------ telemetry & outputs ------
    today = datetime.date.today().isoformat()
    out_dir = f"data/out/daily/{today}"
    os.makedirs(out_dir, exist_ok=True)

    meta = {
        "date": today,
        "pool": len(catalog),
        "eligible": len(eligible),
        "after_skip": len(recs),
        "shown": min(10, len(recs)),
        "weights": {"critic": weights.get("critic_weight"), "audience": weights.get("audience_weight")}
    }
    json.dump({"recs": recs, "meta": meta}, open(f"{out_dir}/recs.json","w"), indent=2)

    # Write a simple text summary for issue creation
    lines = []
    lines.append(f"Run: https://github.com/{os.environ.get('GITHUB_REPOSITORY','<repo>')}/actions/runs/{os.environ.get('GITHUB_RUN_ID','')}")
    lines.append("Top 10")
    for i, r in enumerate(recs[:10], start=1):
        lines.append(f"\t{i}\t{r['match']} — {r['title']} ({r.get('year','')}) [{r.get('type','')}]")
        if r.get("why"):
            lines.append(f"\t   {r['why']}")
    lines.append(f"Telemetry: pool={meta['pool']}, eligible={meta['eligible']}, after_skip={meta['after_skip']}, shown={meta['shown']}")
    lines.append(f"Weights: critic={meta['weights']['critic']:.2f}, audience={meta['weights']['audience']:.2f}")
    lines.append("This product uses the TMDB API but is not endorsed or certified by TMDB.")
    open("issue.md","w").write("\n".join(lines))

    print("[green]Run complete.[/green] See:", out_dir)

if __name__ == "__main__":
    main()