# engine/runner.py
import os, sys, json, datetime
from rich import print
from typing import List, Dict, Any

from tools.ratings import load_imdb_ratings_csv, enrich_with_omdb
from engine.seen_index import update_seen_from_ratings, is_seen
from tools.tmdb_client import fetch_catalog, fetch_providers

def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except Exception:
        return default

def env_list(name: str) -> List[str]:
    raw = os.environ.get(name, "") or ""
    return [t.strip() for t in raw.split(",") if t.strip()]

def _score(item: Dict[str,Any], weights: Dict[str,float]) -> float:
    base = 70.0
    crit = float(item.get("rt_pct", 0) or 0)/100.0
    imdb = float(item.get("imdb_rating", 0) or 0)/10.0
    s = base + 15.0*(weights.get("critic_weight",0.5)*crit + weights.get("audience_weight",0.5)*imdb)
    if item.get("type") == "tvSeries":
        seasons = int(item.get("seasons") or 1)
        if seasons >= 3: s -= 9.0
        elif seasons == 2: s -= 4.0
    if not item.get("rt_pct") and not item.get("imdb_rating"):
        s -= 3.0
    if item.get("providers_on_subs"):
        s += 1.0
    return max(60.0, min(98.0, s))

def _type_from_tmdb(raw: Dict[str,Any]) -> str:
    return "tvSeries" if raw.get("_kind") == "tv" else "movie"

def _year_from_tmdb(raw: Dict[str,Any]) -> int:
    date = raw.get("first_air_date") if raw.get("_kind")=="tv" else raw.get("release_date")
    if not date: return 0
    try:
        return int(date[:4])
    except Exception:
        return 0

def _title_from_tmdb(raw: Dict[str,Any]) -> str:
    return (raw.get("name") if raw.get("_kind")=="tv" else raw.get("title")) or ""

def _providers_on_subs(all_providers: List[str], subs: set) -> bool:
    return any(p in subs for p in all_providers)

def main():
    csv_path = os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv")
    ratings = load_imdb_ratings_csv(csv_path)
    print(f"[bold]IMDb ingest (CSV):[/bold] {csv_path} — {len(ratings)} rows")
    update_seen_from_ratings(ratings)

    weights = {"critic_weight": 0.54, "audience_weight": 0.46}

    region = os.environ.get("REGION","US").strip() or "US"
    pages_movie = env_int("TMDB_PAGES_MOVIE", 12)
    pages_tv    = env_int("TMDB_PAGES_TV", 12)

    # TWEAK: default to English originals if not provided
    langs = env_list("ORIGINAL_LANGS") or ["en"]

    raw_items, diag = fetch_catalog(region, pages_movie, pages_tv, langs)

    subs = set([s.strip() for s in (os.environ.get("SUBS_INCLUDE","") or "").split(",") if s.strip()])
    out: List[Dict[str,Any]] = []
    for r in raw_items:
        title = _title_from_tmdb(r)
        year = _year_from_tmdb(r)
        kind = _type_from_tmdb(r)
        tmdb_id = int(r.get("id") or 0)
        providers = fetch_providers("tv" if kind=="tvSeries" else "movie", tmdb_id, region)
        on_subs = _providers_on_subs(providers, subs) if subs else True
        item = {
            "title": title, "year": year, "type": kind,
            "tmdb_id": tmdb_id, "providers": providers,
            "providers_on_subs": on_subs
        }
        out.append(item)

    out = enrich_with_omdb(out)

    counts = {"raw": len(raw_items), "after_transform": len(out)}
    eligible: List[Dict[str,Any]] = []
    for it in out:
        if is_seen(it["title"], it.get("imdb_id",""), int(it.get("year") or 0)):
            continue
        eligible.append(it)
    counts["eligible"] = len(eligible)

    for it in eligible:
        it["match"] = round(_score(it, weights), 1)
    eligible.sort(key=lambda x: x["match"], reverse=True)

    shortlist = eligible[:50]
    shown = shortlist[:10]

    today = datetime.date.today().isoformat()
    out_dir = f"data/out/daily/{today}"
    os.makedirs(out_dir, exist_ok=True)

    json.dump({
        "date": today,
        "weights": weights,
        "diag": diag,
        "counts": counts,
        "shortlist": shortlist,
        "shown": shown
    }, open(f"{out_dir}/recs.json","w"), indent=2)

    json.dump({
        "pool": counts["after_transform"],
        "eligible": counts["eligible"],
        "after_skip": len(shortlist),
        "shown": len(shown)
    }, open(f"{out_dir}/telemetry.json","w"), indent=2)

    print("[green]Run complete.[/green] See:", out_dir)

if __name__ == "__main__":
    main()