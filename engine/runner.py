# engine/runner.py
from __future__ import annotations
import csv, datetime, json, os, sys, traceback
from pathlib import Path
from typing import Any, Dict, List

from engine.logging_utils import make_heartbeat
from engine.telemetry import Telemetry, provider_histogram

from engine import catalog as cat
from engine import providers as prov
from engine import seen_index as seen

def _get_env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if (v is not None and str(v).strip() != "") else default

def _to_bool(s: str | None, default=False) -> bool:
    if s is None:
        return default
    return str(s).strip().lower() in ("1", "true", "yes", "on")

def _safe(obj: dict, key: str, default=None):
    v = obj.get(key)
    return v if v is not None else default

def _read_ratings_csv(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        for row in r:
            if "imdb_id" not in row and "const" in row:
                row["imdb_id"] = row["const"]
            rows.append(row)
    return rows

def main() -> None:
    today = datetime.date.today().isoformat()
    out_dir = Path(f"data/out/daily/{today}")
    out_dir.mkdir(parents=True, exist_ok=True)

    hb = make_heartbeat(out_dir)
    tel = Telemetry()

    region = _get_env("REGION", "US")
    langs = (_get_env("ORIGINAL_LANGS", "en") or "en").split(",")
    subs_include = (_get_env("SUBS_INCLUDE", "") or "").split(",") if _get_env("SUBS_INCLUDE") else []
    include_tv_seasons = _to_bool(_get_env("INCLUDE_TV_SEASONS", "true"))
    max_catalog = int(_get_env("MAX_CATALOG", "6000") or "6000")

    # SUBSTANTIALLY HIGHER DEFAULTS (still overridable)
    pages_movie = int(_get_env("TMDB_PAGES_MOVIE", "60") or "60")
    pages_tv = int(_get_env("TMDB_PAGES_TV", "60") or "60")

    tel.add_note("language_filter", langs)
    tel.add_note("subs_filter_enforced", bool(subs_include))
    tel.add_note("region", region)

    # IMDb ratings -> seen index
    ratings_path = Path(_get_env("IMDB_RATINGS_CSV_PATH", "data/ratings.csv") or "data/ratings.csv")
    ratings_rows: List[Dict[str, Any]] = []
    if ratings_path.exists():
        ratings_rows = _read_ratings_csv(ratings_path)
        print(f"IMDb ingest (CSV): {ratings_path} — {len(ratings_rows)} rows")
        hb.ping("ratings_ingested", rows=len(ratings_rows))
        try:
            seen.update_seen_from_ratings(ratings_rows)
            hb.ping("seen_index_updated", entries=len(ratings_rows))
        except Exception:
            print("[warn] seen_index update failed:\n" + traceback.format_exc(), file=sys.stderr)
            hb.ping("seen_index_update_error")
    else:
        print(f"[warn] ratings path not found: {ratings_path}")
        hb.ping("ratings_missing", path=str(ratings_path))

    # TMDB pool (uses daily-rotating page plan)
    try:
        pool = cat.fetch_tmdb_base(
            pages_movie=pages_movie,
            pages_tv=pages_tv,
            region=region,
            langs=langs,
            include_tv_seasons=include_tv_seasons,
            max_items=max_catalog,
        )
    except Exception:
        print("[error] TMDB fetch failed:\n" + traceback.format_exc(), file=sys.stderr)
        hb.ping("tmdb_fetch_error")
        pool = []

    tel.mark("tmdb_pool", len(pool))
    hb.ping("tmdb_base", count=len(pool))
    print(f"TMDB pulled base items: {len(pool)}")

    # Enrichments
    def _call(func_name: str, data: List[dict]) -> List[dict]:
        try:
            func = getattr(cat, func_name)
            out = func(data)
            tel.mark(func_name, len(out) if hasattr(out, "__len__") else None)
            hb.ping(func_name, count=(len(out) if hasattr(out, "__len__") else None))
            return out
        except Exception:
            return data

    pool = _call("enrich_with_ids", pool)
    pool = _call("enrich_with_votes", pool)
    pool = _call("enrich_with_ratings", pool)

    # Availability
    try:
        pool = prov.annotate_availability(pool, region=region)
        tel.mark("after_availability", len(pool))
        hb.ping("availability_annotated", count=len(pool))
    except Exception:
        print("[warn] provider availability failed:\n" + traceback.format_exc(), file=sys.stderr)
        hb.ping("availability_error")

    # Filters
    eligible = pool
    try:
        eligible = cat.filter_seen(eligible)
        eligible = cat.filter_by_langs(eligible, langs=langs)
        if subs_include:
            eligible = cat.filter_by_providers(eligible, allowed=subs_include)
    except Exception:
        print("[warn] filtering pipeline threw — continuing:\n" + traceback.format_exc(), file=sys.stderr)

    tel.mark("eligible_unseen", len(eligible))
    hb.ping("eligible", count=len(eligible))

    # Scoring
    recs: List[Dict[str, Any]] = []
    weights: Dict[str, Any] = {}
    try:
        recs, weights = cat.score_and_rank(eligible)
    except Exception:
        print("[warn] scoring failed — continuing with eligible set:\n" + traceback.format_exc(), file=sys.stderr)
        recs = eligible
        weights = {"critic_weight": 0.5, "audience_weight": 0.5, "commitment_cost_scale": 1.0, "novelty_pressure": 0.15}

    shortlist_n = min(50, len(recs))
    shown_n = min(10, len(recs))
    tel.mark("shortlist", shortlist_n)
    tel.mark("shown", shown_n)
    hb.ping("ranked", shortlist=shortlist_n, shown=shown_n)

    # Provider breakdown over shortlist
    basis = recs[:shortlist_n] if shortlist_n else recs
    prov_hist = provider_histogram(basis, field="providers")
    tel.set_provider_breakdown(prov_hist)

    # Writes
    json.dump(
        {"date": today, "recs": recs, "weights": weights},
        open(out_dir / "recs.json", "w"),
        indent=2
    )

    telemetry_full = {
        "pool": tel.counts.get("tmdb_pool", 0),
        "eligible_unseen": tel.counts.get("eligible_unseen", 0),
        "after_skip": tel.counts.get("eligible_unseen", 0),
        "shortlist": tel.counts.get("shortlist", shortlist_n),
        "shown": tel.counts.get("shown", shown_n),
        **tel.to_dict(),
    }
    json.dump(telemetry_full, open(out_dir / "telemetry.json", "w"), indent=2)

    def _row(r: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "imdb_id": _safe(r, "imdb_id", ""),
            "title": _safe(r, "title", ""),
            "year": _safe(r, "year", 0),
            "type": _safe(r, "type", ""),
            "match": _safe(r, "match", 0.0),
            "imdb_rating": _safe(r, "imdb_rating"),
            "rt_tomato": _safe(r, "rt_tomato"),
            "rt_audience": _safe(r, "rt_audience"),
            "tmdb_vote_average": _safe(r, "tmdb_vote_average"),
            "providers": _safe(r, "providers", []),
            "region": _safe(r, "region"),
            "original_language": _safe(r, "original_language"),
        }

    feed = {
        "date": today,
        "weights": weights,
        "telemetry": telemetry_full,
        "top": [_row(r) for r in recs[:25]],
    }
    json.dump(feed, open(out_dir / "assistant_feed.json", "w"), indent=2)

    print(f"Run complete. See: {out_dir}")

if __name__ == "__main__":
    main()