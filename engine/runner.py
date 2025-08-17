# engine/runner.py
from __future__ import annotations
import os, json, time, pathlib, datetime as dt
from typing import Any, Dict, List
from rich import print as rprint

# --- imports from your existing codebase ---
from .ratings_ingest import load_user_ratings_combined
from .weights import load_weights, update_from_ratings
from .catalog_builder import build_catalog
from .seen_index import load_seen_index, filter_unseen
from .provider_filter import summarize_provider_hits  # keep for telemetry
# -------------------------------------------------

OUT_ROOT = pathlib.Path("data/out")
LATEST_DIR = OUT_ROOT / "latest"

def _mkdirs():
    (OUT_ROOT / "daily").mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    (pathlib.Path("data/cache")).mkdir(parents=True, exist_ok=True)
    (pathlib.Path("data/debug")).mkdir(parents=True, exist_ok=True)

def _now_utc_ts() -> int:
    return int(time.time())

def _today_path() -> pathlib.Path:
    d = dt.datetime.utcnow().strftime("%Y-%m-%d")
    p = OUT_ROOT / "daily" / d
    p.mkdir(parents=True, exist_ok=True)
    return p

def _write_json(path: pathlib.Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _write_text(path: pathlib.Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(text)

def _env_list(name: str) -> List[str]:
    raw = os.environ.get(name, "") or ""
    return [x.strip() for x in raw.split(",") if x.strip()]

def _allowed_slugs() -> List[str]:
    # Accept explicit allow-list; else default set you’ve been using
    slugs = _env_list("SUBS_INCLUDE")
    return slugs if slugs else [
        "netflix","prime_video","hulu","max","disney_plus","apple_tv_plus","peacock","paramount_plus"
    ]

def _min_match_cut() -> float:
    try:
        return float(os.environ.get("MIN_MATCH_CUT", "58.0"))
    except Exception:
        return 58.0

def _score_item(it: Dict[str,Any], w: Dict[str,Any]) -> float:
    """
    Score without OMDb. Use TMDB vote as both audience+critic proxy.
    Keep the same 60..98-ish scale feel you had earlier.
    """
    vote = float(it.get("tmdb_vote") or 0.0) / 10.0  # 0..1
    critic_w = float(w.get("critic_weight", 0.35))
    aud_w    = float(w.get("audience_weight", 0.65))
    base = 60.0 + 20.0 * (critic_w*vote + aud_w*vote)

    # Light ‘commitment’ nudge for multi-season series
    if it.get("type") == "tvSeries":
        seasons = int(it.get("seasons") or 1)
        cscale = float(w.get("commitment_cost_scale", 1.0))
        if seasons >= 3:
            base -= 9.0 * cscale
        elif seasons == 2:
            base -= 4.0 * cscale

    return round(max(50.0, min(98.0, base)), 1)

def _split_movies_series(items: List[Dict[str,Any]]) -> (List[Dict[str,Any]], List[Dict[str,Any]]):
    movies = [x for x in items if x.get("type") == "movie"]
    series = [x for x in items if x.get("type") in ("tvSeries","tvMiniSeries")]
    return movies, series

def _summarize_email(top_movies: List[Dict[str,Any]], top_series: List[Dict[str,Any]], telem: Dict[str,Any]) -> str:
    def line(it: Dict[str,Any]) -> str:
        prov = ", ".join(it.get("providers") or [])
        aud  = it.get("audience", 0.0)
        cri  = it.get("critic", 0.0)
        why = []
        if it.get("match") >= 80:   why.append("high fit")
        if "netflix" in (it.get("providers") or []):  why.append("on Netflix")
        if "prime_video" in (it.get("providers") or []): why.append("on Prime")
        if "hulu" in (it.get("providers") or []):     why.append("on Hulu")
        why_s = " — " + ", ".join(why) if why else ""
        return f"- {it.get('title')} ({it.get('year')})  |  match {it.get('match'):.1f}  |  subs: {prov}  |  IMDb proxy {aud:.0f}  |  TMDB proxy {cri:.0f}{why_s}"

    lines = []
    lines.append("**Top 10 Movies**")
    lines += [line(x) for x in top_movies[:10]]
    lines.append("")
    lines.append("**Top 10 Series**")
    lines += [line(x) for x in top_series[:10]]
    lines.append("")
    lines.append("**Telemetry**")
    lines.append(json.dumps(telem, indent=2))
    return "\n".join(lines)

def main():
    _mkdirs()

    rprint(" | catalog:begin")

    # 1) Ratings (CSV + optional public IMDb HTML)
    rows, meta = load_user_ratings_combined()
    rprint(f"IMDb ratings loaded → rows={len(rows)} meta={meta}")

    # 2) Weights (nudge from ratings)
    #    This keeps your adaptive weighting behavior intact.
    weights = update_from_ratings(rows)
    # Add novelty knob (kept from your logs).
    if "novelty_weight" not in weights:
        weights["novelty_weight"] = 0.15
    rprint(f"weights → {weights}")

    # 3) Build catalog (TMDB + providers; no OMDb needed)
    pool = build_catalog()  # already attaches providers + tmdb_vote, may leave critic/audience 0.0
    rprint(f"catalog built → {len(pool)} items")

    # Provider allow-list (simple filter)
    allowed = set(_allowed_slugs())
    kept = []
    hits_for_summary: List[List[str]] = []
    for it in pool:
        provs = [p for p in (it.get("providers") or []) if p in allowed]
        if provs:
            it["providers"] = provs
            kept.append(it)
            hits_for_summary.append(provs)
    rprint(f"provider-filter keep={sorted(allowed)} → {len(kept)} items")

    # 4) Seen index (IDs + robust title/year), then filter
    seen_idx = load_seen_index(os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv"))
    filtered = filter_unseen(kept, seen_idx)
    rprint(f"unseen-only → {len(filtered)} items")

    # 5) Score (no OMDb): use TMDB vote as proxy
    for it in filtered:
        # expose proxies in 0..100 for email readability
        vote = float(it.get("tmdb_vote") or 0.0)
        it["audience"] = round(min(100.0, max(0.0, vote * 10.0)), 1)  # just a readability proxy
        it["critic"]   = round(min(100.0, max(0.0, vote * 10.0)), 1)
        it["match"]    = _score_item(it, weights)

    # 6) Novelty gate & cut
    cut = _min_match_cut()
    feed = [x for x in filtered if x.get("match", 0.0) >= cut]
    feed.sort(key=lambda x: x.get("match", 0.0), reverse=True)

    # Split for email
    movies, series = _split_movies_series(feed)
    # 7) Telemetry for summary
    telem = {
        "pool_sizes": {
            "initial": len(pool),
            "providers": len(kept),
            "unseen": len(filtered),
            "final": len(feed),
        },
        "weights": {
            "audience_weight": round(float(weights.get("audience_weight", 0.65)), 2),
            "critic_weight": round(float(weights.get("critic_weight", 0.35)), 2),
            "commitment_cost_scale": float(weights.get("commitment_cost_scale", 1.0)),
            "novelty_weight": float(weights.get("novelty_weight", 0.15)),
            "min_match_cut": cut,
        },
        "subs": sorted(allowed),
        "provider_hits": summarize_provider_hits(hits_for_summary),
    }

    # 8) Persist all exports (ALWAYS write, even if empty)
    generated_at = _now_utc_ts()
    payload = {
        "generated_at": generated_at,
        "count": len(feed),
        "items": feed,
        "meta": telem,
    }

    # dated + latest
    day_dir = _today_path()
    _write_json(day_dir / "assistant_feed.json", payload)
    _write_json(LATEST_DIR / "assistant_feed.json", payload)

    # separate top10 exports for email convenience
    _write_json(LATEST_DIR / "top10_movies.json", movies[:10])
    _write_json(LATEST_DIR / "top10_series.json", series[:10])

    # plain-text summary (for GitHub notification body)
    summary = _summarize_email(movies, series, telem)
    _write_text(LATEST_DIR / "summary.txt", summary)

    rprint(f" | catalog:end pool={len(pool)} feed={len(feed)} → {day_dir / 'assistant_feed.json'}")
    rprint(f"[out] latest → {LATEST_DIR}")

if __name__ == "__main__":
    # IMPORTANT: never hard-fail on missing OMDb; we don’t use it anymore.
    try:
        main()
    except Exception as e:
        # Make failures visible in Actions logs, but still leave breadcrumbs.
        rprint(f"[red]runner failed: {e!r}[/red]")
        _mkdirs()
        err = {"generated_at": _now_utc_ts(), "error": repr(e)}
        _write_json(LATEST_DIR / "error.json", err)
        raise