# engine/runner.py
from __future__ import annotations
import csv, json, os, sys, datetime, math
from typing import Dict, List, Tuple

from engine.config import load_config
from engine.rotation import plan_pages
from engine.tmdb_client import TMDB, collect_discover, hydrate_items
from engine.provider_filter import title_has_allowed_provider, summarize_provider_hits
from engine import seen_index as seen

def _read_imdb_csv(path: str) -> List[dict]:
    rows: List[dict] = []
    if not os.path.exists(path):
        print(f"IMDb ingest (CSV): {path} — file missing, continuing with empty seen index")
        return rows
    with open(path, "r", encoding="utf-8") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = sniffer.sniff(sample) if sample else csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        for r in reader:
            rows.append(r)
    print(f"IMDb ingest (CSV): {path} — {len(rows)} rows")
    return rows

def _heartbeat(stage: str):
    def inner(done: int, total: int):
        print(f"[heartbeat] {stage}: {done}/{total} hydrated")
    return inner

def _score_items(cfg, items: List[dict]) -> List[dict]:
    # Prepare min/max for audience popularity proxy (tmdb_votes) and vote average
    if not items:
        return []
    vcounts = [max(0, it.get("tmdb_votes") or 0) for it in items]
    vmax = max(vcounts) or 1
    vmin = min(vcounts)
    def scale_votes(v):
        # compress to 0..100 with sqrt to soften huge titles
        return 100.0 * math.sqrt(max(0.0, (v - vmin) / max(1.0, (vmax - vmin)))) 

    now_year = datetime.date.today().year
    scored = []
    for it in items:
        critic = float(it.get("tmdb_vote") or 0.0) * 10.0  # 0..100
        audience = scale_votes(int(it.get("tmdb_votes") or 0))
        novelty = 0.0
        try:
            y = int((it.get("year") or "0")[:4])
            if y >= now_year - 1:
                novelty = 3.0 + 4.0 * (1 if y == now_year else 0)  # mild boost
        except Exception:
            novelty = 0.0
        commit_penalty = 0.0
        if it.get("type") == "tvSeries":
            seasons = int(it.get("seasons") or 0)
            # small penalty for very long shows
            if seasons > 6:
                commit_penalty = cfg.commitment_cost_scale * (seasons - 6) * 0.6
        final = (cfg.critic_weight * critic) + (cfg.audience_weight * audience) + (cfg.novelty_pressure * novelty) - commit_penalty
        it2 = dict(it)
        it2["match"] = round(final, 1)
        scored.append(it2)
    scored.sort(key=lambda r: r["match"], reverse=True)
    return scored

def _ensure_dirs(path: str):
    os.makedirs(path, exist_ok=True)

def main():
    cfg = load_config()
    # Seen index
    rows = _read_imdb_csv(cfg.imdb_ratings_csv_path)
    seen_stats = seen.update_seen_from_ratings(rows)

    # TMDB client
    if not cfg.tmdb_api_key:
        print("TMDB_API_KEY missing; aborting TMDB pull.")
        sys.exit(1)
    tmdb = TMDB(cfg.tmdb_api_key, cache_dir=cfg.cache_dir)

    # Rotation plan (pages change every rotation window)
    movie_pages = plan_pages(cfg.tmdb_pages_movie, cfg.tmdb_rotate_step_movie, cfg.tmdb_rotate_minutes, cfg.tmdb_page_cap)
    tv_pages = plan_pages(cfg.tmdb_pages_tv, cfg.tmdb_rotate_step_tv, cfg.tmdb_rotate_minutes, cfg.tmdb_page_cap)

    # Discover base pools
    base_movie = collect_discover(tmdb, "movie", movie_pages, cfg.tmdb_movie_sort, cfg.original_langs)
    base_tv = collect_discover(tmdb, "tv", tv_pages, cfg.tmdb_tv_sort, cfg.original_langs)
    base_pool = base_movie + base_tv
    # Cut to max_catalog (pre-hydration cap)
    if len(base_pool) > cfg.max_catalog:
        base_pool = base_pool[:cfg.max_catalog]

    print(f"TMDB pulled base items: {len(base_pool)}  (movies pages={len(movie_pages)} tv pages={len(tv_pages)})")

    # Hydrate
    hyd = hydrate_items(
        tmdb,
        base_pool,
        limit=min(cfg.max_id_hydration, len(base_pool)),
        heartbeat_every=cfg.heartbeat_every,
        heartbeat_fn=_heartbeat("hydrate")
    )

    # Provider filter (only your services)
    allowed = set(cfg.subs_include)
    pass_prov: List[dict] = []
    provider_hit_buckets: List[List[str]] = []
    for it in hyd:
        ok, hits = title_has_allowed_provider(it.get("providers") or {}, list(allowed), cfg.region)
        if ok:
            provider_hit_buckets.append(hits)
            pass_prov.append(it)

    # Unseen filter (based on IMDb IDs when available)
    unseen: List[dict] = []
    for it in pass_prov:
        imdb_id = it.get("imdb_id")
        if imdb_id and seen.is_seen_imdb(imdb_id):
            continue
        unseen.append(it)

    # Score & rank
    ranked = _score_items(cfg, unseen)
    topN = ranked[:10]

    # Telemetry & breakdown
    provider_breakdown = summarize_provider_hits(provider_hit_buckets)
    telemetry = {
        "pool": len(base_pool),
        "eligible_unseen": len(unseen),
        "after_skip": len(unseen),  # skip window logic would reduce; not applied here
        "shown": len(topN),
        "notes": {
            "language_filter": cfg.original_langs,
            "subs_filter_enforced": True,
            "movie_pages": movie_pages,
            "tv_pages": tv_pages
        }
    }
    weights = {
        "critic_weight": cfg.critic_weight,
        "audience_weight": cfg.audience_weight,
        "commitment_cost_scale": cfg.commitment_cost_scale,
        "novelty_pressure": cfg.novelty_pressure,
    }

    # Console summary
    print("—"*60)
    print(f"Provider breakdown (kept): {json.dumps(provider_breakdown, indent=2)}")
    print(f"Telemetry: pool={telemetry['pool']}, eligible={telemetry['eligible_unseen']}, shown={telemetry['shown']}")
    print(f"Weights: critic={cfg.critic_weight:.2f}, audience={cfg.audience_weight:.2f}")
    print("—"*60)

    # Persist outputs
    today = datetime.date.today().isoformat()
    out_dir = os.path.join(cfg.out_dir, today)
    _ensure_dirs(out_dir)

    with open(os.path.join(out_dir, "top10.json"), "w", encoding="utf-8") as f:
        json.dump(topN, f, ensure_ascii=False, indent=2)

    with open(os.path.join(out_dir, "telemetry.json"), "w", encoding="utf-8") as f:
        json.dump({"telemetry": telemetry, "weights": weights, "provider_breakdown": provider_breakdown}, f, indent=2)

    # Emit a compact "email/issue" body to stdout for external tooling to pick up
    def fmt_row(i, r):
        return f"{i:>2} {r['match']:.1f} — {r.get('title')} ({r.get('year')}) [{r.get('type')}]"

    print()
    print("Top 10")
    for i, r in enumerate(topN, 1):
        print(fmt_row(i, r))
    print(f"Telemetry: pool={telemetry['pool']}, eligible={telemetry['eligible_unseen']}, after_skip={telemetry['after_skip']}, shown={telemetry['shown']}")
    print(f"Weights: critic={cfg.critic_weight:.2f}, audience={cfg.audience_weight:.2f}")
    print("This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.")
    print()
    print(f"Run complete. See: {out_dir}")

if __name__ == "__main__":
    main()