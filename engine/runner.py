# engine/runner.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple
from rich import print as rprint

# Your existing modules
from .ratings_ingest import load_user_ratings_combined
from .catalog_builder import build_catalog
from .seen_index import load_seen_index, filter_unseen
from .taste import build_taste
from .weights import load_weights, save_weights, update_from_ratings
from .rank import rank_candidates
from .feed import build_and_write_feed

# Optional provider allow-list (matches your helpers / slugs from catalog_builder)
def _subs_from_env() -> List[str]:
    raw = os.environ.get("SUBS_INCLUDE", "").strip()
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x).strip().lower().replace(" ", "_") for x in raw]
    # CSV → slugs
    return [s.strip().lower().replace(" ", "_") for s in raw.split(",") if s.strip()]

def _keep_allowed_providers(items: List[Dict], allowed_slugs: List[str]) -> List[Dict]:
    if not allowed_slugs:
        return items
    allow = set(allowed_slugs)
    out = []
    for it in items:
        provs = [p.strip().lower() for p in (it.get("providers") or [])]
        if not provs:
            continue
        if allow.intersection(provs):
            out.append(it)
    return out

def main():
    rprint("[hb] | catalog:begin")

    # 1) Load user ratings DNA (CSV + incremental HTML if IMDB_USER_ID/URL set)
    user_rows, ingest_meta = load_user_ratings_combined()
    rprint(f"[green]IMDb ratings loaded[/green] → rows={len(user_rows)} meta={ingest_meta}")

    # 2) Adaptive weights (audience > critic by design)
    w = update_from_ratings(user_rows)  # gently nudges based on likes/dislikes
    # Make sure audience > critic if someone edited weights file manually
    if w.get("audience_weight", 0.5) <= w.get("critic_weight", 0.5):
        aw = max(0.55, float(w.get("audience_weight", 0.65)))
        cw = min(0.40, float(w.get("critic_weight", 0.30)))
        nw = float(w.get("novelty_weight", 0.05))
        s = aw + cw + nw
        w["audience_weight"], w["critic_weight"], w["novelty_weight"] = aw/s, cw/s, nw/s
        save_weights(w)
    rprint(f"[cyan]weights[/cyan] → {w}")

    # 3) Build catalog (TMDB discover → detail → OMDb enrich; English-only in your builder)
    catalog = build_catalog()
    rprint(f"[green]catalog built[/green] → {len(catalog)} items")

    # 4) Filter: providers allow-list + seen
    allowed = _subs_from_env()
    if allowed:
        catalog = _keep_allowed_providers(catalog, allowed)
        rprint(f"[blue]provider-filter[/blue] keep={allowed} → {len(catalog)} items")
    # Seen index via CSV + public page
    seen_idx = load_seen_index(os.environ.get("IMDB_RATINGS_CSV_PATH", "data/ratings.csv"))
    catalog = filter_unseen(catalog, seen_idx)
    rprint(f"[blue]unseen-only[/blue] → {len(catalog)} items")

    # 5) Taste profile (per-genre)
    taste = build_taste(user_rows)
    rprint(f"[magenta]taste profile[/magenta] → {len(taste)} genres")

    # 6) Rank
    ranked = rank_candidates(catalog, w, taste_profile=taste, top_k=600)

    # 7) Feed
    feed, dated_path = build_and_write_feed(ranked)
    rprint(f"[hb] | catalog:end pool={len(catalog)} feed={len(feed)} → {dated_path}")

if __name__ == "__main__":
    main()