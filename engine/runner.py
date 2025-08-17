from __future__ import annotations
from typing import List, Dict, Any
import json, math
from pathlib import Path
from collections import defaultdict

from .catalog_builder import build_catalog
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile, compute_genre_weights

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "out" / "latest"
OUT.mkdir(parents=True, exist_ok=True)

def _load_user_profile(env: Dict[str,str]) -> Dict[str, Dict]:
    local = load_ratings_csv()
    remote = fetch_user_ratings_web((env.get("IMDB_USER_ID") or "").strip())
    merged = merge_user_sources(local, remote)
    return to_user_profile(merged)

def _to_list(x):
    if not x: return []
    if isinstance(x, (list, tuple)): return [i for i in x if i]
    return [x]

def _rank_items(items: List[Dict[str,Any]], env: Dict[str,str]) -> Dict[str,Any]:
    profile = _load_user_profile(env)
    genre_weights = compute_genre_weights(profile)

    min_cut = float(env.get("MIN_MATCH_CUT") or 58.0)

    ranked = []
    for it in items:
        # base: take the best rating available
        imdb = it.get("imdb_rating")
        tmdb = it.get("tmdb_vote")
        base10 = None
        try:
            if imdb is not None: base10 = float(imdb)
        except Exception: pass
        try:
            if tmdb is not None: base10 = max(float(tmdb), base10 or 0.0)
        except Exception: pass
        if base10 is None: base10 = 6.0  # default neutral
        base100 = base10 * 10.0

        g = _to_list(it.get("genres"))
        if g and genre_weights:
            fit = sum(genre_weights.get(x, 0.5) for x in g) / len(g)
            adj = (fit - 0.5) * 30.0
        else:
            adj = 0.0

        score = max(0.0, min(100.0, base100 + adj))
        it2 = dict(it)
        it2["match_score"] = round(score, 2)
        ranked.append(it2)

    # cut + sort
    kept = [r for r in ranked if r["match_score"] >= min_cut]
    kept.sort(key=lambda x: (-x["match_score"], x.get("year") or 0, x.get("title") or ""))

    out = {"items": kept, "telemetry": {
        "total": len(items),
        "kept": len(kept),
        "score_cut": min_cut,
    }}
    (OUT / "assistant_ranked.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

def main():
    import os
    env = dict(os.environ)
    print(" | catalog:begin")
    items = build_catalog(env)
    print(f" | catalog:end kept={len(items)}")

    ranked = _rank_items(items, env)

    # hand a compact status to summarize
    debug = {
        "items_in": len(items),
        "ranked_kept": ranked["telemetry"]["kept"],
        "score_cut": ranked["telemetry"]["score_cut"],
    }
    (OUT / "debug_status.json").write_text(json.dumps(debug, indent=2), encoding="utf-8")

    # write summary
    from .summarize import write_summary_md
    write_summary_md(env, genre_weights=compute_genre_weights(_load_user_profile(env)))

if __name__ == "__main__":
    main()