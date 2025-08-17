# engine/runner.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .catalog_builder import build_catalog
from .personalize import genre_weights_from_profile, apply_personal_score
from .imdb_sync import (
    load_ratings_csv,
    fetch_user_ratings_web,
    merge_user_sources,
    to_user_profile,
)

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _env() -> Dict[str, str]:
    return {k: (v or "") for k, v in os.environ.items()}

def _load_user_profile(env: Dict[str, str]) -> Dict[str, Dict]:
    """
    Combine local CSV ratings + public IMDb list (if IMDB_USER_ID is set) into a uniform profile:
    { tconst: { my_rating: float, rated_at: 'YYYY-MM-DD', ... }, ... }
    """
    local = load_ratings_csv()  # data/user/ratings.csv; empty ok
    remote: List[Dict[str, str]] = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        try:
            remote = fetch_user_ratings_web(uid)
        except Exception:
            remote = []
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

def _taste_snapshot_md(genre_weights: Dict[str, float], top_k: int = 12) -> str:
    if not genre_weights:
        return "_Not enough personal data yet — using popularity + quality._"
    # sort by weight desc
    rows = sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)[:top_k]
    lines = ["| Genre | Weight |", "|---|---:|"]
    for g, w in rows:
        lines.append(f"| {g} | {w:.2f} |")
    return "\n".join(lines)

def _providers_str(providers: List[str]) -> str:
    return ", ".join(sorted(set(providers))) if providers else "—"

def _score_cut(items: List[Dict[str, Any]], env: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Apply optional MIN_MATCH_CUT (0-100). If not set, return as-is.
    """
    try:
        cut = float(env.get("MIN_MATCH_CUT", "").strip() or "0")
    except Exception:
        cut = 0.0
    if cut <= 0:
        return items
    return [it for it in items if float(it.get("score", 0.0)) >= cut]

def _summarize_md(
    items: List[Dict[str, Any]],
    env: Dict[str, str],
    genre_weights: Dict[str, float],
) -> str:
    region = env.get("REGION") or "US"
    orig = env.get("ORIGINAL_LANGS") or "—"
    subs = env.get("SUBS_INCLUDE") or "—"
    md: List[str] = []
    md.append(f"# Daily Recommendations — {os.environ.get('GITHUB_RUN_DATE','') or ''}".strip() or "# Daily Recommendations")
    md.append("")
    md.append(f"*Region*: **{region}**  •  *Original langs*: **{orig}**")
    md.append(f"*Subscriptions filtered*: **{subs}**")
    md.append(f"*Candidates after filtering*: **{len(items)}**")
    md.append("")
    md.append("## Your taste snapshot")
    md.append("")
    md.append("Based on your IMDb ratings and watch history, these genres carry the most weight in your personalized ranking:")
    md.append("")
    md.append(_taste_snapshot_md(genre_weights))
    md.append("")
    md.append("## Today’s top picks")
    md.append("")

    topN = min(15, len(items))
    for i, it in enumerate(items[:topN], start=1):
        title = it.get("title") or "(unknown)"
        year = it.get("year") or ""
        kind = it.get("type") or ""
        imdb = it.get("imdb_rating") or "—"
        tmdb = it.get("tmdb_rating") or "—"
        prov = _providers_str(it.get("providers") or [])
        score = it.get("score")
        score_disp = f"{score:.1f}" if isinstance(score, (int, float)) else "—"
        md.append(f"{i}. **{title}** ({year}) — {kind}")
        md.append(f"   *score {score_disp}  •  IMDb {imdb}  •  {prov}*")
        bits = []
        if imdb and imdb != "—":
            bits.append(f"IMDb {imdb}")
        if tmdb and tmdb != "—":
            bits.append(f"TMDB {tmdb}")
        if year:
            bits.append(str(year))
        md.append(f"   > " + "; ".join(bits))
        md.append("")

    md.append(f"---\n_Generated from {len(items)} candidate titles._")
    return "\n".join(md)

def main() -> None:
    env = _env()

    # 1) Build base catalog (handles IMDb TSVs present or absent; enriches with TMDB; filters by providers)
    items = build_catalog(env)

    # 2) Load user profile and compute genre weights
    profile = _load_user_profile(env)
    genre_weights = genre_weights_from_profile(items, profile, imdb_id_field="tconst")

    # 3) Apply personalized score and sort
    apply_personal_score(items, genre_weights, base_key="imdb_rating")
    items.sort(key=lambda it: float(it.get("score", 0.0)), reverse=True)
    items = _score_cut(items, env)

    # 4) Persist scored feed and human-friendly summary
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assistant_scored.json").write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_md = _summarize_md(items, env, genre_weights)
    (OUT_DIR / "summary.md").write_text(summary_md, encoding="utf-8")

    # 5) Minimal run meta (for dashboards / GH summaries)
    meta = {
        "region": env.get("REGION") or "US",
        "original_langs": env.get("ORIGINAL_LANGS") or "",
        "subs_include": env.get("SUBS_INCLUDE") or "",
        "candidates_after_filtering": len(items),
        "has_profile": bool(profile),
        "genre_weights_nonzero": bool(genre_weights),
    }
    (OUT_DIR / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()