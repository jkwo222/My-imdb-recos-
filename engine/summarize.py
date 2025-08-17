# engine/summarize.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Any, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _top_genre_rows(weights: Dict[str, float], top_n: int = 12) -> List[Tuple[str, float]]:
    if not weights:
        return []
    rows = sorted(weights.items(), key=lambda kv: kv[1], reverse=True)
    return rows[:top_n]


def write_summary_md(env: Dict[str, str], *, genre_weights_path: Path) -> None:
    ranked = _load_json(OUT_DIR / "assistant_ranked.json", {"items": []})
    feed = _load_json(OUT_DIR / "assistant_feed.json", {"items": [], "telemetry": {}})
    meta = _load_json(OUT_DIR / "run_meta.json", {})
    weights = _load_json(genre_weights_path, {})

    items: List[Dict[str, Any]] = ranked.get("items", [])
    tel = feed.get("telemetry", {})

    region = meta.get("region") or env.get("REGION") or "US"
    orig_langs = (meta.get("original_langs") or env.get("ORIGINAL_LANGS") or "").strip() or "—"
    subs = meta.get("subs_include") or (env.get("SUBS_INCLUDE") or "").split(",")
    subs_disp = ", ".join(s for s in subs if s) or "—"

    # Telemetry / counts
    cand = int(meta.get("candidates_after_filtering") or tel.get("kept_after_subs") or len(feed.get("items", [])))
    sources = meta.get("sources") or tel.get("sources") or {}
    excluded = int(meta.get("excluded_rated_or_list") or tel.get("excluded_rated_or_list") or 0)
    discover_pages = int(meta.get("discover_pages") or tel.get("discover_pages") or 0)
    profile_loaded = bool(meta.get("user_profile_loaded") or tel.get("user_profile_loaded") or False)

    # Top genre weights table
    top_rows = _top_genre_rows(weights, 12)

    # Build “Today’s top picks”
    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {os.getenv('GITHUB_RUN_DATE','') or ''}".strip())
    lines.append("")
    lines.append(f"*Region*: **{region}**  •  *Original langs*: **{orig_langs}**")
    lines.append(f"*Subscriptions filtered*: **{subs_disp}**")
    lines.append(f"*Candidates after filtering*: **{cand}**")
    lines.append("")
    lines.append("## Your taste snapshot")
    lines.append("")
    if top_rows:
        lines.append("| Genre | Weight |")
        lines.append("|---|---:|")
        for g, w in top_rows:
            lines.append(f"| {g} | {w:.2f} |")
    else:
        lines.append("_No genre evidence yet (ratings not loaded or too sparse)._")
    lines.append("")

    lines.append("## Telemetry")
    lines.append("")
    lines.append(f"- Profile loaded: **{profile_loaded}**")
    lines.append(f"- Excluded due to your lists/ratings: **{excluded}**")
    if sources:
        s_imdb = int(sources.get("imdb_tsv") or 0)
        s_disc = int(sources.get("tmdb_discover") or 0)
        s_pers = int(sources.get("persistent") or 0)
        lines.append(f"- Source mix (merged before filters): IMDb TSV **{s_imdb}**, TMDB Discover **{s_disc}**, Persistent pool **{s_pers}**")
    if discover_pages:
        lines.append(f"- TMDB Discover pages queried: **{discover_pages}**")
    cut = env.get("MIN_MATCH_CUT") or "—"
    lines.append(f"- Score cut threshold: **{cut}**")
    lines.append("")

    lines.append("## Today’s top picks")
    lines.append("")
    for idx, it in enumerate(items[:50], 1):
        title = it.get("title") or "Untitled"
        year = it.get("year") or "—"
        mtype = it.get("type") or "movie"
        score = it.get("match_score") or 0
        imdb = it.get("imdb_rating")
        provs = it.get("providers") or []
        prov_txt = ", ".join(provs) if provs else "—"
        why = it.get("why") or ""
        lines.append(f"{idx}. **{title}** ({year}) — {mtype}")
        lines.append(f"   *score {score:.1f}  •  IMDb {imdb if imdb is not None else '—'}  •  {prov_txt}*")
        if why:
            lines.append(f"   > {why}")
        lines.append("")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")