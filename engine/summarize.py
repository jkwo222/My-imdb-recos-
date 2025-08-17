# engine/summarize.py
from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
OUT_DIR = BASE / "data" / "out" / "latest"

def write_summary_md(env: Dict[str,str], genre_weights: Dict[str,float], picks: List[Dict[str,Any]], kept_count: int, candidate_total: int) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)              # FIX: ensure directory
    path = OUT_DIR / "summary.md"

    region = env.get("REGION","")
    langs = env.get("ORIGINAL_LANGS","")
    subs = env.get("SUBS_INCLUDE","")
    run_date = env.get("RUN_DATE","")

    lines = []
    lines.append(f"# Daily Recommendations — {run_date}".strip())
    lines.append("")
    lines.append(f"*Region*: **{region}**  •  *Original langs*: **{langs}**")
    lines.append(f"*Subscriptions filtered*: **{subs}**")
    lines.append(f"*Candidates after filtering*: **{kept_count}**")
    lines.append("")

    if genre_weights:
        lines.append("## Your taste snapshot")
        lines.append("")
        lines.append("Based on your ratings & public-list signals, these tags carry the most weight in your personalized ranking:")
        lines.append("")
        lines.append("| Tag | Weight |")
        lines.append("|---|---:|")
        top = sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)[:8]
        for k,v in top:
            lines.append(f"| {k} | {v:.2f} |")
        lines.append("")

    lines.append("## Today’s top picks")
    lines.append("")
    for i, it in enumerate(picks[:15], 1):
        provs = ", ".join((it.get("providers") or [])[:8])
        imdb = it.get("imdb_rating","?")
        title = it.get("title") or it.get("primaryTitle") or "?"
        year = it.get("year") or it.get("startYear") or "?"
        ttype = it.get("type") or it.get("titleType") or ""
        tmdb_avg = it.get("tmdb_vote_avg","?")
        score = f"{it.get('score', ''):.1f}" if isinstance(it.get("score"), (int,float)) else it.get("score","")

        lines.append(f"{i}. **{title}** ({year}) — {ttype}")
        lines.append(f"   *score {score}  •  IMDb {imdb}  •  {provs}*")
        lines.append(f"   > IMDb {imdb}; TMDB {tmdb_avg}; {year}")
        lines.append("")

    lines.append("---")
    lines.append(f"_Generated from {candidate_total} candidate titles._")

    path.write_text("\n".join(lines), encoding="utf-8")