# engine/summarize.py
from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"

def _fmt_pct(x: float) -> str:
    return f"{x*100:.1f}%"

def _top_genres(gw: Dict[str, float], topn: int = 8) -> List[str]:
    if not gw:
        return []
    # Normalize weights to sum = 1 for display
    s = sum(gw.values()) or 1.0
    norm = {k: v / s for k, v in gw.items()}
    top = sorted(norm.items(), key=lambda x: x[1], reverse=True)[:topn]
    return [f"- {k}: {_fmt_pct(v)}" for k, v in top]

def write_summary_md(env: Dict[str, Any], *, telemetry: Dict[str, Any], genre_weights: Dict[str, float], ranked: List[Dict[str, Any]]) -> None:
    OUT = OUT_DIR / "summary.md"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    lines.append("# Daily recommendations")
    lines.append("")
    lines.append("## Telemetry")
    lines.append("")
    lines.append(f"- Region: **{telemetry.get('region', env.get('REGION','US'))}**")
    lines.append(f"- SUBS_INCLUDE: `{telemetry.get('subs_include', env.get('SUBS_INCLUDE',''))}`")
    lines.append(f"- Discover pages: **{telemetry.get('discover_pages', env.get('DISCOVER_PAGES', 3))}**")
    lines.append(f"- Discovered (raw): **{telemetry.get('discover_total', 0)}**")
    lines.append(f"- Enriched (details fetched): **{telemetry.get('enriched_total', 0)}**; errors: **{telemetry.get('enrich_errors', 0)}**")
    lines.append(f"- Exclusion list size (ratings + IMDb web): **{telemetry.get('exclusions_total', 0)}**")
    lines.append(f"- Excluded for being seen: **{telemetry.get('excluded_from_seen', 0)}**")
    lines.append(f"- Eligible after exclusions: **{telemetry.get('eligible_after_exclusions', 0)}**")
    lines.append(f"- Above match cut (≥ {telemetry.get('min_match_cut', 0)}): **{telemetry.get('eligible_above_cut', 0)}**")
    lines.append("")

    lines.append("## Your profile: genre weights")
    tg = _top_genres(genre_weights, topn=8)
    if tg:
        lines.extend(tg)
    else:
        lines.append("_No genre weights computed (no ratings.csv?)._")
    lines.append("")

    if ranked:
        lines.append("## Top picks")
        lines.append("")
        for i, it in enumerate(ranked, start=1):
            title = it.get("title") or "Untitled"
            year = it.get("year") or ""
            score = it.get("score")
            media = it.get("media_type", "movie")
            genres = ", ".join(it.get("genres") or [])
            src = "IMDb" if it.get("imdb_id") else "TMDB"
            lines.append(f"{i}. **{title}** ({year}) · {media} · {genres} · score **{score}** · src: {src}")
        lines.append("")
    else:
        lines.append("_No items above cut today._")

    OUT.write_text("\n".join(lines), encoding="utf-8")