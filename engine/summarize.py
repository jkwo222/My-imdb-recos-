from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "out" / "latest"
STATE = ROOT / "data" / "cache" / "state"

def _safe_read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _tab(rows: List[tuple[str,str]]) -> str:
    if not rows: return "_—_"
    lines = ["| Metric | Value |", "|---|---:|"]
    for k,v in rows:
        lines.append(f"| {k} | {v} |")
    return "\n".join(lines)

def write_summary_md(env: Dict[str,str], *, genre_weights: Dict[str,float] | None = None) -> None:
    ranked = _safe_read_json(OUT / "assistant_ranked.json", {"items":[],"telemetry":{}})
    meta = _safe_read_json(OUT / "run_meta.json", {})
    state = _safe_read_json(STATE / "personal_state.json", {})
    items = ranked.get("items", [])
    kept = ranked.get("telemetry", {}).get("kept", len(items))

    # Top N for email
    N = min(30, len(items))
    head = items[:N]

    # Telemetry block (source health)
    tel_rows = [
        ("Region", meta.get("region","")),
        ("Original langs", meta.get("original_langs","")),
        ("Subs filtered", ", ".join(meta.get("subs_include",[])) or "—"),
        ("Discover calls", meta.get("discover",{}).get("discover_calls",0)),
        ("Discover pages (movies)", meta.get("discover",{}).get("discover_movies",0)),
        ("Discover pages (tv)", meta.get("discover",{}).get("discover_tv",0)),
        ("Excluded by IMDb id", meta.get("exclusions",{}).get("excluded_imdb",0)),
        ("Excluded by title+year", meta.get("exclusions",{}).get("excluded_titleyear",0)),
        ("Profile titles", meta.get("profile_size",0)),
        ("Non-zero genre weights", meta.get("genre_weights_nonzero",0)),
        ("Candidates after filtering", meta.get("candidates_after_filtering",0)),
        ("Score cut", ranked.get("telemetry",{}).get("score_cut","")),
        ("Shortlist size", kept),
    ]

    # Genre weights table (top 12)
    gw = (genre_weights or state.get("genre_weights") or {})
    gw_sorted = sorted(gw.items(), key=lambda x: (-x[1], x[0]))[:12]
    gw_lines = ["| Genre | Weight |", "|---|---:|"] if gw_sorted else ["_—_"]
    for g, w in gw_sorted:
        gw_lines.append(f"| {g} | {w:.2f} |")

    # Render picks
    lines = []
    lines.append(f"# Daily Recommendations — {env.get('RUN_DATE') or ''}".strip(" —"))
    lines.append("")
    lines.append(f"*Region*: **{meta.get('region','')}**  •  *Original langs*: **{meta.get('original_langs','')}**")
    subs = ", ".join(meta.get("subs_include",[])) or "—"
    lines.append(f"*Subscriptions filtered*: **{subs}**")
    lines.append(f"*Candidates after filtering*: **{meta.get('candidates_after_filtering', 0)}**")
    lines.append("")
    lines.append("## System telemetry")
    lines.append(_tab(tel_rows))
    lines.append("")
    lines.append("## Your taste snapshot")
    lines.append("\n".join(gw_lines))
    lines.append("")
    lines.append("## Today’s top picks")
    if not head:
        lines.append("_None met the score threshold today._")
    else:
        for i, it in enumerate(head, start=1):
            title = it.get("title") or "Untitled"
            year = it.get("year") or ""
            kind = it.get("type") or it.get("tmdb_media_type") or ""
            score = it.get("match_score")
            imdb = it.get("imdb_rating")
            tmdb = it.get("tmdb_vote")
            prov = ", ".join(it.get("providers") or [])
            # short “why”
            why_bits = []
            if imdb: why_bits.append(f"IMDb {imdb}")
            if tmdb: why_bits.append(f"TMDB {tmdb}")
            if year: why_bits.append(str(year))
            why = "; ".join(why_bits)
            lines.append(f"{i}. **{title}** ({year}) — {kind}")
            lines.append(f"   *score {score:.2f}  •  " +
                         (f"IMDb {imdb}" if imdb else "") +
                         (f"  •  TMDB {tmdb}" if tmdb else "") +
                         (f"  •  {prov}" if prov else ""))
            lines.append(f"   > {why}")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    (OUT / "summary.md").write_text("\n".join(lines), encoding="utf-8")