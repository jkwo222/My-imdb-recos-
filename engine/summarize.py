from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime, timezone
import json

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "out" / "latest"
OUT.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = OUT / "summary.md"  # <- write to a file, not the directory
STATE_DIR = ROOT / "data" / "cache" / "state"

def _safe_read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _tab(rows: List[tuple[str, str]]) -> str:
    if not rows:
        return "_—_"
    lines = ["| Metric | Value |", "|---|---:|"]
    for k, v in rows:
        lines.append(f"| {k} | {v} |")
    return "\n".join(lines)

def _fmt_score(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "—"

def write_summary_md(env: Dict[str, str], *, genre_weights: Dict[str, float] | None = None) -> None:
    ranked = _safe_read_json(OUT / "assistant_ranked.json", {"items": [], "telemetry": {}})
    meta = _safe_read_json(OUT / "run_meta.json", {})
    state = _safe_read_json(STATE_DIR / "personal_state.json", {})

    items: List[Dict[str, Any]] = ranked.get("items", []) or []
    kept = ranked.get("telemetry", {}).get("kept", len(items))

    # Use run date if provided; else UTC today.
    run_date = env.get("RUN_DATE")
    if not run_date:
        run_date = datetime.now(timezone.utc).date().isoformat()

    # Telemetry rows
    tel_rows = [
        ("Region", str(meta.get("region", ""))),
        ("Original langs", str(meta.get("original_langs", ""))),
        ("Subs filtered", ", ".join(meta.get("subs_include", [])) or "—"),
        ("Discover calls", str(meta.get("discover", {}).get("discover_calls", 0))),
        ("Discover results (movies)", str(meta.get("discover", {}).get("discover_movies", 0))),
        ("Discover results (tv)", str(meta.get("discover", {}).get("discover_tv", 0))),
        ("Excluded by IMDb id", str(meta.get("exclusions", {}).get("excluded_imdb", 0))),
        ("Excluded by title+year", str(meta.get("exclusions", {}).get("excluded_titleyear", 0))),
        ("Profile titles", str(meta.get("profile_size", 0))),
        ("Non-zero genre weights", str(meta.get("genre_weights_nonzero", 0))),
        ("Candidates after filtering", str(meta.get("candidates_after_filtering", 0))),
        ("Score cut", str(ranked.get("telemetry", {}).get("score_cut", ""))),
        ("Shortlist size", str(kept)),
    ]

    # Genre weights table (top 12)
    gw = (genre_weights or state.get("genre_weights") or {})
    gw_sorted = sorted(gw.items(), key=lambda x: (-x[1], x[0]))[:12]
    gw_lines = ["| Genre | Weight |", "|---|---:|"] if gw_sorted else ["_—_"]
    for g, w in gw_sorted:
        try:
            gw_lines.append(f"| {g} | {float(w):.2f} |")
        except Exception:
            gw_lines.append(f"| {g} | {w} |")

    # Render top picks
    N = min(30, len(items))
    head = items[:N]

    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {run_date}")
    lines.append("")
    lines.append(f"*Region*: **{meta.get('region','')}**  •  *Original langs*: **{meta.get('original_langs','')}**")
    subs = ", ".join(meta.get("subs_include", [])) or "—"
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
            kind = (it.get("type") or it.get("tmdb_media_type") or "").replace("tvSeries", "TV")
            score = _fmt_score(it.get("match_score"))
            imdb = it.get("imdb_rating")
            tmdb = it.get("tmdb_vote")
            prov = ", ".join(it.get("providers") or [])

            ratings_bits = []
            if imdb:
                ratings_bits.append(f"IMDb {imdb}")
            if tmdb:
                try:
                    ratings_bits.append(f"TMDB {float(tmdb):.1f}")
                except Exception:
                    ratings_bits.append(f"TMDB {tmdb}")
            ratings_str = "  •  ".join(ratings_bits) if ratings_bits else "—"

            lines.append(f"{i}. **{title}** ({year}) — {kind or '—'}")
            # one compact line with score + ratings + providers
            extras = [f"score {score}"]
            if ratings_str != "—":
                extras.append(ratings_str)
            if prov:
                extras.append(prov)
            lines.append("   " + "  •  ".join(extras))

    # Write once to the correct file
    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")