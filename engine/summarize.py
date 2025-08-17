# engine/summarize.py
from __future__ import annotations

import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _fmt_score(x: Optional[float]) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.0f}"
    except Exception:
        return "—"

def _table(rows: List[List[str]]) -> str:
    if not rows:
        return ""
    # Very small markdown helper
    header = rows[0]
    out = ["|" + "|".join(header) + "|", "|" + "|".join("---" for _ in header) + "|"]
    for r in rows[1:]:
        out.append("|" + "|".join(r) + "|")
    return "\n".join(out)

def _top_n(items: List[Dict[str, Any]], n: int = 15) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda x: (x.get("match_score") or 0), reverse=True)[:n]

def write_summary_md(
    env: Dict[str, str],
    *,
    items_ranked: List[Dict[str, Any]],
    genre_weights: Optional[Dict[str, float]],
    director_weights: Optional[Dict[str, float]],
    telemetry: Dict[str, Any],
) -> None:
    region = telemetry.get("region") or env.get("REGION") or "US"
    langs = telemetry.get("original_langs") or env.get("ORIGINAL_LANGS") or "en"
    subs = telemetry.get("subs_include") or [s.strip() for s in (env.get("SUBS_INCLUDE") or "").split(",") if s.strip()]

    # Read affinity keys (genre/director names only) from telemetry; weights are used implicitly in scoring
    gnames = telemetry.get("affinity", {}).get("genres_learned") or []
    dnames = telemetry.get("affinity", {}).get("directors_learned") or []

    # Telemetry details block
    tele_lines = [
        f"*Region*: **{region}**  •  *Original langs*: **{langs}**",
        f"*Subscriptions filtered*: **{', '.join(subs)}**",
        f"*Profile size*: **{telemetry.get('profile_size', 0)}**",
        f"*Inputs*: **{telemetry.get('total_input', 0)}**  •  *Excluded (seen/rated)*: **{telemetry.get('excluded_already_seen', 0)}**",
        f"*Scored*: **{telemetry.get('scored_total', 0)}**  •  *Score cut*: **{_fmt_score(telemetry.get('score_cut'))}**  •  *Kept*: **{telemetry.get('kept', 0)}**",
    ]

    # Taste snapshot tables (names only; weights are baked into match_score)
    genre_rows = [["Genre", "Seen-basis"]]
    if gnames:
        for g in sorted(gnames)[:16]:
            genre_rows.append([g, "✓"])
    else:
        genre_rows.append(["—", "—"])

    director_rows = [["Director", "Seen-basis"]]
    if dnames:
        for d in sorted(dnames)[:10]:
            director_rows.append([d, "✓"])
    else:
        director_rows.append(["—", "—"])

    # Top picks rows
    picks = _top_n(items_ranked, 15)
    pick_lines: List[str] = []
    idx = 1
    for it in picks:
        t = it.get("title") or "(unknown)"
        year = it.get("year") or "—"
        mtype = it.get("type") or it.get("tmdb_media_type") or "title"
        imdb = it.get("imdb_rating")
        provs = it.get("providers") or []
        score = _fmt_score(it.get("match_score"))
        why = it.get("why") or ""
        prov_txt = ", ".join(sorted(set(provs))) if provs else "—"
        pick_lines.append(
            f"{idx}. **{t}** ({year}) — {mtype}\n"
            f"   *score {score}*  •  IMDb {imdb if imdb is not None else '—'}  •  {prov_txt}\n"
            f"   > {why}"
        )
        idx += 1

    md = []
    md.append(f"# Daily Recommendations — {datetime.utcnow().date().isoformat()}")
    md.append("")
    md.extend(tele_lines)
    md.append("")
    md.append("## Your taste snapshot")
    md.append("")
    md.append(_table(genre_rows))
    md.append("")
    md.append("### Director affinities")
    md.append(_table(director_rows))
    md.append("")
    md.append("## Today’s top picks")
    md.append("")
    if pick_lines:
        md.extend(pick_lines)
    else:
        md.append("_None above the score cut today._")
    md.append("")
    md.append(f"---\n_Generated from {telemetry.get('scored_total', 0)} candidate titles (after filtering & exclusions)._")

    (OUT_DIR / "summary.md").write_text("\n".join(md), encoding="utf-8")