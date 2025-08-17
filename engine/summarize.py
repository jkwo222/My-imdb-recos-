# engine/summarize.py
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
STATE_DIR = ROOT / "data" / "cache" / "state"

def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _fmt_score(it: Dict[str,Any]) -> str:
    if it.get("match_score") is not None:
        return f"{float(it['match_score']):.0f}"
    if it.get("score") is not None:
        return f"{float(it['score']):.0f}"
    return "–"

def _fmt_providers(provs: List[str] | None) -> str:
    if not provs:
        return "—"
    return ", ".join(sorted(set(provs)))

def _fmt_ratings(it: Dict[str,Any]) -> str:
    parts = []
    if it.get("imdb_rating") is not None:
        try: parts.append(f"IMDb {float(it['imdb_rating']):.1f}")
        except: pass
    if it.get("tmdb_vote") is not None:
        try: parts.append(f"TMDB {float(it['tmdb_vote']):.1f}")
        except: pass
    return "  •  ".join(parts) if parts else ""

def _top_genre_weights(genre_weights: Dict[str,float], k: int = 8) -> List[tuple[str,float]]:
    if not genre_weights:
        return []
    return sorted(genre_weights.items(), key=lambda x: x[1], reverse=True)[:k]

def write_summary_md(
    env: Dict[str,str],
    items: List[Dict[str,Any]],
    genre_weights: Dict[str,float],
    telemetry: Dict[str,Any],
) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    today = _dt.date.today().isoformat()
    region = telemetry.get("region") or env.get("REGION") or "US"
    orig = telemetry.get("original_langs") or env.get("ORIGINAL_LANGS") or "—"
    subs = telemetry.get("subs_include") or (env.get("SUBS_INCLUDE") or "").split(",")
    subs = [s.strip() for s in subs if str(s).strip()]
    subs_str = ", ".join(subs) if subs else "—"

    kept = len(items)
    # for shortlist, apply a soft cut purely for display (don’t change files)
    min_cut = float(env.get("MIN_MATCH_CUT") or telemetry.get("min_match_cut") or 0)
    shortlist = [x for x in items if float(x.get("match_score") or x.get("score") or 0) >= min_cut]
    # limit to top 40 for the email/issue body
    shortlist = sorted(shortlist, key=lambda x: float(x.get("match_score") or x.get("score") or 0), reverse=True)[:40]

    meta_lines = []
    if telemetry:
        meta_lines.append(f"*Discover fetched*: **{telemetry.get('discovered_total', 0)}**")
        meta_lines.append(f"*Excluded by subs*: **{telemetry.get('excluded_by_providers', 0)}**")
        meta_lines.append(f"*Excluded by your lists*: **{telemetry.get('excluded_by_user_ratings_or_imdb_list', 0)}**")
        meta_lines.append(f"*Pool growth*: **+{telemetry.get('pool_new_this_run', 0)}** (now **{telemetry.get('pool_size_after', 0)}**)")
        meta_lines.append(f"*Candidates kept*: **{telemetry.get('kept_after_filter', kept)}**")
        meta_lines.append(f"*Shortlist ≥ {min_cut:.0f}*: **{len(shortlist)}**")

    # genre weights
    gw = _top_genre_weights(genre_weights, 8)

    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {today}\n")
    lines.append(f"*Region*: **{region}**  •  *Original langs*: **{orig}**")
    lines.append(f"*Subscriptions filtered*: **{subs_str}**")
    lines.append(f"*Candidates after filtering*: **{telemetry.get('kept_after_filter', kept)}**\n")

    lines.append("## Your taste snapshot\n")
    if gw:
        lines.append("| Genre | Weight |")
        lines.append("|---|---:|")
        for g, w in gw:
            lines.append(f"| {g} | {w:.2f} |")
        lines.append("")
    else:
        lines.append("_No genre signal yet (need more overlaps between your ratings and catalog)._ \n")

    if meta_lines:
        lines.append("## Run telemetry\n")
        lines.append("\n".join(f"- {m}" for m in meta_lines))
        lines.append("")

    lines.append("## Today’s top picks\n")
    if not shortlist:
        lines.append("_No items passed the score cut today._\n")
    else:
        for idx, it in enumerate(shortlist, 1):
            title = it.get("title") or "Untitled"
            year = it.get("year") if it.get("year") else "—"
            mtype = it.get("type") or it.get("tmdb_media_type") or "—"
            score_s = _fmt_score(it)
            provs = _fmt_providers(it.get("providers"))
            rating_bits = _fmt_ratings(it)
            # header line
            lines.append(f"{idx}. **{title}** ({year}) — {mtype}")
            # meta line
            if rating_bits:
                lines.append(f"   *score {score_s}  •  {rating_bits}  •  {provs}*")
            else:
                lines.append(f"   *score {score_s}  •  {provs}*")
            # why
            if it.get("why"):
                lines.append(f"   > {it['why']}")
            lines.append("")

    # footer
    lines.append(f"---\n_Generated from {telemetry.get('kept_after_filter', kept)} candidates._\n")

    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")