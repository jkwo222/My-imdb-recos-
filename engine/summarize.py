# engine/summarize.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, List

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
FEED_JSON = OUT_DIR / "assistant_feed.json"
SUMMARY_MD = OUT_DIR / "summary.md"

def _fmt_float(x: Any, nd: int = 1) -> str:
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "—"

def _pick_top(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    # keep only non-hidden scored items
    cand = [it for it in items if isinstance(it.get("score"), (int, float)) and it.get("score", 0) >= 0]
    cand.sort(key=lambda it: (it.get("score", 0), it.get("imdb_rating", 0)), reverse=True)
    return cand[:max(0, int(limit))]

def _render_taste_table(genre_weights: Dict[str, float]) -> str:
    if not genre_weights:
        return "_(No personalized genre signals available yet.)_\n"
    # show top 8 by weight
    top = sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)[:8]
    lines = ["| Genre | Weight |", "|---|---:|"]
    for g, w in top:
        lines.append(f"| {g} | {_fmt_float(w, 2)} |")
    return "\n".join(lines) + "\n"

def _render_pick_row(i: int, it: Dict[str, Any]) -> str:
    title = it.get("title") or "Untitled"
    year = it.get("year") or "—"
    kind = it.get("titleType") or it.get("type") or "title"
    imdb = _fmt_float(it.get("imdb_rating"), 1)
    tmdb = _fmt_float(it.get("tmdb_rating"), 1)
    providers_str = ", ".join(it.get("providers", [])[:10]) if it.get("providers") else "—"
    tconst = it.get("tconst") or ""
    extra = []
    if it.get("penalties"):
        p = it["penalties"]
        # show the parts that are non-zero
        bits = []
        if p.get("title", 0) > 0:
            bits.append(f"title −{_fmt_float(p['title'], 0)}")
        if p.get("genre", 0) > 0:
            bits.append(f"genre −{_fmt_float(p['genre'], 0)}")
        if bits:
            extra.append(f"penalties: {', '.join(bits)}")
    line1 = f"{i}. **{title}** ({year}) — {kind}"
    line2 = f"   *score —  •  IMDb {imdb}  •  {providers_str}*"
    line3 = f"   > IMDb {imdb}; TMDB {tmdb}; {year}"
    if extra:
        line3 += f"  •  _{'; '.join(extra)}_"
    return "\n".join([line1, line2, line3])

def write_summary_md(env: Dict[str, str], genre_weights: Dict[str, float] | None = None, picks_limit: int = 15) -> None:
    """
    Renders a daily summary Markdown file at data/out/latest/summary.md
    using data/out/latest/assistant_feed.json.
    """
    genre_weights = genre_weights or {}

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not FEED_JSON.exists():
        SUMMARY_MD.write_text("_No feed produced for this run._\n", encoding="utf-8")
        return

    try:
        items = json.loads(FEED_JSON.read_text(encoding="utf-8"))
    except Exception:
        items = []

    region = (env.get("REGION") or "—").strip()
    langs = (env.get("ORIGINAL_LANGS") or "—").strip()
    subs = (env.get("SUBS_INCLUDE") or "—").strip()

    kept = sum(1 for it in items if it.get("score", 0) >= 0)
    top = _pick_top(items, picks_limit)

    parts: List[str] = []
    parts.append(f"# Daily Recommendations — {env.get('GITHUB_RUN_DATE','') or ''}".strip())
    parts.append("")
    parts.append(f"*Region*: **{region}**  •  *Original langs*: **{langs}**")
    parts.append(f"*Subscriptions filtered*: **{subs}**")
    parts.append(f"*Candidates after filtering*: **{kept}**")
    parts.append("")
    parts.append("## Your taste snapshot")
    parts.append("")
    parts.append("Based on your IMDb ratings and watch history, these genres carry the most weight in your personalized ranking:")
    parts.append("")
    parts.append(_render_taste_table(genre_weights))
    parts.append("## Today’s top picks")
    parts.append("")

    if not top:
        parts.append("_No eligible picks today after filters._")
    else:
        for i, it in enumerate(top, 1):
            parts.append(_render_pick_row(i, it))

    parts.append("")
    parts.append("---")
    parts.append(f"_Generated from {kept} candidate titles._")
    parts.append("")
    parts.append("**Downvote / hide syntax (reply in this thread):**  ")
    parts.append("`👎 tt1234567` · `downvote The Matrix (1999)` · `skip genre: Western` · `hide: The Godfather (1972)`")
    parts.append("")

    SUMMARY_MD.write_text("\n".join(parts) + "\n", encoding="utf-8")