from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List
import json

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
    except Exception:
        return default

def _as_percent(x: float) -> str:
    return f"{round(x * 100)}%"

def _render_genre_weights(weights: Dict[str, float]) -> str:
    if not weights:
        return "_(no genre data found — add more rated items to improve profile)_"
    # top 12
    top = sorted(weights.items(), key=lambda kv: kv[1], reverse=True)[:12]
    lines = ["| Genre | Weight |", "|---|---|"]
    for g, w in top:
        lines.append(f"| {g} | {_as_percent(w)} |")
    return "\n".join(lines)

def write_summary_md(*, env: Dict[str, str], shortlist: List[Dict[str, Any]], telemetry: Dict[str, Any]) -> None:
    lines: List[str] = []

    region = (env.get("REGION") or "US").upper()
    langs = env.get("ORIGINAL_LANGS") or "—"
    subs = env.get("SUBS_INCLUDE") or "—"

    # Catalog stats from builder (if present)
    cat_stats = _read_json(OUT_DIR / "catalog_stats.json", {})
    using_imdb = cat_stats.get("using_imdb", False)
    imdb_items = cat_stats.get("imdb_items", 0)
    tmdb_items = cat_stats.get("tmdb_items", 0)
    combined_unique = cat_stats.get("combined_unique", 0)
    after_provider = cat_stats.get("after_provider_filter", 0)

    # Header
    lines.append(f"# Daily Recommendations — {region}")
    lines.append("")
    lines.append(f"- Region: **{region}**")
    lines.append(f"- Original langs: **{langs}**")
    lines.append(f"- Subscriptions filtered: **{subs}**")
    lines.append(f"- Score cut: **{telemetry.get('min_cut')}**")
    lines.append("")

    # Telemetry block
    lines.append("## Telemetry")
    lines.append("")
    lines.append(f"- Discovered this run: **{tmdb_items}** (TMDB) + **{imdb_items}** (IMDb TSV) → **{combined_unique}** unique")
    lines.append(f"- After provider filter: **{after_provider}**")
    lines.append(f"- Excluded as already seen (tconst): **{telemetry.get('excluded_seen_tconst',0)}**")
    lines.append(f"- Excluded as already seen (title+year): **{telemetry.get('excluded_seen_titleyear',0)}**")
    lines.append(f"- After exclusions: **{telemetry.get('after_exclusions',0)}**")
    lines.append(f"- Shortlist (≥ cut): **{telemetry.get('shortlist',0)}**")
    lines.append("")

    # Profile snapshot
    lines.append("## Your taste snapshot")
    lines.append("")
    lines.append(_render_genre_weights(telemetry.get("genre_weights") or {}))
    lines.append("")

    # Shortlist
    lines.append("## Today’s top picks")
    lines.append("")
    if not shortlist:
        lines.append("_No picks today. Consider lowering `MIN_MATCH_CUT`, adding more ratings to strengthen your profile, or broadening providers._")
    else:
        for i, it in enumerate(sorted(shortlist, key=lambda x: x.get("score", 0), reverse=True), 1):
            title = it.get("title") or "Untitled"
            yr = f" ({it.get('year')})" if it.get("year") else ""
            score = it.get("score")
            provs = ", ".join(it.get("providers") or [])
            genres = ", ".join(it.get("genres") or [])
            ir = it.get("imdb_rating", "—")
            src = it.get("source", "—")
            lines.append(f"{i}. **{title}{yr}** — score **{score}** · IMDb **{ir}**")
            if genres:
                lines.append(f"   - Genres: {genres}")
            if provs:
                lines.append(f"   - Where: {provs}")
            lines.append(f"   - Source: {src}")
            lines.append("")

    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")