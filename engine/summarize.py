# engine/summarize.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _as_items(obj) -> List[Dict]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict) and "items" in obj and isinstance(obj["items"], list):
        return obj["items"]
    return []

def write_summary_md(env: Dict[str, str]) -> None:
    feed = _read_json(OUT_DIR / "assistant_feed.json", {})
    ranked = _read_json(OUT_DIR / "assistant_ranked.json", {})
    debug_status = _read_json(OUT_DIR / "debug_status.json", {})
    meta = _read_json(OUT_DIR / "run_meta.json", {})

    items = _as_items(feed)
    ranked_items = _as_items(ranked)

    tel = meta.get("telemetry", {})
    counts = tel.get("counts", {})
    pool = tel.get("pool", {})
    srcmix = tel.get("final_source_mix", {})

    region = tel.get("region") or env.get("REGION") or "US"
    olangs = tel.get("original_langs") or env.get("ORIGINAL_LANGS") or "en"
    subs = tel.get("subs_include") or (env.get("SUBS_INCLUDE") or "").split(",")

    # Basic numbers
    kept = counts.get("kept_after_filter", len(items))
    shortlist = len(ranked_items) if ranked_items else kept
    min_cut = env.get("MIN_MATCH_CUT") or debug_status.get("cut_score") or "—"

    # Build markdown
    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {env.get('RUN_DATE','') or ''}".strip())
    lines.append("")
    lines.append(f"*Region*: **{region}**  •  *Original langs*: **{olangs}**")
    if subs:
        if isinstance(subs, list):
            subs_str = ", ".join([s for s in subs if s])
        else:
            subs_str = str(subs)
        lines.append(f"*Subscriptions filtered*: **{subs_str}**")
    lines.append(f"*Candidates after filtering*: **{kept}**")
    lines.append("")

    # Source health / telemetry
    lines.append("## Pipeline Telemetry")
    lines.append("")
    lines.append("| Metric | Count |")
    lines.append("|---|---:|")
    lines.append(f"| IMDb TSV titles loaded | {counts.get('imdb_tsv_loaded', 0)} |")
    lines.append(f"| TMDB Discover titles loaded | {counts.get('tmdb_discover_loaded', 0)} |")
    lines.append(f"| Persisted titles loaded | {counts.get('persist_loaded', 0)} |")
    lines.append(f"| Merged (before user filters) | {counts.get('merged_total_before_user_filter', 0)} |")
    lines.append(f"| Excluded: in ratings.csv | {counts.get('excluded_user_ratings_csv', 0)} |")
    lines.append(f"| Excluded: in public IMDb list | {counts.get('excluded_public_imdb_list', 0)} |")
    lines.append(f"| After user filters | {counts.get('after_user_filters', kept)} |")
    lines.append(f"| Excluded: provider filter | {counts.get('excluded_by_provider_filter', 0)} |")
    lines.append(f"| Kept (final candidates) | {kept} |")
    lines.append("")
    lines.append("### Final source mix (among candidates)")
    lines.append("")
    lines.append("| From IMDb TSV | From TMDB Discover | From Persisted Cache |")
    lines.append("|---:|---:|---:|")
    lines.append(f"| {srcmix.get('from_imdb_tsv', 0)} | {srcmix.get('from_tmdb_discover', 0)} | {srcmix.get('from_persist', 0)} |")
    lines.append("")
    lines.append("### Pool growth")
    lines.append("")
    lines.append("| Pool size (after save) | Newly added this run | Reused cached this run |")
    lines.append("|---:|---:|---:|")
    lines.append(f"| {pool.get('pool_size_after_save', 0)} | {pool.get('newly_added_this_run', 0)} | {pool.get('cached_reused_this_run', 0)} |")
    lines.append("")
    lines.append("### Scoring snapshot")
    lines.append("")
    lines.append("| Cut score | Candidates scored | Shortlist size |")
    lines.append("|---:|---:|---:|")
    lines.append(f"| {min_cut} | {debug_status.get('validated_item_count', kept)} | {shortlist} |")
    lines.append("")
    lines.append(f"_Using IMDb TSVs_: **{str(tel.get('using_imdb_tsv', False))}**  •  _Build time_: **{tel.get('timing_sec','—')}s**")
    lines.append("")

    # Write summary.md
    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    import os
    write_summary_md(os.environ)