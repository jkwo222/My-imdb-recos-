from __future__ import annotations
import json, pathlib, os
from typing import List, Dict

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _fmt_services(svcs: List[str]) -> str:
    return ", ".join(svcs) if svcs else "—"

def _reason(it: Dict) -> str:
    bits = []
    if it.get("match_score") is not None:
        bits.append(f"high match score ({round(float(it['match_score']), 1)})")
    if it.get("imdb_rating"):
        bits.append(f"strong IMDb ({it['imdb_rating']:.1f})")
    if it.get("tmdb_vote"):
        bits.append(f"good TMDB ({it['tmdb_vote']:.1f})")
    # add anything your scorer put on the item
    if it.get("why"):
        bits.append(str(it["why"]))
    return "; ".join(bits) or "fits your taste profile"

def _row(it: Dict) -> str:
    title = it.get("title", "Untitled")
    year = it.get("year") or ""
    t = f"{title} ({year})" if year else title
    score = it.get("match_score")
    score_s = f"{round(float(score),1)}" if score is not None else "—"
    services = _fmt_services(it.get("providers") or [])
    imdb = it.get("imdb_rating")
    imdb_s = f"{imdb:.1f}" if isinstance(imdb, (int, float)) and imdb > 0 else "—"
    tmdb = it.get("tmdb_vote")
    tmdb_s = f"{tmdb:.1f}" if isinstance(tmdb, (int, float)) and tmdb > 0 else "—"
    why = _reason(it)
    return f"| {t} | {score_s} | {services} | {imdb_s} | {tmdb_s} | {why} |"

def _top(items: List[Dict], typ: str, n: int = 10) -> List[Dict]:
    # robust sort by match_score desc, then imdb/tmdb as tiebreakers
    def key(it):
        return (
            float(it.get("match_score") or 0.0),
            float(it.get("imdb_rating") or 0.0),
            float(it.get("tmdb_vote") or 0.0),
        )
    return [it for it in sorted((i for i in items if (i.get("type") or "").startswith(typ)),
                                key=key, reverse=True)][:n]

def write_summary_md(env: Dict[str, str] | None = None) -> pathlib.Path:
    env = env or os.environ
    out_json = OUT_DIR / "assistant_feed.json"
    out_md = OUT_DIR / "summary.md"

    if not out_json.exists():
        out_md.write_text("_No results produced._\n", encoding="utf-8")
        return out_md

    data = json.loads(out_json.read_text(encoding="utf-8"))

    items: List[Dict] = data.get("items") or data if isinstance(data, list) else []
    telemetry: Dict = data.get("telemetry") or {}

    movies = _top(items, "movie", 10)
    series = _top(items, "tv", 10) + _top(items, "tvSeries", 10)  # be lenient with types
    series = series[:10]

    subs = (env.get("SUBS_INCLUDE") or "").split(",")
    subs = [s.strip() for s in subs if s.strip()]
    min_cut = env.get("MIN_MATCH_CUT")

    lines = []
    lines.append("# Daily recommendations\n")
    if telemetry:
        kept = telemetry.get("kept") or len(items)
        total = telemetry.get("total") or kept
        pages_m = env.get("TMDB_PAGES_MOVIE", "12")
        pages_t = env.get("TMDB_PAGES_TV", "12")
        lines.append(f"_Catalog: kept **{kept}** of {total}. TMDB pages: movies={pages_m}, tv={pages_t}. Min cut={min_cut or '—'}._\n")
    if subs:
        lines.append(f"_Services considered:_ {', '.join(subs)}\n")

    def section(title: str, rows: List[Dict]):
        lines.append(f"\n## {title}\n")
        if not rows:
            lines.append("_None for today._\n")
            return
        lines.append("| Title | Match | Where | IMDb | TMDB | Why |\n")
        lines.append("|---|---:|---|---:|---:|---|\n")
        for it in rows:
            lines.append(_row(it) + "\n")

    section("Top 10 Movies (best matches)", movies)
    section("Top 10 Series (best matches)", series)

    # Basic telemetry dump at the bottom for debugging/historical record
    if telemetry:
        lines.append("\n<details>\n<summary>Telemetry</summary>\n\n")
        lines.append("```json\n" + json.dumps(telemetry, indent=2) + "\n```\n")
        lines.append("</details>\n")

    content = "".join(lines).rstrip() + "\n"
    out_md.write_text(content, encoding="utf-8")
    return out_md