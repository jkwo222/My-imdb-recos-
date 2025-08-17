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
        bits.append(f"{round(float(it['match_score']))}/100")
    if it.get("why"):
        bits.append(str(it["why"]))
    return " · ".join(bits) if bits else "fits your taste profile"

def _row(it: Dict) -> str:
    title = it.get("title", "Untitled")
    year = it.get("year") or ""
    t = f"{title} ({year})" if year else title
    score = it.get("match_score")
    score_s = f"{round(float(score))}/100" if score is not None else "—"
    services = _fmt_services(it.get("providers") or [])
    imdb = it.get("imdb_rating")
    imdb_s = f"{imdb:.1f}" if isinstance(imdb, (int, float)) and imdb > 0 else "—"
    tmdb = it.get("tmdb_vote")
    tmdb_s = f"{tmdb:.1f}" if isinstance(tmdb, (int, float)) and tmdb > 0 else "—"
    why = _reason(it)
    return f"| {t} | {score_s} | {services} | {imdb_s} | {tmdb_s} | {why} |"

def _top(items: List[Dict], typ: str, n: int = 10) -> List[Dict]:
    def key(it):
        return (
            float(it.get("match_score") or 0.0),
            float(it.get("imdb_rating") or 0.0),
            float(it.get("tmdb_vote") or 0.0),
        )
    return [it for it in sorted((i for i in items if (i.get("type") or "").startswith(typ)),
                                key=key, reverse=True)][:n]

def _taste_section(genre_weights: Dict[str, float]) -> List[str]:
    if not genre_weights:
        return []
    items = sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)
    top = [g for g, _ in items[:6]]
    bottom = [g for g, _ in sorted(items, key=lambda kv: kv[1])[:4]]
    lines = []
    lines.append("\n### Your taste profile (by genres)\n")
    lines.append("_Learned from your IMDb ratings; higher = stronger preference._\n")
    if top:
        lines.append(f"**You lean toward:** {', '.join(top)}\n")
    if bottom:
        lines.append(f"**You usually avoid:** {', '.join(bottom)}\n")
    return lines

def write_summary_md(env: Dict[str, str] | None = None, *, genre_weights: Dict[str, float] | None = None) -> pathlib.Path:
    env = env or os.environ
    out_json = OUT_DIR / "assistant_feed.json"
    out_md = OUT_DIR / "summary.md"

    if not out_json.exists():
        out_md.write_text("_No results produced._\n", encoding="utf-8")
        return out_md

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    items: List[Dict] = payload.get("items") or payload if isinstance(payload, list) else []
    telemetry: Dict = payload.get("telemetry") or {}

    movies = _top(items, "movie", 10)
    series = _top(items, "tv", 10) + _top(items, "tvSeries", 10)
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

    # Taste profile section
    lines.extend(_taste_section(genre_weights or {}))

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

    if telemetry:
        lines.append("\n<details>\n<summary>Telemetry</summary>\n\n")
        lines.append("```json\n" + json.dumps(telemetry, indent=2) + "\n```\n")
        lines.append("</details>\n")

    OUT_DIR.write_bytes(b"")  # ensure dir exists
    content = "".join(lines).rstrip() + "\n"
    out_md.write_text(content, encoding="utf-8")
    return out_md