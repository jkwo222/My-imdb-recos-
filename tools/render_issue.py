#!/usr/bin/env python3
"""
Renders issue.md from the newest recommendation feed.
Priority:
  1) data/out/latest/assistant_feed.json
  2) newest data/out/daily/YYYY-MM-DD/assistant_feed.json
  3) data/out/assistant_feed.json  (legacy)
"""
from __future__ import annotations
import json, os, glob
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(ROOT, "data", "out")
ISSUE_PATH = os.path.join(ROOT, "issue.md")

def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _find_feed() -> tuple[str, dict] | None:
    # 1) latest
    p = os.path.join(OUT, "latest", "assistant_feed.json")
    if os.path.exists(p):
        return p, _load_json(p)
    # 2) newest daily
    daily_glob = os.path.join(OUT, "daily", "*", "assistant_feed.json")
    candidates = sorted(glob.glob(daily_glob))
    if candidates:
        return candidates[-1], _load_json(candidates[-1])
    # 3) legacy
    p = os.path.join(OUT, "assistant_feed.json")
    if os.path.exists(p):
        return p, _load_json(p)
    return None

def _fmt_type(t: str | None) -> str:
    t = (t or "").lower()
    if t in ("tv", "tvseries", "tvminiseries"): return "tvSeries"
    return "movie" if t == "movie" else (t or "movie")

def main():
    found = _find_feed()
    if not found:
        body = "No results available."
        with open(ISSUE_PATH, "w", encoding="utf-8") as f:
            f.write(body+"\n")
        return

    path, feed = found
    w = feed.get("weights", {}) or {}
    tel = (feed.get("telemetry") or {})
    pp = tel.get("page_plan") or {}

    top10 = feed.get("top10") or []
    lines = []
    lines.append("Top 10")
    for row in top10:
        rank = row.get("rank")
        score = row.get("match")
        title = row.get("title")
        year = row.get("year")
        typ = _fmt_type(row.get("type"))
        lines.append(f"{rank} {score:.1f} — {title} ({year}) [{typ}]")

    # Telemetry summary
    pool = tel.get("pool", 0)
    eligible = tel.get("eligible", 0)
    shown = tel.get("shown", len(top10))
    weights_line = f"Weights: critic={w.get('critic',0):.2f}, audience={w.get('audience',0):.2f}"
    counts_line = f"Counts: tmdb_pool={pool}, eligible_unseen={eligible}, shortlist={tel.get('counts',{}).get('shortlist',0)}, shown={shown}"

    # Page plan summary
    movie_pages = pp.get("movie_pages")
    tv_pages = pp.get("tv_pages")
    rot = pp.get("rotate_minutes")
    slot = pp.get("slot")
    plan_line = f"Page plan: movie_pages={movie_pages} tv_pages={tv_pages} rotate_minutes={rot} slot={slot}"

    providers = ", ".join(pp.get("provider_names", []))
    prov_line = f"Providers: {providers or '(none)'}"

    # Build body
    body = []
    body.extend(lines)
    body.append(weights_line)
    body.append(counts_line)
    body.append(plan_line)
    body.append(f"This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.")
    body.append("")
    body.append("<details><summary>assistant_feed.json (copy & paste into chat)</summary>")
    body.append("")
    body.append("```json")
    body.append(json.dumps(feed, ensure_ascii=False, indent=2))
    body.append("```")
    body.append("")
    body.append("</details>")
    body.append("")
    body.append(f"_Source: {os.path.relpath(path, ROOT)}_")

    with open(ISSUE_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(body) + "\n")

if __name__ == "__main__":
    main()