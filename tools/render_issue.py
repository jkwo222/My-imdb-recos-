# tools/render_issue.py
import json, datetime
from pathlib import Path

today = datetime.date.today().isoformat()
out_dir = Path(f"data/out/daily/{today}")
recs_path = out_dir / "recs.json"
telemetry_path = out_dir / "telemetry.json"
feed_path = out_dir / "assistant_feed.json"

recs = json.load(open(recs_path)) if recs_path.exists() else {"recs": [], "weights": {}}
telemetry = json.load(open(telemetry_path)) if telemetry_path.exists() else {}
feed = json.load(open(feed_path)) if feed_path.exists() else {
    "date": today, "weights": recs.get("weights", {}), "telemetry": telemetry, "top": []
}

top = recs.get("recs", [])[:10]
weights = recs.get("weights", {})

lines = []
lines.append("Top 10")
for i, r in enumerate(top, 1):
    title = r.get("title", "")
    year = r.get("year", "")
    typ = r.get("type", "")
    match = r.get("match", 0.0)
    lines.append(f"{i} {match:.1f} — {title} ({year}) [{typ}]")

pool = telemetry.get("pool") or telemetry.get("considered") or 0
eligible = telemetry.get("eligible_unseen") or telemetry.get("after_skip") or 0
shown = telemetry.get("shown") or min(10, len(top))
lines.append(f"Telemetry: pool={pool}, eligible={eligible}, after_skip={telemetry.get('after_skip', eligible)}, shown={shown}")

if weights:
    lines.append(f"Weights: critic={weights.get('critic_weight',0):.2f}, audience={weights.get('audience_weight',0):.2f}")

counts = telemetry.get("counts") or {}
if counts:
    pretty = ", ".join(f"{k}={v}" for k, v in counts.items())
    lines.append(f"Counts: {pretty}")

providers = telemetry.get("providers") or {}
if providers:
    pb = ", ".join(f"{k}={v}" for k, v in providers.items())
    lines.append(f"Providers: {pb}")

# Page-plan meta (from catalog.get_last_plan_meta via telemetry.notes.page_plan)
notes = telemetry.get("notes") or {}
plan = notes.get("page_plan") if isinstance(notes, dict) else {}
if plan:
    lines.append(
        "Page plan: "
        f"movie_pages={plan.get('movie_pages')} "
        f"tv_pages={plan.get('tv_pages')} "
        f"rotate_minutes={plan.get('rotate_minutes')} "
        f"slot={plan.get('slot')} "
        f"step_movie={plan.get('step_movie')} "
        f"step_tv={plan.get('step_tv')} "
        f"hashes(m,t)={plan.get('movie_hash')},{plan.get('tv_hash')} "
        f"first5(m,t)={plan.get('movie_first5')},{plan.get('tv_first5')}"
    )

lines.append("This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.\n")

lines.append("<details><summary>assistant_feed.json (copy & paste into chat)</summary>\n")
lines.append("```json")
lines.append(json.dumps(feed, ensure_ascii=False, indent=2))
lines.append("```")
lines.append("\n</details>\n")

print("\n".join(lines))