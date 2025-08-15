# tools/render_issue.py
import json, os, pathlib, datetime, sys

ROOT = pathlib.Path("data/out/daily")

def _latest_run_dir():
    today = datetime.date.today().isoformat()
    cand = ROOT / today
    if cand.exists():
        return cand
    # fallback: newest by name (ISO dates sort naturally)
    if not ROOT.exists():
        print("No results directory found.", file=sys.stderr); sys.exit(1)
    dirs = [p for p in ROOT.iterdir() if p.is_dir()]
    if not dirs:
        print("No dated result folders found.", file=sys.stderr); sys.exit(1)
    return sorted(dirs)[-1]

def _load_json(p):
    if not p.exists():
        return {}
    return json.load(open(p, "r", encoding="utf-8"))

def _fmt_rt(x):
    try:
        return f"{int(round(float(x)))}%"
    except Exception:
        return "—"

def _fmt_imdb(x):
    try:
        return f"{float(x):.1f}"
    except Exception:
        return "—"

def _group(recs):
    movies, series = [], []
    for r in recs:
        t = (r.get("type") or "").lower()
        (series if "tv" in t and "series" in t else movies).append(r)
    return movies, series

def main():
    out_dir = _latest_run_dir()
    af = out_dir / "assistant_feed.json"
    recs_json = out_dir / "recs.json"
    tel = out_dir / "telemetry.json"

    data = _load_json(af)
    if data.get("top"):
        top = data["top"]
        weights = data.get("weights", {})
        tele = data.get("telemetry", {})
    else:
        # fallback if assistant_feed.json missing
        top = _load_json(recs_json).get("recs", [])
        weights = _load_json(recs_json).get("weights", {})
        tele = _load_json(tel)

    # Top 10 overall, then grouped sections
    top10 = top[:10]
    movies, series = _group(top10)

    wcrit = weights.get("critic_weight", 0.5)
    waud  = weights.get("audience_weight", 0.5)

    lines = []
    lines.append("Top 10\n")

    def render_block(title, items):
        if not items: return
        lines.append(f"## {title}\n")
        for i, r in enumerate(items, 1):
            name = r.get("title","")
            year = r.get("year","")
            match = r.get("match","")
            rt = _fmt_rt(r.get("critic_rt", ""))  # already % in assistant_feed
            imdb = _fmt_imdb(r.get("audience_imdb",""))
            provs = ", ".join(r.get("providers") or []) or "—"
            lines.append(f"{i}. **{match}** — {name} ({year}) • RT {rt} | IMDb {imdb} • Providers: {provs}")
        lines.append("")  # blank line

    render_block("Movies", movies)
    render_block("Series", series)

    # Telemetry + weights
    if tele:
        pool = tele.get("pool") or tele.get("eligible_after_subs") or 0
        eligible = tele.get("eligible_after_subs", 0)
        after_skip = tele.get("after_skip_window", 0)
        shown = tele.get("shown", len(top10))
        lines.append(f"**Telemetry:** pool={pool}, eligible={eligible}, after_skip={after_skip}, shown={shown}")
    lines.append(f"**Weights:** critic={wcrit:.2f}, audience={waud:.2f}")
    lines.append("\n_This product uses the TMDB API but is not endorsed or certified by TMDB._")

    # Print markdown to stdout (workflow redirects to issue.md)
    print("\n".join(lines))

if __name__ == "__main__":
    main()