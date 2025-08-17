from __future__ import annotations
import json, pathlib, os, time
from .catalog_builder import build_catalog, ensure_imdb_cache
from .summarize import write_summary_md

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _env_list(name: str) -> list[str]:
    v = os.environ.get(name, "")
    return [s.strip() for s in v.split(",") if s.strip()]

def main():
    print(" | catalog:begin")
    ensure_imdb_cache()

    t0 = time.time()
    items = build_catalog()
    total = len(items)
    # provider filter
    keep_names = _env_list("SUBS_INCLUDE")
    if keep_names:
        kept_items = []
        for it in items:
            provs = [p.lower().replace(" ", "_") for p in (it.get("providers") or [])]
            if any(k.replace(" ", "_") in provs for k in keep_names):
                kept_items.append(it)
        items = kept_items
    kept = len(items)

    # scoring (very simple placeholder if you don’t already have one elsewhere)
    # match_score: combine imdb_rating + tmdb_vote + a tiny recency proxy via year
    for it in items:
        imdb = float(it.get("imdb_rating") or 0.0)
        tmdb = float(it.get("tmdb_vote") or 0.0)
        year = int(it.get("year") or 0)
        rec = max(0.0, min(1.0, (year - 1990) / 40.0))  # 1990..2030 -> 0..1-ish
        it["match_score"] = round(10 * (0.55 * (imdb / 10) + 0.35 * (tmdb / 10) + 0.10 * rec), 2)
        it["why"] = f"blend of your taste & ratings; year boost {year or 'n/a'}"

    # cut by MIN_MATCH_CUT if set
    min_cut = os.environ.get("MIN_MATCH_CUT")
    if min_cut:
        try:
            cut = float(min_cut)
            items = [i for i in items if (i.get("match_score") or 0.0) >= cut]
        except Exception:
            pass

    scored_cut = len(items)

    telemetry = {
        "total": total,
        "kept": kept,
        "scored_cut": scored_cut,
        "elapsed_sec": round(time.time() - t0, 2),
        "region": os.environ.get("REGION"),
        "original_langs": os.environ.get("ORIGINAL_LANGS"),
        "subs_include": _env_list("SUBS_INCLUDE"),
        "min_match_cut": os.environ.get("MIN_MATCH_CUT"),
    }

    # write JSON (used by summary + artifact)
    out_json = OUT_DIR / "assistant_feed.json"
    out_json.write_text(json.dumps({"items": items, "telemetry": telemetry}, indent=2), encoding="utf-8")
    print(f" | catalog:end kept={kept} scored_cut={scored_cut} → {out_json}")

    # write Markdown summary for the workflow & GH issue
    write_summary_md(os.environ)

if __name__ == "__main__":
    main()