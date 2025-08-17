from __future__ import annotations
import json, pathlib, os, time

from .catalog_builder import build_catalog, ensure_imdb_cache
from .persona import build_genre_profile, genre_alignment_score
from .summarize import write_summary_md

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _env_list(name: str) -> list[str]:
    v = os.environ.get(name, "")
    return [s.strip() for s in v.split(",") if s.strip()]

def _norm(x, lo, hi):
    if x is None:
        return 0.0
    try:
        x = float(x)
    except Exception:
        return 0.0
    if hi == lo:
        return 0.0
    x = max(lo, min(hi, x))
    return (x - lo) / (hi - lo)

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

    # Build personalization from your IMDb ratings
    genre_weights = build_genre_profile()  # {} if ratings.csv missing

    # Score each item on 0..100
    #   55% quality (IMDb/TMDB)
    #   35% genre alignment to your profile
    #   10% gentle recency bump via year
    for it in items:
        imdb = _norm(it.get("imdb_rating"), 5.0, 9.0)   # clamp; 0..1
        tmdb = _norm(it.get("tmdb_vote"),   5.0, 9.0)   # 0..1
        qual = 0.6 * imdb + 0.4 * tmdb

        year = None
        try:
            year = int(it.get("year") or 0)
        except Exception:
            year = None
        rec = 0.0
        if year:
            # 1995..2030 => 0..1 (soft)
            rec = _norm(year, 1995, 2030)

        genres = it.get("genres") or []
        if isinstance(genres, str):
            genres = [g.strip() for g in genres.replace("|", ",").split(",") if g.strip() and g.strip() != "\\N"]

        align, top_contribs = genre_alignment_score(genres, genre_weights)

        score_0_1 = 0.55 * qual + 0.35 * align + 0.10 * rec
        it["match_score"] = round(100.0 * score_0_1, 2)  # 0..100

        why_bits = []
        if top_contribs:
            # Highlight positive/negative top genres
            pos = [g[:-1] for g in top_contribs if g.endswith("+")]
            neg = [g[:-1] for g in top_contribs if g.endswith("−")]
            if pos:
                why_bits.append(f"hits your top genres: {', '.join(pos)}")
            if neg:
                why_bits.append(f"but watch out for: {', '.join(neg)}")
        if it.get("imdb_rating"):
            why_bits.append(f"IMDb {it['imdb_rating']:.1f}")
        if it.get("tmdb_vote"):
            why_bits.append(f"TMDB {it['tmdb_vote']:.1f}")
        if year:
            why_bits.append(f"{year}")
        it["why"] = "; ".join(why_bits) or "fits your taste profile"

    # cut by MIN_MATCH_CUT (now 0..100 to match your env of 58)
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
        "persona": {
            "genres_learned": sorted(list(genre_weights.keys())),
        },
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_json = OUT_DIR / "assistant_feed.json"
    out_json.write_text(json.dumps({"items": items, "telemetry": telemetry}, indent=2), encoding="utf-8")
    print(f" | catalog:end kept={kept} scored_cut={scored_cut} → {out_json}")

    write_summary_md(os.environ, genre_weights=genre_weights)

if __name__ == "__main__":
    main()