#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, re
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import date, datetime

def load_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

NON = re.compile(r"[^a-z0-9]+")

def norm(s: str) -> str:
    return NON.sub(" ", (s or "").strip().lower()).strip()

def parse_ymd(s: str | None) -> date | None:
    if not s: return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    if len(s) >= 4 and s[:4].isdigit():
        try: return date(int(s[:4]), 1, 1)
        except Exception: return None
    return None

def days_since(d: date | None) -> int | None:
    if not d: return None
    try: return (date.today() - d).days
    except Exception: return None

def title_year_key(title: str | None, year: Any | None) -> str | None:
    if not title: return None
    try:
        yi = int(str(year)[:4]) if year is not None else None
    except Exception:
        yi = None
    return f"{norm(title)}::{yi}" if yi else None

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Inputs
    diag = load_json(run_dir / "diag.json") or {}
    enriched = load_json(run_dir / "items.enriched.json") or []
    discovered = load_json(run_dir / "items.discovered.json") or []
    feed = load_json(run_dir / "assistant_feed.json") or enriched
    seen_index = load_json(run_dir / "exports" / "seen_index.json") or {}
    user_model = load_json(run_dir / "exports" / "user_model.json") or {}
    seen_tv_roots = load_json(run_dir / "exports" / "seen_tv_roots.json") or []
    pool_files = list(Path("data/cache/pool").glob("*"))

    seen_ids = set([x for x in (seen_index.get("imdb_ids") or []) if isinstance(x, str) and x.startswith("tt")])
    seen_keys = set([x for x in (seen_index.get("title_year_keys") or []) if isinstance(x, str) and "::" in x])

    # TopK for checks
    topK = sorted(feed, key=lambda it: float(it.get("score", it.get("tmdb_vote", 0.0)) or 0.0), reverse=True)[:50]

    # Seen-guard violations
    violations: List[Dict[str, Any]] = []
    for it in topK:
        imdb = it.get("imdb_id")
        title = it.get("title") or it.get("name")
        year = it.get("year")
        key = title_year_key(title, year)
        hit = False
        reason = None
        if isinstance(imdb, str) and imdb in seen_ids:
            hit = True; reason = "imdb_id"
        elif key and key in seen_keys:
            hit = True; reason = "title_year"
        if hit:
            violations.append({
                "title": title, "year": year, "imdb_id": imdb, "reason": reason
            })

    # Provider coverage in top 20
    def provs(it): 
        p = it.get("providers") or it.get("providers_slugs") or []
        return sorted(set(p))
    missing_providers = [ (it.get("title") or it.get("name")) for it in topK[:20] if not provs(it) ]

    # Penalties / signals
    kids_hits = sum(1 for it in topK if " kids " in f" { (it.get('why') or '').lower() } ")
    anime_hits = sum(1 for it in topK if " anime " in f" { (it.get('why') or '').lower() } ")
    commitment_hits = sum(1 for it in topK if " long-run " in f" { (it.get('why') or '').lower() } ")
    people_director = sum(1 for it in topK if " director (" in (it.get("why") or ""))
    people_writer = sum(1 for it in topK if " writer (" in (it.get("why") or ""))
    people_cast = sum(1 for it in topK if " cast (" in (it.get("why") or ""))
    keywords_reason = sum(1 for it in topK if " keywords (" in (it.get("why") or ""))

    # Recency stats (as implemented in scoring.py)
    def recency_tag(it) -> List[str]:
        tags = []
        mt = (it.get("media_type") or "").lower()
        if mt == "movie":
            rd = parse_ymd(it.get("release_date"))
            d = days_since(rd)
            if d is not None and d <= 270:
                tags.append("new_movie")
        elif mt == "tv":
            fad = parse_ymd(it.get("first_air_date"))
            lad = parse_ymd(it.get("last_air_date"))
            if fad:
                d = days_since(fad)
                if d is not None and d <= 180:
                    tags.append("new_series")
            if lad:
                d = days_since(lad)
                if d is not None and d <= 120:
                    tags.append("new_season")
                    root = norm(it.get("title") or it.get("name") or "")
                    if root in set(map(norm, seen_tv_roots)):
                        tags.append("follow_up")
        return tags

    recency_counts: Dict[str, int] = {"new_movie":0, "new_series":0, "new_season":0, "follow_up":0}
    for it in topK:
        for t in recency_tag(it):
            recency_counts[t] += 1

    # Pool snapshot (size, files)
    pool_stats = {
        "files": [p.name for p in pool_files],
        "present": bool(pool_files),
    }

    # Model snapshot
    people = (user_model.get("people") or {})
    model_counts = {
        "count_rows": (user_model.get("meta") or {}).get("count"),
        "global_avg": (user_model.get("meta") or {}).get("global_avg"),
        "directors": len(people.get("director") or {}),
        "writers":   len(people.get("writer") or {}),
        "actors":    len(people.get("actor") or {}),
        "keywords":  len(user_model.get("keywords") or {}),
        "studio":    len(user_model.get("studio") or {}),
        "network":   len(user_model.get("network") or {}),
    }

    # Compose
    metrics = {
        "counts": {
            "discovered": len(discovered),
            "enriched": len(enriched),
            "topK_used_for_checks": len(topK),
        },
        "seen_guard": {
            "violations_in_topK": violations,  # should be []
            "seen_ids_count": len(seen_ids),
            "seen_keys_count": len(seen_keys),
        },
        "providers": {
            "missing_in_top20": missing_providers,
        },
        "signals": {
            "kids_penalties_in_topK": kids_hits,
            "anime_penalties_in_topK": anime_hits,
            "commitment_penalties_in_topK": commitment_hits,
            "people_reasons_in_topK": {
                "director": people_director, "writer": people_writer, "cast": people_cast, "keywords": keywords_reason
            }
        },
        "recency": recency_counts,
        "model": model_counts,
        "env": (diag.get("env") or {}),
    }

    # Write outputs
    (out_dir/"metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir/"diag_extended.json").write_text(json.dumps({
        "metrics": metrics,
        "paths": (diag.get("paths") or {}),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    # Short human-readable report
    lines = []
    lines.append("# Debug snapshot report\n")
    lines.append(f"- discovered={metrics['counts']['discovered']} enriched={metrics['counts']['enriched']}")
    lines.append(f"- seen_guard.violations_in_topK={len(metrics['seen_guard']['violations_in_topK'])}")
    if metrics["seen_guard"]["violations_in_topK"]:
        for v in metrics["seen_guard"]["violations_in_topK"][:10]:
            lines.append(f"  - {v['title']} ({v.get('year')}) reason={v.get('reason')}")
    lines.append(f"- providers.missing_in_top20={len(metrics['providers']['missing_in_top20'])}")
    lines.append(f"- signals: kids={metrics['signals']['kids_penalties_in_topK']}, anime={metrics['signals']['anime_penalties_in_topK']}, long_run={metrics['signals']['commitment_penalties_in_topK']}")
    lines.append(f"- recency: {metrics['recency']}")
    lines.append(f"- model: directors={metrics['model']['directors']} writers={metrics['model']['writers']} actors={metrics['model']['actors']} keywords={metrics['model']['keywords']}")
    (out_dir/"diag_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Quick Top 10 CSV for eyeballing
    import csv
    top10 = topK[:10]
    with (out_dir/"top10.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["rank","media_type","title","year","score","providers","why"])
        for i,it in enumerate(top10, start=1):
            title = it.get("title") or it.get("name")
            prov = ";".join(sorted(set(provs(it))))
            w.writerow([i, it.get("media_type"), title, it.get("year"), it.get("score"), prov, (it.get("why") or "")[:240]])
def provs(it): 
    p = it.get("providers") or it.get("providers_slugs") or []
    return p

if __name__ == "__main__":
    main()