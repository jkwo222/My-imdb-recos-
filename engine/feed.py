# engine/feed.py
from __future__ import annotations
import os, csv, json, pathlib, datetime
from typing import List, Dict, Any, Tuple

def _today_dir() -> pathlib.Path:
    d = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    return pathlib.Path("data/out/daily") / d

def _latest_dir() -> pathlib.Path:
    return pathlib.Path("data/out/latest")

def _ensure_dirs(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _split_movies_series(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    movies = [x for x in items if x.get("type") == "movie"]
    series = [x for x in items if x.get("type") != "movie"]
    return movies, series

def _csv_write(path: pathlib.Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    _ensure_dirs(path.parent)
    # stable column order
    cols = ["title","year","type","match","providers","imdb_id","tmdb_id","audience","critic","tmdb_vote","popularity","why"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            out = {k: r.get(k) for k in cols}
            if isinstance(out.get("providers"), list):
                out["providers"] = ", ".join(out["providers"])
            if isinstance(out.get("why"), list):
                out["why"] = " | ".join(out["why"])
            w.writerow(out)

def _json_write(path: pathlib.Path, payload: Dict[str, Any]) -> None:
    _ensure_dirs(path.parent)
    json.dump(payload, open(path, "w", encoding="utf-8"), indent=2)

def _md_list_block(title: str, items: List[Dict[str, Any]]) -> str:
    lines = [f"### {title}"]
    if not items:
        lines.append("_No items._")
        return "\n".join(lines)
    for it in items:
        prov = ", ".join(it.get("providers") or [])
        rt = it.get("critic"); imdb = it.get("audience")
        rt_s = f"RT {int(rt*100)}%" if isinstance(rt, (int,float)) and rt>0 else "RT n/a"
        imdb_s = f"IMDb {round(float(imdb)*10,1)}/10" if isinstance(imdb, (int,float)) and imdb>0 else "IMDb n/a"
        why = " — " + "; ".join(it.get("why") or []) if it.get("why") else ""
        lines.append(f"- **{it.get('title')}** ({it.get('year')}) — {it.get('match')}% match · {prov or '—'} · {rt_s}, {imdb_s}{why}")
    return "\n".join(lines)

def build_feed(ranked: List[Dict[str, Any]],
               meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Writes JSON, CSV and Markdown summary to daily + latest folders.
    Returns the payload used for JSON.
    """
    movies, series = _split_movies_series(ranked)
    top_movies = movies[:10]
    top_series = series[:10]

    payload = {
        "generated_at": int(datetime.datetime.utcnow().timestamp()),
        "count": len(ranked),
        "items": ranked,
        "meta": meta,
        "top": {
            "movies": top_movies,
            "series": top_series
        }
    }

    # Paths
    d_today = _today_dir()
    d_latest = _latest_dir()
    _ensure_dirs(d_today); _ensure_dirs(d_latest)

    # Files
    json_path_today   = d_today / "assistant_feed.json"
    json_path_latest  = d_latest / "assistant_feed.json"
    csv_all_today     = d_today / "all_items.csv"
    csv_movies_today  = d_today / "top_movies.csv"
    csv_series_today  = d_today / "top_series.csv"
    csv_all_latest    = d_latest / "all_items.csv"
    csv_movies_latest = d_latest / "top_movies.csv"
    csv_series_latest = d_latest / "top_series.csv"
    md_summary_today  = d_today / "SUMMARY.md"
    md_summary_latest = d_latest / "SUMMARY.md"

    # Write JSON
    _json_write(json_path_today, payload)
    _json_write(json_path_latest, payload)

    # Write CSVs
    _csv_write(csv_all_today, ranked)
    _csv_write(csv_all_latest, ranked)
    _csv_write(csv_movies_today, top_movies)
    _csv_write(csv_movies_latest, top_movies)
    _csv_write(csv_series_today, top_series)
    _csv_write(csv_series_latest, top_series)

    # Markdown summary (nice for GH Actions job summary / notification)
    sizes = meta.get("pool_sizes", {})
    weights = meta.get("weights", {})
    subs = ", ".join(meta.get("subs") or [])
    telemetry = (
        f"**Telemetry** — pool initial={sizes.get('initial',0)}, "
        f"providers={sizes.get('providers',0)}, unseen={sizes.get('unseen',0)}, "
        f"fresh={sizes.get('fresh',0)}, final={sizes.get('final',0)}  \n"
        f"**Weights** — audience={weights.get('audience_weight')}, critic={weights.get('critic_weight')}, "
        f"novelty={weights.get('novelty_weight')}, commitment={weights.get('commitment_cost_scale')}  \n"
        f"**Services** — {subs or '—'}"
    )
    md = [
        "# Your Daily Recommendations",
        telemetry,
        "",
        _md_list_block("Top 10 Movies", top_movies),
        "",
        _md_list_block("Top 10 Series", top_series),
    ]
    summary_md = "\n\n".join(md)
    for p in (md_summary_today, md_summary_latest):
        _ensure_dirs(p.parent)
        with open(p, "w", encoding="utf-8") as f:
            f.write(summary_md)

    return payload