# engine/feed.py
from __future__ import annotations
import csv, json, os, pathlib, datetime as dt
from typing import Dict, List, Any

# keep this import, fixed earlier
from .provider_filter import any_allowed

# Safe writer for multiple export flavors
def _ensure_dirs():
    latest = pathlib.Path("data/out/latest")
    daily = pathlib.Path("data/out/daily") / dt.date.today().isoformat()
    latest.mkdir(parents=True, exist_ok=True)
    daily.mkdir(parents=True, exist_ok=True)
    return latest, daily

def _as_pct(x: float) -> float:
    try:
        return round(float(x) * 100.0, 1)
    except Exception:
        return 0.0

def _write_json(path: pathlib.Path, payload: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def _write_csv(path: pathlib.Path, rows: List[Dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    # stable column order for spreadsheets
    cols = [
        "title","year","type","tmdb_id","imdb_id",
        "providers","critic_pct","audience_pct",
        "language_primary","genres","tmdb_vote","popularity","match"
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=cols)
        wr.writeheader()
        for r in rows:
            wr.writerow({
                "title": r.get("title",""),
                "year": r.get("year",""),
                "type": r.get("type",""),
                "tmdb_id": r.get("tmdb_id",""),
                "imdb_id": r.get("imdb_id",""),
                "providers": ";".join(r.get("providers",[])),
                "critic_pct": _as_pct(r.get("critic",0.0)),
                "audience_pct": _as_pct(r.get("audience",0.0)),
                "language_primary": r.get("language_primary",""),
                "genres": ";".join(r.get("genres",[])),
                "tmdb_vote": r.get("tmdb_vote",""),
                "popularity": r.get("popularity",""),
                "match": r.get("match",""),
            })

def _write_md(path: pathlib.Path, items: List[Dict[str, Any]], meta: Dict[str, Any]):
    lines = ["# Assistant Feed (top 25)\n"]
    for i, it in enumerate(items[:25], 1):
        prov = ", ".join(it.get("providers", []))
        lines.append(
            f"{i}. **{it.get('title','?')}** ({it.get('year','')}) — {it.get('type','')}"
            f" · Match {it.get('match','?')} · IMDb {_as_pct(it.get('audience',0.0))}%"
            f" · RT {_as_pct(it.get('critic',0.0))}%"
            f"{' · ' + prov if prov else ''}"
        )
    if meta:
        lines.append("\n---\n**meta**:\n")
        lines.append("```json")
        lines.append(json.dumps(meta, indent=2))
        lines.append("```")
    path.write_text("\n".join(lines), encoding="utf-8")

def export_feed(items: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    latest, daily_root = _ensure_dirs()
    payload = {
        "generated_at": int(dt.datetime.utcnow().timestamp()),
        "count": len(items),
        "items": items,
        "meta": meta or {},
    }
    # JSON
    _write_json(latest / "assistant_feed.json", payload)
    _write_json(daily_root / "assistant_feed.json", payload)
    # CSV (flat)
    _write_csv(latest / "assistant_feed.csv", items)
    _write_csv(daily_root / "assistant_feed.csv", items)
    # Quick MD summary
    _write_md(latest / "assistant_feed.md", items, meta)
    _write_md(daily_root / "assistant_feed.md", items, meta)

# Backward/forwards-compatible shim: runner may call build_feed(*args)
def build_feed(items: List[Dict[str, Any]] | None = None,
               meta: Dict[str, Any] | None = None,
               **kwargs) -> List[Dict[str, Any]]:
    """
    Make this tolerant to call signatures. If items/meta not provided,
    write an empty payload so the workflow still uploads a file.
    """
    items = items or []
    meta = meta or {}
    export_feed(items, meta)
    return items