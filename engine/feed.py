# engine/feed.py
from __future__ import annotations
import json, os, time, pathlib, random
from typing import Any, Dict, List, Tuple
from .recency import should_skip, mark_shown

JSON = Dict[str, Any]
OUT_LATEST = pathlib.Path("data/out/latest/assistant_feed.json")

def _now_datestr() -> str:
    import datetime as _dt
    return _dt.datetime.utcnow().strftime("%Y-%m-%d")

def _ensure_dirs(dst_path: pathlib.Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)

def _diversify(rows: List[JSON], max_per_provider: int = 6, max_per_genre: int = 8) -> List[JSON]:
    by_provider: Dict[str, int] = {}
    by_genre: Dict[str, int] = {}
    out: List[JSON] = []
    for r in rows:
        provs = r.get("providers") or []
        genres = [g.lower() for g in (r.get("genres") or [])]
        # pick a single canonical provider key for quota (first if multiple)
        key = (provs[0] if provs else "unknown")
        if by_provider.get(key, 0) >= max_per_provider:
            continue
        # genre quotas (apply to each genre)
        if any(by_genre.get(g, 0) >= max_per_genre for g in genres):
            continue
        out.append(r)
        by_provider[key] = by_provider.get(key, 0) + 1
        for g in set(genres):
            by_genre[g] = by_genre.get(g, 0) + 1
        if len(out) >= 50:
            break
    return out

def _apply_recency(rows: List[JSON]) -> List[JSON]:
    fresh = [r for r in rows if not should_skip(r.get("imdb_id","") or r.get("title",""))]
    # If recency pruned too hard, fall back to originals
    return fresh if len(fresh) >= 20 else rows

def build_and_write_feed(ranked: List[JSON]) -> Tuple[List[JSON], pathlib.Path]:
    # Remove super-low confidence
    rows = [r for r in ranked if (r.get("match") or 0) >= 60.0]
    rows = _apply_recency(rows)
    rows = _diversify(rows, max_per_provider=6, max_per_genre=8)

    # Write latest + dated
    _ensure_dirs(OUT_LATEST)
    payload = {
        "generated_at": int(time.time()),
        "count": len(rows),
        "items": rows,
    }
    with OUT_LATEST.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    dated = pathlib.Path(f"data/out/daily/{_now_datestr()}/assistant_feed.json")
    _ensure_dirs(dated)
    with dated.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # mark recency
    mark_shown([r.get("imdb_id","") for r in rows])

    return rows, dated