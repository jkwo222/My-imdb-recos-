# engine/store.py
from __future__ import annotations
import json, os, pathlib, time
from typing import Dict, List

STORE = pathlib.Path("data/store.json")

def load_store() -> Dict:
    if STORE.exists():
        try:
            return json.load(open(STORE, "r", encoding="utf-8"))
        except Exception:
            pass
    return {"items": [], "seen_tmdb": {}, "seen_imdb": {}, "updated_at": 0}

def save_store(d: Dict) -> None:
    STORE.parent.mkdir(parents=True, exist_ok=True)
    d["updated_at"] = int(time.time())
    json.dump(d, open(STORE, "w", encoding="utf-8"), indent=2)

def merge_pool(existing: List[Dict], new_items: List[Dict]) -> List[Dict]:
    by_key = {}
    for r in existing:
        k = f"{r.get('imdb_id','')}|{r.get('tmdb_id','')}"; by_key[k] = r
    for r in new_items:
        k = f"{r.get('imdb_id','')}|{r.get('tmdb_id','')}"
        if k not in by_key:
            by_key[k] = r
        else:
            # update missing ratings / providers if the new one is richer
            old = by_key[k]
            if (old.get("imdb_rating") or 0.0) == 0.0 and (r.get("imdb_rating") or 0.0) > 0.0:
                old["imdb_rating"] = r.get("imdb_rating")
                old["imdb_votes"] = r.get("imdb_votes", 0)
            if (not old.get("providers")) and r.get("providers"):
                old["providers"] = r["providers"]
    out = list(by_key.values())
    out.sort(key=lambda x: (-float(x.get("imdb_votes") or 0), -float(x.get("popularity") or 0.0)))
    return out

def remember(d: Dict, new_items: List[Dict]) -> Dict:
    d = dict(d)
    d["items"] = merge_pool(d.get("items", []), new_items)
    return d