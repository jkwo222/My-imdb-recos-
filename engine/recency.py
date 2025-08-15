# engine/recency.py
import os, json, time, pathlib
from typing import Iterable

REC_PATH = pathlib.Path("data/recency.json")

def _load():
    if REC_PATH.exists():
        return json.load(open(REC_PATH,"r",encoding="utf-8"))
    return {"last_shown": {}}

def _save(d):
    REC_PATH.parent.mkdir(parents=True, exist_ok=True)
    json.dump(d, open(REC_PATH,"w",encoding="utf-8"), indent=2)

def should_skip(imdb_id: str, days: int = 4) -> bool:
    if not imdb_id: return False
    d = _load()["last_shown"]
    ts = d.get(imdb_id)
    if not ts: return False
    return (time.time() - ts) < days*86400

def mark_shown(imdb_ids: Iterable[str]):
    d = _load()
    now = time.time()
    for i in imdb_ids:
        if not i: continue
        d["last_shown"][i] = now
    _save(d)