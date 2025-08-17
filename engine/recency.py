# engine/recency.py
"""
Only tracks what we've shown recently so we don't repeat too soon.
All scoring now happens in engine/rank.py.
"""
from __future__ import annotations
import json, time, pathlib
from typing import Iterable

REC_PATH = pathlib.Path("data/recency.json")

def _load() -> dict:
    if REC_PATH.exists():
        try:
            return json.load(open(REC_PATH, "r", encoding="utf-8"))
        except Exception:
            pass
    return {"last_shown": {}}

def _save(d: dict) -> None:
    REC_PATH.parent.mkdir(parents=True, exist_ok=True)
    json.dump(d, open(REC_PATH, "w", encoding="utf-8"), indent=2)

def should_skip(imdb_or_title: str, days: int = 4) -> bool:
    if not imdb_or_title:
        return False
    ts = _load()["last_shown"].get(imdb_or_title)
    return bool(ts and (time.time() - ts) < days * 86400)

def mark_shown(keys: Iterable[str]) -> None:
    d = _load()
    now = time.time()
    for k in keys:
        if k:
            d["last_shown"][k] = now
    _save(d)