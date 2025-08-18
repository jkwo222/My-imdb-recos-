# engine/recency.py
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

ROTATION_FILE = Path("data/cache/rotation.json")

def _load() -> Dict[str, float]:
    if not ROTATION_FILE.exists():
        return {}
    try:
        return json.loads(ROTATION_FILE.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}

def _save(d: Dict[str, float]) -> None:
    ROTATION_FILE.parent.mkdir(parents=True, exist_ok=True)
    ROTATION_FILE.write_text(json.dumps(d, indent=2), encoding="utf-8")

def key_for_item(it: Dict) -> Optional[str]:
    imdb = (it.get("imdb_id") or "").strip()
    if imdb:
        return imdb
    tid = it.get("tmdb_id") or it.get("id")
    mt = (it.get("media_type") or "movie").lower()
    if tid:
        return f"tm:{mt}:{tid}"
    title = (it.get("title") or it.get("name") or "").strip().lower()
    year = it.get("year") or ""
    if title:
        return f"title:{title}:{year}"
    return None

def should_skip_key(key: str, *, cooldown_days: int = 5) -> bool:
    data = _load()
    last_ts = data.get(key)
    if not last_ts:
        return False
    days = (time.time() - float(last_ts)) / (24*3600.0)
    return days < float(cooldown_days)

def mark_shown_keys(keys: List[str]) -> None:
    data = _load()
    ts = time.time()
    for k in keys:
        data[k] = ts
    _save(data)