# engine/recency.py
"""
Rotation / cooldown memory for titles we've shown recently.

- Persists to data/cache/rotation/last_shown.json
- Backward-compatible: will read legacy data/recency.json if present
- Keys: prefer imdb_id; fallback to tmdb_id; fallback to "normtitle::year"
"""

from __future__ import annotations
import json, time, re
from pathlib import Path
from typing import Dict, Iterable, Optional

REC_DIR  = Path("data/cache/rotation")
REC_PATH = REC_DIR / "last_shown.json"
LEGACY_PATH = Path("data/recency.json")

_NON = re.compile(r"[^a-z0-9]+")

def _norm(s: str) -> str:
    return _NON.sub(" ", (s or "").strip().lower()).strip()

def _ensure_dir() -> None:
    REC_DIR.mkdir(parents=True, exist_ok=True)

def _load_raw() -> Dict[str, float]:
    # prefer new file
    if REC_PATH.exists():
        try:
            d = json.loads(REC_PATH.read_text(encoding="utf-8"))
            return d.get("last_shown") or {}
        except Exception:
            pass
    # fallback to legacy
    if LEGACY_PATH.exists():
        try:
            d = json.loads(LEGACY_PATH.read_text(encoding="utf-8"))
            return d.get("last_shown") or {}
        except Exception:
            pass
    return {}

def _save_raw(last_shown: Dict[str, float]) -> None:
    _ensure_dir()
    data = {"last_shown": last_shown}
    tmp = REC_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(REC_PATH)

def key_for_item(item: dict) -> Optional[str]:
    imdb = (item.get("imdb_id") or "").strip()
    if imdb.startswith("tt"):
        return imdb
    tid = item.get("tmdb_id")
    if tid:
        try:
            return f"tm:{int(tid)}"
        except Exception:
            pass
    title = (item.get("title") or item.get("name") or "").strip()
    year  = item.get("year") or item.get("release_year") or item.get("first_air_year")
    if not title or not year:
        return None
    try:
        yi = int(str(year)[:4])
    except Exception:
        return None
    return f"{_norm(title)}::{yi}"

def should_skip_key(key: Optional[str], *, cooldown_days: int = 5) -> bool:
    if not key:
        return False
    last = _load_raw().get(key)
    if not last:
        return False
    return (time.time() - float(last)) < (max(0, cooldown_days) * 86400)

def mark_shown_keys(keys: Iterable[str]) -> None:
    now = time.time()
    data = _load_raw()
    for k in keys:
        if k:
            data[k] = now
    _save_raw(data)