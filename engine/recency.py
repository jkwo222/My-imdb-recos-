# engine/recency.py
from __future__ import annotations
import os
import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# -----------------------------
# Rotation cooldown (existing)
# -----------------------------
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
    """
    Stable key for rotation tracking (prefers IMDb ID, then TMDB id, then title:year).
    """
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

def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip())
    except Exception:
        return default

def should_skip_key(key: str, *, cooldown_days: Optional[int] = None) -> bool:
    """
    Return True if key is still within cooldown.
    If cooldown_days is None, use ROTATION_COOLDOWN_DAYS env (default 5).
    """
    if cooldown_days is None:
        cooldown_days = _env_int("ROTATION_COOLDOWN_DAYS", 5)
    data = _load()
    last_ts = data.get(key)
    if not last_ts:
        return False
    days = (time.time() - float(last_ts)) / (24 * 3600.0)
    return days < float(cooldown_days)

def mark_shown_keys(keys: List[str]) -> None:
    """
    Mark a batch of items as shown 'now' to start/restart their cooldown.
    """
    data = _load()
    ts = time.time()
    for k in keys:
        data[k] = ts
    _save(data)

# -----------------------------
# Recency labeling (new)
# -----------------------------

# Tunables via env (safe defaults)
_RECENT_MOVIE_MONTHS = _env_int("RECENT_MOVIE_MONTHS", 9)
_RECENT_SERIES_DAYS  = _env_int("RECENT_SERIES_DAYS", 120)
_RECENT_SEASON_DAYS  = _env_int("RECENT_SEASON_DAYS", 120)

def _parse_date(s: Any) -> Optional[date]:
    """
    Accept strings 'YYYY-MM-DD' | 'YYYY-MM' | 'YYYY'; returns a date (missing parts -> 1).
    """
    if not s:
        return None
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    txt = str(s).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            d = datetime.strptime(txt, fmt).date()
            # Normalize partial dates to day=1
            if fmt == "%Y-%m":
                d = d.replace(day=1)
            elif fmt == "%Y":
                d = d.replace(month=1, day=1)
            return d
        except Exception:
            continue
    return None

def _months_between(a: date, b: date) -> int:
    """Whole-months difference (approximate, good for recency gating)."""
    return (a.year - b.year) * 12 + (a.month - b.month) - (1 if a.day < b.day else 0)

def _days_between(a: date, b: date) -> int:
    return (a - b).days

def is_recent_movie(item: dict) -> Optional[str]:
    """
    Returns:
      'NEW_MOVIE' if release_date within RECENT_MOVIE_MONTHS, else None.
    """
    rd = _parse_date(item.get("release_date"))
    if not rd:
        # Fallback to year if present
        y = item.get("year")
        if isinstance(y, int) and y > 0:
            rd = date(y, 1, 1)
        else:
            return None
    today = date.today()
    months = _months_between(today, rd)
    if months <= _RECENT_MOVIE_MONTHS:
        return "NEW_MOVIE"
    return None

def is_recent_show(item: dict) -> Optional[str]:
    """
    Returns:
      'NEW_SERIES' if first_air_date within RECENT_SERIES_DAYS
      'NEW_SEASON' if last_air_date within RECENT_SEASON_DAYS and number_of_seasons > 1
      else None.
    """
    fad = _parse_date(item.get("first_air_date"))
    lad = _parse_date(item.get("last_air_date"))
    try:
        seasons = int(item.get("number_of_seasons") or 0)
    except Exception:
        seasons = 0

    today = date.today()

    # Prefer "new series" if the show itself is newly premiered
    if fad and _days_between(today, fad) <= _RECENT_SERIES_DAYS:
        return "NEW_SERIES"

    # Otherwise consider "new season" if a recent last_air_date exists and there is >1 season
    if lad and seasons > 1 and _days_between(today, lad) <= _RECENT_SEASON_DAYS:
        return "NEW_SEASON"

    return None