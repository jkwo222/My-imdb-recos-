# engine/cache.py
from __future__ import annotations
import json, time, os
from pathlib import Path
from typing import Any, Dict, Optional
import requests

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache"
TMDB_DIR = CACHE_DIR / "tmdb"
STATE_DIR = CACHE_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
TMDB_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TIMEOUT = 15  # seconds

def _now() -> float:
    return time.time()

def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_json(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

def fetch_json_cached(url: str, dest: Path, ttl_seconds: int, headers: Optional[Dict[str,str]]=None) -> Optional[Dict[str, Any]]:
    """Fetch URL to JSON file with TTL. Returns JSON dict or None."""
    fresh = dest.exists() and ( _now() - dest.stat().st_mtime <= ttl_seconds )
    if fresh:
        return _read_json(dest)
    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT, headers=headers or {})
        if r.status_code == 200:
            data = r.json()
            _write_json(dest, data)
            return data
        # keep stale if exists
        if dest.exists():
            return _read_json(dest)
        return None
    except Exception:
        # keep stale if exists
        if dest.exists():
            return _read_json(dest)
        return None

def tmdb_details_cached(tmdb_id: int, api_key: str, media_type: str="movie", ttl_days: int=14) -> Optional[Dict[str,Any]]:
    path = TMDB_DIR / media_type / f"{tmdb_id}.json"
    ttl = ttl_days * 86400
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={api_key}&language=en-US"
    return fetch_json_cached(url, path, ttl)

def tmdb_providers_cached(tmdb_id: int, api_key: str, media_type: str="movie", ttl_days: int=7) -> Optional[Dict[str,Any]]:
    path = TMDB_DIR / "providers" / media_type / f"{tmdb_id}.json"
    ttl = ttl_days * 86400
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers?api_key={api_key}"
    return fetch_json_cached(url, path, ttl)

# Persistent state blobs (grow over time)
def load_state(name: str, default: dict) -> dict:
    p = STATE_DIR / f"{name}.json"
    js = _read_json(p)
    return js if isinstance(js, dict) else dict(default)

def save_state(name: str, data: dict) -> None:
    p = STATE_DIR / f"{name}.json"
    _write_json(p, data)