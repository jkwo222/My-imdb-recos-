# engine/cache.py
from __future__ import annotations
from typing import Dict, Iterable, List, Any, Tuple, Optional
from pathlib import Path
import json, io, os, time, tempfile, hashlib
from datetime import datetime, timedelta

import requests

# ---------- Paths / dirs ----------
BASE = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE / "data" / "cache"
TMDB_DIR = CACHE_DIR / "tmdb"
IMDB_DIR = CACHE_DIR / "imdb"
USER_DIR = CACHE_DIR / "user"
FEEDBACK_DIR = CACHE_DIR / "feedback"
STATE_DIR = CACHE_DIR / "state"

def ensure_dirs() -> None:
    for p in [CACHE_DIR, TMDB_DIR, IMDB_DIR, USER_DIR, FEEDBACK_DIR, STATE_DIR]:
        p.mkdir(parents=True, exist_ok=True)

ensure_dirs()

# ---------- Atomic IO ----------
def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent)) as tmp:
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)

def atomic_write_json(path: Path, obj: Any) -> None:
    _atomic_write_bytes(path, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"))

def read_json(path: Path, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default

def read_jsonl_indexed(path: Path, key: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not path.exists():
        return out
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                k = str(obj.get(key))
                if k and k not in ("None", "null"):
                    out[k] = obj
            except Exception:
                continue
    return out

def upsert_jsonl(path: Path, key: str, rows: Iterable[Dict[str, Any]]) -> Tuple[int,int]:
    existing = read_jsonl_indexed(path, key)
    upserts = 0
    skipped = 0
    for r in rows:
        k = str(r.get(key))
        if not k:
            skipped += 1
            continue
        if k in existing and existing[k] == r:
            skipped += 1
        else:
            existing[k] = r
            upserts += 1
    buf = io.StringIO()
    for _, v in existing.items():
        buf.write(json.dumps(v, ensure_ascii=False))
        buf.write("\n")
    _atomic_write_bytes(path, buf.getvalue().encode("utf-8"))
    return (upserts, skipped)

# ---------- Time helpers ----------
def stale(ts_iso: str, ttl_days: int) -> bool:
    try:
        ts = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
    except Exception:
        return True
    return datetime.utcnow() - ts > timedelta(days=ttl_days)

def touch_now(obj: Dict[str,Any]) -> None:
    obj["cached_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

# ---------- Simple state blobs ----------
def _state_path(name: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)
    return STATE_DIR / f"{safe}.json"

def load_state(name: str, default: Any = None) -> Any:
    path = _state_path(name)
    return read_json(path, default if default is not None else {})

def save_state(name: str, obj: Any) -> None:
    path = _state_path(name)
    atomic_write_json(path, obj)

# ---------- TMDB cached HTTP ----------
_TMDB_API_BASE = "https://api.themoviedb.org/3"
_DEFAULT_TIMEOUT = (5, 20)  # connect, read
_DEFAULT_UA = "my-imdb-recos/1.0 (+github actions)"

def _tmdb_key() -> str:
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise RuntimeError("TMDB_API_KEY (v3) not set")
    return key

def _hash_key(url: str, params: Dict[str, Any]) -> str:
    blob = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()

def _tmdb_cache_path(kind: str, url: str, params: Dict[str, Any]) -> Path:
    h = _hash_key(url, params)
    return TMDB_DIR / f"{kind}_{h}.json"

def _tmdb_get_json_cached(kind: str, url: str, params: Dict[str, Any], *, ttl_days: int) -> Dict[str, Any]:
    ensure_dirs()
    path = _tmdb_cache_path(kind, url, params)
    cached = read_json(path, default=None)
    if isinstance(cached, dict) and "data" in cached and "cached_at" in cached and not stale(cached["cached_at"], ttl_days):
        return cached["data"]

    headers = {
        "Accept": "application/json",
        "User-Agent": _DEFAULT_UA,
    }

    # inject API key param (v3 style)
    params = dict(params)
    params["api_key"] = _tmdb_key()

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=_DEFAULT_TIMEOUT)
            if r.status_code == 429:
                time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            wrapper = {"cached_at": datetime.utcnow().isoformat(timespec="seconds") + "Z", "data": data}
            atomic_write_json(path, wrapper)
            return data
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1 + attempt)

    return {}

def _coerce_media_type(x: Optional[str], fallback: str) -> str:
    x = (x or "").lower()
    if x in ("movie", "tv"):
        return x
    return fallback

# ---------- Public TMDB cache APIs ----------
def tmdb_find_by_imdb_cached(imdb_id: str, api_key: Optional[str] = None, *, ttl_days: int = 30) -> Dict[str, Any]:
    if not imdb_id:
        return {}
    _ = api_key or _tmdb_key()
    url = f"{_TMDB_API_BASE}/find/{imdb_id}"
    params = {"external_source": "imdb_id", "language": "en-US"}
    return _tmdb_get_json_cached("find", url, params, ttl_days=ttl_days)

def tmdb_details_cached(tmdb_id: int, media_type: str, api_key: Optional[str] = None, *, ttl_days: int = 30) -> Dict[str, Any]:
    if not tmdb_id:
        return {}
    _ = api_key or _tmdb_key()
    mtype = _coerce_media_type(media_type, "movie")
    url = f"{_TMDB_API_BASE}/{mtype}/{tmdb_id}"
    params = {"language": "en-US"}
    return _tmdb_get_json_cached("details", url, params, ttl_days=ttl_days)

def tmdb_providers_cached(tmdb_id: int, api_key: Optional[str] = None, media_type: str = "movie", *, ttl_days: int = 7) -> Dict[str, Any]:
    if not tmdb_id:
        return {}
    _ = api_key or _tmdb_key()
    mtype = _coerce_media_type(media_type, "movie")
    url = f"{_TMDB_API_BASE}/{mtype}/{tmdb_id}/watch/providers"
    params = {}
    return _tmdb_get_json_cached("providers", url, params, ttl_days=ttl_days)