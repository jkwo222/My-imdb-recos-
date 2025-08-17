# engine/cache.py
from __future__ import annotations
from typing import Dict, Iterable, List, Any, Tuple, Optional
from pathlib import Path
import json, io, os, time, tempfile, shutil
from datetime import datetime, timedelta

# TMDB helpers (already provided in your engine/tmdb.py)
from .tmdb import find_by_imdb_id, search_title_year, watch_providers

BASE = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE / "data" / "cache"

# Files that persist across runs
STATE_DIR = CACHE_DIR / "state"
TMDB_MAP_PATH = CACHE_DIR / "tmdb_map.json"          # imdb tconst -> {"media_type": "...", "tmdb_id": 123}
TMDB_PROV_PATH = CACHE_DIR / "tmdb_providers.json"   # "movie:123" -> full payload from /watch/providers

def ensure_dirs() -> None:
    for p in [
        CACHE_DIR,
        CACHE_DIR / "tmdb",
        CACHE_DIR / "imdb",
        CACHE_DIR / "user",
        CACHE_DIR / "feedback",
        STATE_DIR,
    ]:
        p.mkdir(parents=True, exist_ok=True)

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
        # Corrupted? Return default rather than blowing up the run.
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
    """
    Upsert rows into JSONL by 'key'. Returns (upserts, skipped).
    """
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
    # write back
    buf = io.StringIO()
    for _, v in existing.items():
        buf.write(json.dumps(v, ensure_ascii=False))
        buf.write("\n")
    _atomic_write_bytes(path, buf.getvalue().encode("utf-8"))
    return (upserts, skipped)

def stale(ts_iso: str, ttl_days: int) -> bool:
    try:
        ts = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
    except Exception:
        return True
    return datetime.utcnow() - ts > timedelta(days=ttl_days)

def touch_now(obj: Dict[str,Any]) -> None:
    obj["cached_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"


# -----------------------------
# Simple named state persistence
# -----------------------------

def _state_path(name: str) -> Path:
    ensure_dirs()
    safe = name.replace("/", "_")
    return STATE_DIR / f"{safe}.json"

def load_state(name: str, default: Any = None) -> Any:
    path = _state_path(name)
    return read_json(path, default if default is not None else {})

def save_state(name: str, obj: Any) -> None:
    path = _state_path(name)
    atomic_write_json(path, obj)


# -----------------------------------------
# TMDB ID resolution + provider cache layer
# -----------------------------------------

def _prov_key(media_type: str, tmdb_id: int) -> str:
    media_type = "movie" if media_type == "movie" else "tv"
    return f"{media_type}:{int(tmdb_id)}"

def resolve_tmdb_id(
    *,
    imdb_tconst: Optional[str] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
    media_hint: str = "movie",   # 'movie'|'tv'
) -> tuple[str, int] | None:
    """
    Returns ('movie'|'tv', tmdb_id) or None.
    Strategy:
      1) cache hit via imdb_tconst
      2) TMDB /find/{tconst}
      3) TMDB /search/{kind} using title/year
    Caches the result back into tmdb_map.json.
    """
    ensure_dirs()
    tmdb_map: Dict[str, Any] = read_json(TMDB_MAP_PATH, {})

    # 1) cache by tconst
    if imdb_tconst:
        entry = tmdb_map.get(imdb_tconst)
        if entry and isinstance(entry.get("tmdb_id"), int):
            mtype = entry.get("media_type") or media_hint
            return (mtype, int(entry["tmdb_id"]))

    # 2) /find by tconst
    if imdb_tconst:
        try:
            data = find_by_imdb_id(imdb_tconst)
            for bucket, mtype in (("movie_results","movie"),("tv_results","tv")):
                res = (data.get(bucket) or [])
                if res:
                    tmdb_id = int(res[0]["id"])
                    tmdb_map[imdb_tconst] = {"media_type": mtype, "tmdb_id": tmdb_id}
                    atomic_write_json(TMDB_MAP_PATH, tmdb_map)
                    return (mtype, tmdb_id)
        except Exception:
            # fall through to search
            pass

    # 3) /search with title/year hint
    if title:
        try:
            mtype = "movie" if media_hint == "movie" else "tv"
            data = search_title_year(title, year, mtype)
            results = data.get("results") or []
            if results:
                tmdb_id = int(results[0]["id"])
                if imdb_tconst:
                    tmdb_map[imdb_tconst] = {"media_type": mtype, "tmdb_id": tmdb_id}
                    atomic_write_json(TMDB_MAP_PATH, tmdb_map)
                return (mtype, tmdb_id)
        except Exception:
            return None

    return None

def tmdb_providers_cached(
    tmdb_id: Optional[int],
    api_key: str,     # intentionally unused here; kept for signature compatibility
    media_type: str,
    *,
    imdb_tconst: Optional[str] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Main entry used by catalog_builder:
      - If tmdb_id present: return (cached) providers
      - Else: resolve via imdb_tconst -> tmdb_id (then cache)
      - Else: search(title/year) -> tmdb_id (then cache)

    Returns the raw TMDB /watch/providers payload (with .results[REGION]...).
    """
    ensure_dirs()
    prov_cache: Dict[str, Any] = read_json(TMDB_PROV_PATH, {})

    # Ensure we have a TMDB id
    if tmdb_id is None:
        resolved = resolve_tmdb_id(
            imdb_tconst=imdb_tconst,
            title=title,
            year=year,
            media_hint=media_type,
        )
        if not resolved:
            return {}
        media_type, tmdb_id = resolved

    key = _prov_key(media_type, int(tmdb_id))
    if key in prov_cache:
        return prov_cache[key]

    try:
        payload = watch_providers(media_type, int(tmdb_id))
    except Exception:
        return {}

    prov_cache[key] = payload
    atomic_write_json(TMDB_PROV_PATH, prov_cache)
    return payload