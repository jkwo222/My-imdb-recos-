# engine/cache.py
from __future__ import annotations
from typing import Dict, Iterable, List, Any, Tuple
from pathlib import Path
import json, io, os, time, tempfile, shutil
from datetime import datetime, timedelta

BASE = Path(__file__).resolve().parents[1]
CACHE_DIR = BASE / "data" / "cache"

def ensure_dirs() -> None:
    for p in [
        CACHE_DIR,
        CACHE_DIR / "tmdb",
        CACHE_DIR / "imdb",
        CACHE_DIR / "user",
        CACHE_DIR / "feedback",
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