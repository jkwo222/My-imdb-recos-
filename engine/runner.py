from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

from .catalog import build_pool
from .config import load_config

LOCK_PATH = os.path.join("data", "run.lock")
LOCK_STALE_SECONDS = 25 * 60  # consider a lock stale after 25 minutes

def _log(msg: str) -> None:
    print(msg, flush=True)

def _acquire_lock() -> bool:
    os.makedirs("data", exist_ok=True)
    now = time.time()
    # if lock exists but is stale, remove it
    if os.path.exists(LOCK_PATH):
        try:
            mtime = os.path.getmtime(LOCK_PATH)
            if now - mtime > LOCK_STALE_SECONDS:
                os.remove(LOCK_PATH)
            else:
                return False
        except Exception:
            # if we can't stat, try to remove; otherwise fail closed
            try:
                os.remove(LOCK_PATH)
            except Exception:
                return False
    try:
        with open(LOCK_PATH, "w", encoding="utf-8") as f:
            f.write(json.dumps({"pid": os.getpid(), "ts": int(now)}))
        return True
    except Exception:
        return False

def _release_lock() -> None:
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except Exception:
        pass

def main() -> None:
    _log("[bootstrap] runner starting")
    if not _acquire_lock():
        _log("[bootstrap] another run appears to be in progress; exiting.")
        return
    try:
        cfg = load_config()
        pool, meta = build_pool(cfg)

        # minimal telemetry dump (optional)
        out_dir = os.path.join("data", "out", "latest")
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "assistant_feed.json"), "w", encoding="utf-8") as f:
            json.dump({"pool_count": len(pool), "meta": meta}, f, ensure_ascii=False)

        _log("[bootstrap] runner finished")
    finally:
        _release_lock()

if __name__ == "__main__":
    main()