# engine/debug_pack.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict
import zipfile
import datetime as _dt
import platform

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
CACHE_DIR = ROOT / "data" / "cache"
STATE_DIR = CACHE_DIR / "state"
FEEDBACK_DIR = CACHE_DIR / "feedback"
TMDB_DIR = CACHE_DIR / "tmdb"
IMDB_DIR = CACHE_DIR / "imdb"

ZIP_PATH = OUT_DIR / "debug-data.zip"

def _gather(paths: List[Path]) -> List[Path]:
    found: List[Path] = []
    for p in paths:
        if p.is_file():
            found.append(p)
        elif p.is_dir():
            for sub in p.rglob("*"):
                if sub.is_file():
                    found.append(sub)
    return found

def _limit(files: List[Path], cap: int) -> List[Path]:
    # deterministic: sort by path, then take last 'cap' (most nested) to keep variety
    files = sorted(files)
    if len(files) <= cap:
        return files
    return files[-cap:]

def _env_snapshot() -> Dict:
    env_keys = [
        "REGION", "ORIGINAL_LANGS", "SUBS_INCLUDE", "MIN_MATCH_CUT",
        "IMDB_USER_ID",
    ]
    snap = {k: os.environ.get(k) for k in env_keys}
    snap["python"] = platform.python_version()
    snap["platform"] = platform.platform()
    snap["timestamp_utc"] = _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    return snap

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    include_files: List[Path] = []
    # Always include run outputs
    include_files += _gather([
        OUT_DIR / "assistant_feed.json",
        OUT_DIR / "assistant_ranked.json",
        OUT_DIR / "run_meta.json",
        OUT_DIR / "genre_weights.json",
        OUT_DIR / "summary.md",
        OUT_DIR / "debug_status.json",
    ])

    # State / history / feedback
    include_files += _gather([
        STATE_DIR,
        FEEDBACK_DIR,
        ROOT / "data" / "user" / "ratings.csv",  # if present
        CACHE_DIR / "state" / "persistent_pool.json",
        CACHE_DIR / "state" / "personal_state.json",
        CACHE_DIR / "state" / "personal_history.json",
    ])

    # Caches (cap to keep archive small-ish)
    tmdb_files = _limit(_gather([TMDB_DIR]), 500)
    imdb_files = _limit(_gather([IMDB_DIR]), 100)

    include_files += tmdb_files + imdb_files

    # Build manifest
    manifest = {
        "env": _env_snapshot(),
        "counts": {
            "out_files": len([p for p in include_files if str(p).startswith(str(OUT_DIR))]),
            "state_files": len([p for p in include_files if str(p).startswith(str(STATE_DIR))]),
            "feedback_files": len([p for p in include_files if str(p).startswith(str(FEEDBACK_DIR))]),
            "tmdb_cached": len(tmdb_files),
            "imdb_cached": len(imdb_files),
            "total_in_archive": len(include_files) + 1,  # +1 for manifest itself
        },
        "roots": {
            "OUT_DIR": str(OUT_DIR),
            "CACHE_DIR": str(CACHE_DIR),
        },
        "notes": [
            "Archive contains a subset of caches for size control.",
            "If something is missing, we can bump the caps in debug_pack.py.",
        ],
    }

    # Write the zip
    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # manifest first
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        for f in include_files:
            try:
                # store paths relative to repo root for clarity
                arcname = f.relative_to(ROOT)
            except ValueError:
                arcname = f.name
            if f.exists():
                zf.write(f, arcname)

    print(f"[debug_pack] wrote → {ZIP_PATH} ({ZIP_PATH.stat().st_size} bytes)")

if __name__ == "__main__":
    main()