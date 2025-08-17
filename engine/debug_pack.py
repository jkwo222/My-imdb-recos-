# engine/debug_pack.py
from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
CACHE_DIR = ROOT / "data" / "cache"
STATE_DIR = CACHE_DIR / "state"
DEBUG_ZIP = OUT_DIR / "debug-data.zip"

def _add_file(zf: ZipFile, path: Path, arcname: str | None = None) -> None:
    if path.exists():
        zf.write(path, arcname=arcname or str(path.relative_to(ROOT)))

def _add_dir(zf: ZipFile, root: Path, relbase: Path) -> None:
    if not root.exists():
        return
    for p in root.rglob("*"):
        if p.is_file():
            arc = relbase / p.relative_to(root)
            zf.write(p, str(arc))

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with ZipFile(buf, "w", ZIP_DEFLATED) as z:
        # Primary outputs
        _add_file(z, OUT_DIR / "assistant_feed.json", "out/assistant_feed.json")
        _add_file(z, OUT_DIR / "run_meta.json", "out/run_meta.json")
        _add_file(z, OUT_DIR / "debug_status.json", "out/debug_status.json")
        _add_file(z, OUT_DIR / "summary.md", "out/summary.md")
        _add_file(z, OUT_DIR / "raw_discovered.json", "out/raw_discovered.json")
        _add_file(z, OUT_DIR / "raw_filtered.json", "out/raw_filtered.json")

        # Personalization state
        _add_file(z, STATE_DIR / "personal_state.json", "state/personal_state.json")
        _add_file(z, STATE_DIR / "personal_history.json", "state/personal_history.json")
        _add_file(z, STATE_DIR / "persistent_pool.json", "state/persistent_pool.json")

        # Feedback memory
        feedback_dir = CACHE_DIR / "feedback"
        _add_dir(z, feedback_dir, Path("feedback"))

        # IMDb + TMDB request caches
        _add_dir(z, CACHE_DIR / "tmdb", Path("cache/tmdb"))
        _add_dir(z, CACHE_DIR / "imdb", Path("cache/imdb"))

    # write zip to disk
    with open(DEBUG_ZIP, "wb") as f:
        f.write(buf.getvalue())

if __name__ == "__main__":
    main()