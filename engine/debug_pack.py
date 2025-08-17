# engine/debug_pack.py
import os
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
DEBUG_DIR = ROOT / "data" / "debug"

def make_debug_zip():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    # timestamped filename
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_path = DEBUG_DIR / f"debug-data-{ts}.zip"

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        def add_safe(path: Path, arcname: str = None):
            try:
                if path.is_file():
                    zf.write(path, arcname or str(path.relative_to(ROOT)))
            except Exception as e:
                print(f"[warn] skipping {path}: {e}")

        # Core outputs
        for p in (OUT_DIR / "assistant_feed.json",
                  OUT_DIR / "assistant_ranked.json",
                  OUT_DIR / "summary.md"):
            add_safe(p)

        # Caches & state
        state_dir = ROOT / "data" / "cache" / "state"
        for p in state_dir.glob("*.json"):
            add_safe(p)

        feedback_dir = ROOT / "data" / "cache" / "feedback"
        if feedback_dir.exists():
            for p in feedback_dir.glob("*.json"):
                add_safe(p)

        # TMDB + IMDb caches (small parts only)
        tmdb_cache = ROOT / "data" / "cache" / "tmdb"
        imdb_cache = ROOT / "data" / "cache" / "imdb"
        for d in (tmdb_cache, imdb_cache):
            if d.exists():
                for p in d.glob("*.json"):
                    add_safe(p)

        # User ratings list
        add_safe(ROOT / "data" / "user" / "ratings.csv")

    print(f"[debug-pack] wrote {zip_path}")
    return zip_path

if __name__ == "__main__":
    make_debug_zip()