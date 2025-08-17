from __future__ import annotations
import json
import os
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "out" / "latest"
CACHE = DATA / "cache"

def _add_if_exists(z: ZipFile, p: Path, arc: str) -> None:
    if p.exists():
        if p.is_file():
            z.write(p, arc)
        else:
            for sub in p.rglob("*"):
                if sub.is_file():
                    z.write(sub, str(Path(arc) / sub.relative_to(p)))

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    zp = OUT / "debug-data.zip"
    with ZipFile(zp, "w", ZIP_DEFLATED) as z:
        _add_if_exists(z, OUT / "assistant_feed.json", "out/assistant_feed.json")
        _add_if_exists(z, OUT / "assistant_ranked.json", "out/assistant_ranked.json")
        _add_if_exists(z, OUT / "summary.md", "out/summary.md")
        _add_if_exists(z, OUT / "run_meta.json", "out/run_meta.json")
        _add_if_exists(z, OUT / "catalog_stats.json", "out/catalog_stats.json")
        _add_if_exists(z, OUT / "debug_status.json", "out/debug_status.json")
        _add_if_exists(z, CACHE / "state" / "personal_state.json", "cache/state/personal_state.json")
        _add_if_exists(z, CACHE / "state" / "personal_history.json", "cache/state/personal_history.json")
        _add_if_exists(z, CACHE / "state" / "persistent_pool.json", "cache/state/persistent_pool.json")
        _add_if_exists(z, CACHE / "imdb" / "user_ratings.json", "cache/imdb/user_ratings.json")
        # TMDB/IMDb request caches (folders if you keep them)
        _add_if_exists(z, CACHE / "tmdb", "cache/tmdb")
        _add_if_exists(z, CACHE / "imdb", "cache/imdb_raw")
        # raw user file
        _add_if_exists(z, DATA / "user" / "ratings.csv", "user/ratings.csv")
    print(f"wrote → {zp}")

if __name__ == "__main__":
    main()