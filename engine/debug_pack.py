from __future__ import annotations
from pathlib import Path
import zipfile, os, json, time

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "out" / "latest"
OUT.mkdir(parents=True, exist_ok=True)

INCLUDE = [
    "out/latest/assistant_feed.json",
    "out/latest/assistant_ranked.json",
    "out/latest/summary.md",
    "out/latest/run_meta.json",
    "out/latest/debug_status.json",
    "cache/state/personal_state.json",
    "cache/state/personal_history.json",
    "cache/state/persistent_pool.json",  # if it exists
    "cache/imdb",                         # imdb tsv/cache if present
    "cache/tmdb",                         # tmdb cache
    "user/ratings.csv",
]

def main():
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    zpath = OUT / "debug-data.zip"
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for rel in INCLUDE:
            src = DATA / rel
            if src.is_file():
                z.write(src, f"data/{rel}")
            elif src.is_dir():
                for root, _, files in os.walk(src):
                    for f in files:
                        full = Path(root) / f
                        arc = f"data/{full.relative_to(DATA)}"
                        z.write(full, arc)
    print(f"wrote → {zpath}")

if __name__ == "__main__":
    main()