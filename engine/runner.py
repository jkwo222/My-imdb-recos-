import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from .config import load_config
from . import catalog as cat
from . import scoring as sc  # your existing scoring module

def _slot(rotate_minutes: int) -> int:
    # Stable “time slot” integer; changes every N minutes
    return int(time.time() // (rotate_minutes * 60))

def main() -> None:
    cfg = load_config()

    # Basic sanity
    if not cfg.tmdb_api_key:
        print("TMDB_API_KEY missing")
        sys.exit(1)

    # Optional: create debug dir
    Path(cfg.debug_dir).mkdir(parents=True, exist_ok=True)

    # Seen index (unchanged — uses your existing rating CSV path)
    seen_idx = sc.load_seen_index(cfg.imdb_ratings_csv_path)
    print(f"IMDb ingest: {cfg.imdb_ratings_csv_path} — {len(seen_idx)} rows")
    print(f"Seen index: {len(seen_idx)} keys (+0 new)")

    slot = _slot(cfg.rotate_minutes)
    print("[hb] %s | catalog:begin" % time.strftime("%H:%M:%S"))

    pool, meta = cat.build_pool(cfg, slot)

    print("[hb] %s | catalog:end pool=%d movie=%d tv=%d" %
          (time.strftime("%H:%M:%S"), len(pool), meta["pool_counts"]["movie"], meta["pool_counts"]["tv"]))

    # Filter unseen
    print("[hb] %s | filter:unseen" % time.strftime("%H:%M:%S"))
    pool_unseen = sc.filter_unseen(pool, seen_idx)
    print("[hb] %s | filter:end kept=%d dropped=%d" %
          (time.strftime("%H:%M:%S"), len(pool_unseen), len(pool) - len(pool_unseen)))

    # Score (audience > critic per your setting)
    cw = float(os.environ.get("CRITIC_WEIGHT", "0.25"))
    aw = float(os.environ.get("AUDIENCE_WEIGHT", "0.75"))
    np = float(os.environ.get("NOVELTY_PRESSURE", "0.15"))
    cc = float(os.environ.get("COMMITMENT_COST_SCALE", "1.0"))

    print("[hb] %s | score:begin cw=%.3f aw=%.3f np=%.2f cc=%.1f" %
          (time.strftime("%H:%M:%S"), cw, aw, np, cc))

    ranked = sc.rank(pool_unseen, critic_weight=cw, audience_weight=aw,
                     novelty_pressure=np, commitment_cost_scale=cc)
    print("[hb] %s | score:end ranked=%d" % (time.strftime("%H:%M:%S"), len(ranked)))

    # Emit top N (uses your existing output logic)
    sc.emit_daily(ranked, cfg.out_dir, meta, weights={"critic": cw, "audience": aw})

if __name__ == "__main__":
    main()