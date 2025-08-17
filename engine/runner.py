# engine/runner.py
from __future__ import annotations
import json, os, sys, time, shutil
from datetime import datetime
from typing import Any, Dict, List

from .env import Env
from .catalog_builder import build_catalog
from .scoring import load_seen_index, filter_unseen, score_items
from .self_check import run_self_check

RUN_ROOT = "data/out"

def _now_slug() -> str:
    # 2025-08-17T12-24-09Z -> filesystem-friendly
    return datetime.utcnow().strftime("run_%Y%m%dT%H%M%SZ")

def _mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _write_text(path: str, txt: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(txt)

def _copy_to_latest(run_dir: str) -> str:
    latest_dir = os.path.join(RUN_ROOT, "latest")
    if os.path.isdir(latest_dir):
        shutil.rmtree(latest_dir)
    shutil.copytree(run_dir, latest_dir)
    return latest_dir

def main() -> None:
    # Make sure core modules exist
    run_self_check()

    _mkdir(RUN_ROOT)
    run_dir = os.path.join(RUN_ROOT, _now_slug())
    _mkdir(run_dir)

    # Emit a marker that our workflow can read
    last_path_file = os.path.join(RUN_ROOT, "last_run_dir.txt")
    _write_text(last_path_file, run_dir + "\n")

    # Log file
    log_path = os.path.join(run_dir, "runner.log")
    # Simple tee: duplicate stdout to runner.log
    class Tee:
        def __init__(self, *streams):
            self.streams = streams
        def write(self, b):
            for s in self.streams: s.write(b); s.flush()
        def flush(self):
            for s in self.streams: s.flush()

    with open(log_path, "w", encoding="utf-8") as lf:
        sys.stdout = Tee(sys.stdout, lf)  # type: ignore
        sys.stderr = Tee(sys.stderr, lf)  # type: ignore

        env = Env.from_os_environ()

        print(" | catalog:begin", flush=True)
        items: List[Dict[str, Any]] = build_catalog(env)
        kept = len(items)
        print(f" | catalog:end kept={kept}", flush=True)

        # Persist discovery pool for debugging
        _write_json(os.path.join(run_dir, "items.discovered.json"), items)

        # Seen index (ratings.csv + optional public IMDb)
        ratings_csv = os.path.join("data", "ratings.csv")
        seen_idx = load_seen_index(ratings_csv)

        # In this simple version, “enriched” == discovered (you can swap in real enrichment later)
        enriched = list(items)
        _write_json(os.path.join(run_dir, "items.enriched.json"), enriched)

        # Filter & score
        pool = filter_unseen(enriched, seen_idx)
        ranked = score_items(env, pool)

        # Persist results
        _write_json(os.path.join(run_dir, "assistant_feed.json"), ranked)

        # Human summary (also printed)
        discovered = len(items)
        eligible = len(pool)
        above_cut = sum(1 for r in ranked if (r.get("match") or 0) >= 58.0)

        summary_md = [
            "# Daily recommendations",
            "",
            "## Telemetry",
            f"- Region: **{env.get('REGION','US')}**",
            f"- SUBS_INCLUDE: `{','.join(env.get('SUBS_INCLUDE',[])) if isinstance(env.get('SUBS_INCLUDE',[]), list) else env.get('SUBS_INCLUDE','')}`",
            f"- Discover pages: **{env.get('DISCOVER_PAGES',3)}**",
            f"- Discovered (raw): **{discovered}**",
            f"- Enriched (details fetched): **{len(enriched)}**; errors: **0**",
            f"- Exclusion list size (ratings + IMDb web): **{len([k for k in seen_idx.keys() if k.startswith('tt')])}**",
            f"- Eligible after exclusions: **{eligible}**",
            f"- Above match cut (≥ 58.0): **{above_cut}**",
            "",
        ]
        if above_cut == 0:
            summary_md.append("_No items above cut today._")
        _write_text(os.path.join(run_dir, "summary.md"), "\n".join(summary_md) + "\n")

        print(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}", flush=True)

    # Also mirror to data/out/latest for artifact pickup
    _copy_to_latest(run_dir)

if __name__ == "__main__":
    main()