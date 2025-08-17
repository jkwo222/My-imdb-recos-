# engine/runner.py
from __future__ import annotations
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List

from .env import Env
from .catalog_builder import build_catalog
from .self_check import run_self_check

try:
    from .scoring import score_items  # type: ignore
except Exception:
    score_items = None  # type: ignore

try:
    from .exclusions import load_seen_index, filter_unseen  # type: ignore
except Exception:
    filter_unseen = None  # type: ignore
    load_seen_index = None  # type: ignore

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")


def _safe_json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _stamp_last_run(run_dir: Path) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    marker = OUT_ROOT / "last_run_dir.txt"
    marker.write_text(str(run_dir), encoding="utf-8")

    latest = OUT_ROOT / "latest"
    if latest.exists():
        if latest.is_symlink() or latest.is_file():
            latest.unlink()
        else:
            import shutil
            shutil.rmtree(latest, ignore_errors=True)

    try:
        from os import path as _osp
        target_rel = _osp.relpath(run_dir.resolve(), OUT_ROOT.resolve())
        latest.symlink_to(target_rel, target_is_directory=True)
    except Exception:
        import shutil
        shutil.copytree(run_dir, latest)


def _env_from_os() -> Env:
    region = os.getenv("REGION", "US").strip() or "US"
    raw_langs = os.getenv("ORIGINAL_LANGS", '["en"]').strip()
    try:
        if raw_langs.startswith("["):
            langs = [str(x).strip() for x in json.loads(raw_langs) if str(x).strip()]
        else:
            langs = [x.strip() for x in raw_langs.split(",") if x.strip()]
    except Exception:
        langs = ["en"]

    raw_subs = os.getenv("SUBS_INCLUDE", "").strip()
    if raw_subs.startswith("["):
        try:
            subs_list = [str(x).strip() for x in json.loads(raw_subs)]
        except Exception:
            subs_list = []
    else:
        subs_list = [x.strip() for x in raw_subs.split(",") if x.strip()]

    pages_env = os.getenv("DISCOVER_PAGES", "").strip()
    try:
        pages = int(pages_env) if pages_env else 12
    except Exception:
        pages = 12
    pages = max(1, min(50, pages))

    return Env.from_mapping({
        "REGION": region,
        "ORIGINAL_LANGS": langs,
        "SUBS_INCLUDE": subs_list,
        "DISCOVER_PAGES": pages,
    })


def _build_run_dir() -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rd = OUT_ROOT / f"run_{ts}"
    rd.mkdir(parents=True, exist_ok=True)
    return rd


def _summarize(items_scored: List[Dict[str, Any]], env: Env) -> str:
    discovered = int(env.get("DISCOVERED_COUNT", 0))
    eligible = int(env.get("ELIGIBLE_COUNT", 0))
    above_cut = int(env.get("ABOVE_CUT_COUNT", 0))
    subs = env.get("SUBS_INCLUDE", [])
    region = env.get("REGION", "US")
    pages = env.get("DISCOVER_PAGES", 1)
    prov_map = env.get("PROVIDER_MAP", {})

    lines = []
    lines.append("# Daily recommendations\n")
    lines.append("## Telemetry\n")
    lines.append(f"- Region: **{region}**")
    lines.append(f"- SUBS_INCLUDE: `{','.join(subs)}`" if subs else "- SUBS_INCLUDE: _none_")
    lines.append(f"- Provider map: {json.dumps(prov_map, ensure_ascii=False)}")
    lines.append(f"- Discover pages: **{pages}**")
    lines.append(f"- Discovered (raw): **{discovered}**")
    lines.append(f"- Eligible after exclusions: **{eligible}**")
    lines.append(f"- Above match cut (≥ 58.0): **{above_cut}**\n")

    top = items_scored[:30]
    lines.append("## Top candidates\n")
    lines.append(json.dumps(top, ensure_ascii=False, indent=2))
    return "\n".join(lines)


def main() -> None:
    t0 = time.time()
    try:
        run_self_check()
    except SystemExit as e:
        print(str(e), file=sys.stderr, flush=True)
        raise

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)

    run_dir = _build_run_dir()
    log_path = run_dir / "runner.log"
    diag_path = run_dir / "diag.json"
    log_lines: List[str] = []

    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

    env = _env_from_os()

    # Env validation for TMDB
    if not os.getenv("TMDB_API_KEY") and not os.getenv("TMDB_BEARER"):
        msg = "[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER. Set one of them and re-run."
        _log(msg)
        _safe_json_dump(diag_path, {"error": msg})
        sys.exit(2)

    # Build and pool catalog
    _log(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        _log(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    _log(f" | catalog:end discovered={env.get('DISCOVERED_COUNT', 0)} pooled={len(items)}")

    discovered = int(env.get("DISCOVERED_COUNT", 0))
    eligible = len(items)
    above_cut = 0

    _safe_json_dump(run_dir / "items.discovered.json", items)

    # Exclusions (unseen)
    final_list: List[Dict[str, Any]] = items
    try:
        seen_idx: Dict[str, bool] = {}
        if callable(load_seen_index):
            ratings_csv = Path("data/user/ratings.csv")
            if ratings_csv.exists():
                seen_idx = load_seen_index(ratings_csv)  # imdb ids
                _log(f"[exclusions] loaded seen index from {ratings_csv} (n={len(seen_idx)})")
            else:
                _log("[exclusions] ratings.csv not found at data/user/ratings.csv")
        if callable(filter_unseen):
            final_list = filter_unseen(final_list, seen_idx)  # drops imdb_id in seen
        eligible = len(final_list)
    except Exception as ex:
        _log(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()

    # Scoring
    try:
        if callable(score_items):
            ranked = score_items(env, final_list)  # type: ignore
        else:
            ranked = final_list
        try:
            above_cut = sum(1 for it in ranked if float(it.get("score", 0.0)) >= 58.0)
        except Exception:
            above_cut = 0

        _safe_json_dump(run_dir / "items.enriched.json", ranked)
        _safe_json_dump(run_dir / "assistant_feed.json", ranked)
        (run_dir / "summary.md").write_text(_summarize(ranked, env), encoding="utf-8")

    except Exception as ex:
        _log(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()

    _log(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}")

    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    except Exception:
        pass

    try:
        diag = {
            "ran_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "run_seconds": round(time.time() - t0, 3),
            "env": {
                "REGION": env.get("REGION", "US"),
                "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS", []),
                "SUBS_INCLUDE": env.get("SUBS_INCLUDE", []),
                "DISCOVER_PAGES": env.get("DISCOVER_PAGES", 0),
                "PROVIDER_MAP": env.get("PROVIDER_MAP", {}),
            },
            "discover_pages": env.get("DISCOVER_PAGE_TELEMETRY", []),
            "paths": {
                "assistant_feed": str((run_dir / "assistant_feed.json").resolve()),
                "items_discovered": str((run_dir / "items.discovered.json").resolve()),
                "items_enriched": str((run_dir / "items.enriched.json").resolve()),
                "summary": str((run_dir / "summary.md").resolve()),
                "runner_log": str((run_dir / "runner.log").resolve()),
            },
        }
        _safe_json_dump(diag_path, diag)
    except Exception:
        pass

    _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()