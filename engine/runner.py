# engine/runner.py
from __future__ import annotations
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List

# Local imports
from .env import Env
from .catalog_builder import build_catalog
from .self_check import run_self_check

# Optional scoring pieces (keep the import loose so runner still finishes if scoring changes)
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
    """
    Write data/out/last_run_dir.txt and refresh data/out/latest -> run_dir.
    Prefer a symlink for speed; copy if symlinks are disallowed.
    """
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
    """
    Build Env from OS environment with safe coercions.
    The Env class supports dict-like access (get) and attributes.
    """
    # REGION
    region = os.getenv("REGION", "US").strip() or "US"

    # ORIGINAL_LANGS can arrive as a JSON-looking string '["en"]' or CSV like 'en,es'
    raw_langs = os.getenv("ORIGINAL_LANGS", '["en"]').strip()
    langs: List[str]
    if raw_langs.startswith("["):
        try:
            parsed = json.loads(raw_langs)
            langs = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            langs = ["en"]
    else:
        langs = [x.strip() for x in raw_langs.split(",") if x.strip()]
        if not langs:
            langs = ["en"]

    # SUBS_INCLUDE arrives as CSV (preferred), but allow JSON array too
    raw_subs = os.getenv("SUBS_INCLUDE", "").strip()
    if raw_subs.startswith("["):
        try:
            subs_list = [str(x).strip() for x in json.loads(raw_subs)]
        except Exception:
            subs_list = []
    else:
        subs_list = [x.strip() for x in raw_subs.split(",") if x.strip()]

    # DISCOVER_PAGES (supports legacy TMDB_PAGES_MOVIE/TV)
    pages_env = os.getenv("DISCOVER_PAGES", "").strip()
    if not pages_env:
        # Legacy compatibility
        movie_pages = os.getenv("TMDB_PAGES_MOVIE", "").strip()
        tv_pages = os.getenv("TMDB_PAGES_TV", "").strip()
        # Choose the larger to approximate prior behavior
        try:
            candidates = [int(x) for x in [movie_pages, tv_pages] if x]
            pages = max(candidates) if candidates else 12
        except Exception:
            pages = 12
    else:
        try:
            pages = int(pages_env)
        except Exception:
            pages = 12

    # ---- permanent sanity checks ----
    if pages < 1:
        pages = 1
    if pages > 50:
        # hard cap to keep API/time under control; adjust if you want
        pages = 50

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
    # Minimal human summary for summary.md
    discovered = int(env.get("DISCOVERED_COUNT", 0))
    eligible = int(env.get("ELIGIBLE_COUNT", 0))
    above_cut = int(env.get("ABOVE_CUT_COUNT", 0))
    subs = env.get("SUBS_INCLUDE", [])
    region = env.get("REGION", "US")
    pages = env.get("DISCOVER_PAGES", 1)

    lines = []
    lines.append("# Daily recommendations\n")
    lines.append("## Telemetry\n")
    lines.append(f"- Region: **{region}**")
    lines.append(f"- SUBS_INCLUDE: `{','.join(subs)}`" if subs else "- SUBS_INCLUDE: _none_")
    lines.append(f"- Discover pages: **{pages}**")
    lines.append(f"- Discovered (raw): **{discovered}**")
    lines.append(f"- Eligible after exclusions: **{eligible}**")
    lines.append(f"- Above match cut (≥ 58.0): **{above_cut}**\n")

    # simple list of top few
    top = items_scored[:30]
    lines.append(json.dumps(top, ensure_ascii=False, indent=2))
    return "\n".join(lines)


def main() -> None:
    t0 = time.time()
    # Self-check (tmdb discover functions present, etc.)
    try:
        run_self_check()
    except SystemExit as e:
        print(str(e), file=sys.stderr, flush=True)
        raise

    # Prepare dirs
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)

    # Create a new run directory
    run_dir = _build_run_dir()
    log_path = run_dir / "runner.log"
    diag_path = run_dir / "diag.json"
    log_lines: List[str] = []

    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

    # Build Env
    env = _env_from_os()

    # Validate critical environment
    missing_hard = []
    if not os.getenv("TMDB_API_KEY") and not os.getenv("TMDB_BEARER"):
        missing_hard.append("TMDB_API_KEY or TMDB_BEARER")
    if missing_hard:
        msg = f"[env] Missing required environment: {', '.join(missing_hard)}. Set these and re-run."
        _log(msg)
        # Write a minimal diag detailing the problem
        try:
            _safe_json_dump(diag_path, {"error": msg, "missing": missing_hard})
        except Exception:
            pass
        sys.exit(2)

    # Soft warnings (none needed if you are not using OMDb)
    # If you want a reminder, uncomment below:
    # if not os.getenv("OMDB_API_KEY"):
    #     _log("[env] WARN: OMDB_API_KEY not set (OMDb enrichment disabled)")

    ratings_csv_1 = Path("data/user/ratings.csv")
    ratings_csv_2 = Path("data/ratings.csv")
    if not os.getenv("IMDB_USER_ID") and not ratings_csv_1.exists() and not ratings_csv_2.exists():
        _log("[env] WARN: IMDB_USER_ID and ratings.csv missing (unseen filtering disabled)")

    # Discover pool
    _log(" | catalog:begin")
    try:
        items = build_catalog(env)  # list of dicts
    except Exception as ex:
        _log(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    kept = len(items)
    _log(f" | catalog:end kept={kept}")

    # Default telemetry
    discovered = kept
    eligible = kept
    above_cut = 0

    # Persist discovered
    _safe_json_dump(run_dir / "items.discovered.json", items)

    # Apply exclusions and scoring if available
    final_list: List[Dict[str, Any]] = items
    ratings_source = None
    seen_count_hint = 0
    try:
        # Exclusions: local ratings.csv + optional IMDb public
        seen_idx: Dict[str, bool] = {}
        if callable(load_seen_index):
            # Prefer data/user/ratings.csv, fallback to data/ratings.csv
            ratings_csv = Path("data/user/ratings.csv")
            if not ratings_csv.exists():
                ratings_csv = Path("data/ratings.csv")
            if ratings_csv.exists():
                try:
                    ratings_source = str(ratings_csv)
                    seen_idx = load_seen_index(ratings_csv)  # type: ignore
                    seen_count_hint = sum(1 for k, v in seen_idx.items() if isinstance(k, str) and k.startswith("tt"))
                    _log(f"[scoring] loaded ratings from {ratings_source} (~{seen_count_hint} imdb ids)")
                except Exception as sx:
                    _log(f"[scoring] FAILED to load ratings from {ratings_csv}: {sx!r}")
            else:
                _log("[scoring] no ratings.csv found under data/user or data/")

        if callable(filter_unseen):
            final_list = filter_unseen(final_list, seen_idx)  # type: ignore
        eligible = len(final_list)

        # Scoring
        if callable(score_items):
            ranked = score_items(env, final_list)  # type: ignore
        else:
            ranked = final_list

        # Cutoff and counters
        try:
            above_cut = sum(1 for it in ranked if float(it.get("score", 0.0)) >= 58.0)
        except Exception:
            above_cut = 0

        # Persist outputs
        _safe_json_dump(run_dir / "items.enriched.json", ranked)
        _safe_json_dump(run_dir / "assistant_feed.json", ranked)
        (run_dir / "summary.md").write_text(_summarize(ranked, env), encoding="utf-8")

    except Exception as ex:
        _log(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()

    # Final counts log
    _log(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}")

    # Write log file
    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    except Exception:
        pass

    # Write a compact diag.json for the debug bundle
    try:
        diag = {
            "ran_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "run_seconds": round(time.time() - t0, 3),
            "env": {
                "REGION": env.get("REGION", "US"),
                "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS", []),
                "SUBS_INCLUDE": env.get("SUBS_INCLUDE", []),
                "DISCOVER_PAGES": env.get("DISCOVER_PAGES", 0),
            },
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

    # Stamp last run files/links
    _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()