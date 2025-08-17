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
    from .scoring import load_seen_index, filter_unseen, score_items
except Exception:  # pragma: no cover
    load_seen_index = None  # type: ignore
    filter_unseen = None    # type: ignore
    score_items = None      # type: ignore

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")


def _safe_json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _stamp_last_run(run_dir: Path) -> None:
    """
    - Write data/out/last_run_dir.txt
    - Refresh data/out/latest to point to run_dir
      * Prefer a symlink
      * Validate the symlink points to the intended directory
      * Fallback to copy if symlinks are blocked
    - Write a tiny sanity report into run_dir (helps debug future issues)
    """
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    marker = OUT_ROOT / "last_run_dir.txt"
    marker.write_text(str(run_dir), encoding="utf-8")

    latest = OUT_ROOT / "latest"

    # Remove any existing file/dir/symlink at latest
    if latest.exists() or latest.is_symlink():
        try:
            if latest.is_dir() and not latest.is_symlink():
                import shutil
                shutil.rmtree(latest, ignore_errors=True)
            else:
                latest.unlink()
        except Exception:
            # Last-ditch: rename out of the way
            try:
                latest.rename(OUT_ROOT / f"latest.old.{int(time.time())}")
            except Exception:
                pass

    # Try to create a symlink
    created_symlink = False
    try:
        # Use an absolute path target to avoid weird relative resolutions
        latest.symlink_to(run_dir.resolve(), target_is_directory=True)
        created_symlink = True
    except Exception:
        created_symlink = False

    # Validate symlink points to the intended directory
    if created_symlink:
        try:
            resolved = latest.resolve(strict=True)
            if resolved != run_dir.resolve():
                # Bad link (e.g., loops to data/out) -> replace with copy
                created_symlink = False
                latest.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            # Could not resolve; treat as bad
            created_symlink = False
            try:
                latest.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass

    # Fallback to copytree if symlink wasn’t created or failed validation
    if not created_symlink:
        import shutil
        shutil.copytree(run_dir, latest)

    # Write a small sanity file in the run dir
    sanity = {
        "run_dir": str(run_dir.resolve()),
        "latest_exists": latest.exists() or latest.is_symlink(),
        "latest_is_symlink": latest.is_symlink(),
        "latest_points_to": (str(latest.resolve()) if latest.exists() or latest.is_symlink() else None),
        "timestamp": int(time.time()),
    }
    _safe_json_dump(run_dir / "links.sanity.json", sanity)


def _env_from_os() -> Env:
    """
    Build Env from OS environment with safe coercions.
    The Env class supports dict-like access (get) and attributes.
    Also write a one-shot options sanity file to the run directory (set by caller).
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

    # DISCOVER_PAGES
    try:
        pages = int(os.getenv("DISCOVER_PAGES", "12").strip())
    except Exception:
        pages = 12

    # Compose Env
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
    # Self-check (tmdb discover functions present, etc.)
    try:
        run_self_check()
    except SystemExit as e:
        print(str(e), file=sys.stderr, flush=True)
        raise

    # Prepare dirs
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)

    # Create a new run directory (early, so we can drop sanity dumps even on failure)
    run_dir = _build_run_dir()

    # Build Env
    env = _env_from_os()

    # Persist a one-shot options sanity file inside the run dir (permanent aid for debugging)
    try:
        _safe_json_dump(run_dir / "options.sanity.json", {
            "REGION": env.get("REGION"),
            "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS"),
            "SUBS_INCLUDE": env.get("SUBS_INCLUDE"),
            "DISCOVER_PAGES": env.get("DISCOVER_PAGES"),
            "timestamp": int(time.time()),
        })
    except Exception:
        pass

    # Logging capture
    log_path = run_dir / "runner.log"
    log_lines: List[str] = []

    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

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

    # Write discovered
    _safe_json_dump(run_dir / "items.discovered.json", items)

    # Apply exclusions and scoring if available
    final_list: List[Dict[str, Any]] = items
    try:
        # Exclusions: local ratings.csv + optional IMDb public
        seen_idx = {}
        if callable(load_seen_index):
            ratings_csv = Path("data/ratings.csv")
            seen_idx = load_seen_index(str(ratings_csv))
        if callable(filter_unseen):
            final_list = filter_unseen(final_list, seen_idx)  # type: ignore
        eligible = len(final_list)

        # Scoring
        if callable(score_items):
            ranked = score_items(env, final_list)  # type: ignore
        else:
            # Fallback trivial ranking if scoring not wired
            ranked = list(final_list)

        # Decide cut
        def _get_match(d: Dict[str, Any]) -> float:
            v = d.get("match")
            if isinstance(v, (int, float)):
                return float(v)
            va = d.get("vote_average")
            try:
                return float(va) * 10.0 if va is not None else 0.0
            except Exception:
                return 0.0

        ranked.sort(key=_get_match, reverse=True)
        above_cut = sum(1 for r in ranked if _get_match(r) >= 58.0)

        # Persist enriched/scored lists
        _safe_json_dump(run_dir / "items.enriched.json", ranked)

        # Assistant feed (what your site likely consumes)
        _safe_json_dump(run_dir / "assistant_feed.json", ranked)

        # Summary
        env["DISCOVERED_COUNT"] = discovered
        env["ELIGIBLE_COUNT"] = eligible
        env["ABOVE_CUT_COUNT"] = above_cut
        (run_dir / "summary.md").write_text(_summarize(ranked, env), encoding="utf-8")

    except Exception as ex:
        _log(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()

    _log(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}")

    # Write log file then stamp last run (with robust symlink handling)
    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    finally:
        _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()