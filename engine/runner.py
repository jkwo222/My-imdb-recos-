# engine/runner.py
from __future__ import annotations
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Local imports
from .env import Env
from .catalog_builder import build_catalog
from .self_check import run_self_check

# Optional scoring pieces (keep imports loose so runner still finishes if scoring changes)
try:
    from .scoring import load_seen_index, filter_unseen, score_items
except Exception:  # pragma: no cover
    load_seen_index = None  # type: ignore
    filter_unseen = None    # type: ignore
    score_items = None      # type: ignore

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")
REPO_ROOT = Path(__file__).resolve().parents[1]  # project root


# ----------------------------
# Small helpers
# ----------------------------

def _safe_json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _build_run_dir() -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rd = OUT_ROOT / f"run_{ts}"
    rd.mkdir(parents=True, exist_ok=True)
    return rd


def _stamp_last_run(run_dir: Path) -> None:
    """
    Write data/out/last_run_dir.txt and refresh data/out/latest -> run_dir.
    Symlink when possible, otherwise copy.
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

    # Try symlink first; fall back to copy where symlinks are blocked
    try:
        latest.symlink_to(run_dir, target_is_directory=True)
    except Exception:
        import shutil
        shutil.copytree(run_dir, latest)


def _prune_old_runs(keep: int, logger) -> None:
    """
    Delete older data/out/run_* directories, keeping `keep` most recent.
    keep <= 0 disables pruning.
    """
    try:
        if keep <= 0:
            return
        runs = sorted([p for p in OUT_ROOT.glob("run_*") if p.is_dir()],
                      key=lambda p: p.stat().st_mtime, reverse=True)
        to_delete = runs[keep:]
        for d in to_delete:
            try:
                import shutil
                shutil.rmtree(d, ignore_errors=True)
                logger(f"[housekeeping] pruned {d.name}")
            except Exception as ex:
                logger(f"[housekeeping] prune failed for {d.name}: {ex!r}")
    except Exception as ex:
        logger(f"[housekeeping] prune scan failed: {ex!r}")


def _write_env_snap(run_dir: Path, env: Env) -> None:
    """
    Persist a snapshot of the env we actually used.
    """
    # JSON
    try:
        _safe_json_dump(run_dir / "env.json", dict(env))  # Env is mapping-like
    except Exception:
        # very defensive: if __iter__ misbehaves, fallback
        fallback = {
            "REGION": env.get("REGION", None),
            "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS", None),
            "SUBS_INCLUDE": env.get("SUBS_INCLUDE", None),
            "DISCOVER_PAGES": env.get("DISCOVER_PAGES", None),
        }
        _safe_json_dump(run_dir / "env.json", fallback)

    # TXT (greppable)
    lines = []
    for k in ("REGION", "ORIGINAL_LANGS", "SUBS_INCLUDE", "DISCOVER_PAGES"):
        lines.append(f"{k}={env.get(k, '')}")
    (run_dir / "env.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_tree(path: Path, out_file: Path, max_files: int = 5000) -> None:
    """
    Produce a simple tree/listing with file sizes and (for symlinks) targets.
    Bounded to avoid giant logs.
    """
    def fmt(rel: Path, full: Path) -> str:
        try:
            if full.is_symlink():
                tgt = os.readlink(str(full))
                return f"{rel} -> {tgt}"
            sz = full.stat().st_size if full.is_file() else 0
            return f"{rel} ({sz} B)"
        except Exception:
            return f"{rel} (?)"

    rows: List[str] = []
    try:
        count = 0
        for root, dirs, files in os.walk(path):
            root_p = Path(root)
            # sort for stability
            for name in sorted(dirs + files):
                if count >= max_files:
                    rows.append(f"... truncated at {max_files} entries ...")
                    raise StopIteration
                full = root_p / name
                rel = full.relative_to(path)
                rows.append(fmt(rel, full))
                count += 1
    except StopIteration:
        pass
    except Exception as ex:
        rows.append(f"[tree] error: {ex!r}")
    out_file.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _env_from_os() -> Env:
    """
    Build Env from OS environment with safe coercions.
    The Env class supports dict-like access (get) and attributes.
    """
    # REGION
    region = os.getenv("REGION", "US").strip() or "US"

    # ORIGINAL_LANGS can arrive as JSON '["en"]' or CSV 'en,es'
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

    # Optional pruning (0 = no prune)
    try:
        prune_keep = int(os.getenv("PRUNE_RUNS_KEEP", "0").strip())
    except Exception:
        prune_keep = 0

    # Compose Env (mapping-like)
    return Env.from_mapping({
        "REGION": region,
        "ORIGINAL_LANGS": langs,
        "SUBS_INCLUDE": subs_list,
        "DISCOVER_PAGES": pages,
        "PRUNE_RUNS_KEEP": prune_keep,
    })


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
    # simple list of top few for quick eyeballing
    top = items_scored[:30]
    lines.append(json.dumps(top, ensure_ascii=False, indent=2))
    return "\n".join(lines)


# ----------------------------
# Main
# ----------------------------

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

    # Create a new run directory + logging buffer
    run_dir = _build_run_dir()
    log_path = run_dir / "runner.log"
    log_lines: List[str] = []

    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

    # Build Env
    env = _env_from_os()
    _write_env_snap(run_dir, env)

    # Optional prune of old runs
    _prune_old_runs(int(env.get("PRUNE_RUNS_KEEP", 0) or 0), _log)

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

    # Persist discovered pool
    _safe_json_dump(run_dir / "items.discovered.json", items)

    # Permanent “sanity check” of seen index (IDs + title/year pairs)
    seen_idx: Dict[str, Any] = {}
    try:
        if callable(load_seen_index):
            ratings_csv = Path("data/ratings.csv")
            seen_idx = load_seen_index(str(ratings_csv))  # may also scrape IMDb public via IMDB_USER_ID
            num_ids = sum(1 for k in seen_idx.keys() if isinstance(k, str) and k.startswith("tt"))
            num_pairs = len(seen_idx.get("_titles_norm_pairs", [])) if isinstance(seen_idx, dict) else 0
            _log(f"[seen] loaded imdb_ids={num_ids} title_pairs={num_pairs}")
            _safe_json_dump(run_dir / "seen.stats.json", {
                "imdb_ids_count": num_ids,
                "title_pairs_count": num_pairs,
                "source_csv_exists": Path("data/ratings.csv").exists(),
                "imdb_user_id_env": bool(os.getenv("IMDB_USER_ID", "").strip()),
            })
        else:
            _log("[seen] scoring.load_seen_index unavailable; skipping seen sanity snapshot")
    except Exception as ex:
        _log(f"[seen] FAILED to build seen index: {ex!r}")
        traceback.print_exc()

    # Apply exclusions and scoring if available
    final_list: List[Dict[str, Any]] = items
    try:
        # Exclusions by seen index
        if callable(filter_unseen):
            final_list = filter_unseen(final_list, seen_idx)  # type: ignore
        else:
            _log("[scoring] filter_unseen unavailable; skipping exclusions")
        eligible = len(final_list)

        # Scoring
        if callable(score_items):
            ranked = score_items(env, final_list)  # type: ignore
        else:
            _log("[scoring] score_items unavailable; passing through unscored items")
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

    # Final result line for CI logs
    _log(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}")

    # Extra diagnostics: repo tree + cache tree (compact)
    try:
        _write_tree(REPO_ROOT, run_dir / "tree.txt", max_files=5000)
    except Exception as ex:
        _log(f"[diag] tree failed: {ex!r}")
    try:
        _write_tree(CACHE_ROOT, run_dir / "cache_tree.txt", max_files=5000)
    except Exception as ex:
        _log(f"[diag] cache tree failed: {ex!r}")

    # Write log file and stamp last run files/links
    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    finally:
        _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()