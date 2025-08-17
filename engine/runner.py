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

# Optional scoring pieces (runner should still complete if these are absent)
try:  # pragma: no cover
    from .scoring import load_seen_index, filter_unseen, score_items
except Exception:  # pragma: no cover
    load_seen_index = None  # type: ignore
    filter_unseen = None    # type: ignore
    score_items = None      # type: ignore

# Optional diagnostics helper (safe if missing)
try:  # pragma: no cover
    from .diag import write_diag
except Exception:  # pragma: no cover
    write_diag = None  # type: ignore

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")


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
    try:
        latest.symlink_to(run_dir, target_is_directory=True)
    except Exception:
        import shutil
        shutil.copytree(run_dir, latest)


def _safe_json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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

    # DISCOVER_PAGES
    try:
        pages = int(os.getenv("DISCOVER_PAGES", "12").strip())
    except Exception:
        pages = 12

    # Compose Env (fall back to Env(mapping) if older class lacks from_mapping)
    if hasattr(Env, "from_mapping"):
        return Env.from_mapping({
            "REGION": region,
            "ORIGINAL_LANGS": langs,
            "SUBS_INCLUDE": subs_list,
            "DISCOVER_PAGES": pages,
        })
    # Back-compat shim: Env behaves like a dict
    e = Env()  # type: ignore
    try:
        e.update({  # type: ignore
            "REGION": region,
            "ORIGINAL_LANGS": langs,
            "SUBS_INCLUDE": subs_list,
            "DISCOVER_PAGES": pages,
        })
    except Exception:
        # last resort: wrap in a tiny dict-like object
        return Env.from_dict({  # type: ignore[attr-defined]
            "REGION": region,
            "ORIGINAL_LANGS": langs,
            "SUBS_INCLUDE": subs_list,
            "DISCOVER_PAGES": pages,
        })
    return e  # type: ignore


def _build_run_dir() -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rd = OUT_ROOT / f"run_{ts}"
    rd.mkdir(parents=True, exist_ok=True)
    return rd


def _write_fs_reports(root: Path) -> None:
    """
    Emit:
      - files.txt: flat list with size info under repo root (limited for safety)
      - tree.txt: a lightweight ASCII tree starting at repo root
      - env.json: selected environment snapshot
    """
    repo_root = Path(".").resolve()

    def _rel(p: Path) -> str:
        try:
            return str(p.relative_to(repo_root))
        except Exception:
            return str(p)

    files_out = root / "files.txt"
    tree_out = root / "tree.txt"
    env_out = root / "env.json"

    # files.txt
    lines: List[str] = []
    max_files = 5000
    count = 0
    for base, dirs, files in os.walk(repo_root):
        # Skip .git and venvs/caches to keep output small
        if any(seg in {".git", ".github", "__pycache__", ".venv", "venv", ".mypy_cache"} for seg in Path(base).parts):
            continue
        for fname in files:
            path = Path(base) / fname
            try:
                size = path.stat().st_size
            except Exception:
                size = -1
            lines.append(f"{_rel(path)}\t{size}")
            count += 1
            if count >= max_files:
                lines.append("... (truncated) ...")
                break
        if count >= max_files:
            break
    files_out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # tree.txt (brief)
    tree_lines: List[str] = []
    max_depth = 6
    max_entries_per_dir = 200

    def walk(prefix: str, dir_path: Path, depth: int) -> None:
        if depth > max_depth:
            tree_lines.append(prefix + "…")
            return
        try:
            entries = sorted(dir_path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except Exception:
            return
        shown = 0
        for i, entry in enumerate(entries):
            if entry.name in {".git", "__pycache__", ".venv", "venv", ".mypy_cache"}:
                continue
            connector = "└── " if i == len(entries) - 1 else "├── "
            tree_lines.append(prefix + connector + entry.name)
            shown += 1
            if entry.is_dir():
                new_prefix = prefix + ("    " if i == len(entries) - 1 else "│   ")
                walk(new_prefix, entry, depth + 1)
            if shown >= max_entries_per_dir:
                tree_lines.append(prefix + "… (truncated)")
                break

    walk("", repo_root, 0)
    tree_out.write_text("\n".join(tree_lines) + "\n", encoding="utf-8")

    # env.json snapshot of interesting vars
    env_snapshot = {
        "REGION": os.getenv("REGION"),
        "ORIGINAL_LANGS": os.getenv("ORIGINAL_LANGS"),
        "SUBS_INCLUDE": os.getenv("SUBS_INCLUDE"),
        "DISCOVER_PAGES": os.getenv("DISCOVER_PAGES"),
        "HAS_TMDB_ACCESS_TOKEN": bool(os.getenv("TMDB_ACCESS_TOKEN")),
        "HAS_TMDB_API_KEY": bool(os.getenv("TMDB_API_KEY")),
        "PYTHON": sys.version,
        "CWD": str(Path.cwd()),
    }
    _safe_json_dump(env_out, env_snapshot)


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
    started_ts = time.time()

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
    log_lines: List[str] = []

    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

    # Build Env
    env = _env_from_os()

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
    ranked: List[Dict[str, Any]]
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
        # still write a minimal assistant_feed so the workflow has something
        ranked = list(final_list)
        _safe_json_dump(run_dir / "assistant_feed.json", ranked)

    _log(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}")

    # Write log file
    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    except Exception:
        pass

    # Emit diag.json if helper is available
    try:
        if callable(write_diag):
            # We don’t have provider_ids here; catalog_builder prints them.
            # Capture a compact env snapshot:
            env_snapshot = {
                "REGION": env.get("REGION", "US"),
                "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS", []),
                "SUBS_INCLUDE": env.get("SUBS_INCLUDE", []),
                "DISCOVER_PAGES": env.get("DISCOVER_PAGES", 0),
            }
            write_diag(
                run_dir,
                discovered=int(discovered),
                eligible=int(eligible),
                above_cut=int(above_cut),
                provider_ids=[],  # unknown at this layer
                env_snapshot=env_snapshot,
                started_ts=started_ts,
                finished_ts=time.time(),
                notes="runner emitted diag.json",
            )
    except Exception:
        # Never fail the run on diag issues
        pass

    # Extra filesystem & env reports for debug bundle
    try:
        _write_fs_reports(run_dir)
    except Exception:
        pass

    # Finally stamp last run markers/links
    _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()