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
from .tmdb import get_title_watch_providers, providers_from_env

# Optional scoring pieces
try:
    from .scoring import load_seen_index, filter_unseen, score_items, seen_index_stats
except Exception:  # pragma: no cover
    load_seen_index = None  # type: ignore
    filter_unseen = None    # type: ignore
    score_items = None      # type: ignore
    seen_index_stats = None # type: ignore

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
    region = os.getenv("REGION", "US").strip() or "US"

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

    raw_subs = os.getenv("SUBS_INCLUDE", "").strip()
    if raw_subs.startswith("["):
        try:
            subs_list = [str(x).strip() for x in json.loads(raw_subs)]
        except Exception:
            subs_list = []
    else:
        subs_list = [x.strip() for x in raw_subs.split(",") if x.strip()]

    try:
        pages = int(os.getenv("DISCOVER_PAGES", "12").strip())
    except Exception:
        pages = 12

    # rotation / caps (optional)
    def _to_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)).strip())
        except Exception:
            return default

    rotate_minutes = _to_int("ROTATE_MINUTES", 180)
    page_cap = _to_int("DISCOVER_PAGE_CAP", 200)
    rotate_step = _to_int("ROTATE_STEP", 17)

    return Env.from_mapping({
        "REGION": region,
        "ORIGINAL_LANGS": langs,
        "SUBS_INCLUDE": subs_list,
        "DISCOVER_PAGES": pages,
        "ROTATE_MINUTES": rotate_minutes,
        "DISCOVER_PAGE_CAP": page_cap,
        "ROTATE_STEP": rotate_step,
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
    lines = []
    lines.append("# Daily recommendations\n")
    lines.append("## Telemetry\n")
    lines.append(f"- Region: **{region}**")
    lines.append(f"- SUBS_INCLUDE: `{','.join(subs)}`" if subs else "- SUBS_INCLUDE: _none_")
    lines.append(f"- Discover pages: **{pages}**")
    lines.append(f"- Discovered (raw): **{discovered}**")
    lines.append(f"- Eligible after exclusions: **{eligible}**")
    lines.append(f"- Above match cut (≥ 58.0): **{above_cut}**\n")
    top = items_scored[:30]
    lines.append(json.dumps(top, ensure_ascii=False, indent=2))
    return "\n".join(lines)


def _enrich_watch_lists(items: List[Dict[str, Any]], region: str, max_calls: int = 600) -> Dict[str, int]:
    """
    Add watch_available list for each item using per-title provider API.
    Returns counters for diagnostics.
    """
    ok, fail, skipped = 0, 0, 0
    calls = 0
    for it in items:
        kind = "movie" if (it.get("media_type") == "movie") else ("tv" if it.get("media_type") == "tv" else None)
        tid = it.get("tmdb_id")
        if not kind or not tid:
            skipped += 1
            continue
        if calls >= max_calls:
            skipped += 1
            continue
        provs = get_title_watch_providers(kind, int(tid), region)
        it["watch_available"] = provs
        calls += 1
        if provs:
            ok += 1
        else:
            fail += 1
    return {"ok": ok, "fail": fail, "skipped": skipped, "api_calls": calls}


def main() -> None:
    # Self-check (tmdb discover functions present, etc.)
    try:
        run_self_check()
    except SystemExit as e:
        print(str(e), file=sys.stderr, flush=True)
        raise

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)

    run_dir = _build_run_dir()
    log_path = run_dir / "runner.log"
    log_lines: List[str] = []
    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

    env = _env_from_os()

    # Discover pool
    _log(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        _log(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    kept = len(items)
    _log(f" | catalog:end kept={kept}")

    discovered = kept
    eligible = kept
    above_cut = 0

    # Write discovered
    _safe_json_dump(run_dir / "items.discovered.json", items)

    # Diagnostics object we'll write as diag.json at the end
    diag: Dict[str, Any] = {
        "region": env.get("REGION", "US"),
        "langs": env.get("ORIGINAL_LANGS", []),
        "pages_requested": env.get("DISCOVER_PAGES", 0),
        "rotate_minutes": env.get("ROTATE_MINUTES", 0),
        "page_cap": env.get("DISCOVER_PAGE_CAP", 0),
        "subs_include": env.get("SUBS_INCLUDE", []),
        "imdb_user_id_set": 1 if os.getenv("IMDB_USER_ID", "").strip() else 0,
        "tmdb_token_set": 1 if os.getenv("TMDB_ACCESS_TOKEN", "").strip() else 0,
        "ratings_csv_present": Path("data/ratings.csv").exists(),
    }

    # Apply exclusions + enrich + scoring
    final_list: List[Dict[str, Any]] = items
    try:
        # Exclusions: local ratings.csv
        seen_idx: Dict[str, Any] = {}
        ratings_csv = Path("data/ratings.csv")
        if callable(load_seen_index):
            seen_idx = load_seen_index(str(ratings_csv))
            if callable(seen_index_stats):
                id_count, title_count, csv_stats, web_stats = seen_index_stats(seen_idx)  # type: ignore
                _log(f"[seen] ratings.csv rows={csv_stats.get('rows',0)} ids={csv_stats.get('ids',0)} titles={csv_stats.get('titles',0)}")
                diag["seen_index"] = {
                    "ids": id_count, "titles": title_count,
                    "csv_stats": csv_stats, "web_stats": web_stats,
                }
        if callable(filter_unseen):
            before = len(final_list)
            final_list = filter_unseen(final_list, seen_idx)  # type: ignore
            after = len(final_list)
            _log(f"[seen] filtered by seen index: before={before} after={after} dropped={before-after}")
        eligible = len(final_list)

        # Per-title watch provider enrichment (so feed shows watch_available)
        enrich_counts = _enrich_watch_lists(final_list, env.get("REGION", "US"), max_calls=800)
        _log(f"[enrich] watch providers: {enrich_counts}")
        diag["enrich"] = enrich_counts

        # Scoring
        if callable(score_items):
            ranked = score_items(env, final_list)  # type: ignore
        else:
            ranked = list(final_list)

        # Cut (unchanged)
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

        _safe_json_dump(run_dir / "items.enriched.json", ranked)
        _safe_json_dump(run_dir / "assistant_feed.json", ranked)

        env["DISCOVERED_COUNT"] = discovered
        env["ELIGIBLE_COUNT"] = eligible
        env["ABOVE_CUT_COUNT"] = above_cut
        (run_dir / "summary.md").write_text(_summarize(ranked, env), encoding="utf-8")

    except Exception as ex:
        _log(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()

    _log(f" | results: discovered={discovered} eligible={eligible} above_cut={above_cut}")

    # diag.json
    try:
        _safe_json_dump(run_dir / "diag.json", diag)
    except Exception:
        pass

    # Write log file and stamp last run files/links
    try:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    finally:
        _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()