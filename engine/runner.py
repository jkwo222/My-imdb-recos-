# engine/runner.py
from __future__ import annotations
import csv
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List

try:
    from .env import Env
except Exception:
    from engine.env import Env  # type: ignore

try:
    from .catalog_builder import build_catalog
except Exception:
    from engine.catalog_builder import build_catalog  # type: ignore

try:
    from .self_check import run_self_check
except Exception:
    def run_self_check() -> None:
        print("SELF-CHECK: (fallback)")

try:
    from .scoring import score_items
except Exception:
    score_items = None  # type: ignore

try:
    from .exclusions import load_seen_index, filter_unseen
except Exception:
    load_seen_index = None  # type: ignore
    filter_unseen = None    # type: ignore

try:
    from . import tmdb
except Exception:
    tmdb = None  # type: ignore

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")


def _safe_json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _stamp_last_run(run_dir: Path) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "last_run_dir.txt").write_text(str(run_dir), encoding="utf-8")
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
    def _json_or_list(s: str) -> List[str]:
        s = s.strip()
        if s.startswith("["):
            try:
                import json as _json
                return [str(x).strip() for x in _json.loads(s)]
            except Exception:
                return []
        return [x.strip() for x in s.split(",") if x.strip()]

    raw_langs = os.getenv("ORIGINAL_LANGS", '["en"]')
    langs = _json_or_list(raw_langs)

    raw_subs = os.getenv("SUBS_INCLUDE", "")
    subs_list = _json_or_list(raw_subs)

    def _int_env(name: str, default: int) -> int:
        try:
            v = os.getenv(name, "")
            return int(v) if v else default
        except Exception:
            return default

    pages = _int_env("DISCOVER_PAGES", 12)
    pages = max(1, min(50, pages))

    # Pool knobs
    pool_max = _int_env("POOL_MAX_ITEMS", 20000)
    prune_at = _int_env("POOL_PRUNE_AT", 0)
    prune_keep = _int_env("POOL_PRUNE_KEEP", max(0, prune_at - 5000) if prune_at > 0 else 0)

    # Provider enrichment knob
    enrich_top_n = _int_env("ENRICH_PROVIDERS_TOP_N", 150)

    return Env.from_mapping({
        "REGION": os.getenv("REGION", "US").strip() or "US",
        "ORIGINAL_LANGS": langs,
        "SUBS_INCLUDE": subs_list,
        "DISCOVER_PAGES": pages,
        "POOL_MAX_ITEMS": pool_max,
        "POOL_PRUNE_AT": prune_at,
        "POOL_PRUNE_KEEP": prune_keep,
        "ENRICH_PROVIDERS_TOP_N": enrich_top_n,
    })


def _build_run_dir() -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rd = OUT_ROOT / f"run_{ts}"
    rd.mkdir(parents=True, exist_ok=True)
    return rd


def _markdown_summary(items: List[Dict[str, Any]], env: Env, top_n: int = 25) -> str:
    discovered = int(env.get("DISCOVERED_COUNT", 0))
    eligible = int(env.get("ELIGIBLE_COUNT", 0))
    subs = env.get("SUBS_INCLUDE", [])
    region = env.get("REGION", "US")
    pages = env.get("DISCOVER_PAGES", 1)
    prov_map = env.get("PROVIDER_MAP", {})
    unmatched = env.get("PROVIDER_UNMATCHED", [])
    pool_t = env.get("POOL_TELEMETRY", {}) or {}

    lines = []
    lines.append("# Daily recommendations\n")
    lines.append("## Telemetry")
    lines.append(f"- Region: **{region}**")
    lines.append(f"- SUBS_INCLUDE: `{','.join(subs)}`" if subs else "- SUBS_INCLUDE: _none_")
    lines.append(f"- Provider map: `{json.dumps(prov_map, ensure_ascii=False)}`")
    if unmatched:
        lines.append(f"- Provider slugs not matched this region: `{unmatched}`")
    lines.append(f"- Discover pages: **{pages}**")
    lines.append(f"- Discovered (raw): **{discovered}**")
    lines.append(f"- Eligible after exclusions: **{eligible}**")
    if pool_t:
        lines.append(f"- Pool: file_lines_before={pool_t.get('file_lines_before')} → file_lines_after={pool_t.get('file_lines_after')}, "
                     f"unique_keys_est={pool_t.get('unique_keys_est')}, loaded_unique={pool_t.get('loaded_unique')}, "
                     f"appended_this_run={pool_t.get('appended_this_run')}, pool_max_items={pool_t.get('pool_max_items')}")
        if pool_t.get("prune_at", 0):
            lines.append(f"- Pool prune policy: prune_at={pool_t.get('prune_at')}, keep={pool_t.get('prune_keep')}")
    lines.append("")
    return "\n".join(lines)

def _enrich_top_providers(items: List[Dict[str, Any]], env: Env, top_n: int = 150) -> None:
    if tmdb is None:
        return
    region = env.get("REGION", "US")
    for it in items[:top_n]:
        if it.get("providers"):
            continue
        kind = it.get("media_type")
        tid = it.get("tmdb_id")
        if not kind or not tid:
            continue
        try:
            provs = tmdb.get_title_watch_providers(kind, int(tid), region)
            if provs:
                it["providers"] = provs
        except Exception:
            pass


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
    exports_dir = run_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    log_lines: List[str] = []

    def _log(line: str) -> None:
        print(line, flush=True)
        log_lines.append(line)

    env = _env_from_os()

    if not os.getenv("TMDB_API_KEY") and not os.getenv("TMDB_BEARER"):
        msg = "[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER. Set one of them and re-run."
        _log(msg)
        _safe_json_dump(diag_path, {"error": msg})
        sys.exit(2)

    _log(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        _log(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    _log(f" | catalog:end discovered={env.get('DISCOVERED_COUNT', 0)} "
         f"pooled={len(items)} pool_file_lines={pool_t.get('file_lines_after')} loaded_unique={pool_t.get('loaded_unique')}")

    discovered = int(env.get("DISCOVERED_COUNT", 0))
    eligible = len(items)

    _safe_json_dump(run_dir / "items.discovered.json", items)

    # Exclusions (unseen via ratings.csv)
    try:
        seen_idx: Dict[str, bool] = {}
        if callable(load_seen_index):
            ratings_csv = Path("data/user/ratings.csv")
            if ratings_csv.exists():
                seen_idx = load_seen_index(ratings_csv)
                _log(f"[exclusions] loaded seen index from {ratings_csv} (n={len(seen_idx)})")
            else:
                _log("[exclusions] ratings.csv not found at data/user/ratings.csv")
        if callable(filter_unseen):
            items = filter_unseen(items, seen_idx)
        eligible = len(items)
    except Exception as ex:
        _log(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()

    # Enrich more titles with providers so the later service filter has data
    try:
        _enrich_top_providers(items, env, top_n=int(env.get("ENRICH_PROVIDERS_TOP_N", 150)))
    except Exception as ex:
        _log(f"[providers] enrichment failed: {ex!r}")

    ranked: List[Dict[str, Any]] = items
    try:
        if callable(score_items):
            ranked = score_items(env, items)
        ranked = sorted(ranked, key=lambda it: it.get("score", it.get("match", it.get("tmdb_vote", 0.0))), reverse=True)
    except Exception as ex:
        _log(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()

    def _score_of(it: Dict[str, Any]) -> float:
        try:
            return float(it.get("score", it.get("match", 0.0)) or 0.0)
        except Exception:
            return 0.0

    above_cut = sum(1 for it in ranked if _score_of(it) >= 58.0)

    _safe_json_dump(run_dir / "items.enriched.json", ranked)
    _safe_json_dump(run_dir / "assistant_feed.json", ranked)

    # Quick markdown telemetry (the digest email is built later by engine.summarize)
    try:
        (run_dir / "summary.md").write_text(_markdown_summary(ranked, env, top_n=25), encoding="utf-8")
    except Exception as ex:
        _log(f"[summary] FAILED: {ex!r}")

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
                "PROVIDER_UNMATCHED": env.get("PROVIDER_UNMATCHED", []),
                "POOL_TELEMETRY": env.get("POOL_TELEMETRY", {}),
            },
            "discover_pages": env.get("DISCOVER_PAGE_TELEMETRY", []),
            "paths": {
                "assistant_feed": str((run_dir / "assistant_feed.json").resolve()),
                "items_discovered": str((run_dir / "items.discovered.json").resolve()),
                "items_enriched": str((run_dir / "items.enriched.json").resolve()),
                "summary": str((run_dir / "summary.md").resolve()),
                "runner_log": str((run_dir / "runner.log").resolve()),
                "exports_dir": str((run_dir / "exports").resolve()),
            },
        }
        _safe_json_dump(diag_path, diag)
    except Exception:
        pass

    _stamp_last_run(run_dir)


if __name__ == "__main__":
    main()