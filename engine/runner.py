# engine/runner.py
from __future__ import annotations
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

from .exclusions import (
    load_seen_index as _load_seen_index,
    filter_unseen as _filter_unseen,
    merge_with_public as _merge_seen_public,
)

from .profile import build_user_model
from . import tmdb

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
        s = (s or "").strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                import json as _json
                return [str(x).strip() for x in _json.loads(s)]
            except Exception:
                return []
        return [x.strip() for x in s.split(",") if x.strip()]

    raw_langs = os.getenv("ORIGINAL_LANGS", '["en"]')
    langs = _json_or_list(raw_langs)
    subs_list = _json_or_list(os.getenv("SUBS_INCLUDE", ""))

    def _int_env(name: str, default: int) -> int:
        try:
            v = os.getenv(name, "")
            return int(v) if v else default
        except Exception:
            return default

    pages = max(1, min(50, _int_env("DISCOVER_PAGES", 12)))
    pool_max = _int_env("POOL_MAX_ITEMS", 20000)
    prune_at = _int_env("POOL_PRUNE_AT", 0)
    prune_keep = _int_env("POOL_PRUNE_KEEP", max(0, prune_at - 5000) if prune_at > 0 else 0)
    enrich_providers_n = _int_env("ENRICH_PROVIDERS_TOP_N", 220)
    enrich_scoring_n = _int_env("ENRICH_SCORING_TOP_N", 220)
    enrich_extids_excl_n = _int_env("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800)
    enrich_extids_final_n = _int_env("ENRICH_EXTERNALIDS_TOP_N", 60)
    enrich_providers_final_n = _int_env("ENRICH_PROVIDERS_FINAL_TOP_N", 40)

    return Env.from_mapping({
        "REGION": os.getenv("REGION", "US").strip() or "US",
        "ORIGINAL_LANGS": langs,
        "SUBS_INCLUDE": subs_list,
        "DISCOVER_PAGES": pages,
        "POOL_MAX_ITEMS": pool_max,
        "POOL_PRUNE_AT": prune_at,
        "POOL_PRUNE_KEEP": prune_keep,
        "ENRICH_PROVIDERS_TOP_N": enrich_providers_n,
        "ENRICH_SCORING_TOP_N": enrich_scoring_n,
        "ENRICH_EXTERNALIDS_EXCL_TOP_N": enrich_extids_excl_n,
        "ENRICH_EXTERNALIDS_TOP_N": enrich_extids_final_n,
        "ENRICH_PROVIDERS_FINAL_TOP_N": enrich_providers_final_n,
    })

def _build_run_dir() -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rd = OUT_ROOT / f"run_{ts}"
    rd.mkdir(parents=True, exist_ok=True)
    return rd

def _markdown_summary(items: List[Dict[str, Any]], env: Env) -> str:
    discovered = int(env.get("DISCOVERED_COUNT", 0))
    eligible = int(env.get("ELIGIBLE_COUNT", 0))
    subs = env.get("SUBS_INCLUDE", [])
    region = env.get("REGION", "US")
    pages = env.get("DISCOVER_PAGES", 1)
    prov_map = env.get("PROVIDER_MAP", {})
    unmatched = env.get("PROVIDER_UNMATCHED", [])
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    prof_t = env.get("PROFILE_TELEMETRY", {}) or {}

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
        lines.append(f"- Pool: before={pool_t.get('file_lines_before')} → after={pool_t.get('file_lines_after')}, "
                     f"unique_keys_est={pool_t.get('unique_keys_est')}, loaded_unique={pool_t.get('loaded_unique')}, "
                     f"appended_this_run={pool_t.get('appended_this_run')}, cap={pool_t.get('pool_max_items')}")
        if pool_t.get("prune_at", 0):
            lines.append(f"- Pool prune policy: prune_at={pool_t.get('prune_at')}, keep={pool_t.get('prune_keep')}")
    if prof_t:
        lines.append(f"- Profile: rows={prof_t.get('rows')} global_avg={prof_t.get('global_avg')} model={prof_t.get('path')}")
    lines.append("")
    return "\n".join(lines)

# ---------- enrichment helpers ----------
def _base_for_select(it: Dict[str, Any]) -> float:
    try:
        v = float(it.get("tmdb_vote") or 0.0)
    except Exception:
        v = 0.0
    try:
        p = float(it.get("popularity") or 0.0)
    except Exception:
        p = 0.0
    import math
    return (v * 2.0) + (math.log1p(p) * 0.5)

def _select_top(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_base_for_select, reverse=True)[:max(0, n)]

def _enrich_providers(items: List[Dict[str, Any]], region: str, top_n: int) -> None:
    for it in _select_top(items, top_n):
        if it.get("providers"): continue
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid: continue
        try:
            provs = tmdb.get_title_watch_providers(kind, int(tid), region)
            if provs: it["providers"] = provs
        except Exception:
            pass

def _enrich_scoring_signals(items: List[Dict[str, Any]], top_n: int) -> None:
    for it in _select_top(items, top_n):
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid: continue
        tid = int(tid)
        try:
            det = tmdb.get_details(kind, tid)
            for k, v in det.items():
                if v and it.get(k) in (None, [], "", {}):
                    it[k] = v
        except Exception:
            pass
        try:
            cred = tmdb.get_credits(kind, tid)
            if cred.get("directors"): it["directors"] = cred["directors"]
            if cred.get("writers"):   it["writers"] = cred["writers"][:4]
            if cred.get("cast"):      it["cast"] = cred["cast"][:6]
        except Exception:
            pass
        try:
            kws = tmdb.get_keywords(kind, tid)
            if kws: it["keywords"] = kws[:20]
        except Exception:
            pass

def _enrich_external_ids(items: List[Dict[str, Any]], top_n: int) -> None:
    for it in _select_top(items, top_n):
        if it.get("imdb_id"): 
            continue
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid: 
            continue
        try:
            ex = tmdb.get_external_ids(kind, int(tid))
            if ex.get("imdb_id"):
                it["imdb_id"] = ex["imdb_id"]
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

    # TMDB auth (v3 key or any v4 token)
    if not (
        os.getenv("TMDB_API_KEY")
        or os.getenv("TMDB_BEARER")
        or os.getenv("TMDB_ACCESS_TOKEN")
        or os.getenv("TMDB_V4_TOKEN")
    ):
        msg = "[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER/ACCESS_TOKEN. Set one and re-run."
        _log(msg)
        _safe_json_dump(diag_path, {"error": msg})
        sys.exit(2)

    # ---------- catalog ----------
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
    _safe_json_dump(run_dir / "items.discovered.json", items)

    # ---------- enrich external IDs (pre-exclusion) ----------
    try:
        _enrich_external_ids(items, top_n=int(env.get("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800)))
    except Exception as ex:
        _log(f"[extids-pre] FAILED: {ex!r}")

    # ---------- exclusions ----------
    excl_info = {"ratings_rows": 0, "public_ids": 0, "excluded_count": 0}
    seen_export = {"imdb_ids": [], "title_year_keys": []}
    try:
        seen_idx: Dict[str, bool] = {}
        ratings_csv = Path("data/user/ratings.csv")
        if ratings_csv.exists():
            seen_idx = _load_seen_index(ratings_csv)
            excl_info["ratings_rows"] = sum(1 for k in seen_idx.keys() if isinstance(k, str) and k.startswith("tt"))
        before_pub = len(seen_idx)
        seen_idx = _merge_seen_public(seen_idx)
        excl_info["public_ids"] = max(0, len(seen_idx) - before_pub)

        pre_ct = len(items)
        items = _filter_unseen(items, seen_idx)
        excl_info["excluded_count"] = pre_ct - len(items)
        seen_export["imdb_ids"] = [k for k in seen_idx.keys() if isinstance(k, str) and k.startswith("tt")]
        seen_export["title_year_keys"] = [k for k in seen_idx.keys() if "::" in k]
        _safe_json_dump(exports_dir / "seen_index.json", seen_export)
        _log(f"[exclusions] strict filter: removed={excl_info['excluded_count']} "
             f"(ratings_ids~{excl_info['ratings_rows']}, public_ids_add={excl_info['public_ids']})")
    except Exception as ex:
        _log(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()

    eligible = len(items)

    # ---------- provider enrichment (pre-scoring broad) ----------
    try:
        _enrich_providers(items, env.get("REGION", "US"), top_n=int(env.get("ENRICH_PROVIDERS_TOP_N", 220)))
    except Exception as ex:
        _log(f"[providers-pre] FAILED: {ex!r}")

    # ---------- build user profile model ----------
    profile_t = {}
    model_path = str(exports_dir / "user_model.json")
    try:
        model = build_user_model(Path("data/user/ratings.csv"), exports_dir)
        profile_t = {
            "rows": int(model.get("meta", {}).get("count", 0)),
            "global_avg": model.get("meta", {}).get("global_avg"),
            "path": model_path,
        }
    except Exception as ex:
        _log(f"[profile] FAILED: {ex!r}")
        traceback.print_exc()

    # ---------- enrichment for scoring (details/credits/keywords) ----------
    try:
        _enrich_scoring_signals(items, top_n=int(env.get("ENRICH_SCORING_TOP_N", 220)))
        # also ensure ext-ids on the scoring set (helps later guards)
        _enrich_external_ids(items, top_n=int(env.get("ENRICH_SCORING_TOP_N", 220)))
    except Exception as ex:
        _log(f"[scoring-enrich] FAILED: {ex!r}")
        traceback.print_exc()

    # ---------- scoring ----------
    ranked: List[Dict[str, Any]] = items
    try:
        env["USER_MODEL_PATH"] = model_path
        if callable(score_items):
            ranked = score_items(env, items)
        ranked = sorted(
            ranked,
            key=lambda it: it.get("score", it.get("match", it.get("tmdb_vote", 0.0))),
            reverse=True
        )
    except Exception as ex:
        _log(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()

    # ---------- post-scoring targeted enrichment (final Top K) ----------
    final_top_n = int(env.get("ENRICH_PROVIDERS_FINAL_TOP_N", 40))
    try:
        _enrich_providers(ranked, env.get("REGION", "US"), top_n=final_top_n)
        _enrich_external_ids(ranked, top_n=int(env.get("ENRICH_EXTERNALIDS_TOP_N", 60)))
    except Exception as ex:
        _log(f"[post-enrich] FAILED: {ex!r}")

    _safe_json_dump(run_dir / "items.enriched.json", ranked)
    _safe_json_dump(run_dir / "assistant_feed.json", ranked)

    # summary scaffold (email body built/updated by summarize)
    try:
        (run_dir / "summary.md").write_text(_markdown_summary(ranked, env), encoding="utf-8")
    except Exception as ex:
        _log(f"[summary] FAILED: {ex!r}")

    above_cut = sum(1 for it in ranked if float(it.get("score", 0) or 0) >= 58.0)
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
                "EXCLUSIONS": excl_info,
                "PROFILE_TELEMETRY": profile_t,
                "USER_MODEL_PATH": model_path,
                "ENRICH_EXTERNALIDS_EXCL_TOP_N": env.get("ENRICH_EXTERNALIDS_EXCL_TOP_N"),
                "ENRICH_EXTERNALIDS_TOP_N": env.get("ENRICH_EXTERNALIDS_TOP_N"),
                "ENRICH_PROVIDERS_FINAL_TOP_N": env.get("ENRICH_PROVIDERS_FINAL_TOP_N"),
            },
            "discover_pages": env.get("DISCOVER_PAGE_TELEMETRY", []),
            "paths": {
                "assistant_feed": str((run_dir / "assistant_feed.json").resolve()),
                "items_discovered": str((run_dir / "items.discovered.json").resolve()),
                "items_enriched": str((run_dir / "items.enriched.json").resolve()),
                "summary": str((run_dir / "summary.md").resolve()),
                "runner_log": str((run_dir / "runner.log").resolve()),
                "exports_dir": str((run_dir / "exports").resolve()),
                "seen_index_json": str((exports_dir / "seen_index.json").resolve()),
                "user_model_json": str((exports_dir / "user_model.json").resolve()),
                "profile_report": str((exports_dir / "profile_report.md").resolve()),
            },
        }
        _safe_json_dump(diag_path, diag)
    except Exception:
        pass

    _stamp_last_run(run_dir)

if __name__ == "__main__":
    main()