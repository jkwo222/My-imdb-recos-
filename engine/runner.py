# engine/runner.py
from __future__ import annotations
import json, os, sys, time, traceback
from pathlib import Path
from typing import Any, Dict, List

from .catalog_builder import build_catalog
from .scoring import score_items
from .exclusions import (
    load_seen_index as _load_seen_index,
    filter_unseen as _filter_unseen,
    merge_with_public as _merge_seen_public,
)
from .profile import build_user_model
from . import summarize
from . import tmdb

try:
    from .self_check import run_self_check
except Exception:
    def run_self_check() -> None: print("SELF-CHECK: (fallback)")

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")

def _safe_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _stamp_last_run(run_dir: Path) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "last_run_dir.txt").write_text(str(run_dir), encoding="utf-8")
    latest = OUT_ROOT / "latest"
    if latest.exists():
        if latest.is_symlink() or latest.is_file(): latest.unlink()
        else:
            import shutil; shutil.rmtree(latest, ignore_errors=True)
    try:
        from os import path as _p
        rel = _p.relpath(run_dir.resolve(), OUT_ROOT.resolve())
        latest.symlink_to(rel, target_is_directory=True)
    except Exception:
        import shutil; shutil.copytree(run_dir, latest)

def _json_or_list(s: str) -> List[str]:
    s=(s or "").strip()
    if not s: return []
    if s.startswith("["):
        try:
            import json as _j; return [str(x).strip() for x in _j.loads(s)]
        except Exception: return []
    return [x.strip() for x in s.split(",") if x.strip()]

def _env_from_os() -> Dict[str, Any]:
    def _i(n: str, d: int) -> int:
        try: v = os.getenv(n, ""); return int(v) if v else d
        except Exception: return d
    return {
        "REGION": os.getenv("REGION","US").strip() or "US",
        "ORIGINAL_LANGS": _json_or_list(os.getenv("ORIGINAL_LANGS",'["en"]')),
        "SUBS_INCLUDE": _json_or_list(os.getenv("SUBS_INCLUDE","")),
        "DISCOVER_PAGES": max(1, min(50, _i("DISCOVER_PAGES", 12))),
        "POOL_MAX_ITEMS": _i("POOL_MAX_ITEMS", 20000),
        "POOL_PRUNE_AT": _i("POOL_PRUNE_AT", 0),
        "POOL_PRUNE_KEEP": _i("POOL_PRUNE_KEEP", 0),
        # enrichment sizes
        "ENRICH_PROVIDERS_TOP_N": _i("ENRICH_PROVIDERS_TOP_N", 220),
        "ENRICH_SCORING_TOP_N": _i("ENRICH_SCORING_TOP_N", 260),
        "ENRICH_EXTERNALIDS_EXCL_TOP_N": _i("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800),
        "ENRICH_EXTERNALIDS_TOP_N": _i("ENRICH_EXTERNALIDS_TOP_N", 60),
        "ENRICH_PROVIDERS_FINAL_TOP_N": _i("ENRICH_PROVIDERS_FINAL_TOP_N", 400),  # bump to 400
    }

def _base_for_select(it: Dict[str, Any]) -> float:
    try: v = float(it.get("tmdb_vote") or 0.0)
    except Exception: v = 0.0
    try: p = float(it.get("popularity") or 0.0)
    except Exception: p = 0.0
    import math
    return (v * 2.0) + (math.log1p(p) * 0.5)

def _select_top(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_base_for_select, reverse=True)[: max(0, n)]

# --- enrichment helpers (cached in tmdb.py) ---
def _enrich_external_ids(items: List[Dict[str, Any]], top_n: int) -> None:
    for it in _select_top(items, top_n):
        if it.get("imdb_id"): continue
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid: continue
        try:
            ex = tmdb.get_external_ids(kind, int(tid))
            if ex.get("imdb_id"): it["imdb_id"] = ex["imdb_id"]
        except Exception:
            pass

def _enrich_providers(items: List[Dict[str, Any]], region: str, top_n: int) -> None:
    # pace a little to avoid 429; tmdb layer caches anyway
    count = 0
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
        count += 1
        if (count % 25) == 0:
            time.sleep(0.1)

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
            if cred.get("writers"):   it["writers"]   = cred["writers"][:4]
            if cred.get("cast"):      it["cast"]      = cred["cast"][:6]
        except Exception:
            pass
        try:
            kws = tmdb.get_keywords(kind, tid)
            if kws: it["keywords"] = kws[:20]
        except Exception:
            pass

def _collect_seen_tv_roots(ratings_csv: Path) -> List[str]:
    import csv, re
    roots: List[str] = []
    if not ratings_csv.exists(): return roots
    _non = re.compile(r"[^a-z0-9]+")
    def norm(s: str) -> str: return _non.sub(" ", (s or "").strip().lower()).strip()
    with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
        rd = csv.DictReader(fh)
        for r in rd:
            t = (r.get("Title") or r.get("Primary Title") or r.get("Original Title") or "").strip()
            tt = (r.get("Title Type") or "").lower()
            if t and ("tv" in tt or "series" in tt or "episode" in tt):
                roots.append(norm(t))
    out, seen = [], set()
    for x in roots:
        if x not in seen: out.append(x); seen.add(x)
    return out

def main() -> None:
    t0 = time.time()
    run_self_check()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    run_dir = OUT_ROOT / f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    diag_path = run_dir / "diag.json"
    exports_dir = run_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    # Env guard
    if not (os.getenv("TMDB_API_KEY") or os.getenv("TMDB_BEARER") or os.getenv("TMDB_ACCESS_TOKEN") or os.getenv("TMDB_V4_TOKEN")):
        msg = "[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER/ACCESS_TOKEN."
        print(msg); _safe_json(diag_path, {"error": msg}); sys.exit(2)

    env = _env_from_os()

    # 1) Build pool (TMDB discover + optional IMDb TSV)
    print(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        print(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    pool_t = env.get("POOL_TELEMETRY") or {}
    print(f" | catalog:end pooled={len(items)}")

    # 2) Pre-exclusion: hydrate external IDs for strict 'never-seen'
    try: _enrich_external_ids(items, top_n=int(env.get("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800)))
    except Exception as ex: print(f"[extids-pre] FAILED: {ex!r}")

    # 3) Exclusions
    excl_info = {"ratings_rows": 0, "public_ids": 0, "excluded_count": 0}
    seen_tv_roots: List[str] = []
    try:
        seen_idx: Dict[str, Any] = {}
        ratings_csv = Path("data/user/ratings.csv")
        if ratings_csv.exists():
            from .exclusions import load_seen_index as _lsi
            seen_idx = _lsi(ratings_csv)
            excl_info["ratings_rows"] = sum(1 for k in seen_idx if isinstance(k, str) and k.startswith("tt"))
            seen_tv_roots = _collect_seen_tv_roots(ratings_csv)
            (exports_dir / "seen_tv_roots.json").write_text(json.dumps(seen_tv_roots, indent=2), encoding="utf-8")
        before_pub = len(seen_idx)
        seen_idx = _merge_seen_public(seen_idx)
        excl_info["public_ids"] = max(0, len(seen_idx) - before_pub)
        pre = len(items)
        items = _filter_unseen(items, seen_idx)
        excl_info["excluded_count"] = pre - len(items)
        (exports_dir / "seen_index.json").write_text(
            json.dumps({
                "imdb_ids": [k for k in seen_idx if isinstance(k, str) and k.startswith("tt")],
                "title_year_keys": [k for k in seen_idx if "::" in k],
            }, indent=2), encoding="utf-8")
        print(f"[exclusions] strict filter removed={excl_info['excluded_count']}")
    except Exception as ex:
        print(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()

    eligible = len(items)

    # 4) Pre-scoring enrichment (details/credits/keywords), providers for some
    try:
        _enrich_scoring_signals(items, top_n=int(env.get("ENRICH_SCORING_TOP_N", 260)))
    except Exception as ex:
        print(f"[scoring-enrich] FAILED: {ex!r}")

    try:
        _enrich_providers(items, env.get("REGION","US"), top_n=int(env.get("ENRICH_PROVIDERS_TOP_N", 220)))
    except Exception as ex:
        print(f"[providers-pre] FAILED: {ex!r}")

    # 5) Profile model
    model_path = str((exports_dir / "user_model.json"))
    try:
        model = build_user_model(Path("data/user/ratings.csv"), exports_dir)
    except Exception as ex:
        print(f"[profile] FAILED: {ex!r}")
        traceback.print_exc()
        model = {}
    env["USER_MODEL_PATH"] = model_path
    env["SEEN_TV_TITLE_ROOTS"] = seen_tv_roots

    # 6) Score
    try:
        ranked = score_items(env, items)
        ranked = sorted(ranked, key=lambda it: it.get("score", it.get("tmdb_vote", it.get("popularity", 0.0))), reverse=True)
    except Exception as ex:
        print(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()
        ranked = items

    # 7) Post-scoring enrichment (providers + external_ids) on bigger final cut
    try:
        _enrich_providers(ranked, env.get("REGION","US"), top_n=int(env.get("ENRICH_PROVIDERS_FINAL_TOP_N", 400)))
        _enrich_external_ids(ranked, top_n=int(env.get("ENRICH_EXTERNALIDS_TOP_N", 60)))
    except Exception as ex:
        print(f"[post-enrich] FAILED: {ex!r}")

    # 8) Persist core outputs
    (run_dir / "items.enriched.json").write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "assistant_feed.json").write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8")

    # 9) Write diag **before** building summary so telemetry shows up
    _safe_json(
        diag_path,
        {
            "ran_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "run_seconds": round(time.time() - t0, 3),
            "counts": {
                "discovered": env.get("DISCOVERED_COUNT", 0),
                "eligible": eligible,
                "scored": len(ranked),
                "excluded_seen": excl_info.get("excluded_count", 0),
            },
            "env": {
                "REGION": env.get("REGION","US"),
                "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS",[]),
                "SUBS_INCLUDE": env.get("SUBS_INCLUDE",[]),
                "DISCOVER_PAGES": env.get("DISCOVER_PAGES",0),
                "POOL_TELEMETRY": env.get("POOL_TELEMETRY",{}),
                "USER_MODEL_PATH": model_path,
                "SEEN_TV_TITLE_ROOTS_COUNT": len(seen_tv_roots),
            },
            "paths": {
                "assistant_feed": str((run_dir / "assistant_feed.json").resolve()),
                "items_discovered": str((run_dir / "items.discovered.json").resolve()),
                "items_enriched": str((run_dir / "items.enriched.json").resolve()),
                "summary": str((run_dir / "summary.md").resolve()),
                "exports_dir": str((exports_dir).resolve()),
                "seen_index_json": str((exports_dir / "seen_index.json").resolve()),
                "user_model_json": str((exports_dir / "user_model.json").resolve()),
                "seen_tv_roots": str((exports_dir / "seen_tv_roots.json").resolve()),
            },
        },
    )

    # 10) Build summary (now telemetry will include counts)
    try:
        summarize.write_email_markdown(
            run_dir=run_dir,
            ranked_items_path=run_dir / "items.enriched.json",
            env={"REGION": env.get("REGION", "US"), "SUBS_INCLUDE": env.get("SUBS_INCLUDE", [])},
            seen_index_path=exports_dir / "seen_index.json",
            seen_tv_roots_path=exports_dir / "seen_tv_roots.json",
        )
    except Exception as ex:
        print(f"[summarize] FAILED: {ex!r}")
        (run_dir / "summary.md").write_text("# Daily Recommendations\n\n_Summary generation failed._\n", encoding="utf-8")

    # 11) Final log + stamp
    above_cut = sum(1 for it in ranked if float(it.get("score", 0) or 0) >= 58.0)
    print(f" | results: discovered={env.get('DISCOVERED_COUNT',0)} eligible={eligible} above_cut={above_cut}")
    _stamp_last_run(run_dir)

if __name__ == "__main__":
    main()