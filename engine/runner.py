# engine/runner.py
from __future__ import annotations
import json, os, sys, time, traceback
from pathlib import Path
from typing import Any, Dict, List

from .catalog_builder import build_catalog
from .scoring import score_items
from .exclusions import load_seen_index as _load_seen_index
from .exclusions import filter_unseen as _filter_unseen
from .exclusions import merge_with_public as _merge_seen_public
from .profile import build_user_model
from . import summarize
from . import tmdb
from . import imdb_scrape
from . import feedback as fb

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

def _i(n: str, d: int) -> int:
    try: v = os.getenv(n, ""); return int(v) if v else d
    except Exception: return d

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

    if not (os.getenv("TMDB_API_KEY") or os.getenv("TMDB_BEARER") or os.getenv("TMDB_ACCESS_TOKEN") or os.getenv("TMDB_V4_TOKEN")):
        msg = "[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER/ACCESS_TOKEN."
        print(msg); _safe_json(diag_path, {"error": msg}); sys.exit(2)

    env: Dict[str, Any] = {}

    # 1) Catalog (pool build & telemetry)
    print(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        print(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    pt = env.get("POOL_TELEMETRY", {})
    print(f" | catalog:end pool_before={pt.get('pool_size_before','?')} pool_after={pt.get('pool_size_after','?')} appended={pt.get('pool_appended_this_run',0)}")

    # 2) External IDs early
    try:
        from .tmdb import get_external_ids
        for it in items[: _i("ENRICH_EXTERNALIDS_EXCL_TOP_N", 1000)]:
            if it.get("imdb_id"): continue
            kind = (it.get("media_type") or "").lower()
            tid = it.get("tmdb_id")
            if not kind or not tid: continue
            try:
                ex = get_external_ids(kind, int(tid))
                if ex.get("imdb_id"): it["imdb_id"] = ex["imdb_id"]
            except Exception:
                pass
    except Exception as ex:
        print(f"[extids-pre] FAILED: {ex!r}")

    # 3) Exclusions (strict seen-filter using CSV + public IMDb page)
    excl_info = {"ratings_rows": 0, "public_ids": 0, "excluded_count": 0}
    try:
        seen_idx: Dict[str, Any] = {}
        ratings_csv = Path("data/user/ratings.csv")
        if ratings_csv.exists():
            seen_idx = _load_seen_index(ratings_csv)
            excl_info["ratings_rows"] = len(seen_idx)
        before_pub = len(seen_idx)
        seen_idx = _merge_seen_public(seen_idx)
        excl_info["public_ids"] = max(0, len(seen_idx) - before_pub)
        pre = len(items)
        items = _filter_unseen(items, seen_idx)  # strict: must not be in CSV or public list
        excl_info["excluded_count"] = pre - len(items)
        (exports_dir / "seen_index.json").write_text(json.dumps(seen_idx, indent=2), encoding="utf-8")
    except Exception as ex:
        print(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()

    eligible = len(items)

    # 4) Scoring enrichment
    try:
        # details/credits/keywords from TMDB for top slice
        from .tmdb import get_details, get_credits, get_keywords
        N = _i("ENRICH_SCORING_TOP_N", 320)
        for it in sorted(items, key=lambda x: float(x.get("popularity", 0.0)), reverse=True)[:N]:
            kind=(it.get("media_type") or "").lower()
            tid = it.get("tmdb_id")
            if not kind or not tid: continue
            tid = int(tid)
            try:
                det = get_details(kind, tid)
                for k, v in det.items():
                    if v and it.get(k) in (None, [], "", {}):
                        it[k] = v
            except Exception:
                pass
            try:
                cred = get_credits(kind, tid)
                if cred.get("directors"): it["directors"] = cred["directors"]
                if cred.get("writers"):   it["writers"]   = cred["writers"][:4]
                if cred.get("cast"):      it["cast"]      = cred["cast"][:6]
            except Exception:
                pass
            try:
                kws = get_keywords(kind, tid)
                if kws:
                    it["keywords"] = list(dict.fromkeys((it.get("keywords") or []) + kws))[:30]
            except Exception:
                pass
    except Exception as ex:
        print(f"[scoring-enrich] FAILED: {ex!r}")

    # 5) Providers enrichment (larger sweep to avoid provider-miss drops)
    try:
        from .tmdb import get_title_watch_providers
        N = _i("ENRICH_PROVIDERS_TOP_N", 400)
        region = os.getenv("REGION","US")
        for it in sorted(items, key=lambda x: float(x.get("popularity", 0.0)), reverse=True)[:N]:
            if it.get("providers"): continue
            kind=(it.get("media_type") or "").lower()
            tid = it.get("tmdb_id")
            if not kind or not tid: continue
            try:
                provs = get_title_watch_providers(kind, int(tid), region)
                if provs: it["providers"] = provs
            except Exception:
                pass
    except Exception as ex:
        print(f"[providers-pre] FAILED: {ex!r}")

    # 6) IMDb augmentation (fill in sparse items + keywords)
    if (os.getenv("IMDB_SCRAPE_ENABLE","true").lower() in {"1","true","yes","on"}):
        try:
            from . import imdb_scrape
            # details pass
            for it in sorted(items, key=lambda x: float(x.get("popularity", 0.0)), reverse=True)[: _i("IMDB_SCRAPE_TOP_N", 160)]:
                imdb_id = it.get("imdb_id")
                if not imdb_id: continue
                # need augmentation?
                need = not it.get("runtime") or not it.get("directors") or not it.get("genres") or not it.get("keywords") or not it.get("audience")
                if not need: continue
                try:
                    data = imdb_scrape.fetch_title(imdb_id)
                    if data:
                        for k in ("runtime","genres","directors","writers","cast","audience","title","year"):
                            v = data.get(k)
                            if v and it.get(k) in (None, [], "", {}):
                                it[k] = v
                        if data.get("imdb_url"):
                            it["imdb_url"] = data["imdb_url"]
                except Exception:
                    pass
            # keywords pass
            for it in sorted(items, key=lambda x: float(x.get("popularity", 0.0)), reverse=True)[: _i("IMDB_SCRAPE_KEYWORDS_TOP_N", 140)]:
                imdb_id = it.get("imdb_id")
                if not imdb_id: continue
                existing = it.get("keywords") or []
                if isinstance(existing, list) and len(existing) >= 5:
                    continue
                try:
                    kws = imdb_scrape.fetch_keywords(imdb_id, limit=_i("IMDB_SCRAPE_KEYWORDS_MAX", 40))
                    if kws:
                        it["keywords"] = list(dict.fromkeys(existing + kws))[:40]
                except Exception:
                    pass
        except Exception as ex:
            print(f"[imdb-augment] FAILED: {ex!r}")

    # 7) Build & export user model (guaranteed)
    try:
        model = build_user_model(Path("data/user/ratings.csv"), exports_dir)
        env["USER_MODEL_PATH"] = str((exports_dir / "user_model.json"))
    except Exception as ex:
        print(f"[profile] FAILED: {ex!r}")
        env["USER_MODEL_PATH"] = ""

    # 8) Feedback learning
    fb_stats={}
    try:
        fb_json = Path(os.getenv("FEEDBACK_JSON_PATH","data/user/feedback.json"))
        fb_data = fb.load_feedback(fb_json)
        bank, suppress, fb_stats = fb.update_feature_bank(
            items, fb_data,
            cooldown_days=_i("FEEDBACK_DOWN_COOLDOWN_DAYS", 14),
            decay=float(os.getenv("FEEDBACK_DECAY","0.98") or 0.98),
        )
        env["FEEDBACK_FEATURES"] = bank
        env["FEEDBACK_SUPPRESS_KEYS"] = list(suppress)
        env["FEEDBACK_ITEMS"] = fb_data.get("items", {})
        (exports_dir / "feedback_bank.json").write_text(json.dumps(bank, indent=2), encoding="utf-8")
        (exports_dir / "feedback_suppress.json").write_text(json.dumps(sorted(list(suppress)), indent=2), encoding="utf-8")
        (exports_dir / "feedback_raw.json").write_text(json.dumps(fb_data, indent=2), encoding="utf-8")
    except Exception as ex:
        print(f"[feedback] FAILED: {ex!r}")

    # 9) Score and rank
    try:
        ranked = score_items(env, items)
        ranked = sorted(
            ranked,
            key=lambda it: it.get("score", it.get("tmdb_vote", it.get("popularity", 0.0))),
            reverse=True,
        )
    except Exception as ex:
        print(f"[scoring] FAILED: {ex!r}")
        traceback.print_exc()
        ranked = items

    # 10) Final providers pass (larger)
    try:
        from .tmdb import get_title_watch_providers, get_external_ids
        region = os.getenv("REGION","US")
        for it in ranked[: _i("ENRICH_PROVIDERS_FINAL_TOP_N", 800)]:
            if not it.get("providers"):
                try:
                    provs = get_title_watch_providers((it.get("media_type") or "").lower(), int(it.get("tmdb_id")), region)
                    if provs: it["providers"] = provs
                except Exception: pass
            if not it.get("imdb_id"):
                try:
                    ex = get_external_ids((it.get("media_type") or "").lower(), int(it.get("tmdb_id")))
                    if ex.get("imdb_id"): it["imdb_id"] = ex["imdb_id"]
                except Exception: pass
    except Exception as ex:
        print(f"[post-enrich] FAILED: {ex!r}")

    # 11) Persist lists
    (run_dir / "items.enriched.json").write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "assistant_feed.json").write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8")

    # 12) diag with clearer pool telemetry labels
    pt = env.get("POOL_TELEMETRY", {})
    _safe_json(
        diag_path,
        {
            "ran_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "run_seconds": round(time.time() - t0, 3),
            "counts": {
                "pool_appended": pt.get("pool_appended_this_run", 0),
                "pool_before": pt.get("pool_size_before"),
                "pool_after": pt.get("pool_size_after"),
                "eligible": eligible,
                "scored": len(ranked),
                "excluded_seen": (pt.get("pool_size_before",0)+pt.get("pool_appended_this_run",0)-eligible) if pt else 0,
            },
            "pool": pt,
            "feedback": fb_stats,
            "paths": {
                "assistant_feed": str((run_dir / "assistant_feed.json").resolve()),
                "items_enriched": str((run_dir / "items.enriched.json").resolve()),
                "summary": str((run_dir / "summary.md").resolve()),
                "exports_dir": str((exports_dir).resolve()),
                "user_model_json": str((exports_dir / "user_model.json").resolve()),
            },
        },
    )

    # 13) Summary (now also exports selection_breakdown.json)
    try:
        summarize.write_email_markdown(
            run_dir=run_dir,
            ranked_items_path=run_dir / "items.enriched.json",
            env={
                "REGION": os.getenv("REGION", "US"),
                "SUBS_INCLUDE": (os.getenv("SUBS_INCLUDE","") or "").split(",") if os.getenv("SUBS_INCLUDE") else [],
                "FEEDBACK_SUPPRESS_KEYS": env.get("FEEDBACK_SUPPRESS_KEYS", []),
            },
        )
    except Exception as ex:
        print(f"[summarize] FAILED: {ex!r}")
        (run_dir / "summary.md").write_text("# Daily Recommendations\n\n_Summary generation failed._\n", encoding="utf-8")

    # 14) Final log + stamp
    print(f" | results: pool_appended={pt.get('pool_appended_this_run',0)} eligible={eligible} scored={len(ranked)}")
    _stamp_last_run(run_dir)

if __name__ == "__main__":
    main()