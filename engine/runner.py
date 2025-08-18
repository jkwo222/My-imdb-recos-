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
from . import imdb_scrape
from . import feedback as fb  # NEW

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

def _b(n: str, d: bool) -> bool:
    v=(os.getenv(n,"") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return d

def _i(n: str, d: int) -> int:
    try: v = os.getenv(n, ""); return int(v) if v else d
    except Exception: return d

def _env_from_os() -> Dict[str, Any]:
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
        "ENRICH_PROVIDERS_FINAL_TOP_N": _i("ENRICH_PROVIDERS_FINAL_TOP_N", 400),

        # IMDb scrape knobs
        "IMDB_SCRAPE_ENABLE": _b("IMDB_SCRAPE_ENABLE", True),
        "IMDB_SCRAPE_TOP_N": _i("IMDB_SCRAPE_TOP_N", 120),
        "IMDB_SCRAPE_KEYWORDS_TOP_N": _i("IMDB_SCRAPE_KEYWORDS_TOP_N", 80),
        "IMDB_SCRAPE_KEYWORDS_MAX": _i("IMDB_SCRAPE_KEYWORDS_MAX", 30),

        # Feedback
        "FEEDBACK_ENABLE": _b("FEEDBACK_ENABLE", True),
        "FEEDBACK_JSON_PATH": os.getenv("FEEDBACK_JSON_PATH", "data/user/feedback.json"),
        "FEEDBACK_DOWN_COOLDOWN_DAYS": _i("FEEDBACK_DOWN_COOLDOWN_DAYS", 14),
        "FEEDBACK_DECAY": float(os.getenv("FEEDBACK_DECAY","0.98") or 0.98),
        "FEEDBACK_UP_DIRECT_BONUS": float(os.getenv("FEEDBACK_UP_DIRECT_BONUS","10") or 10),
        "FEEDBACK_DOWN_DIRECT_PENALTY": float(os.getenv("FEEDBACK_DOWN_DIRECT_PENALTY","18") or 18),
        "FEEDBACK_SIMILAR_ACTOR_W": float(os.getenv("FEEDBACK_SIMILAR_ACTOR_W","1.4") or 1.4),
        "FEEDBACK_SIMILAR_DIRECTOR_W": float(os.getenv("FEEDBACK_SIMILAR_DIRECTOR_W","0.8") or 0.8),
        "FEEDBACK_SIMILAR_WRITER_W": float(os.getenv("FEEDBACK_SIMILAR_WRITER_W","0.6") or 0.6),
        "FEEDBACK_SIMILAR_GENRE_W": float(os.getenv("FEEDBACK_SIMILAR_GENRE_W","0.6") or 0.6),
        "FEEDBACK_SIMILAR_KEYWORD_W": float(os.getenv("FEEDBACK_SIMILAR_KEYWORD_W","0.2") or 0.2),
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

def _enrich_external_ids(items: List[Dict[str, Any]], top_n: int) -> None:
    from .tmdb import get_external_ids
    for it in _select_top(items, top_n):
        if it.get("imdb_id"): continue
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid: continue
        try:
            ex = get_external_ids(kind, int(tid))
            if ex.get("imdb_id"): it["imdb_id"] = ex["imdb_id"]
        except Exception:
            pass

def _enrich_providers(items: List[Dict[str, Any]], region: str, top_n: int) -> None:
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

def _needs_imdb_augment(it: Dict[str, Any]) -> bool:
    if not it.get("imdb_id"): return False
    if not it.get("runtime"): return True
    if not it.get("directors"): return True
    if not it.get("genres") and not it.get("tmdb_genres"): return True
    if not it.get("keywords"): return True
    try:
        aud = float(it.get("audience") or it.get("tmdb_vote") or 0.0)
        if aud <= 0.0: return True
    except Exception:
        return True
    return False

def _augment_from_imdb(items: List[Dict[str, Any]], top_n: int, kw_top_n: int, kw_limit: int) -> None:
    # details augmentation
    if top_n > 0:
        count = 0
        for it in _select_top(items, top_n):
            if not _needs_imdb_augment(it): continue
            imdb_id = it.get("imdb_id")
            try:
                data = imdb_scrape.fetch_title(imdb_id)
                if not data: continue
                for k in ("runtime","genres","directors","writers","cast","audience","title","year"):
                    v = data.get(k)
                    if v and it.get(k) in (None, [], "", {}):
                        it[k] = v
                if data.get("imdb_url"):
                    it["imdb_url"] = data["imdb_url"]
            except Exception:
                continue
            count += 1
            if (count % 20) == 0:
                time.sleep(0.05)

    # keywords augmentation
    if kw_top_n > 0 and kw_limit > 0:
        count = 0
        for it in _select_top(items, kw_top_n):
            if not it.get("imdb_id"): continue
            existing = it.get("keywords") or []
            if isinstance(existing, list) and len(existing) >= 5:
                continue
            imdb_id = it.get("imdb_id")
            try:
                kws = imdb_scrape.fetch_keywords(imdb_id, limit=kw_limit)
                if kws:
                    merged = list(dict.fromkeys([*(existing or []), *kws]))
                    it["keywords"] = merged
            except Exception:
                continue
            count += 1
            if (count % 20) == 0:
                time.sleep(0.05)

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

    env = _env_from_os()

    # 1) Catalog
    print(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        print(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    print(f" | catalog:end pooled={len(items)}")

    # 2) External IDs (to strengthen seen-filter & feedback key mapping)
    try: _enrich_external_ids(items, top_n=int(env.get("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800)))
    except Exception as ex: print(f"[extids-pre] FAILED: {ex!r}")

    # 3) Exclusions (CSV + public IMDb page)
    excl_info = {"ratings_rows": 0, "public_ids": 0, "excluded_count": 0}
    seen_tv_roots: List[str] = []
    try:
        seen_idx: Dict[str, Any] = {}
        ratings_csv = Path("data/user/ratings.csv")
        if ratings_csv.exists():
            from .exclusions import load_seen_index as _lsi
            seen_idx = _lsi(ratings_csv)
            excl_info["ratings_rows"] = sum(1 for k in seen_idx if isinstance(k, str))
            # collect TV roots (title-normalized)
            import csv, re
            _non = re.compile(r"[^a-z0-9]+"); norm=lambda s:_non.sub(" ", (s or "").strip().lower()).strip()
            with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
                rd = csv.DictReader(fh); roots=[]
                for r in rd:
                    t=(r.get("Title") or r.get("Primary Title") or r.get("Original Title") or "").strip()
                    tt=(r.get("Title Type") or "").lower()
                    if t and ("tv" in tt or "series" in tt or "episode" in tt):
                        roots.append(norm(t))
                seen_tv_roots = list(dict.fromkeys(roots))
            (exports_dir / "seen_tv_roots.json").write_text(json.dumps(seen_tv_roots, indent=2), encoding="utf-8")
        before_pub = len(seen_idx)
        seen_idx = _merge_seen_public(seen_idx)
        excl_info["public_ids"] = max(0, len(seen_idx) - before_pub)
        pre = len(items)
        items = _filter_unseen(items, seen_idx)
        excl_info["excluded_count"] = pre - len(items)
        (exports_dir / "seen_index.json").write_text(json.dumps(seen_idx, indent=2), encoding="utf-8")
        print(f"[exclusions] strict filter removed={excl_info['excluded_count']}")
    except Exception as ex:
        print(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()

    eligible = len(items)

    # 4) Pre-scoring enrichment
    try: _enrich_scoring_signals(items, top_n=int(env.get("ENRICH_SCORING_TOP_N", 260)))
    except Exception as ex: print(f"[scoring-enrich] FAILED: {ex!r}")

    try: _enrich_providers(items, env.get("REGION","US"), top_n=int(env.get("ENRICH_PROVIDERS_TOP_N", 220)))
    except Exception as ex: print(f"[providers-pre] FAILED: {ex!r}")

    # 5) Optional IMDb augmentation (details + keywords)
    if env.get("IMDB_SCRAPE_ENABLE", True):
        try:
            _augment_from_imdb(
                items,
                top_n=int(env.get("IMDB_SCRAPE_TOP_N", 120)),
                kw_top_n=int(env.get("IMDB_SCRAPE_KEYWORDS_TOP_N", 80)),
                kw_limit=int(env.get("IMDB_SCRAPE_KEYWORDS_MAX", 30)),
            )
        except Exception as ex:
            print(f"[imdb-augment] FAILED: {ex!r}")

    # 6) Profile model
    model_path = str((exports_dir / "user_model.json"))
    try:
        _ = build_user_model(Path("data/user/ratings.csv"), exports_dir)
    except Exception as ex:
        print(f"[profile] FAILED: {ex!r}")
        traceback.print_exc()
    env["USER_MODEL_PATH"] = model_path
    env["SEEN_TV_TITLE_ROOTS"] = seen_tv_roots

    # 7) Feedback learning & suppressions
    fb_stats = {}
    if env.get("FEEDBACK_ENABLE", True):
        try:
            fb_json = Path(env.get("FEEDBACK_JSON_PATH") or "data/user/feedback.json")
            fb_data = fb.load_feedback(fb_json)
            bank, suppress, fb_stats = fb.update_feature_bank(
                items,
                fb_data,
                cooldown_days=int(env.get("FEEDBACK_DOWN_COOLDOWN_DAYS", 14)),
                decay=float(env.get("FEEDBACK_DECAY", 0.98)),
            )
            env["FEEDBACK_FEATURES"] = bank
            env["FEEDBACK_SUPPRESS_KEYS"] = list(suppress)
            env["FEEDBACK_ITEMS"] = fb_data.get("items", {})
            # export
            (exports_dir / "feedback_bank.json").write_text(json.dumps(bank, indent=2), encoding="utf-8")
            (exports_dir / "feedback_suppress.json").write_text(json.dumps(sorted(list(suppress)), indent=2), encoding="utf-8")
            (exports_dir / "feedback_raw.json").write_text(json.dumps(fb_data, indent=2), encoding="utf-8")
        except Exception as ex:
            print(f"[feedback] FAILED: {ex!r}")
            traceback.print_exc()

    # 8) Score
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

    # 9) Post-scoring enrichment (providers + external_ids)
    try:
        _enrich_providers(ranked, env.get("REGION","US"), top_n=int(env.get("ENRICH_PROVIDERS_FINAL_TOP_N", 400)))
        _enrich_external_ids(ranked, top_n=int(env.get("ENRICH_EXTERNALIDS_TOP_N", 60)))
    except Exception as ex:
        print(f"[post-enrich] FAILED: {ex!r}")

    # 10) Persist lists
    (run_dir / "items.enriched.json").write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "assistant_feed.json").write_text(json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8")

    # 11) diag BEFORE summary
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
            "feedback": fb_stats,
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
                "feedback_bank": str((exports_dir / "feedback_bank.json").resolve()),
                "feedback_suppress": str((exports_dir / "feedback_suppress.json").resolve()),
                "feedback_raw": str((exports_dir / "feedback_raw.json").resolve()),
            },
        },
    )

    # 12) Summary (email)
    try:
        summarize.write_email_markdown(
            run_dir=run_dir,
            ranked_items_path=run_dir / "items.enriched.json",
            env={
                "REGION": env.get("REGION", "US"),
                "SUBS_INCLUDE": env.get("SUBS_INCLUDE", []),
                "FEEDBACK_SUPPRESS_KEYS": env.get("FEEDBACK_SUPPRESS_KEYS", []),
            },
        )
    except Exception as ex:
        print(f"[summarize] FAILED: {ex!r}")
        (run_dir / "summary.md").write_text("# Daily Recommendations\n\n_Summary generation failed._\n", encoding="utf-8")

    # 13) Final log + stamp
    above_cut = sum(1 for it in ranked if float(it.get("score", 0) or 0) >= 58.0)
    print(f" | results: discovered={env.get('DISCOVERED_COUNT',0)} eligible={eligible} above_cut={above_cut}")
    _stamp_last_run(run_dir)

if __name__ == "__main__":
    main()