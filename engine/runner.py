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

from .scoring import score_items
from .exclusions import (
    load_seen_index as _load_seen_index,
    filter_unseen as _filter_unseen,
    merge_with_public as _merge_seen_public,
)
from .profile import build_user_model
from . import tmdb
from . import summarize  # writes summary.md with inline labels

OUT_ROOT = Path("data/out")
CACHE_ROOT = Path("data/cache")


def _safe_json(path: Path, data: Any) -> None:
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
        from os import path as _p
        rel = _p.relpath(run_dir.resolve(), OUT_ROOT.resolve())
        latest.symlink_to(rel, target_is_directory=True)
    except Exception:
        import shutil
        shutil.copytree(run_dir, latest)


def _json_or_list(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            import json as _j

            return [str(x).strip() for x in _j.loads(s)]
        except Exception:
            return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _env_from_os() -> Env:
    def _i(name: str, d: int) -> int:
        try:
            v = os.getenv(name, "")
            return int(v) if v else d
        except Exception:
            return d

    langs = _json_or_list(os.getenv("ORIGINAL_LANGS", '["en"]'))
    subs = _json_or_list(os.getenv("SUBS_INCLUDE", ""))

    return Env.from_mapping(
        {
            "REGION": os.getenv("REGION", "US").strip() or "US",
            "ORIGINAL_LANGS": langs,
            "SUBS_INCLUDE": subs,
            "DISCOVER_PAGES": max(1, min(50, _i("DISCOVER_PAGES", 12))),
            # pool
            "POOL_MAX_ITEMS": _i("POOL_MAX_ITEMS", 20000),
            "POOL_PRUNE_AT": _i("POOL_PRUNE_AT", 0),
            "POOL_PRUNE_KEEP": _i("POOL_PRUNE_KEEP", 0),
            # enrichment sizes
            "ENRICH_PROVIDERS_TOP_N": _i("ENRICH_PROVIDERS_TOP_N", 220),
            "ENRICH_SCORING_TOP_N": _i("ENRICH_SCORING_TOP_N", 260),
            "ENRICH_EXTERNALIDS_EXCL_TOP_N": _i("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800),
            "ENRICH_EXTERNALIDS_TOP_N": _i("ENRICH_EXTERNALIDS_TOP_N", 60),
            "ENRICH_PROVIDERS_FINAL_TOP_N": _i("ENRICH_PROVIDERS_FINAL_TOP_N", 50),
        }
    )


def _build_run_dir() -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rd = OUT_ROOT / f"run_{ts}"
    rd.mkdir(parents=True, exist_ok=True)
    return rd


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
    return sorted(items, key=_base_for_select, reverse=True)[: max(0, n)]


def _enrich_providers(items: List[Dict[str, Any]], region: str, top_n: int) -> None:
    for it in _select_top(items, top_n):
        if it.get("providers"):
            continue
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid:
            continue
        try:
            provs = tmdb.get_title_watch_providers(kind, int(tid), region)
            if provs:
                it["providers"] = provs
        except Exception:
            pass


def _enrich_scoring_signals(items: List[Dict[str, Any]], top_n: int) -> None:
    for it in _select_top(items, top_n):
        kind = (it.get("media_type") or "").lower()
        tid = it.get("tmdb_id")
        if not kind or not tid:
            continue
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
            if cred.get("directors"):
                it["directors"] = cred["directors"]
            if cred.get("writers"):
                it["writers"] = cred["writers"][:4]
            if cred.get("cast"):
                it["cast"] = cred["cast"][:6]
        except Exception:
            pass
        try:
            kws = tmdb.get_keywords(kind, tid)
            if kws:
                it["keywords"] = kws[:20]
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


def _collect_seen_tv_roots(ratings_csv: Path) -> List[str]:
    """Normalize TV titles from ratings.csv so we can boost follow-up seasons."""
    roots: List[str] = []
    if not ratings_csv.exists():
        return roots

    import csv
    import re

    _non = re.compile(r"[^a-z0-9]+")

    def norm(s: str) -> str:
        return _non.sub(" ", (s or "").strip().lower()).strip()

    with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
        rd = csv.DictReader(fh)
        for r in rd:
            t = (
                r.get("Title")
                or r.get("Primary Title")
                or r.get("Original Title")
                or ""
            ).strip()
            tt = (r.get("Title Type") or "").lower()
            if t and ("tv" in tt or "series" in tt or "episode" in tt):
                roots.append(norm(t))

    # unique, stable order
    out: List[str] = []
    seen: set[str] = set()
    for x in roots:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def main() -> None:
    t0 = time.time()
    run_self_check()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    run_dir = _build_run_dir()
    log_path = run_dir / "runner.log"
    diag_path = run_dir / "diag.json"
    exports_dir = run_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    log_lines: List[str] = []

    def _log(s: str) -> None:
        print(s, flush=True)
        log_lines.append(s)

    if not (
        os.getenv("TMDB_API_KEY")
        or os.getenv("TMDB_BEARER")
        or os.getenv("TMDB_ACCESS_TOKEN")
        or os.getenv("TMDB_V4_TOKEN")
    ):
        msg = (
            "[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER/ACCESS_TOKEN."
        )
        _log(msg)
        _safe_json(diag_path, {"error": msg})
        sys.exit(2)

    env = _env_from_os()

    _log(" | catalog:begin")
    try:
        items = build_catalog(env)
    except Exception as ex:
        _log(f"[catalog] FAILED: {ex!r}")
        traceback.print_exc()
        items = []
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    _log(
        f" | catalog:end discovered={env.get('DISCOVERED_COUNT',0)} pooled={len(items)} "
        f"pool_file_lines={pool_t.get('file_lines_after')} loaded_unique={pool_t.get('loaded_unique')}"
    )
    _safe_json(run_dir / "items.discovered.json", items)

    # Pre-exclusion: external IDs harden "never seen"
    try:
        _enrich_external_ids(
            items, top_n=int(env.get("ENRICH_EXTERNALIDS_EXCL_TOP_N", 800))
        )
    except Exception as ex:
        _log(f"[extids-pre] FAILED: {ex!r}")

    # Exclusions & exports (strict "never show seen")
    excl_info = {"ratings_rows": 0, "public_ids": 0, "excluded_count": 0}
    seen_export = {"imdb_ids": [], "title_year_keys": []}
    seen_tv_roots: List[str] = []

    try:
        seen_idx: Dict[str, Any] = {}
        ratings_csv = Path("data/user/ratings.csv")
        if ratings_csv.exists():
            seen_idx = _load_seen_index(ratings_csv)
            excl_info["ratings_rows"] = sum(
                1
                for k in seen_idx.keys()
                if isinstance(k, str) and k.startswith("tt")
            )
            # Build TV title roots for follow-up season boosts
            seen_tv_roots = _collect_seen_tv_roots(ratings_csv)
            _safe_json(exports_dir / "seen_tv_roots.json", seen_tv_roots)

        before_pub = len(seen_idx)
        seen_idx = _merge_seen_public(seen_idx)  # uses env for public list (if set)
        excl_info["public_ids"] = max(0, len(seen_idx) - before_pub)

        pre = len(items)
        items = _filter_unseen(items, seen_idx)
        excl_info["excluded_count"] = pre - len(items)

        seen_export["imdb_ids"] = [
            k for k in seen_idx.keys() if isinstance(k, str) and k.startswith("tt")
        ]
       