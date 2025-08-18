# engine/runner.py
from __future__ import annotations
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from . import catalog_builder
from . import enrich
from . import profile
from . import scorer
from . import filters

RUN_ROOT = Path("data/out")
LATEST   = RUN_ROOT / "latest"

def _ensure_dirs() -> None:
    (Path("data/cache")).mkdir(parents=True, exist_ok=True)
    RUN_ROOT.mkdir(parents=True, exist_ok=True)
    LATEST.mkdir(parents=True, exist_ok=True)

def _env() -> Dict[str, Any]:
    e = dict(os.environ)
    # Normalize some known knobs
    def _list_env(name: str) -> List[str]:
        v = (e.get(name) or "").strip()
        if not v:
            return []
        return [x.strip() for x in v.split(",") if x.strip()]
    e["SUBS_INCLUDE"] = _list_env("SUBS_INCLUDE")
    return e

def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

def _self_check() -> List[str]:
    msgs=[]
    def _opt(mod_name: str) -> str:
        try:
            __import__(f"engine.{mod_name}")
            return "present"
        except Exception:
            return "absent"
    for m in ("persona", "taste", "personalization", "util"):
        msgs.append(f"SELF-CHECK: optional engine.{m}: {_opt(m)}")
    return msgs

def main() -> None:
    _ensure_dirs()
    env = _env()
    run_dir = LATEST

    # --- pre-run sanity output (stdout becomes runner.log in workflow artifacts)
    for line in _self_check():
        print(line)

    # --- Hard env sanity (TMDB)
    if not (os.getenv("TMDB_API_KEY") or os.getenv("TMDB_BEARER") or os.getenv("TMDB_ACCESS_TOKEN")):
        print("[env] Missing required environment: TMDB_API_KEY or TMDB_BEARER. Set these and re-run.", file=sys.stderr)
        sys.exit(2)

    # =========================
    # 1) Build/extend the catalog pool
    # =========================
    pool_items = catalog_builder.build_catalog(env)
    pool_tel = env.get("POOL_TELEMETRY", {})
    # Persist "discovered" view for debugging (the pool snapshot considered this run)
    disc_path = run_dir / "items.discovered.json"
    _write_json(disc_path, pool_items)

    # =========================
    # 2) Build strict seen index (CSV + optional IMDb public snapshot if present)
    # =========================
    ratings_csv = Path("data/user/ratings.csv")
    imdb_public_seen = Path("data/cache/imdb_public/seen.json")
    seen_index = filters.build_seen_index(ratings_csv, imdb_public_seen if imdb_public_seen.exists() else None)

    # Filter hard "never show seen"
    eligible, seen_counts = filters.filter_seen(pool_items, seen_index)
    # Persist for inspection if you want
    _write_json(run_dir / "assistant_feed.json", eligible)  # light snapshot (pre-enrichment)

    # =========================
    # 3) Enrich (now with search_multi() fallback)
    # =========================
    enriched_path = run_dir / "items.enriched.json"
    enrich.write_enriched(items_in_path=disc_path, out_path=enriched_path, run_dir=run_dir)

    # Load enriched and re-apply seen filter (in case enrichment added imdb_id improving de-dupe)
    enriched = _read_json(enriched_path) or []
    eligible2, seen_counts2 = filters.filter_seen(enriched, seen_index)

    # =========================
    # 4) Build/update user profile DNA from CSV (export → exports/user_model.json)
    # =========================
    exports_dir = run_dir / "exports"
    user_model = profile.build_user_model(ratings_csv, exports_dir)

    # =========================
    # 5) Score
    # =========================
    ranked = scorer.score_items(eligible2, user_model, env)
    # Overwrite enriched.json with ranked list (historically consumers look here)
    _write_json(enriched_path, ranked)

    # =========================
    # 6) Write diagnostics
    # =========================
    counts = {
        "pool_before": pool_tel.get("pool_size_before"),
        "pool_after": pool_tel.get("pool_size_after"),
        "pool_appended": pool_tel.get("pool_appended_this_run", 0),
        "eligible_pre_enrich": len(eligible),
        "eligible": len(eligible2),
        "excluded_seen_pre_enrich": seen_counts.get("excluded", 0),
        "excluded_seen": seen_counts2.get("excluded", 0),
        "scored": len(ranked),
    }
    diag_path = run_dir / "diag.json"
    prior_diag = _read_json(diag_path) or {}
    prior_diag["counts"] = {**prior_diag.get("counts", {}), **counts}
    prior_diag["pool"] = pool_tel
    _write_json(diag_path, prior_diag)

    # runner.log hint
    print(f" | catalog:begin")
    print(f" | catalog:end kept={len(pool_items)}")
    print(f" | results: discovered={len(pool_items)} eligible={len(eligible2)} above_cut={len(ranked)}")

if __name__ == "__main__":
    main()