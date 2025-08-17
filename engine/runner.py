from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# --- Local imports
from .catalog_builder import build_catalog
from .self_check import run_self_check
from .scoring import (
    load_seen_index_from_paths,
    filter_unseen,
    add_match_scores,
    apply_match_cut,
    summarize_selection,
)

# Optional: rotation is used inside catalog_builder in many setups,
# but we import it here to keep parity with the existing repo layout.
# from .rotation import plan_pages


# ============================================================
# Files / IO helpers
# ============================================================

ROOT = Path(os.getcwd())
OUT_DIR = ROOT / "out"
DEBUG_DIR = OUT_DIR / "debug"
DATA_DIR = ROOT / "data"

def _ensure_dirs() -> None:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# ============================================================
# Env
# ============================================================

@dataclass
class Cfg:
    region: str
    subs_include: List[str]
    discover_pages: int
    rotate_minutes: int
    min_match_cut: float

def _parse_env_list(val: str) -> List[str]:
    return [p.strip() for p in val.split(",") if p.strip()]

def _load_env() -> Dict[str, Any]:
    """
    Return a plain dict so downstream code can call env.get(...).
    """
    env: Dict[str, Any] = {}
    # Basics
    env["REGION"] = os.environ.get("REGION", "US").strip() or "US"
    env["SUBS_INCLUDE"] = os.environ.get(
        "SUBS_INCLUDE",
        "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",
    )
    env["DISCOVER_PAGES"] = int(os.environ.get("DISCOVER_PAGES", "3") or "3")
    env["ROTATE_MINUTES"] = int(os.environ.get("ROTATE_MINUTES", "60") or "60")
    env["MIN_MATCH_CUT"] = float(os.environ.get("MIN_MATCH_CUT", "58") or "58")

    # Allow an alternate ratings path (optional)
    env["RATINGS_PATH"] = os.environ.get("RATINGS_PATH", "").strip()

    # Surface other knobs untouched (providers, langs, etc.) if present
    # so catalog_builder can look them up.
    passthrough_keys = [
        "TMDB_ACCESS_TOKEN",
        "TMDB_REGION",
        "WITH_ORIGINAL_LANGS",
        "WITH_WATCH_PROVIDERS",
        "PROVIDER_REGION",
        "PAGE_CAP",
        "STEP",
    ]
    for k in passthrough_keys:
        v = os.environ.get(k)
        if v is not None:
            env[k] = v
    return env

def _cfg_from_env(env: Dict[str, Any]) -> Cfg:
    return Cfg(
        region=str(env.get("REGION", "US")),
        subs_include=_parse_env_list(str(env.get("SUBS_INCLUDE", ""))),
        discover_pages=int(env.get("DISCOVER_PAGES", 3)),
        rotate_minutes=int(env.get("ROTATE_MINUTES", 60)),
        min_match_cut=float(env.get("MIN_MATCH_CUT", 58.0)),
    )


# ============================================================
# Telemetry helpers
# ============================================================

def _telemetry_markdown(cfg: Cfg, discovered: int, enriched: int, kept_after_exclusions: int,
                        above_cut: int, errors: int, excl_seen: int, excl_list_sz: int) -> str:
    sublist = ",".join(cfg.subs_include)
    return f"""# Daily recommendations

## Telemetry

- Region: **{cfg.region}**
- SUBS_INCLUDE: `{sublist}`
- Discover pages: **{cfg.discover_pages}**
- Discovered (raw): **{discovered}**
- Enriched (details fetched): **{enriched}**; errors: **{errors}**
- Exclusion list size (ratings + IMDb web): **{excl_list_sz}**
- Excluded for being seen: **{excl_seen}**
- Eligible after exclusions: **{kept_after_exclusions}**
- Above match cut (≥ {cfg.min_match_cut}): **{above_cut}**
"""

# ============================================================
# Main pipeline
# ============================================================

def main() -> None:
    run_self_check()  # fail early if essential symbols are missing
    _ensure_dirs()

    env = _load_env()                # plain dict (has .get)
    cfg = _cfg_from_env(env)

    print(" | catalog:begin", flush=True)
    # Build your catalog using the env dict. We assume build_catalog returns a list of dict items.
    # Items are expected to already include fields like: title, year, media_type/kind/type, tmdb_vote/vote_average, providers/watch_available, possibly imdb_id.
    items: List[Dict[str, Any]] = build_catalog(env)
    print(f" | catalog:end kept={len(items)}", flush=True)

    # ---------- Load ratings / seen ----------
    # Accept explicit RATINGS_PATH, else try typical defaults
    ratings_candidates: List[Path] = []
    if env.get("RATINGS_PATH"):
        ratings_candidates.append(Path(str(env["RATINGS_PATH"])))
    ratings_candidates.extend([DATA_DIR / "ratings.csv", ROOT / "ratings.csv"])

    seen_ids, seen_pairs, ratings_diag = load_seen_index_from_paths(ratings_candidates)

    # ---------- Exclusions (seen) ----------
    before_seen = len(items)
    eligible = filter_unseen(items, seen_ids=seen_ids, seen_pairs=seen_pairs)
    excluded_seen = before_seen - len(eligible)

    # ---------- Scoring + cut ----------
    add_match_scores(eligible, tv_penalty_points=2.0)
    winners = apply_match_cut(eligible, cfg.min_match_cut)

    # ---------- Outputs ----------
    # 1) assistant_feed.json (your recommendation payload)
    feed_path = OUT_DIR / "assistant_feed.json"
    _write_json(feed_path, winners)

    # 2) telemetry summary (markdown)
    telemetry = _telemetry_markdown(
        cfg=cfg,
        discovered=len(items),
        enriched=len(items),     # if your build step enriches details; set appropriately if you split phases
        kept_after_exclusions=len(eligible),
        above_cut=len(winners),
        errors=0,                # plug in your actual error count if you track it in catalog_builder
        excl_seen=excluded_seen,
        excl_list_sz=len(seen_ids) + sum(1 for t, _ in seen_pairs if t),
    )
    _write_text(OUT_DIR / "telemetry.md", telemetry)

    # 3) diagnostics.json (rich debug blob)
    debug_payload: Dict[str, Any] = {
        "env_effective": env,
        "cfg": {
            "region": cfg.region,
            "subs_include": cfg.subs_include,
            "discover_pages": cfg.discover_pages,
            "rotate_minutes": cfg.rotate_minutes,
            "min_match_cut": cfg.min_match_cut,
        },
        "ratings_diagnostics": ratings_diag,
        "counts": {
            "raw_discovered": len(items),
            "eligible_after_seen": len(eligible),
            "excluded_seen": excluded_seen,
            "above_cut": len(winners),
        },
        "sample_eligible": [
            {
                "title": it.get("title") or it.get("name"),
                "year": it.get("year"),
                "media_type": it.get("media_type") or it.get("type") or it.get("kind"),
                "tmdb_vote": it.get("tmdb_vote") or it.get("vote_average"),
                "match": it.get("match"),
                "watch_available": it.get("watch_available") or it.get("providers"),
            }
            for it in eligible[:20]
        ],
        "summary": summarize_selection(eligible, cfg.min_match_cut),
    }
    _write_json(DEBUG_DIR / "diagnostics.json", debug_payload)

    # Also helpful to print a one-liner to logs:
    print(f" | results: discovered={len(items)} eligible={len(eligible)} above_cut={len(winners)}", flush=True)


if __name__ == "__main__":
    main()