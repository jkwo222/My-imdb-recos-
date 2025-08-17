# engine/runner.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from .catalog_builder import build_catalog
from .personalize import genre_weights_from_profile, apply_personal_score
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, merge_user_sources, to_user_profile
from .summarize import write_summary_md

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
STATE_DIR = ROOT / "data" / "cache" / "state"

def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def _load_user_profile(env: Dict[str,str]) -> Dict[str,Dict]:
    local = load_ratings_csv()
    remote = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        try:
            remote = fetch_user_ratings_web(uid)
        except Exception:
            remote = []
    merged = merge_user_sources(local, remote)
    profile = to_user_profile(merged)
    return profile

def _ensure_why(items: List[Dict[str,Any]]) -> None:
    for it in items:
        bits = []
        if it.get("imdb_rating") is not None:
            try: bits.append(f"IMDb {float(it['imdb_rating']):.1f}")
            except: pass
        if it.get("tmdb_vote") is not None:
            try: bits.append(f"TMDB {float(it['tmdb_vote']):.1f}")
            except: pass
        if it.get("year"):
            bits.append(str(it["year"]))
        if bits:
            it["why"] = "; ".join(bits)

def main() -> None:
    # 1) Env
    env = {k: v for k, v in os.environ.items()}

    # 2) Build/enrich catalog (discover every run + TMDB enrich + provider filter + telemetry files)
    print(" | catalog:begin")
    items = build_catalog(env)
    print(f" | catalog:end kept={len(items)}")

    # 3) Load user profile
    user_profile = _load_user_profile(env)

    # 4) Genre weights using imdb_id field (since discovered items carry imdb_id, not tconst)
    genre_weights = genre_weights_from_profile(items, user_profile, imdb_id_field="imdb_id")

    # 5) Personal score (uses best of IMDb/TMDB rating as base)
    apply_personal_score(items, genre_weights)

    # 6) Minimal validation stats
    ids_present = sum(1 for x in items if x.get("imdb_id") or x.get("tconst"))
    genres_present = sum(1 for x in items if x.get("genres"))
    print(f"validation: items={len(items)} ids_present={ids_present} genres_present={genres_present}")

    # 7) Apply optional score cut (for ranked output only; summary does its own soft cut display)
    min_cut = float(env.get("MIN_MATCH_CUT") or 0)
    ranked = sorted(items, key=lambda x: float(x.get("match_score") or x.get("score") or 0), reverse=True)
    kept_cut = [x for x in ranked if float(x.get("match_score") or x.get("score") or 0) >= min_cut]
    print(f"score-cut {min_cut}: kept {len(kept_cut)} / {len(items)}")

    # 8) Ensure 'why' populated for display lines
    _ensure_why(kept_cut)

    # 9) Write ranked outputs
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(OUT_DIR / "assistant_ranked.json", {"items": kept_cut})

    # 10) Telemetry (from catalog builder)
    meta = _read_json(OUT_DIR / "run_meta.json", default={}) or {}
    # also include the match cut used for the display
    meta["min_match_cut"] = env.get("MIN_MATCH_CUT")
    # store genre weights snapshot for debugging
    _write_json(OUT_DIR / "genre_weights.json", genre_weights)

    # 11) Summary markdown (includes telemetry + genre weights + picks)
    write_summary_md(env, kept_cut, genre_weights, meta)

    # 12) Extra quick status
    status = {
        "total_items_scored": len(items),
        "kept_after_cut": len(kept_cut),
        "min_match_cut": env.get("MIN_MATCH_CUT"),
    }
    _write_json(OUT_DIR / "debug_status.json", status)

    print(f"wrote → {OUT_DIR / 'assistant_ranked.json'}")
    print(f"wrote → {OUT_DIR / 'summary.md'}")
    print(f"wrote → {OUT_DIR / 'debug_status.json'}")

if __name__ == "__main__":
    main()