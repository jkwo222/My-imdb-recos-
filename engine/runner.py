# engine/runner.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from pathlib import Path
from datetime import datetime
import os, json

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data"
OUT_DIR = DATA_DIR / "out" / "latest"
CACHE_DIR = DATA_DIR / "cache"
FEEDBACK_DIR = CACHE_DIR / "feedback"
FEEDBACK_DIR.mkdir(parents=True, exist_ok=True)

from .catalog_builder import build_catalog
from .imdb_sync import load_ratings_csv, fetch_user_ratings_web, fetch_public_list, merge_user_sources, to_user_profile
from .personalize import genre_weights_from_profile, apply_personal_score
from .summarize import write_summary_md

def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def _load_downvotes() -> Dict[str,Any]:
    path = FEEDBACK_DIR / "downvotes.jsonl"
    idx: Dict[str,Any] = {}
    if not path.exists():
        return idx
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                t = str(obj.get("tconst") or "")
                if t:
                    idx[t] = obj
            except Exception:
                pass
    return idx

def _load_feed() -> List[Dict[str,Any]]:
    p = OUT_DIR / "assistant_feed.json"
    if p.exists():
        return _read_json(p, [])
    return []

def _save_pool(items: List[Dict[str,Any]]) -> None:
    _write_json(CACHE_DIR / "persistent_pool.json", items)

def _load_pool() -> List[Dict[str,Any]]:
    return _read_json(CACHE_DIR / "persistent_pool.json", [])

def main() -> None:
    os.environ.setdefault("RUN_DATE", datetime.utcnow().date().isoformat())

    # 1) Build catalog (uses your existing catalog_builder and your existing cache/tmdb code)
    env = dict(os.environ)
    items = build_catalog(env)
    total_candidates = len(items)

    # 2) Load user signals (works even when ratings.csv is missing)
    local = load_ratings_csv()  # data/user/ratings.csv (may be empty)
    remote = []
    public_list = []
    if env.get("IMDB_USER_ID"):
        remote = fetch_user_ratings_web(env["IMDB_USER_ID"])
    if env.get("IMDB_PUBLIC_LIST_URL"):
        public_list = fetch_public_list(env["IMDB_PUBLIC_LIST_URL"])

    merged_rows = merge_user_sources(local, remote, public_list)
    user_profile = to_user_profile(merged_rows)

    print(f"[profile] local_csv={len(local)} remote_user_hits={len(remote)} public_list_hits={len(public_list)} merged={len(user_profile)}")

    # 3) Downvote memory
    downvotes = _load_downvotes()

    # 4) Personalize + score
    genre_weights = genre_weights_from_profile(items, user_profile, imdb_id_field="tconst")
    apply_personal_score(items, genre_weights, base_key="imdb_rating", downvote_index=downvotes)

    # 5) Sort + cut
    try:
        min_cut = int(env.get("MIN_MATCH_CUT","100"))
    except Exception:
        min_cut = 100
    ranked = sorted(items, key=lambda it: (-(float(it.get("score") or 0.0)), -(float(it.get("imdb_rating") or 0.0))))
    picks = ranked[:min_cut]

    # 6) Update persistent pool (accumulate across runs)
    pool = _load_pool()
    seen = { (it.get("tconst"), it.get("type")) for it in pool }
    added = 0
    for it in ranked:
        key = (it.get("tconst"), it.get("type"))
        if key not in seen:
            pool.append(it)
            seen.add(key)
            added += 1
    _save_pool(pool)

    # 7) Write artifacts
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(OUT_DIR / "assistant_feed.json", ranked)
    write_summary_md(env, genre_weights=genre_weights, picks=picks, kept_count=len(ranked), candidate_total=total_candidates)

    print(f"[runner] catalog={total_candidates} ranked={len(ranked)} picks={len(picks)} pool+={added} downvotes={len(downvotes)}")

if __name__ == "__main__":
    main()