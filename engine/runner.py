# engine/runner.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .catalog_builder import build_catalog
from .personalize import genre_weights_from_profile, apply_personal_score

# Optional imports — soft-fail if imdb_sync isn't present
try:
    from .imdb_sync import (
        load_ratings_csv,
        fetch_user_ratings_web,
        merge_user_sources,
        to_user_profile,
    )
except Exception:
    def load_ratings_csv() -> List[Dict[str, str]]:
        return []
    def fetch_user_ratings_web(uid: str) -> List[Dict[str, str]]:
        return []
    def merge_user_sources(a, b):
        return list(a) + list(b)
    def to_user_profile(rows):
        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            t = str(r.get("tconst") or r.get("imdb_id") or "").strip()
            mr = None
            try:
                if r.get("my_rating") is not None:
                    mr = float(r.get("my_rating"))
            except Exception:
                mr = None
            if t:
                out[t] = {"my_rating": mr}
        return out

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = ROOT / "data" / "cache"
FEEDBACK_DOWNVOTES = CACHE_DIR / "feedback" / "downvotes.jsonl"

def _read_jsonl_indexed(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows

def _load_user_profile_from_env(env: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    local = load_ratings_csv()  # expects data/user/ratings.csv
    remote = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    return to_user_profile(merged)

def _collect_item_id(it: Dict[str, Any]) -> str:
    # Prefer tconst if present; else imdb_id
    return str(it.get("tconst") or it.get("imdb_id") or "").strip()

def _collect_downvote_id(row: Dict[str, Any]) -> str:
    # Accept either tconst or imdb_id in the feedback log
    return str(row.get("tconst") or row.get("imdb_id") or "").strip()

def _read_downvote_ids() -> set[str]:
    """Return a set of ids (tconst or imdb_id) the user has downvoted."""
    rows = _read_jsonl_indexed(FEEDBACK_DOWNVOTES)
    bad = set()
    for r in rows:
        if r.get("downvote") is True or str(r.get("type")).lower() == "downvote":
            k = _collect_downvote_id(r)
            if k:
                bad.add(k)
    return bad

def _fmt_providers(it: Dict[str, Any]) -> str:
    provs = it.get("providers") or []
    if not provs:
        return ""
    return ", ".join(sorted(set(provs)))

def _fmt_score(it: Dict[str, Any]) -> str:
    s = it.get("score")
    if s is None:
        return ""
    try:
        return f"{float(s):.0f}"
    except Exception:
        return ""

def _fmt_imdb(it: Dict[str, Any]) -> str:
    r = it.get("imdb_rating")
    if r is None:
        return ""
    try:
        return f"IMDb {float(r):.1f}"
    except Exception:
        try:
            return f"IMDb {float(str(r)):.1f}"
        except Exception:
            return ""

def _ensure_list(x) -> List[str]:
    if not x:
        return []
    if isinstance(x, list):
        return [str(v) for v in x if v]
    if isinstance(x, tuple):
        return [str(v) for v in x if v]
    return [str(x)]

def _write_summary_md(env: Dict[str, str], ranked: List[Dict[str, Any]], *, genre_weights: Dict[str, float], candidates_count: int) -> None:
    region = env.get("REGION") or "US"
    original_langs = env.get("ORIGINAL_LANGS") or ""
    subs = env.get("SUBS_INCLUDE") or ""

    gw_sorted = sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)
    top_gw = gw_sorted[:8]

    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {__import__('datetime').datetime.utcnow().date().isoformat()}")
    lines.append("")
    lines.append(f"*Region*: **{region}**  •  *Original langs*: **{original_langs}**")
    if subs:
        lines.append(f"*Subscriptions filtered*: **{subs}**")
    lines.append(f"*Candidates after filtering*: **{candidates_count}**")
    lines.append("")
    lines.append("## Your taste snapshot")
    lines.append("")
    lines.append("Based on your IMDb ratings and watch history, these genres carry the most weight in your personalized ranking:")
    lines.append("")
    lines.append("| Genre | Weight |")
    lines.append("|---|---:|")
    if top_gw:
        for g, w in top_gw:
            lines.append(f"| {g} | {w:.2f} |")
    else:
        lines.append("| — | — |")
    lines.append("")
    lines.append("## Today’s top picks")
    lines.append("")

    for i, it in enumerate(ranked[:15], start=1):
        title = it.get("title") or it.get("primaryTitle") or "Untitled"
        year = it.get("year")
        ytxt = f" ({year})" if year else ""
        mtype = it.get("type") or it.get("titleType") or ""
        imdbtxt = _fmt_imdb(it)
        provtxt = _fmt_providers(it)
        scoretxt = _fmt_score(it)

        lines.append(f"{i}. **{title}**{ytxt} — {mtype}")
        meta_bits = []
        if scoretxt:
            meta_bits.append(f"score {scoretxt}")
        if imdbtxt:
            meta_bits.append(imdbtxt)
        if provtxt:
            meta_bits.append(provtxt)
        if meta_bits:
            lines.append(f"   *{'  •  '.join(meta_bits)}*")
        yr = it.get("year")
        if imdbtxt or yr:
            lines.append(f"   > {imdbtxt}; {yr if yr else ''}".rstrip("; ").strip())
        lines.append("")

    (OUT_DIR / "summary.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

def main() -> None:
    env = dict(os.environ)

    print(" | catalog:begin")
    items = build_catalog(env)
    print(f" | catalog:end kept={len(items)}")

    # Validate presence of IDs and genres
    ids_present = sum(1 for it in items if _collect_item_id(it))
    genres_present = sum(1 for it in items if _ensure_list(it.get("genres")))
    print(f"validation: items={len(items)} ids_present={ids_present} genres_present={genres_present}")

    # Load user profile
    user_profile = _load_user_profile_from_env(env)

    # Compute genre weights — the function already falls back from tconst to imdb_id internally.
    genre_weights = genre_weights_from_profile(items, user_profile, imdb_id_field="tconst")

    # Personalize
    apply_personal_score(items, genre_weights, base_key="imdb_rating")

    # Downvote memory (match either imdb_id or tconst)
    downvoted = _read_downvote_ids()
    if downvoted:
        before = len(items)
        kept: List[Dict[str, Any]] = []
        for it in items:
            iid = _collect_item_id(it)
            if iid and iid in downvoted:
                continue
            kept.append(it)
        items = kept
        print(f"downvote-filter: removed {before - len(items)} items (persisted memory)")

    # Optional cut
    cut = None
    try:
        if env.get("MIN_MATCH_CUT"):
            cut = float(env.get("MIN_MATCH_CUT"))
    except Exception:
        cut = None
    if cut is not None:
        before = len(items)
        items = [it for it in items if isinstance(it.get("score"), (int, float)) and float(it["score"]) >= cut]
        print(f"score-cut {cut}: kept {len(items)} / {before}")

    # Sort
    def _key(it: Dict[str, Any]) -> Tuple:
        s = it.get("score"); ir = it.get("imdb_rating"); yr = it.get("year") or 0
        try: s = float(s) if s is not None else -1.0
        except Exception: s = -1.0
        try: ir = float(ir) if ir is not None else -1.0
        except Exception: ir = -1.0
        return (-s, -ir, -yr)

    ranked = sorted(items, key=_key)

    # Write ranked + summary + debug
    (OUT_DIR / "assistant_ranked.json").write_text(
        json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _write_summary_md(env, ranked, genre_weights=genre_weights, candidates_count=len(ranked))

    status = {
        "candidates_ranked": len(ranked),
        "downvotes_seen": len(downvoted),
        "has_user_profile": bool(user_profile),
        "env_snapshot": {
            "REGION": env.get("REGION"),
            "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS"),
            "SUBS_INCLUDE": env.get("SUBS_INCLUDE"),
            "MIN_MATCH_CUT": env.get("MIN_MATCH_CUT"),
            "IMDB_USER_ID": bool(env.get("IMDB_USER_ID")),
        },
        "top_genres": sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)[:8],
        "validation": {
            "items_total": len(items),
            "items_with_ids": ids_present,
            "items_with_genres": genres_present,
            "genres_missing_count": max(0, len(items) - genres_present),
        },
    }
    (OUT_DIR / "debug_status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"wrote → {OUT_DIR/'assistant_ranked.json'}")
    print(f"wrote → {OUT_DIR/'summary.md'}")
    print(f"wrote → {OUT_DIR/'debug_status.json'}")

if __name__ == "__main__":
    main()