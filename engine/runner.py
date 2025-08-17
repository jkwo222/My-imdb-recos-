# engine/runner.py
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .catalog_builder import build_catalog
from .personalize import genre_weights_from_profile, apply_personal_score

# Optional imports — these may exist in your repo already
try:
    from .imdb_sync import (
        load_ratings_csv,
        fetch_user_ratings_web,
        merge_user_sources,
        to_user_profile,
    )
except Exception:
    # Fallbacks if imdb_sync isn't available — treat as empty profile
    def load_ratings_csv() -> List[Dict[str, str]]:
        return []
    def fetch_user_ratings_web(uid: str) -> List[Dict[str, str]]:
        return []
    def merge_user_sources(a, b):
        return list(a) + list(b)
    def to_user_profile(rows):
        # expected format: {tconst: {"my_rating": float, ...}, ...}
        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            t = str(r.get("tconst") or "").strip()
            try:
                mr = float(r.get("my_rating")) if r.get("my_rating") is not None else None
            except Exception:
                mr = None
            if t:
                out[t] = {"my_rating": mr}
        return out

# Minimal helpers to read feedback downvotes without depending on another module
def _read_jsonl_indexed(path: Path, key: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                k = str(obj.get(key))
                if k and k not in ("None", "null"):
                    out[k] = obj
            except Exception:
                continue
    return out

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = ROOT / "data" / "cache"
FEEDBACK_DOWNVOTES = CACHE_DIR / "feedback" / "downvotes.jsonl"

def _load_user_profile_from_env(env: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    local = load_ratings_csv()  # expects data/user/ratings.csv
    remote = []
    uid = (env.get("IMDB_USER_ID") or "").strip()
    if uid:
        remote = fetch_user_ratings_web(uid)
    merged = merge_user_sources(local, remote)
    return to_user_profile(merged)

def _read_downvote_ids() -> set[str]:
    """Return a set of tconst IDs the user has downvoted (if any)."""
    idx = _read_jsonl_indexed(FEEDBACK_DOWNVOTES, key="tconst")
    bad = set()
    for tid, row in idx.items():
        # Allow either {"downvote": true} or a "type": "downvote"
        if row.get("downvote") is True or str(row.get("type")).lower() == "downvote":
            bad.add(str(tid))
    return bad

def _fmt_providers(it: Dict[str, Any]) -> str:
    provs = it.get("providers") or []
    if not provs:
        return ""
    # make it a short comma list
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
        # sometimes ratings are strings like "8.7"
        try:
            return f"IMDb {float(str(r)):.1f}"
        except Exception:
            return ""

def _write_summary_md(env: Dict[str, str], ranked: List[Dict[str, Any]], *, genre_weights: Dict[str, float], candidates_count: int) -> None:
    # header bits
    region = env.get("REGION") or "US"
    original_langs = env.get("ORIGINAL_LANGS") or ""
    subs = env.get("SUBS_INCLUDE") or ""
    # taste snapshot (top few)
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

    # Include up to 15 entries
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
        # tiny extra line with quick facts
        # (keep short to avoid clutter — we already show IMDb + year)
        yr = it.get("year")
        if imdbtxt or yr:
            lines.append(f"   > {imdbtxt}; {yr if yr else ''}".rstrip("; ").strip())
        lines.append("")

    md_path = OUT_DIR / "summary.md"
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

def main() -> None:
    env = dict(os.environ)

    print(" | catalog:begin")
    items = build_catalog(env)  # writes assistant_feed.json (base set)
    print(f" | catalog:end kept={len(items)}")

    # Load user profile (local CSV + optional IMDb public list merge)
    user_profile = _load_user_profile_from_env(env)

    # Compute genre weights based on *mapped* titles in catalog
    genre_weights = genre_weights_from_profile(items, user_profile, imdb_id_field="tconst")

    # Apply personalization
    apply_personal_score(items, genre_weights, base_key="imdb_rating")

    # Downvote memory
    downvoted = _read_downvote_ids()
    if downvoted:
        before = len(items)
        items = [it for it in items if str(it.get("tconst")) not in downvoted]
        print(f"downvote-filter: removed {before - len(items)} items (persisted memory)")

    # Optional scored cut
    try:
        cut = int(env.get("MIN_MATCH_CUT")) if env.get("MIN_MATCH_CUT") else None
    except Exception:
        cut = None
    if cut is not None:
        before = len(items)
        items = [it for it in items if (isinstance(it.get("score"), (int, float)) and it["score"] >= cut)]
        print(f"score-cut {cut}: kept {len(items)} / {before}")

    # Sort by personalized score (desc), then IMDb rating (desc), then votes/year fallback
    def _key(it: Dict[str, Any]) -> Tuple:
        s = it.get("score")
        try:
            s = float(s) if s is not None else -1.0
        except Exception:
            s = -1.0
        ir = it.get("imdb_rating")
        try:
            ir = float(ir) if ir is not None else -1.0
        except Exception:
            ir = -1.0
        yr = it.get("year") or 0
        return (-s, -ir, -yr)

    ranked = sorted(items, key=_key)

    # Write ranked output
    (OUT_DIR / "assistant_ranked.json").write_text(
        json.dumps(ranked, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Summary markdown for the GH step summary + issue comment
    _write_summary_md(env, ranked, genre_weights=genre_weights, candidates_count=len(ranked))

    # Some debug stats to help validate runs
    status = {
        "candidates_ranked": len(ranked),
        "downvotes_seen": len(downvoted),
        "has_user_profile": bool(user_profile),
        "env_snapshot": {
            "REGION": env.get("REGION"),
            "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS"),
            "SUBS_INCLUDE": env.get("SUBS_INCLUDE"),
            "MIN_MATCH_CUT": env.get("MIN_MATCH_CUT"),
            "IMDB_USER_ID": bool(env.get("IMDB_USER_ID")),  # redact actual id
        },
        "top_genres": sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)[:8],
    }
    (OUT_DIR / "debug_status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"wrote → {OUT_DIR/'assistant_ranked.json'}")
    print(f"wrote → {OUT_DIR/'summary.md'}")
    print(f"wrote → {OUT_DIR/'debug_status.json'}")

if __name__ == "__main__":
    main()