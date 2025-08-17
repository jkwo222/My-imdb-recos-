# engine/runner.py
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Local imports
from .catalog_builder import build_catalog
from .personalize import apply_personal_score

# ---------- Paths ----------
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
CACHE_DIR = ROOT / "data" / "cache"
STATE_DIR = CACHE_DIR / "state"

OUT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

PERSONAL_STATE_PATH = STATE_DIR / "personal_state.json"


# ---------- Helpers ----------
def _env_list(csvish: str | None) -> List[str]:
    if not csvish:
        return []
    return [s.strip() for s in csvish.split(",") if s.strip()]

def _read_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _to_list(x) -> List[str]:
    if not x:
        return []
    if isinstance(x, (list, tuple)):
        return [str(i) for i in x if i]
    return [str(x)]

def _fmt_sources(it: Dict[str, Any]) -> str:
    prov = _to_list(it.get("providers"))
    if not prov:
        return "—"
    # Keep it short but useful
    if len(prov) <= 6:
        return ", ".join(sorted(prov))
    return ", ".join(sorted(prov)[:6]) + ", …"

def _rating_str(it: Dict[str, Any]) -> str:
    r = it.get("imdb_rating")
    try:
        if r is None or r == "":
            return "—"
        return f"{float(r):.1f}"
    except Exception:
        return "—"

def _safe_year(it: Dict[str, Any]) -> str:
    y = it.get("year")
    try:
        return str(int(y)) if y is not None else "—"
    except Exception:
        return "—"

def _title_line(it: Dict[str, Any]) -> str:
    n = it.get("title") or "?"
    kind = it.get("type") or "movie"
    yr = _safe_year(it)
    return f"**{n}** ({yr}) — {kind}"

def _score_guard(x) -> float:
    try:
        fx = float(x)
        # clamp to 0..100
        return max(0.0, min(100.0, fx))
    except Exception:
        return 0.0

def _apply_min_cut(items: List[Dict[str, Any]], cut: float | None) -> List[Dict[str, Any]]:
    if cut is None:
        return items
    th = float(cut)
    return [it for it in items if _score_guard(it.get("score")) >= th]

def _best(items: List[Dict[str, Any]], k: int = 20) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda it: _score_guard(it.get("score")), reverse=True)[:k]


# ---------- Summary writer ----------
def write_summary_md(env: Dict[str, str], *, genre_weights: Dict[str, float]) -> None:
    """
    Creates/overwrites data/out/latest/summary.md.
    """
    region = (env.get("REGION") or "US").upper()
    orig_langs = env.get("ORIGINAL_LANGS") or "—"
    subs = env.get("SUBS_INCLUDE") or ""
    subs_disp = subs if subs else "—"

    # Read meta saved by catalog_builder
    meta = _read_json(OUT_DIR / "run_meta.json", default={})
    candidates_count = int(meta.get("candidates_after_filtering") or 0)
    using_imdb = bool(meta.get("using_imdb"))
    note = meta.get("note") or ("Using IMDb TSVs" if using_imdb else "Using TMDB fallback")

    # Load the scored feed (written below in main)
    scored = _read_json(OUT_DIR / "assistant_feed_scored.json", default=[])
    top = _best(scored, 15)

    # Build the markdown
    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {os.getenv('GITHUB_RUN_DATE') or ''}".strip())
    lines.append("")
    lines.append(f"*Region*: **{region}**  •  *Original langs*: **{orig_langs}**")
    lines.append(f"*Subscriptions filtered*: **{subs_disp if subs_disp else '—'}**")
    lines.append(f"*Candidates after filtering*: **{candidates_count}**")
    lines.append("")
    lines.append("## Your taste snapshot")
    if genre_weights:
        lines.append("")
        lines.append("Based on your IMDb ratings and watch history, these genres carry the most weight in your personalized ranking:")
        lines.append("")
        lines.append("| Genre | Weight |")
        lines.append("|---|---:|")
        # top 12 weights
        for g, w in sorted(genre_weights.items(), key=lambda kv: kv[1], reverse=True)[:12]:
            lines.append(f"| {g} | {w:.2f} |")
    else:
        lines.append("")
        lines.append("_No genre weights available (TMDB fallback mode or no ratings found)._")

    lines.append("")
    lines.append("## Today’s top picks")
    if not top:
        lines.append("")
        lines.append("_No picks after filters/cut; try lowering MIN_MATCH_CUT or expanding subscriptions._")
    else:
        for i, it in enumerate(top, 1):
            tline = _title_line(it)
            imdb_r = _rating_str(it)
            provs = _fmt_sources(it)
            score_s = f"{_score_guard(it.get('score')):.1f}"
            lines.append("")
            lines.append(f"{i}. {tline}")
            lines.append(f"   *score {score_s}  •  IMDb {imdb_r}  •  {provs}*")
            # one short info line
            year = _safe_year(it)
            lines.append(f"   > IMDb {imdb_r}; {year}")

    lines.append("")
    lines.append(f"---\n_{note}_")

    # Write to summary.md
    (OUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")


# ---------- Main ----------
def main() -> None:
    # Read env (GitHub Actions passes these)
    env = dict(os.environ)
    min_cut_env = env.get("MIN_MATCH_CUT")
    min_cut: float | None = None
    try:
        if min_cut_env:
            min_cut = float(min_cut_env)
    except Exception:
        min_cut = None

    print(" | catalog:begin")
    items = build_catalog(env)  # never throws now; falls back if IMDb TSVs are missing
    print(f" | catalog:end kept={len(items)} → {str((OUT_DIR / 'assistant_feed.json').relative_to(ROOT))}")

    # Personalization: load genre weights saved by catalog_builder
    personal_state = _read_json(PERSONAL_STATE_PATH, default={})
    genre_weights: Dict[str, float] = personal_state.get("genre_weights") or {}

    # Score items in-place, sort, apply min cut
    apply_personal_score(items, genre_weights, base_key="imdb_rating")
    items_sorted = sorted(items, key=lambda it: _score_guard(it.get("score")), reverse=True)
    if min_cut is not None:
        items_sorted = _apply_min_cut(items_sorted, min_cut)

    # Persist scored list (used by summary + debugging)
    (OUT_DIR / "assistant_feed_scored.json").write_text(
        json.dumps(items_sorted, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # Write human summary for GH Step Summary
    write_summary_md(env, genre_weights=genre_weights)


if __name__ == "__main__":
    main()