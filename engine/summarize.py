# engine/summarize.py
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Paths
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
FEED_JSON = OUT_DIR / "assistant_feed.json"
SUMMARY_MD = OUT_DIR / "summary.md"

# --- Helpers -----------------------------------------------------------------

def _safe_num(x: Any, default: float = float("nan")) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _fmt_score(v: Any) -> str:
    f = _safe_num(v, default=float("nan"))
    if math.isnan(f):
        return "—"
    return f"{int(round(f)):d}/100"

def _fmt_imdb(v: Any) -> str:
    f = _safe_num(v, default=float("nan"))
    if math.isnan(f):
        return "—"
    # IMDb is 0–10, show one decimal
    return f"{f:.1f}"

def _join(items: List[str], sep: str = ", ") -> str:
    return sep.join([s for s in items if s])

def _as_list(x: Any) -> List[str]:
    if not x:
        return []
    if isinstance(x, (list, tuple)):
        return [str(i) for i in x if i is not None]
    return [str(x)]

def _top_k(d: Dict[str, float], k: int = 8) -> List[Tuple[str, float]]:
    return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:k]

def _chipify(words: List[str], max_len: int = 6, limit: int = 6) -> str:
    chips = []
    for w in words[:limit]:
        s = (w or "").strip()
        if not s:
            continue
        # keep chips short
        if len(s) > 24:
            s = s[:21] + "…"
        chips.append(f"`{s}`")
    return " ".join(chips)

def _load_feed() -> List[Dict[str, Any]]:
    if not FEED_JSON.exists():
        return []
    try:
        data = json.loads(FEED_JSON.read_text(encoding="utf-8"))
    except Exception:
        return []
    # Expect either {"items":[...]} or a raw list
    if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
        items = data["items"]
    elif isinstance(data, list):
        items = data
    else:
        items = []
    # Normalize a few fields we rely on later
    norm = []
    for it in items:
        t = dict(it)
        # common field aliases
        t.setdefault("title", t.get("name") or t.get("original_title"))
        t.setdefault("type", t.get("media_type") or t.get("kind"))
        t.setdefault("year", t.get("release_year") or t.get("year"))
        t.setdefault("score", t.get("user_score") or t.get("rank_score"))
        t.setdefault("imdb_rating", t.get("imdb") or t.get("imdb_rating"))
        t.setdefault("providers", t.get("where_to_watch") or t.get("providers") or [])
        t.setdefault("genres", t.get("genres") or [])
        t.setdefault("why", t.get("why") or t.get("explanation") or "")
        norm.append(t)
    return norm

def _env_list(env: Dict[str, str], key: str) -> List[str]:
    raw = env.get(key) or ""
    return [s.strip() for s in raw.split(",") if s.strip()]

# --- Markdown builder ---------------------------------------------------------

def _header_lines(env: Dict[str, str], item_count: int) -> List[str]:
    now = datetime.now(timezone.utc).astimezone()
    date_str = now.strftime("%Y-%m-%d")
    region = env.get("REGION") or "US"
    subs = _env_list(env, "SUBS_INCLUDE")
    langs = _env_list(env, "ORIGINAL_LANGS")

    lines = []
    lines.append(f"# Daily Recommendations — {date_str}")
    lines.append("")
    lines.append(f"*Region*: **{region}**" + (f"  •  *Original langs*: **{_join(langs)}**" if langs else ""))
    if subs:
        lines.append(f"*Subscriptions filtered*: **{_join(subs)}**")
    lines.append(f"*Candidates after filtering*: **{item_count}**")
    lines.append("")
    return lines

def _taste_profile_lines(genre_weights: Dict[str, float] | None) -> List[str]:
    if not genre_weights:
        return []

    top = _top_k(genre_weights, k=8)
    if not top:
        return []

    lines = []
    lines.append("## Your taste snapshot")
    lines.append("")
    lines.append("Based on your IMDb ratings and watch history, these genres carry the most weight in your personalized ranking:")
    lines.append("")
    # Simple two-column table: Genre | Weight
    lines.append("| Genre | Weight |")
    lines.append("|---|---:|")
    for g, w in top:
        lines.append(f"| {g} | {w:.2f} |")
    lines.append("")
    return lines

def _explain_alignment(genres: List[str], genre_weights: Dict[str, float] | None) -> str:
    if not genre_weights or not genres:
        return ""
    parts = []
    for g in genres:
        w = genre_weights.get(g)
        if w and w > 0:
            parts.append(f"{g} (+{w:.2f})")
    if not parts:
        return ""
    return "Genre fit: " + ", ".join(parts[:4])

def _items_lines(items: List[Dict[str, Any]], genre_weights: Dict[str, float] | None, limit: int = 15) -> List[str]:
    if not items:
        return ["_No recommendations available today._", ""]

    # Sort by our unified 0–100 score if present; fall back to IMDb
    def _key(it: Dict[str, Any]) -> Tuple[float, float]:
        score = _safe_num(it.get("score"), default=float("nan"))
        imdb = _safe_num(it.get("imdb_rating"), default=float("nan"))
        # use -score for descending; nan should sort last
        score_key = score if not math.isnan(score) else -1.0
        imdb_key = imdb if not math.isnan(imdb) else -1.0
        return (score_key, imdb_key)

    ranked = sorted(items, key=_key, reverse=True)[:limit]

    lines: List[str] = []
    lines.append("## Today’s top picks")
    lines.append("")
    for idx, it in enumerate(ranked, start=1):
        title = (it.get("title") or "Untitled").strip()
        year = str(it.get("year") or "").strip()
        mtype = (it.get("type") or "").strip()
        score = _fmt_score(it.get("score"))
        imdb = _fmt_imdb(it.get("imdb_rating"))
        genres = _as_list(it.get("genres"))
        providers = _as_list(it.get("providers"))
        why = (it.get("why") or "").strip()

        # Build the headline line
        head_bits = [f"**{title}**"]
        if year:
            head_bits[-1] += f" ({year})"
        if mtype:
            head_bits.append(mtype)
        head = " — ".join([b for b in head_bits if b])

        metrics = []
        metrics.append(f"score {score}")
        if imdb != "—":
            metrics.append(f"IMDb {imdb}")
        if providers:
            metrics.append(_join(providers))
        metrics_str = "  •  ".join(metrics)

        # chips
        chips = _chipify(genres, limit=6)
        align = _explain_alignment(genres, genre_weights)

        lines.append(f"{idx}. {head}")
        if metrics_str:
            lines.append(f"   *{metrics_str}*")
        if chips:
            lines.append(f"   {chips}")
        if why:
            lines.append(f"   > {why}")
        if align:
            lines.append(f"   _{align}_")
        lines.append("")  # blank line between items

    return lines

def _footer_lines(total_items: int) -> List[str]:
    lines = []
    lines.append("---")
    lines.append(f"_Generated from {total_items} candidate titles._")
    lines.append("")
    return lines

# --- Public API ---------------------------------------------------------------

def write_summary_md(
    env: Dict[str, str],
    genre_weights: Dict[str, float] | None = None,
    picks_limit: int = 15,
) -> Path:
    """
    Build the markdown summary into data/out/latest/summary.md.

    Parameters
    ----------
    env : dict
        Environment variables (REGION, SUBS_INCLUDE, ORIGINAL_LANGS, etc.)
    genre_weights : dict or None
        Per-genre weights computed upstream from the user's ratings/history.
    picks_limit : int
        How many ranked items to include in the summary.
    """
    items = _load_feed()
    # Header
    lines: List[str] = []
    lines += _header_lines(env, item_count=len(items))
    # Taste profile (if supplied)
    lines += _taste_profile_lines(genre_weights)
    # Recommendations
    lines += _items_lines(items, genre_weights, limit=picks_limit)
    # Footer
    lines += _footer_lines(total_items=len(items))

    # Ensure directory, then write file (fix for previous bug)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines).rstrip() + "\n"
    SUMMARY_MD.write_text(content, encoding="utf-8")
    return SUMMARY_MD