from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import json
import math

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out" / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------- tiny utils ---------

def _read_json(p: Path, default: Any) -> Any:
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def _fmt_num(n: Optional[float | int], digits: int = 1) -> str:
    if n is None:
        return "—"
    try:
        if isinstance(n, str) and n.strip() == "":
            return "—"
        if isinstance(n, str):
            n = float(n)
        if isinstance(n, int):
            return f"{n}"
        return f"{n:.{digits}f}"
    except Exception:
        return "—"

def _coalesce(*vals):
    for v in vals:
        if v not in (None, "", [], {}, "NaN", "nan"):
            return v
    return None

def _providers_human(providers: Optional[Iterable[str] | Iterable]) -> str:
    if not providers:
        return "—"
    # Already human strings in our pipeline; just join defensively
    try:
        return ", ".join(str(p) for p in providers if str(p).strip())
    except Exception:
        return "—"

def _safe_year(item: Dict[str, Any]) -> str:
    # prefer explicit `year`, then any TMDB dates (yyyy-mm-dd)
    y = item.get("year")
    if isinstance(y, int):
        return str(y)
    if isinstance(y, str) and y.isdigit():
        return y
    fd = item.get("first_air_date") or item.get("release_date")
    if isinstance(fd, str) and len(fd) >= 4 and fd[:4].isdigit():
        return fd[:4]
    return "—"

def _title_line(item: Dict[str, Any]) -> str:
    title = item.get("title") or item.get("name") or "Untitled"
    media_type = item.get("type") or item.get("tmdb_media_type") or "movie"
    year = _safe_year(item)

    imdb = _fmt_num(_coalesce(item.get("imdb_rating"), item.get("imdb_vote")))
    tmdb = _fmt_num(item.get("tmdb_vote"))
    score = _fmt_num(item.get("match_score"), 2)

    provs = _providers_human(item.get("providers"))
    why = item.get("why")
    why_seg = f" — {why}" if why else ""

    return f"- **{title}** ({year}) · {media_type} · IMDb {_fmt_num(imdb)} · TMDB {_fmt_num(tmdb)} · Match {score} · {provs}{why_seg}"

# --------- public API ---------

def write_summary_md(
    env: Dict[str, str],
    *,
    ranked_items: Optional[List[Dict[str, Any]]] = None,
    genre_weights: Optional[Dict[str, float]] = None,
    telemetry: Optional[Dict[str, Any]] = None,
) -> Path:
    """
    Builds data/out/latest/summary.md using available outputs + optional in-memory data.
    Safe to call even if files are missing; it’ll degrade gracefully.

    Inputs it will look for on disk (if not passed in):
      - assistant_ranked.json
      - run_meta.json
      - debug_status.json
      - data/cache/state/personal_state.json (genre weights fallback)
    """
    # Resolve inputs
    ranked = ranked_items
    if ranked is None:
        ranked = _read_json(OUT_DIR / "assistant_ranked.json", default=[])
    meta = _read_json(OUT_DIR / "run_meta.json", default={})
    dbg = _read_json(OUT_DIR / "debug_status.json", default={})

    # Genre weights: prefer explicit param; else personal_state; else {}.
    gw = genre_weights
    if gw is None:
        personal_state = _read_json(ROOT / "data" / "cache" / "state" / "personal_state.json", default={})
        gw = personal_state.get("genre_weights", {}) if isinstance(personal_state, dict) else {}

    tel = telemetry or {}
    def tget(k: str, default: Any = 0):
        v = tel.get(k, default)
        # also allow nested under 'counts'
        if v in (None, "") and isinstance(tel.get("counts"), dict):
            v = tel["counts"].get(k, default)
        return v

    # Pull a few envs for display
    region = (env.get("REGION") or "US").upper()
    langs = env.get("ORIGINAL_LANGS") or "—"
    subs = env.get("SUBS_INCLUDE") or "—"
    min_cut = env.get("MIN_MATCH_CUT") or meta.get("min_match_cut") or "—"
    discover_pages = env.get("DISCOVER_PAGES") or "—"

    # Counts (with safe defaults)
    c_discover_new = int(tget("discover_new", 0))
    c_imdb_rows = int(tget("imdb_rows", 0))
    c_pool_pre = int(tget("pool_before_filter", 0))
    c_ex_user_csv = int(tget("excluded_user_csv", 0))
    c_ex_user_web = int(tget("excluded_user_web", 0))
    c_after_ex = int(tget("pool_after_exclusions", 0))
    c_after_subs = int(tget("pool_after_subs_filter", 0))
    c_ranked = len(ranked or [])
    c_scored_cut = int(tget("scored_cut", c_ranked))

    # Build genre weights table lines (skip empty/near-zero)
    gw_lines: List[str] = []
    if isinstance(gw, dict) and gw:
        # Show top 12 by weight
        top = sorted(gw.items(), key=lambda kv: kv[1], reverse=True)[:12]
        for g, w in top:
            if w is None:
                continue
            try:
                val = float(w)
            except Exception:
                continue
            if math.isfinite(val) and val > 0:
                gw_lines.append(f"| {g} | {val:.2f} |")
    if not gw_lines:
        gw_lines.append("| — | — |")

    # Pick list: take up to 30
    pick_lines: List[str] = []
    for it in (ranked or [])[:30]:
        pick_lines.append(_title_line(it))

    # Compose Markdown
    lines: List[str] = []
    lines.append(f"# Daily Recommendations — {region}")
    lines.append("")
    lines.append(f"*Region:* **{region}**  •  *Original langs:* **{langs}**  •  *Subscriptions filtered:* **{subs}**")
    lines.append(f"*Discover pages:* **{discover_pages}**  •  *Score cutoff:* **{min_cut}**")
    lines.append("")
    lines.append("## Data health / sources")
    lines.append("")
    imdb_used = "Yes" if meta.get("using_imdb") else "No"
    lines.append(f"- IMDb TSV available: **{imdb_used}**  •  Discover new titles today: **{c_discover_new}**  •  IMDb rows: **{c_imdb_rows}**")
    lines.append(f"- Pool before exclusions: **{c_pool_pre}**")
    lines.append(f"  - Excluded (your ratings.csv): **{c_ex_user_csv}**")
    lines.append(f"  - Excluded (IMDb web ratings): **{c_ex_user_web}**")
    lines.append(f"- Pool after exclusions: **{c_after_ex}**")
    lines.append(f"- After subscription filter: **{c_after_subs}**")
    lines.append(f"- Ranked items: **{c_ranked}**  •  After score cut: **{c_scored_cut}**")
    lines.append("")
    lines.append("## Your taste snapshot (top genres)")
    lines.append("")
    lines.append("| Genre | Weight |")
    lines.append("|------:|------:|")
    lines.extend(gw_lines)
    lines.append("")
    lines.append("## Today’s top picks")
    lines.append("")
    if pick_lines:
        lines.extend(pick_lines)
    else:
        lines.append("_No picks after filtering_")

    # Write to a FILE, not the directory (bug fix)
    out_md = OUT_DIR / "summary.md"
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return out_md