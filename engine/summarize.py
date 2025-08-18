# engine/summarize.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from datetime import date, datetime
import json
import re

# ============= Env toggles (with sane defaults) =============
def _bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v in {"1", "true", "yes", "on"}: return True
    if v in {"0", "false", "no", "off"}: return False
    return default

def _int(name: str, default: int) -> int:
    try:
        val = os.getenv(name, "")
        return int(val) if val else default
    except Exception:
        return default

EMAIL_TOPK                 = _int("EMAIL_TOPK", 10)
EMAIL_SCORE_MIN            = _int("EMAIL_SCORE_MIN", 60)

# recency labeling (labels shown inline in title line)
EMAIL_LABEL_NEW_MOVIE      = _bool("EMAIL_INCLUDE_NEW_MOVIE_LABEL", True)
EMAIL_LABEL_NEW_SEASON     = _bool("EMAIL_INCLUDE_NEW_SEASON_LABEL", True)
EMAIL_LABEL_NEW_SERIES     = _bool("EMAIL_INCLUDE_NEW_SERIES_LABEL", False)  # optional

# windows are shared with scoring but we keep local defaults too
REC_MOVIE_WINDOW_DAYS      = _int("RECENCY_MOVIE_WINDOW_DAYS", 270)   # ~9 months
REC_TV_FIRST_WINDOW        = _int("RECENCY_TV_FIRST_WINDOW", 180)     # new series window
REC_TV_LAST_WINDOW         = _int("RECENCY_TV_LAST_WINDOW", 120)      # new season window

# ============= Provider name formatting =============
DISPLAY_PROVIDER = {
    "netflix": "Netflix",
    "max": "Max",
    "paramount_plus": "Paramount+",
    "disney_plus": "Disney+",
    "apple_tv_plus": "Apple TV+",
    "peacock": "Peacock",
    "hulu": "Hulu",
    "prime_video": "Prime Video",
}

def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    if s in {"hbo", "hbo_max", "hbomax"}: return "max"
    return s

# ============= Helpers =============
_NON = re.compile(r"[^a-z0-9]+")

def _norm_title(s: str) -> str:
    return _NON.sub(" ", (s or "").strip().lower()).strip()

def _parse_ymd(s: Optional[str]) -> Optional[date]:
    if not s: return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    if len(s) >= 4 and s[:4].isdigit():
        try:
            return date(int(s[:4]), 1, 1)
        except Exception:
            return None
    return None

def _days_since(d: Optional[date]) -> Optional[int]:
    if not d: return None
    try:
        return (date.today() - d).days
    except Exception:
        return None

def _audience_pct(it: Dict[str, Any]) -> Optional[int]:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0: f *= 10.0
        return int(round(max(0.0, min(100.0, f))))
    except Exception:
        return None

def _providers_for_item(it: Dict[str, Any], allowed: Iterable[str]) -> List[str]:
    allowed_set = {_normalize_slug(x) for x in (allowed or [])}
    provs = it.get("providers") or it.get("providers_slugs") or []
    provs = {_normalize_slug(str(p)) for p in provs}
    show = [DISPLAY_PROVIDER.get(p, p.replace("_", " ").title()) for p in sorted(provs & allowed_set)]
    return show

def _recency_label(it: Dict[str, Any]) -> Optional[str]:
    mt = (it.get("media_type") or "").lower()
    if mt == "movie" and EMAIL_LABEL_NEW_MOVIE:
        d = _parse_ymd(it.get("release_date"))
        days = _days_since(d)
        if days is not None and days <= REC_MOVIE_WINDOW_DAYS:
            return "New Movie"
    if mt == "tv":
        if EMAIL_LABEL_NEW_SEASON:
            lad = _parse_ymd(it.get("last_air_date"))
            ds = _days_since(lad)
            if ds is not None and ds <= REC_TV_LAST_WINDOW:
                return "New Season"
        if EMAIL_LABEL_NEW_SERIES:
            fad = _parse_ymd(it.get("first_air_date"))
            df = _days_since(fad)
            if df is not None and df <= REC_TV_FIRST_WINDOW:
                return "New Series"
    return None

def _media_emoji(it: Dict[str, Any]) -> str:
    mt = (it.get("media_type") or "").lower()
    return "🍿" if mt == "movie" else "📺"

def _fmt_title_line(it: Dict[str, Any], providers: List[str]) -> str:
    title = it.get("title") or it.get("name") or "Untitled"
    year = it.get("year")
    label = _recency_label(it)
    parts = [f"{_media_emoji(it)} *{title}*{f' ({year})' if year else ''}"]
    if label:
        parts.append(f"— **{label}**")
    return " ".join(parts)

def _fmt_meta_line(it: Dict[str, Any], providers: List[str]) -> str:
    match = it.get("score")
    aud = _audience_pct(it)
    why = (it.get("why") or "").strip()
    prov_md = ", ".join(f"**{p}**" for p in providers) if providers else "_Not on your services_"
    bits = []
    if isinstance(match, (int, float)):
        bits.append(f"Match {int(round(match))}")
    if isinstance(aud, int):
        bits.append(f"Audience {aud}")
    bits.append(prov_md)
    out = " • ".join(bits)
    if why:
        out += f"\n  - why: {why}"
    return out

# ============= Public API =============
def render_email(
    ranked_items: List[Dict[str, Any]],
    *,
    region: str = "US",
    allowed_provider_slugs: Optional[List[str]] = None,
    seen_index_path: Optional[Path] = None
) -> str:
    """
    Build the full markdown email body with Top Picks and telemetry.
    """
    allowed_provider_slugs = allowed_provider_slugs or []

    # Optional: double-check “never show seen” with a final pass
    seen_ids, seen_keys = set(), set()
    if seen_index_path and seen_index_path.exists():
        try:
            seen = json.loads(seen_index_path.read_text(encoding="utf-8", errors="replace"))
            for x in seen.get("imdb_ids", []):
                if isinstance(x, str) and x.startswith("tt"):
                    seen_ids.add(x)
            for x in seen.get("title_year_keys", []):
                if isinstance(x, str) and "::" in x:
                    seen_keys.add(x)
        except Exception:
            pass

    def _is_seen(it: Dict[str, Any]) -> bool:
        imdb = it.get("imdb_id")
        title = it.get("title") or it.get("name")
        year = it.get("year")
        key = f"{_norm_title(title)}::{year}" if title and year else None
        return (isinstance(imdb, str) and imdb in seen_ids) or (key and key in seen_keys)

    # Filter & select Top Picks
    picks: List[str] = []
    shown = 0
    for it in sorted(ranked_items, key=lambda x: float(x.get("score", x.get("tmdb_vote", 0.0)) or 0.0), reverse=True):
        if shown >= EMAIL_TOPK:
            break
        if float(it.get("score", 0) or 0) < EMAIL_SCORE_MIN:
            continue
        if _is_seen(it):
            continue
        provs = _providers_for_item(it, allowed_provider_slugs)
        if not provs:
            continue
        # title line + meta line
        picks.append(f"{_fmt_title_line(it, provs)}\n  { _fmt_meta_line(it, provs) }")
        shown += 1

    lines: List[str] = []
    lines.append("# Daily Recommendations\n")
    if picks:
        lines.append("## Top Picks\n")
        for p in picks:
            lines.append(f"- {p}")
        lines.append("")
    else:
        lines.append("_No eligible Top Picks today (after filters)._\n")

    # Minimal telemetry (runner also writes detailed diag)
    lines.append("## Telemetry")
    subs = os.getenv("SUBS_INCLUDE", "")
    lines.append(f"- Region: **{region}**")
    lines.append(f"- SUBS_INCLUDE: `{subs}`")
    lines.append(f"- Labels: movie={EMAIL_LABEL_NEW_MOVIE}, new_season={EMAIL_LABEL_NEW_SEASON}, new_series={EMAIL_LABEL_NEW_SERIES}")
    lines.append("")
    return "\n".join(lines)


def write_email_markdown(
    run_dir: Path,
    ranked_items_path: Path,
    env: Dict[str, Any],
    seen_index_path: Optional[Path] = None,
) -> Path:
    """
    Convenience wrapper: read items, render, and write summary.md into run_dir.
    """
    try:
        ranked = json.loads(ranked_items_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        ranked = []

    region = str(env.get("REGION") or "US")
    allowed = env.get("SUBS_INCLUDE", []) or []

    body = render_email(
        ranked,
        region=region,
        allowed_provider_slugs=allowed,
        seen_index_path=seen_index_path,
    )
    out = run_dir / "summary.md"
    out.write_text(body, encoding="utf-8")
    return out