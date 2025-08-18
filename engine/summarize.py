# engine/summarize.py
from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from datetime import date, datetime

# ---------------- Env toggles ----------------
def _bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return default

def _int(name: str, default: int) -> int:
    try: return int(os.getenv(name, "") or default)
    except Exception: return default

EMAIL_TOP_MOVIES           = _int("EMAIL_TOP_MOVIES", 10)
EMAIL_TOP_TV               = _int("EMAIL_TOP_TV", 10)
EMAIL_SCORE_MIN            = _int("EMAIL_SCORE_MIN", 60)
EMAIL_INCLUDE_TELEMETRY    = _bool("EMAIL_INCLUDE_TELEMETRY", True)
EMAIL_EXCLUDE_ANIME        = _bool("EMAIL_EXCLUDE_ANIME", True)

EMAIL_LABEL_NEW_MOVIE      = _bool("EMAIL_INCLUDE_NEW_MOVIE_LABEL", True)
EMAIL_LABEL_NEW_SEASON     = _bool("EMAIL_INCLUDE_NEW_SEASON_LABEL", True)
EMAIL_LABEL_NEW_SERIES     = _bool("EMAIL_INCLUDE_NEW_SERIES_LABEL", True)  # enable since we split labels properly

REC_MOVIE_WINDOW_DAYS      = _int("RECENCY_MOVIE_WINDOW_DAYS", 270)
REC_TV_FIRST_WINDOW        = _int("RECENCY_TV_FIRST_WINDOW", 180)
REC_TV_LAST_WINDOW         = _int("RECENCY_TV_LAST_WINDOW", 120)

# ---------------- Provider display ----------------
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
    if s in {"hbo","hbomax","hbo_max"}: return "max"
    return s

# ---------------- Helpers ----------------
_NON = re.compile(r"[^a-z0-9]+")

def _norm(s: str) -> str:
    return _NON.sub(" ", (s or "").strip().lower()).strip()

def _parse_ymd(s: Optional[str]) -> Optional[date]:
    if not s: return None
    s = s.strip()
    for fmt in ("%Y-%m-%d","%Y/%m/%d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    if len(s) >= 4 and s[:4].isdigit():
        try: return date(int(s[:4]),1,1)
        except Exception: return None
    return None

def _days_since(d: Optional[date]) -> Optional[int]:
    if not d: return None
    try: return (date.today() - d).days
    except Exception: return None

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
    show = [DISPLAY_PROVIDER.get(p, p.replace("_"," ").title()) for p in sorted(provs & allowed_set)]
    return show

def _is_anime_like(it: Dict[str, Any]) -> bool:
    title = _norm(it.get("title") or it.get("name") or "")
    genres = []
    for g in (it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"): genres.append(g["name"].lower())
        elif isinstance(g, str): genres.append(g.lower())
    genres = set(genres)
    lang = (it.get("original_language") or "").lower()
    countries = set(str(c).upper() for c in (it.get("production_countries") or []))
    if "anime" in genres: return True
    if "animation" in genres and (lang == "ja" or "JP" in countries): return True
    if any(k in title for k in ("one piece","dandadan","dragon ball","naruto","jujutsu kaisen","attack on titan",
                                 "my hero academia","chainsaw man","spy x family")):
        return True
    return False

def _media_emoji(it: Dict[str, Any]) -> str:
    return "🍿" if (it.get("media_type") or "").lower() == "movie" else "📺"

def _recency_label(it: Dict[str, Any]) -> Optional[str]:
    mt = (it.get("media_type") or "").lower()
    if mt == "movie" and EMAIL_LABEL_NEW_MOVIE:
        d = _parse_ymd(it.get("release_date")); days = _days_since(d)
        if days is not None and days <= REC_MOVIE_WINDOW_DAYS:
            return "New Movie"
    if mt == "tv":
        seasons = 0
        try: seasons = int(it.get("number_of_seasons") or 0)
        except Exception: seasons = 0
        # Prefer New Season only when there are >=2 seasons
        if EMAIL_LABEL_NEW_SEASON and seasons >= 2:
            lad = _parse_ymd(it.get("last_air_date")); ds = _days_since(lad)
            if ds is not None and ds <= REC_TV_LAST_WINDOW:
                return "New Season"
        if EMAIL_LABEL_NEW_SERIES:
            fad = _parse_ymd(it.get("first_air_date")); df = _days_since(fad)
            if df is not None and df <= REC_TV_FIRST_WINDOW:
                return "New Series"
    return None

def _fmt_title_line(it: Dict[str, Any]) -> str:
    title = it.get("title") or it.get("name") or "Untitled"
    year  = it.get("year")
    label = _recency_label(it)
    parts = [f"{_media_emoji(it)} *{title}*{f' ({year})' if year else ''}"]
    if label:
        parts.append(f"— **{label}**")
    return " ".join(parts)

def _fmt_meta_line(it: Dict[str, Any], providers: List[str]) -> str:
    match = it.get("score")
    aud   = _audience_pct(it)
    why   = (it.get("why") or "").strip()
    prov_md = ", ".join(f"**{p}**" for p in providers) if providers else "_Not on your services_"
    bits = []
    if isinstance(match, (int, float)): bits.append(f"Match {int(round(match))}")
    if isinstance(aud, int): bits.append(f"Audience {aud}")
    bits.append(prov_md)
    out = " • ".join(bits)
    if why:
        out += f"\n  - why: {why}"
    return out

# ---------------- public API ----------------
def render_email(
    ranked_items: List[Dict[str, Any]],
    *,
    region: str = "US",
    allowed_provider_slugs: Optional[List[str]] = None,
    seen_index: Optional[Dict[str, Any]] = None,
    seen_tv_roots: Optional[List[str]] = None,
    diag: Optional[Dict[str, Any]] = None,
) -> str:
    allowed_provider_slugs = allowed_provider_slugs or []
    seen_tv_roots = seen_tv_roots or []
    seen_ids = set()
    seen_keys = set()
    if seen_index:
        for x in seen_index.get("imdb_ids", []):
            if isinstance(x, str) and x.startswith("tt"):
                seen_ids.add(x)
        for x in seen_index.get("title_year_keys", []):
            if isinstance(x, str) and "::" in x:
                seen_keys.add(x)
    seen_root_set = {_norm(s) for s in seen_tv_roots}

    def _is_seen(it: Dict[str, Any]) -> bool:
        imdb = it.get("imdb_id")
        title = it.get("title") or it.get("name")
        year = it.get("year")
        key = f"{_norm(title)}::{year}" if title and year else None
        if (isinstance(imdb, str) and imdb in seen_ids) or (key and key in seen_keys):
            return True
        # extra guard for TV based on normalized root (covers public-list cases lacking imdb ids)
        if (it.get("media_type") or "").lower() == "tv":
            root = _norm(title or "")
            if root in seen_root_set:
                return True
        return False

    # partition & filter
    movies: List[str] = []
    shows:  List[str] = []
    picked_movies = 0
    picked_tv = 0

    allowed = allowed_provider_slugs

    def _eligible(it: Dict[str, Any]) -> Optional[str]:
        if float(it.get("score", 0) or 0) < EMAIL_SCORE_MIN: return "score"
        if _is_seen(it): return "seen"
        if EMAIL_EXCLUDE_ANIME and _is_anime_like(it): return "anime"
        provs = _providers_for_item(it, allowed)
        if not provs: return "provider"
        return None

    for it in sorted(ranked_items, key=lambda x: float(x.get("score", x.get("tmdb_vote", 0.0)) or 0.0), reverse=True):
        mt = (it.get("media_type") or "").lower()
        reason = _eligible(it)
        if reason is not None:
            continue
        provs = _providers_for_item(it, allowed)
        line = f"- {_fmt_title_line(it)}\n  {_fmt_meta_line(it, provs)}"
        if mt == "movie":
            if picked_movies < EMAIL_TOP_MOVIES:
                movies.append(line); picked_movies += 1
        else:
            if picked_tv < EMAIL_TOP_TV:
                shows.append(line); picked_tv += 1
        if picked_movies >= EMAIL_TOP_MOVIES and picked_tv >= EMAIL_TOP_TV:
            break

    # compose markdown
    lines: List[str] = []
    lines.append("# Daily Recommendations\n")

    lines.append("## Top Movies\n")
    if movies: lines.extend(movies)
    else: lines.append("_No eligible movies today after filters._")
    lines.append("")

    lines.append("## Top Shows & Series\n")
    if shows: lines.extend(shows)
    else: lines.append("_No eligible shows today after filters._")
    lines.append("")

    if EMAIL_INCLUDE_TELEMETRY:
        lines.append("## Telemetry")
        subs = os.getenv("SUBS_INCLUDE", "")
        lines.append(f"- Region: **{region}**")
        lines.append(f"- SUBS_INCLUDE: `{subs}`")
        # Include counters from runner's diag.json if present
        c = (diag or {}).get("counts", {}) if diag else {}
        env_pool = ((diag or {}).get("env") or {}).get("POOL_TELEMETRY") or {}
        if c:
            lines.append(f"- Discovered this run: **{c.get('discovered', 0)}**")
            lines.append(f"- Eligible after strict seen-filter: **{c.get('eligible', 0)}**")
            lines.append(f"- Scored items: **{c.get('scored', 0)}**")
            lines.append(f"- Excluded as seen (strict): **{c.get('excluded_seen', 0)}**")
        if env_pool:
            if env_pool.get("file_lines_after") is not None:
                lines.append(f"- Pool size (lines): **{env_pool.get('file_lines_after')}**")
            if env_pool.get("appended_this_run") is not None:
                lines.append(f"- New titles appended to pool: **{env_pool.get('appended_this_run')}**")
            if env_pool.get("unique_keys_est") is not None:
                lines.append(f"- Pool unique keys (est): **{env_pool.get('unique_keys_est')}**")
        lines.append("")

    return "\n".join(lines)


def write_email_markdown(
    run_dir: Path,
    ranked_items_path: Path,
    env: Dict[str, Any],
    seen_index_path: Optional[Path] = None,
    seen_tv_roots_path: Optional[Path] = None,
) -> Path:
    try:
        ranked = json.loads(ranked_items_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        ranked = []

    # load seen artifacts
    seen_index = None
    if seen_index_path and seen_index_path.exists():
        try: seen_index = json.loads(seen_index_path.read_text(encoding="utf-8", errors="replace"))
        except Exception: seen_index = None

    seen_tv_roots = []
    if seen_tv_roots_path and seen_tv_roots_path.exists():
        try: seen_tv_roots = json.loads(seen_tv_roots_path.read_text(encoding="utf-8", errors="replace"))
        except Exception: seen_tv_roots = []

    # diag for telemetry
    diag_path = run_dir / "diag.json"
    diag = None
    if diag_path.exists():
        try: diag = json.loads(diag_path.read_text(encoding="utf-8", errors="replace"))
        except Exception: diag = None

    region = str(env.get("REGION") or "US")
    allowed = env.get("SUBS_INCLUDE", []) or []

    body = render_email(
        ranked_items=ranked,
        region=region,
        allowed_provider_slugs=allowed,
        seen_index=seen_index,
        seen_tv_roots=seen_tv_roots,
        diag=diag,
    )
    out = run_dir / "summary.md"
    out.write_text(body, encoding="utf-8")
    return out