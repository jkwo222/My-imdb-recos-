# engine/summarize.py
from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from datetime import date, datetime

from . import recency  # cooldown memory

# -------- env helpers --------
def _bool(n:str,d:bool)->bool:
    v=(os.getenv(n,"") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return d
def _int(n:str,d:int)->int:
    try: return int(os.getenv(n,"") or d)
    except Exception: return d
def _float(n:str,d:float)->float:
    try: return float(os.getenv(n,"") or d)
    except Exception: return d

# -------- email layout knobs --------
EMAIL_TOP_MOVIES        = _int("EMAIL_TOP_MOVIES", 10)
EMAIL_TOP_TV            = _int("EMAIL_TOP_TV", 10)
EMAIL_SCORE_MIN         = _int("EMAIL_SCORE_MIN", 60)
EMAIL_INCLUDE_TELEMETRY = _bool("EMAIL_INCLUDE_TELEMETRY", True)
EMAIL_EXCLUDE_ANIME     = _bool("EMAIL_EXCLUDE_ANIME", True)
EMAIL_NETWORK_FALLBACK  = _bool("EMAIL_NETWORK_FALLBACK", True)

LAB_NEW_MOVIE           = _bool("EMAIL_INCLUDE_NEW_MOVIE_LABEL", True)
LAB_NEW_SEASON          = _bool("EMAIL_INCLUDE_NEW_SEASON_LABEL", True)
LAB_NEW_SERIES          = _bool("EMAIL_INCLUDE_NEW_SERIES_LABEL", True)

REC_MOVIE_WINDOW        = _int("RECENCY_MOVIE_WINDOW_DAYS", 270)
REC_TV_FIRST_WINDOW     = _int("RECENCY_TV_FIRST_WINDOW", 180)
REC_TV_LAST_WINDOW      = _int("RECENCY_TV_LAST_WINDOW", 120)

# Rotation / cooldown knobs
ROTATION_ENABLE         = _bool("ROTATION_ENABLE", True)
ROTATION_COOLDOWN_DAYS  = _int("ROTATION_COOLDOWN_DAYS", 5)
ROTATION_EXEMPT_SCORE   = _float("ROTATION_EXEMPT_SCORE", 90.0)

# -------- provider display map --------
DISPLAY_PROVIDER = {
    "netflix": "Netflix",
    "max": "HBO Max",           # <- changed from "Max"
    "paramount_plus": "Paramount+",
    "disney_plus": "Disney+",
    "apple_tv_plus": "Apple TV+",
    "peacock": "Peacock",
    "hulu": "Hulu",
    "prime_video": "Prime Video",
}
_NON = re.compile(r"[^a-z0-9]+")

def _norm(s:str)->str: return _NON.sub(" ", (s or "").strip().lower()).strip()

def _parse_ymd(s: Optional[str]) -> Optional[date]:
    if not s: return None
    s=s.strip()
    for fmt in ("%Y-%m-%d","%Y/%m/%d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    if len(s)>=4 and s[:4].isdigit():
        try: return date(int(s[:4]),1,1)
        except Exception: return None
    return None

def _days_since(d: Optional[date]) -> Optional[int]:
    if not d: return None
    try: return (date.today()-d).days
    except Exception: return None

def _audience_pct(it: Dict[str, Any]) -> Optional[int]:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0: f *= 10.0
        return int(round(max(0.0, min(100.0, f))))
    except Exception:
        return None

def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    if s in {"hbo","hbomax","hbo_max"}: return "max"
    return s

def _slugify_provider_name(name: str) -> str:
    n=(name or "").strip().lower()
    if not n: return ""
    if "apple tv+" in n or n == "apple tv plus": return "apple_tv_plus"
    if "netflix" in n: return "netflix"
    if n in {"hbo","hbo max","hbomax","max"}: return "max"
    if "paramount+" in n: return "paramount_plus"
    if "disney+" in n: return "disney_plus"
    if "peacock" in n: return "peacock"
    if "hulu" in n: return "hulu"
    if "prime video" in n or "amazon" in n: return "prime_video"
    return n.replace(" ", "_")

def _providers_for_item(it: Dict[str, Any], allowed: Iterable[str]) -> List[str]:
    allowed_set = {_normalize_slug(x) for x in (allowed or [])}
    provs = it.get("providers") or it.get("providers_slugs") or []
    provs = {_normalize_slug(str(p)) for p in provs}
    # Fallback via TV network names -> provider slugs
    if not provs and EMAIL_NETWORK_FALLBACK and (it.get("media_type") or "").lower() == "tv":
        for net in it.get("networks") or []:
            slug = _slugify_provider_name(str(net))
            if slug: provs.add(slug)
    show = [DISPLAY_PROVIDER.get(p, p.replace("_"," ").title()) for p in sorted(provs & allowed_set)]
    return show

def _is_anime_like(it: Dict[str, Any]) -> bool:
    title=_norm(it.get("title") or it.get("name") or "")
    genres=[]
    for g in (it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"): genres.append(g["name"].lower())
        elif isinstance(g, str): genres.append(g.lower())
    genres=set(genres)
    lang=(it.get("original_language") or "").lower()
    countries=set(str(c).upper() for c in (it.get("production_countries") or []))
    if "anime" in genres: return True
    if "animation" in genres and (lang=="ja" or "JP" in countries): return True
    if any(k in title for k in ("one piece","dandadan","dragon ball","naruto","jujutsu kaisen",
                                 "attack on titan","my hero academia","chainsaw man","spy x family")):
        return True
    return False

def _recency_label(it: Dict[str, Any]) -> Optional[str]:
    mt=(it.get("media_type") or "").lower()
    if mt=="movie" and LAB_NEW_MOVIE:
        d=_days_since(_parse_ymd(it.get("release_date")))
        if d is not None and d<=REC_MOVIE_WINDOW:
            return "New Movie"
    if mt=="tv":
        seasons=0
        try: seasons=int(it.get("number_of_seasons") or 0)
        except Exception: seasons=0
        if LAB_NEW_SERIES:
            df=_days_since(_parse_ymd(it.get("first_air_date")))
            if df is not None and df<=REC_TV_FIRST_WINDOW:
                return "New Series"
        if LAB_NEW_SEASON and seasons>=2:
            dl=_days_since(_parse_ymd(it.get("last_air_date")))
            if dl is not None and dl<=REC_TV_LAST_WINDOW:
                return "New Season"
    return None

def _fmt_runtime(it: Dict[str, Any]) -> Optional[str]:
    if (it.get("media_type") or "").lower()=="movie":
        try:
            m = int(float(it.get("runtime") or 0))
            if m <= 0: return None
            h, mm = divmod(m, 60)
            return (f"{h}h {mm}m" if h else f"{mm}m")
        except Exception:
            return None
    else:
        ert = it.get("episode_run_time") or []
        try:
            m = int(float(ert[0])) if ert else 0
            return f"{m}m" if m > 0 else None
        except Exception:
            return None

def _fmt_title_line(it: Dict[str, Any]) -> str:
    title=it.get("title") or it.get("name") or "Untitled"
    year =it.get("year")
    lab  =_recency_label(it)
    bits=[f"***{title}***{f' ({year})' if year else ''}"]
    if lab: bits.append(f"— **{lab}**")
    return " ".join(bits)

def _fmt_meta_line(it: Dict[str, Any], providers: List[str]) -> str:
    match=it.get("score")
    aud=_audience_pct(it)
    prov_md=", ".join(f"**{p}**" for p in providers) if providers else "_Not on your services_"
    rt=_fmt_runtime(it)
    director = (it.get("directors") or [None])[0]
    parts=[]
    if isinstance(match,(int,float)): parts.append(f"Match {int(round(match))}")
    if isinstance(aud,int): parts.append(f"Audience {aud}")
    parts.append(prov_md)
    if rt: parts.append(rt)
    if director: parts.append(f"Dir. {director}")
    return " • ".join(parts)

# --- "why" cleaning / enhancement ---
_DROP_PATTERNS = (
    "imdb details augmented",
    "imdb keywords augmented",
    "long-run",
    "anime",
    "kids",
    "penalty",
    "old",
    "b&w",
    "black and white",
    "provider",
)
_KEEP_HINTS = (
    "new movie",
    "new series",
    "new season",
    "actor", "cast", "star",
    "director",
    "writer",
    "genre",
    "franchise",
    "sequel",
    "because you liked",
    "similar to",
)

def _clean_why(raw: str, recency_lab: Optional[str]) -> Optional[str]:
    parts=[p.strip() for p in (raw or "").split(";") if p.strip()]
    out: List[str]=[]
    if recency_lab:
        out.append(recency_lab.lower())
    for p in parts:
        low=p.lower()
        if any(x in low for x in _DROP_PATTERNS):
            continue
        if any(x in low for x in _KEEP_HINTS):
            out.append(p)
    if not out:
        return None
    # collapse duplicates, keep short
    out2=list(dict.fromkeys(out))[:3]
    return "; ".join(out2)

def _audience_pct(it: Dict[str, Any]) -> Optional[int]:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0: f *= 10.0
        return int(round(max(0.0, min(100.0, f))))
    except Exception:
        return None

def _rotation_skip(it: Dict[str, Any]) -> bool:
    if not ROTATION_ENABLE: return False
    try:
        score=float(it.get("score",0) or 0)
    except Exception:
        score=0.0
    if score>=ROTATION_EXEMPT_SCORE: return False
    key=recency.key_for_item(it)
    return recency.should_skip_key(key, cooldown_days=ROTATION_COOLDOWN_DAYS)

def render_email(
    ranked_items: List[Dict[str, Any]],
    *,
    region: str = "US",
    allowed_provider_slugs: Optional[List[str]] = None,
    seen_index: Optional[Dict[str, Any]] = None,
    seen_tv_roots: Optional[List[str]] = None,
    diag: Optional[Dict[str, Any]] = None,
) -> str:
    allowed = allowed_provider_slugs or []

    rotation_skipped=0

    def _eligible(it: Dict[str, Any]) -> bool:
        nonlocal rotation_skipped
        if float(it.get("score", 0) or 0) < EMAIL_SCORE_MIN: return False
        # anime gate handled upstream, but double-protect here
        if EMAIL_EXCLUDE_ANIME and "anime" in (", ".join(str(g).lower() for g in (it.get("genres") or []))):
            return False
        if _rotation_skip(it):
            rotation_skipped += 1
            return False
        if not _providers_for_item(it, allowed): return False
        return True

    movies: List[str] = []
    shows:  List[str] = []
    chosen_keys: List[str]=[]
    m_cnt=s_cnt=0

    for it in sorted(ranked_items, key=lambda x: float(x.get("score", x.get("tmdb_vote", 0.0)) or 0.0), reverse=True):
        if not _eligible(it): continue
        provs=_providers_for_item(it, allowed)
        title_line=f"- {_fmt_title_line(it)}"
        meta_line =f"  • {_fmt_meta_line(it, provs)}"
        rec_lab=_recency_label(it)
        why_clean=_clean_why(it.get("why") or "", rec_lab)
        why_line = f"  • why: {why_clean}" if why_clean else None
        block = [title_line, meta_line] + ([why_line] if why_line else [])
        block_text = "\n".join(block)
        key = recency.key_for_item(it)
        if (it.get("media_type") or "").lower()=="movie":
            if m_cnt<EMAIL_TOP_MOVIES:
                movies.append(block_text); m_cnt+=1
                if key: chosen_keys.append(key)
        else:
            if s_cnt<EMAIL_TOP_TV:
                shows.append(block_text); s_cnt+=1
                if key: chosen_keys.append(key)
        if m_cnt>=EMAIL_TOP_MOVIES and s_cnt>=EMAIL_TOP_TV:
            break

    if ROTATION_ENABLE and chosen_keys:
        try: recency.mark_shown_keys(chosen_keys)
        except Exception: pass

    lines=["# Daily Recommendations","\n## 🍿 Top Movies\n"]
    lines.extend(movies or ["_No eligible movies today after filters._"])
    lines.append("")
    lines.append("## 📺 Top Shows & Series\n")
    lines.extend(shows or ["_No eligible shows today after filters._"])
    lines.append("")

    if EMAIL_INCLUDE_TELEMETRY:
        lines.append("## Telemetry")
        subs = os.getenv("SUBS_INCLUDE","")
        lines.append(f"- Region: **{region}**")
        lines.append(f"- SUBS_INCLUDE: `{subs}`")
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
            if env_pool.get("pages"):
                lines.append(f"- Discovery pages this run: **{env_pool.get('pages')}** ({env_pool.get('paging_mode')})")
        if ROTATION_ENABLE:
            lines.append(f"- Rotation: **on** (cooldown {ROTATION_COOLDOWN_DAYS} days; exempt ≥ {int(ROTATION_EXEMPT_SCORE)})")
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
    diag = None
    diag_path = run_dir / "diag.json"
    if diag_path.exists():
        try: diag = json.loads(diag_path.read_text(encoding="utf-8", errors="replace"))
        except Exception: diag = None
    body = render_email(
        ranked_items=ranked,
        region=str(env.get("REGION") or "US"),
        allowed_provider_slugs=(env.get("SUBS_INCLUDE") or []),
        seen_index=None,
        seen_tv_roots=None,
        diag=diag,
    )
    out = run_dir / "summary.md"
    out.write_text(body, encoding="utf-8")
    return out