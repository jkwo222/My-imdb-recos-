# engine/summarize.py
from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from datetime import date, datetime

from . import recency
from . import tmdb  # used to fetch providers on-demand

def _bool(n: str, d: bool) -> bool:
    v = (os.getenv(n, "") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return d
def _int(n: str, d: int) -> int:
    try: return int(os.getenv(n, "") or d)
    except Exception: return d

# Email / selection knobs
EMAIL_TOP_MOVIES                 = _int("EMAIL_TOP_MOVIES", 10)
EMAIL_TOP_TV                     = _int("EMAIL_TOP_TV", 10)
EMAIL_SCORE_MIN                  = _int("EMAIL_SCORE_MIN", 30)
EMAIL_INCLUDE_TELEMETRY          = _bool("EMAIL_INCLUDE_TELEMETRY", True)
EMAIL_EXCLUDE_ANIME              = _bool("EMAIL_EXCLUDE_ANIME", True)
EMAIL_NETWORK_FALLBACK           = _bool("EMAIL_NETWORK_FALLBACK", True)

LAB_NEW_MOVIE                    = _bool("EMAIL_INCLUDE_NEW_MOVIE_LABEL", True)
LAB_NEW_SEASON                   = _bool("EMAIL_INCLUDE_NEW_SEASON_LABEL", True)
LAB_NEW_SERIES                   = _bool("EMAIL_INCLUDE_NEW_SERIES_LABEL", True)

REC_MOVIE_WINDOW                 = _int("RECENCY_MOVIE_WINDOW_DAYS", 270)
REC_TV_FIRST_WINDOW              = _int("RECENCY_TV_FIRST_WINDOW", 180)
REC_TV_LAST_WINDOW               = _int("RECENCY_TV_LAST_WINDOW", 120)

ROTATION_ENABLE                  = _bool("ROTATION_ENABLE", True)
ROTATION_COOLDOWN_DAYS           = _int("ROTATION_COOLDOWN_DAYS", 5)

# Backfill (pass 2)
EMAIL_BACKFILL                   = _bool("EMAIL_BACKFILL", True)
EMAIL_BACKFILL_MIN               = _int("EMAIL_BACKFILL_MIN", 20)
EMAIL_BACKFILL_MOVIE_MIN         = _int("EMAIL_BACKFILL_MOVIE_MIN", EMAIL_BACKFILL_MIN)  # new (optional)
EMAIL_BACKFILL_TV_MIN            = _int("EMAIL_BACKFILL_TV_MIN", EMAIL_BACKFILL_MIN)     # new (optional)
EMAIL_BACKFILL_ALLOW_ROTATE      = _bool("EMAIL_BACKFILL_ALLOW_ROTATE", True)
EMAIL_BACKFILL_FETCH_PROVIDERS   = _bool("EMAIL_BACKFILL_FETCH_PROVIDERS", True)

# Early provider fetch (pass 1)
EMAIL_EARLY_FETCH_PROVIDERS      = _bool("EMAIL_EARLY_FETCH_PROVIDERS", True)

DISPLAY_PROVIDER = {
    "netflix": "Netflix",
    "max": "HBO Max",
    "paramount_plus": "Paramount+",
    "disney_plus": "Disney+",
    "apple_tv_plus": "Apple TV+",
    "peacock": "Peacock",
    "hulu": "Hulu",
    "prime_video": "Prime Video",
}

_NON = re.compile(r"[^a-z0-9]+")
def _norm(s: str) -> str:
    return _NON.sub(" ", (s or "").strip().lower()).strip()

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
        f=float(v)
        if f<=10.0: f*=10.0
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

    # Fallback to network names for TV if configured (now handles dicts)
    if not provs and EMAIL_NETWORK_FALLBACK and (it.get("media_type") or "").lower() == "tv":
        for net in it.get("networks") or []:
            if isinstance(net, dict):
                name = str(net.get("name") or "").strip()
            else:
                name = str(net).strip()
            if not name: continue
            slug = _slugify_provider_name(name)
            if slug: provs.add(slug)

    show = [DISPLAY_PROVIDER.get(p, p.replace("_"," ").title()) for p in sorted(provs & allowed_set)]
    return show

def _ensure_providers(it: Dict[str, Any], region: str) -> None:
    kind=(it.get("media_type") or "").lower()
    tid = it.get("tmdb_id")
    if not kind or not tid: return
    if it.get("providers"): return
    try:
        provs = tmdb.get_title_watch_providers(kind, int(tid), region)
        if provs: it["providers"] = provs
    except Exception:
        pass

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
    if mt=="movie":
        d=_days_since(_parse_ymd(it.get("release_date")))
        if d is not None and d<=REC_MOVIE_WINDOW and LAB_NEW_MOVIE: return "New Movie"
    if mt=="tv":
        seasons=int(it.get("number_of_seasons") or 0)
        df=_days_since(_parse_ymd(it.get("first_air_date")))
        if df is not None and df<=REC_TV_FIRST_WINDOW and LAB_NEW_SERIES: return "New Series"
        if seasons>=2:
            dl=_days_since(_parse_ymd(it.get("last_air_date")))
            if dl is not None and dl<=REC_TV_LAST_WINDOW and LAB_NEW_SEASON: return "New Season"
    return None

def _fmt_runtime(it: Dict[str, Any]) -> Optional[str]:
    if (it.get("media_type") or "").lower()=="movie":
        try:
            m=int(float(it.get("runtime") or 0))
            if m<=0: return None
            h,mm=divmod(m,60)
            return f"{h}h {mm}m" if h else f"{mm}m"
        except Exception: return None
    else:
        ert=it.get("episode_run_time") or []
        try:
            m=int(float(ert[0])) if ert else 0
            return f"{m}m" if m>0 else None
        except Exception: return None

def _fmt_title_line(it: Dict[str, Any]) -> str:
    title=it.get("title") or it.get("name") or "Untitled"
    year =it.get("year")
    lab  =_recency_label(it)
    bits=[f"***{title}***{f' ({year})' if year else ''}"]
    if lab: bits.append(f"— **{lab}**")
    return " ".join(bits)

def _audience_pct_wrap(it: Dict[str, Any]) -> Optional[int]:
    return _audience_pct(it)

def _fmt_meta_line(it: Dict[str, Any], providers: List[str]) -> str:
    try: match=int(round(float(it.get("score",0) or 0)))
    except Exception: match=None
    aud=_audience_pct_wrap(it)
    prov_md=", ".join(f"**{p}**" for p in providers) if providers else "_Not on your services_"
    rt=_fmt_runtime(it)
    director = (it.get("directors") or [None])[0]
    parts=[]
    if isinstance(match,(int,float)): parts.append(f"Match {int(match)}")
    if isinstance(aud,int): parts.append(f"Audience {aud}")
    parts.append(prov_md)
    if rt: parts.append(rt)
    if director: parts.append(f"Dir. {director}")
    return " • ".join(parts)

_DROP_PATTERNS = ("imdb details augmented","imdb keywords augmented","penalty","black & white","b&w","old","provider","anime","kids","long-run")
_KEEP_HINTS = ("new movie","new series","new season","actor","cast","star","director","writer","genre","keyword","because you liked","similar")

def _clean_why(raw: str, recency_lab: Optional[str]) -> Optional[str]:
    parts=[p.strip() for p in (raw or "").split(";") if p.strip()]
    out=[]
    if recency_lab: out.append(recency_lab.lower())
    for p in parts:
        low=p.lower()
        if any(x in low for x in _DROP_PATTERNS): continue
        if any(x in low for x in _KEEP_HINTS): out.append(p)
    if not out: return None
    out2=list(dict.fromkeys(out))[:3]
    return "; ".join(out2)

def render_email(ranked_items: List[Dict[str, Any]], *, region: str="US",
                 allowed_provider_slugs: Optional[List[str]]=None,
                 env_extra: Optional[Dict[str, Any]]=None,
                 diag: Optional[Dict[str, Any]]=None) -> str:
    allowed = allowed_provider_slugs or []
    env_extra = env_extra or {}
    rotation_skipped = 0
    feedback_skipped = 0
    suppress_keys = set(env_extra.get("FEEDBACK_SUPPRESS_KEYS") or [])
    breakdown = {
        "score_below_cutoff": 0,
        "anime_excluded": 0,
        "feedback_suppressed": 0,
        "rotation_cooldown": 0,
        "no_allowed_provider": 0,
        "selected_movies": 0,
        "selected_tv": 0,
    }

    def _elig_reason(it: Dict[str, Any], min_score: int, allow_rotate: bool) -> Optional[str]:
        nonlocal rotation_skipped, feedback_skipped
        if float(it.get("score",0) or 0) < min_score: return "score_below_cutoff"
        if EMAIL_EXCLUDE_ANIME and _is_anime_like(it): return "anime_excluded"
        k = recency.key_for_item(it)
        if k and k in suppress_keys:
            feedback_skipped += 1
            return "feedback_suppressed"
        if not allow_rotate and k and recency.should_skip_key(k, cooldown_days=ROTATION_COOLDOWN_DAYS):
            rotation_skipped += 1
        provs = _providers_for_item(it, allowed)
        if not provs: return "no_allowed_provider"
        if ROTATION_ENABLE and allow_rotate and recency.should_skip_key(k, cooldown_days=ROTATION_COOLDOWN_DAYS):
            rotation_skipped += 1
            return "rotation_cooldown"
        return None

    def _collect(min_score: int, *, allow_rotate: bool, try_fetch_providers: bool):
        movies, shows, chosen_keys = [], [], []
        m_cnt = s_cnt = 0
        for it in sorted(ranked_items, key=lambda x: float(x.get("score", x.get("tmdb_vote", 0.0)) or 0.0), reverse=True):
            # Early provider fetch in pass
            if try_fetch_providers and not (it.get("providers") or it.get("providers_slugs")):
                _ensure_providers(it, region)

            reason = _elig_reason(it, min_score, allow_rotate)

            # One-shot provider fetch if missing providers was the only blocker
            if reason == "no_allowed_provider" and not try_fetch_providers:
                _ensure_providers(it, region)
                reason = _elig_reason(it, min_score, allow_rotate)

            if reason:
                breakdown[reason] += 1
                continue

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
                    movies.append(block_text); m_cnt+=1; breakdown["selected_movies"]+=1
                    if key: chosen_keys.append(key)
            else:
                if s_cnt<EMAIL_TOP_TV:
                    shows.append(block_text); s_cnt+=1; breakdown["selected_tv"]+=1
                    if key: chosen_keys.append(key)
            if m_cnt>=EMAIL_TOP_MOVIES and s_cnt>=EMAIL_TOP_TV:
                break
        return movies, shows, chosen_keys

    # Pass 1
    movies, shows, keys = _collect(EMAIL_SCORE_MIN, allow_rotate=True, try_fetch_providers=EMAIL_EARLY_FETCH_PROVIDERS)

    # Pass 2 (backfill)
    used_backfill=False
    if EMAIL_BACKFILL and (len(movies)<EMAIL_TOP_MOVIES or len(shows)<EMAIL_TOP_TV):
        used_backfill=True
        bf_movies, bf_shows, bf_keys = _collect(
            min_score=min(EMAIL_BACKFILL_MOVIE_MIN, EMAIL_SCORE_MIN) if len(movies)<EMAIL_TOP_MOVIES else min(EMAIL_BACKFILL_TV_MIN, EMAIL_SCORE_MIN),
            allow_rotate=EMAIL_BACKFILL_ALLOW_ROTATE,
            try_fetch_providers=EMAIL_BACKFILL_FETCH_PROVIDERS
        )
        if len(movies) < EMAIL_TOP_MOVIES:
            need = EMAIL_TOP_MOVIES - len(movies)
            movies.extend(bf_movies[:need]); keys.extend(bf_keys[:need])
        if len(shows) < EMAIL_TOP_TV:
            need = EMAIL_TOP_TV - len(shows)
            shows.extend(bf_shows[:need]); keys.extend(bf_keys[:need])

    # Emergency tiny third pass if TV still short: lower floor by 5 (bounded at 12)
    if len(shows) < EMAIL_TOP_TV and EMAIL_BACKFILL:
        low = max(12, EMAIL_BACKFILL_TV_MIN - 5)
        bf_movies, bf_shows, bf_keys = _collect(
            min_score=low, allow_rotate=EMAIL_BACKFILL_ALLOW_ROTATE, try_fetch_providers=True
        )
        if len(shows) < EMAIL_TOP_TV:
            need = EMAIL_TOP_TV - len(shows)
            shows.extend(bf_shows[:need]); keys.extend(bf_keys[:need])

    if ROTATION_ENABLE and keys:
        try: recency.mark_shown_keys(keys)
        except Exception: pass

    lines=["# Daily Recommendations","","## 🍿 Top Movies",""]
    lines.extend(movies or ["_No eligible movies today after filters._"])
    lines.append("")
    lines.append("## 📺 Top Shows & Series"); lines.append("")
    lines.extend(shows or ["_No eligible shows today after filters._"])
    lines.append("")

    if EMAIL_INCLUDE_TELEMETRY:
        lines.append("## Telemetry")
        subs = os.getenv("SUBS_INCLUDE","")
        region = os.getenv("REGION","US")
        lines.append(f"- Region: **{region}**")
        lines.append(f"- SUBS_INCLUDE: `{subs}`")
        if diag:
            c = (diag.get("counts") or {})
            if c:
                lines.append(f"- Pool appended this run: **{c.get('pool_appended', c.get('discovered', 0))}**")
                lines.append(f"- Pool size before → after: **{c.get('pool_before', '?')} → {c.get('pool_after', '?')}**")
                lines.append(f"- Eligible after strict seen-filter: **{c.get('eligible', 0)}**")
                lines.append(f"- Scored items: **{c.get('scored', 0)}**")
                lines.append(f"- Excluded as seen (strict): **{c.get('excluded_seen', 0)}**")
        lines.append(f"- Skipped by rotation (cooldown): **{rotation_skipped}**")
        lines.append(f"- Backfill used: **{'yes' if used_backfill else 'no'}**")
        lines.append("")
    return "\n".join(lines), breakdown

def write_email_markdown(run_dir: Path, ranked_items_path: Path, env: Dict[str, Any],
                         seen_index_path: Optional[Path]=None, seen_tv_roots_path: Optional[Path]=None) -> Path:
    try:
        ranked = json.loads(ranked_items_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        ranked = []
    diag = None
    diag_path = run_dir / "diag.json"
    if diag_path.exists():
        try: diag = json.loads(diag_path.read_text(encoding="utf-8", errors="replace"))
        except Exception: diag = None

    body, breakdown = render_email(
        ranked_items=ranked,
        region=str(env.get("REGION") or "US"),
        allowed_provider_slugs=(env.get("SUBS_INCLUDE") or []),
        env_extra=env,
        diag=diag,
    )
    out = run_dir / "summary.md"
    out.write_text(body, encoding="utf-8")

    exp = run_dir / "exports"
    exp.mkdir(parents=True, exist_ok=True)
    (exp / "selection_breakdown.json").write_text(json.dumps(breakdown, indent=2), encoding="utf-8")
    return out