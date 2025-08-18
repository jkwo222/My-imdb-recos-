# engine/summarize.py
from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from datetime import date, datetime

from . import recency
from . import tmdb

def _bool(n: str, d: bool) -> bool:
    v = (os.getenv(n, "") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return d
def _int(n: str, d: int) -> int:
    try: return int(os.getenv(n, "").strip())
    except Exception: return d
def _float(n: str, d: float) -> float:
    try: return float(os.getenv(n, "").strip())
    except Exception: return d

EMAIL_TOP_MOVIES                 = _int("EMAIL_TOP_MOVIES", 10)
EMAIL_TOP_TV                     = _int("EMAIL_TOP_TV", 10)
EMAIL_SCORE_MIN                  = _int("EMAIL_SCORE_MIN", 30)

EMAIL_BACKFILL                   = _bool("EMAIL_BACKFILL", True)
EMAIL_BACKFILL_MIN               = _int("EMAIL_BACKFILL_MIN", 20)
EMAIL_BACKFILL_MOVIE_MIN         = _int("EMAIL_BACKFILL_MOVIE_MIN", EMAIL_BACKFILL_MIN)
EMAIL_BACKFILL_TV_MIN            = _int("EMAIL_BACKFILL_TV_MIN", EMAIL_BACKFILL_MIN)
EMAIL_BACKFILL_ALLOW_ROTATE      = _bool("EMAIL_BACKFILL_ALLOW_ROTATE", True)
EMAIL_BACKFILL_FETCH_PROVIDERS   = _bool("EMAIL_BACKFILL_FETCH_PROVIDERS", True)
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
    return _NON.sub("-", (s or "").lower()).strip("-")

def _slugify_provider_name(name: str) -> Optional[str]:
    if not name: return None
    n = name.strip().lower()
    # accept already-known slugs
    if n in DISPLAY_PROVIDER: return n
    # basic normalizations
    n = n.replace("hbo max","max").replace("hbo","max").replace("hbomax","max")
    n = n.replace("paramount plus","paramount+").replace("disney plus","disney+").replace("apple tv plus","apple tv+")
    n = n.replace("amazon prime video","prime video").replace("amazon video","prime video")
    # map to slugs
    table = {
        "netflix":"netflix","max":"max","paramount+":"paramount_plus","disney+":"disney_plus",
        "apple tv+":"apple_tv_plus","peacock":"peacock","hulu":"hulu","prime video":"prime_video"
    }
    return table.get(n)

def _audience_pct(it: Dict[str, Any]) -> Optional[int]:
    try:
        v = it.get("audience")
        if v is None:
            va = it.get("vote_average")
            if va is not None: v = float(va) * 10.0
        if v is None: return None
        return max(0, min(100, int(round(float(v)))))
    except Exception:
        return None

def _fmt_runtime(it: Dict[str, Any]) -> Optional[str]:
    kind=(it.get("media_type") or "").lower()
    if kind=="movie":
        rt = it.get("runtime")
        if isinstance(rt, int) and rt>0: return f"{rt}m" if rt<60 else f"{rt//60}h {rt%60}m"
    else:
        ep = it.get("episode_run_time") or []
        if isinstance(ep, list) and ep:
            m = [e for e in ep if isinstance(e,int) and e>0]
            if m: return f"{min(m)}–{max(m)}m" if len(set(m))>1 else f"{m[0]}m"
    return None

def _fmt_title_line(it: Dict[str, Any], recency_lab: Optional[str]) -> str:
    kind=(it.get("media_type") or "").lower()
    title = it.get("title") or it.get("name") or "Untitled"
    year = it.get("year")
    label = ""
    if recency_lab:
        if recency_lab=="NEW_MOVIE" and kind=="movie": label = " — **New Movie**"
        elif recency_lab=="NEW_SERIES" and kind=="tv": label = " — **New Series**"
        elif recency_lab=="NEW_SEASON" and kind=="tv": label = " — **New Season**"
    if year: return f"- ***{title}*** ({year}){label}"
    return f"- ***{title}***{label}"

def _fmt_meta_line(it: Dict[str, Any], providers: List[str]) -> str:
    try: match=int(round(float(it.get("score",0) or 0)))
    except Exception: match=None
    aud=_audience_pct(it)
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

_DROP_PATTERNS = ("imdb details augmented","imdb keywords augmented","provider","anime","kids","long-run","black & white","b&w","old")
_KEEP_HINTS = ("new movie","new series","new season","actor","cast","director","writer","genre","keyword","because you liked","similar")

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

def _recency_label(it: Dict[str, Any]) -> Optional[str]:
    kind=(it.get("media_type") or "").lower()
    if kind=="movie":
        return recency.is_recent_movie(it)
    else:
        return recency.is_recent_show(it)

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
    g=[(x or "").lower() for x in (it.get("genres") or [])]
    k=[(x or "").lower() for x in (it.get("keywords") or [])]
    return any("anime" in x for x in g+k)

def _is_kids_cartoon(it: Dict[str, Any]) -> bool:
    g=[(x or "").lower() for x in (it.get("genres") or [])]
    k=[(x or "").lower() for x in (it.get("keywords") or [])]
    t=(it.get("title") or it.get("name") or "").lower()
    return ("animation" in g and "children" in g) or "preschool" in " ".join(k) or "bluey" in t

def _providers_display_for_item(it: Dict[str, Any], allowed_slugs: Iterable[str], region: str) -> List[str]:
    # Use embedded providers when available; optionally fetch early
    slugs = set([p for p in (it.get("providers") or []) if isinstance(p,str)])
    if not slugs and EMAIL_EARLY_FETCH_PROVIDERS:
        _ensure_providers(it, region)
        slugs = set([p for p in (it.get("providers") or []) if isinstance(p,str)])
    allowed_set = set([s.strip().lower() for s in allowed_slugs if s])
    provs=set()
    # include actual providers
    for s in slugs:
        if s in allowed_set: provs.add(s)
    # include TV network names if they map to providers
    nets = it.get("networks") or []
    for n in nets:
        name = None
        if isinstance(n, dict):
            name = n.get("name") or n.get("abbr") or n.get("network")
        elif isinstance(n, str):
            name = n
        if not name: continue
        slug = _slugify_provider_name(name)
        if slug: provs.add(slug)
    show = [DISPLAY_PROVIDER.get(p, p.replace("_"," ").title()) for p in sorted(provs & allowed_set)]
    return show

def _build_lines(ranked_items: List[Dict[str,Any]], *, region: str, allowed_provider_slugs: List[str], env_extra: Dict[str,Any], diag: Optional[Dict[str,Any]]):
    # Split by type and apply score threshold
    movies=[it for it in ranked_items if (it.get("media_type") or "").lower()=="movie" and (it.get("score") or 0)>=EMAIL_SCORE_MIN]
    shows=[it for it in ranked_items if (it.get("media_type") or "").lower()=="tv" and (it.get("score") or 0)>=EMAIL_SCORE_MIN]

    # Backfill within type if requested
    if EMAIL_BACKFILL:
        if len(movies)<EMAIL_TOP_MOVIES:
            # consider sub-threshold movies down to MOVIE_MIN
            mmin=EMAIL_BACKFILL_MOVIE_MIN
            extras=[it for it in ranked_items if (it.get("media_type") or "").lower()=="movie" and (it.get("score") or 0)>=mmin and it not in movies]
            movies = (movies + extras)[:max(EMAIL_TOP_MOVIES, len(movies))]
        if len(shows)<EMAIL_TOP_TV:
            tmin=EMAIL_BACKFILL_TV_MIN
            extras=[it for it in ranked_items if (it.get("media_type") or "").lower()=="tv" and (it.get("score") or 0)>=tmin and it not in shows]
            shows = (shows + extras)[:max(EMAIL_TOP_TV, len(shows))]

    # Enforce provider restriction at render time
    def render_items(items: List[Dict[str,Any]], top_n: int) -> List[str]:
        out=[]
        for it in items:
            providers = _providers_display_for_item(it, allowed_provider_slugs, region)
            if not providers:
                continue
            rec = _recency_label(it)
            title_line = _fmt_title_line(it, rec)
            meta_line = _fmt_meta_line(it, providers)
            why = _clean_why(it.get("why"), rec)
            out.append(title_line)
            out.append(f"  • {meta_line}")
            if why:
                out.append(f"  • why: {why}")
        return out[:top_n*3]  # 3 lines per item

    movie_lines = render_items(movies, EMAIL_TOP_MOVIES)
    show_lines  = render_items(shows, EMAIL_TOP_TV)

    # Selection breakdown for diagnostics
    breakdown = {
        "score_below_cutoff": len([it for it in ranked_items if (it.get("score") or 0) < EMAIL_SCORE_MIN]),
        "anime_excluded": len([it for it in ranked_items if _is_anime_like(it)]),
        "feedback_suppressed": 0,  # deprecated, kept for telemetry continuity
        "rotation_cooldown": 0,    # computed elsewhere; placeholder
        "no_allowed_provider": len([it for it in ranked_items if not _providers_display_for_item(it, allowed_provider_slugs, region)]),
        "selected_movies": len(movie_lines)//3,
        "selected_tv": len(show_lines)//3,
    }

    # Build body
    lines=["# Daily Recommendations","","## 🍿 Top Movies",""]
    lines.extend(movie_lines or ["_No eligible movies today after filters._"])
    lines.append("")
    lines.append("## 📺 Top Shows & Series"); lines.append("")
    lines.extend(show_lines or ["_No eligible shows today after filters._"])
    lines.append("")

    if _bool("EMAIL_INCLUDE_TELEMETRY", True):
        lines.append("## Telemetry")
        subs = os.getenv("SUBS_INCLUDE","")
        region = os.getenv("REGION","US")
        lines.append(f"- Region: **{region}**")
        lines.append(f"- SUBS_INCLUDE: `{subs}`")
        lines.append("")

    body = "\n".join(lines)
    return body, breakdown

def write_email_markdown(run_dir: Path, ranked_items_path: Path, env: Dict[str, Any],
                         seen_index_path: Optional[Path]=None, seen_tv_roots_path: Optional[Path]=None) -> Path:
    try:
        ranked = json.loads(ranked_items_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        ranked = []
    # Optional diag for future: attach counts if you want
    diag_path = run_dir / "diag.json"
    try:
        diag = json.loads(diag_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        diag = None

    body, breakdown = _build_lines(
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