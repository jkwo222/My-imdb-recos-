# engine/scoring.py
from __future__ import annotations
import os, re
from typing import Any, Dict, List, Iterable, Optional
from datetime import date, datetime
import math

# ===== env helpers =====
def _bool(n:str, d:bool)->bool:
    v=(os.getenv(n,"") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return d
def _int(n:str, d:int)->int:
    try: return int(os.getenv(n,"") or d)
    except Exception: return d
def _float(n:str, d:float)->float:
    try: return float(os.getenv(n,"") or d)
    except Exception: return d

_NON = re.compile(r"[^a-z0-9]+")
def _norm(s: str) -> str:
    return _NON.sub(" ", (s or "").strip().lower()).strip()

def _to_year(s: Any) -> Optional[int]:
    if s is None: return None
    s=str(s)
    if len(s)>=4 and s[:4].isdigit():
        try: return int(s[:4])
        except Exception: return None
    return None

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

def _listify(x) -> List[str]:
    out: List[str]=[]
    if not x: return out
    if isinstance(x, list):
        for v in x:
            if isinstance(v, dict) and v.get("name"):
                out.append(str(v["name"]).strip())
            else:
                out.append(str(v).strip())
    else:
        out.append(str(x).strip())
    return [t for t in out if t]

# ===== knobs =====
AUDIENCE_PRIOR_LAMBDA = _float("AUDIENCE_PRIOR_LAMBDA", 0.30)
PROVIDER_PREF_LAMBDA  = _float("PROVIDER_PREF_LAMBDA", 0.50)

# People weights (reduced director boost)
ACTOR_WEIGHT    = _float("ACTOR_WEIGHT", 2.2)
DIRECTOR_WEIGHT = _float("DIRECTOR_WEIGHT", 1.0)   # reduced
WRITER_WEIGHT   = _float("WRITER_WEIGHT", 0.8)
GENRE_WEIGHT    = _float("GENRE_WEIGHT", 0.9)
KEYWORD_WEIGHT  = _float("KEYWORD_WEIGHT", 0.25)

# Anime / Kids penalties
PENALIZE_KIDS         = _bool("PENALIZE_KIDS", True)
PENALIZE_ANIME        = _bool("PENALIZE_ANIME", True)
KIDS_CARTOON_PENALTY  = max(0, _int("KIDS_CARTOON_PENALTY", 25))
ANIME_PENALTY         = max(0, _int("ANIME_PENALTY", 20))
KIDS_MOVIE_MIN_RUNTIME= _int("KIDS_MOVIE_MIN_RUNTIME", 70)  # don't penalize Pixar-length features

# Romance penalties (movies)
ROMANCE_PENALTY       = max(0, _int("ROMANCE_PENALTY", 12))
ROMCOM_PENALTY        = max(0, _int("ROMCOM_PENALTY", 16))

# Old & black-and-white penalties
OLD_CONTENT_YEAR_CUTOFF = _int("OLD_CONTENT_YEAR_CUTOFF", 1984)
OLD_CONTENT_PENALTY     = max(0, _int("OLD_CONTENT_PENALTY", 18))
BLACK_WHITE_PENALTY     = max(0, _int("BLACK_WHITE_PENALTY", 22))

# TV commitment penalties
COMMITMENT_ENABLED         = _bool("COMMITMENT_ENABLED", True)
COMMITMENT_UNSEEN_THRESHOLD= _int("COMMITMENT_UNSEEN_THRESHOLD", 1)  # start after S1 if you haven't seen it
COMMITMENT_SEEN_THRESHOLD  = _int("COMMITMENT_SEEN_THRESHOLD", 4)    # kinder if you're following it
COMMITMENT_SEASON_PENALTY  = _float("COMMITMENT_SEASON_PENALTY", 3.0)
COMMITMENT_MAX_PENALTY     = _float("COMMITMENT_MAX_PENALTY", 18.0)

# Recency
REC_MOVIE_WINDOW_DAYS  = _int("RECENCY_MOVIE_WINDOW_DAYS", 270)
REC_MOVIE_BONUS_MAX    = _float("RECENCY_MOVIE_BONUS_MAX", 10.0)
REC_TV_FIRST_WINDOW    = _int("RECENCY_TV_FIRST_WINDOW", 180)
REC_TV_FIRST_BONUS_MAX = _float("RECENCY_TV_FIRST_BONUS_MAX", 8.0)
REC_TV_LAST_WINDOW     = _int("RECENCY_TV_LAST_WINDOW", 120)
REC_TV_LAST_BONUS_MAX  = _float("RECENCY_TV_LAST_BONUS_MAX", 7.0)

_ALLOWED_PROVIDERS = None  # filled in by score_items

def _audience_prior(it: Dict[str, Any]) -> float:
    v=it.get("audience") or it.get("tmdb_vote")
    try:
        f=float(v)
        if f<=10.0: f*=10.0
        return max(0.0, min(100.0, f)) * AUDIENCE_PRIOR_LAMBDA
    except Exception:
        return 0.0

def _provider_pref(it: Dict[str, Any]) -> float:
    global _ALLOWED_PROVIDERS
    if not _ALLOWED_PROVIDERS: return 0.0
    provs=set()
    for p in (it.get("providers") or it.get("providers_slugs") or []):
        provs.add(str(p).strip().lower())
    if provs & _ALLOWED_PROVIDERS:
        return 6.0 * PROVIDER_PREF_LAMBDA
    return 0.0

def _genres_lower(it: Dict[str, Any]) -> List[str]:
    out=[]
    for g in (it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"): out.append(g["name"].lower())
        elif isinstance(g, str): out.append(g.lower())
    return out

def _is_anime(it: Dict[str, Any]) -> bool:
    title=_norm(it.get("title") or it.get("name") or "")
    genres=_genres_lower(it)
    lang=(it.get("original_language") or "").lower()
    countries=set(str(c).upper() for c in (it.get("production_countries") or []))
    if "anime" in genres: return True
    if "animation" in genres and (lang=="ja" or "JP" in countries): return True
    if any(k in title for k in ("one piece","dandadan","dragon ball","naruto","jujutsu kaisen",
                                "attack on titan","my hero academia","chainsaw man","spy x family")):
        return True
    return False

def _is_kids_cartoon(it: Dict[str, Any]) -> bool:
    title=_norm(it.get("title") or it.get("name") or "")
    genres=_genres_lower(it)
    if "animation" in genres and any(k in title for k in ("bluey","cocomelon","peppa pig","paw patrol","bubble guppies")):
        return True
    if (it.get("media_type") or "").lower()=="movie":
        try:
            rt=int(float(it.get("runtime") or 0))
        except Exception:
            rt=0
        if "animation" in genres and rt and rt < KIDS_MOVIE_MIN_RUNTIME:
            return True
    return False

def _is_romance_movie(it: Dict[str, Any]) -> bool:
    if (it.get("media_type") or "").lower()!="movie": return False
    g=_genres_lower(it)
    if "romantic comedy" in g or "rom-com" in g or "romcom" in g: return True
    if "romance" in g and ("comedy" in g): return True
    if "romance" in g: return True
    return False

def _is_black_and_white(it: Dict[str, Any]) -> bool:
    kws=set(k.lower() for k in (it.get("keywords") or []))
    patterns={"black and white","black-and-white","b&w","black & white"}
    return bool(kws & patterns)

def _recency_bonus(it: Dict[str, Any]) -> float:
    mt=(it.get("media_type") or "").lower()
    if mt=="movie":
        d=_days_since(_parse_ymd(it.get("release_date")))
        if d is None or d>REC_MOVIE_WINDOW_DAYS: return 0.0
        frac=max(0.0, 1.0 - (d/REC_MOVIE_WINDOW_DAYS))
        return REC_MOVIE_BONUS_MAX * frac
    if mt=="tv":
        b=0.0
        df=_days_since(_parse_ymd(it.get("first_air_date")))
        if df is not None and df<=REC_TV_FIRST_WINDOW:
            frac=max(0.0, 1.0-(df/REC_TV_FIRST_WINDOW))
            b+= REC_TV_FIRST_BONUS_MAX * frac
        dl=_days_since(_parse_ymd(it.get("last_air_date")))
        seasons=0
        try: seasons=int(it.get("number_of_seasons") or 0)
        except Exception: seasons=0
        if seasons>=2 and dl is not None and dl<=REC_TV_LAST_WINDOW:
            frac=max(0.0, 1.0-(dl/REC_TV_LAST_WINDOW))
            b+= REC_TV_LAST_BONUS_MAX * frac
        return b
    return 0.0

def _commitment_penalty(it: Dict[str, Any], seen_tv_roots: Iterable[str]) -> float:
    if not COMMITMENT_ENABLED: return 0.0
    if (it.get("media_type") or "").lower()!="tv": return 0.0
    try: seasons=int(it.get("number_of_seasons") or 0)
    except Exception: seasons=0
    title_root=_norm(it.get("title") or it.get("name") or "")
    threshold = COMMITMENT_SEEN_THRESHOLD if title_root in (set(seen_tv_roots or [])) else COMMITMENT_UNSEEN_THRESHOLD
    over=max(0, seasons - threshold)
    if over<=0: return 0.0
    return min(COMMITMENT_MAX_PENALTY, over*COMMITMENT_SEASON_PENALTY)

def _old_bw_penalty(it: Dict[str, Any]) -> float:
    y = _to_year(it.get("year") or it.get("release_year") or it.get("first_air_year"))
    pen = 0.0
    if y is not None and y < OLD_CONTENT_YEAR_CUTOFF:
        pen += OLD_CONTENT_PENALTY
    if _is_black_and_white(it):
        pen += BLACK_WHITE_PENALTY
    return pen

def _romance_penalty(it: Dict[str, Any]) -> float:
    if _is_romance_movie(it):
        g=_genres_lower(it)
        if "romantic comedy" in g or "rom-com" in g or ("romance" in g and "comedy" in g):
            return ROMCOM_PENALTY
        return ROMANCE_PENALTY
    return 0.0

def _anime_kids_penalty(it: Dict[str, Any]) -> float:
    pen=0.0
    if PENALIZE_ANIME and _is_anime(it): pen += ANIME_PENALTY
    if PENALIZE_KIDS and _is_kids_cartoon(it): pen += KIDS_CARTOON_PENALTY
    return pen

def _affinity_contrib(it: Dict[str, Any], model: Dict[str, Any]) -> float:
    """Lightweight overlap against user model; safe even if model is empty."""
    if not model: return 0.0
    score=0.0
    reasons=[]
    # Actors
    actors = [a.lower() for a in _listify(it.get("cast"))][:8]
    top_actors = {k.lower():v for k,v in (model.get("top_actors") or {}).items()}
    hit = [a for a in actors if a in top_actors]
    if hit:
        score += ACTOR_WEIGHT * sum(top_actors[a] for a in hit)
        reasons.append(f"+{int(round(ACTOR_WEIGHT*sum(top_actors[a] for a in hit)))} actor overlap ({', '.join(hit[:2])})")
    # Directors
    directors = [d.lower() for d in _listify(it.get("directors"))][:4]
    top_dirs = {k.lower():v for k,v in (model.get("top_directors") or {}).items()}
    hit = [d for d in directors if d in top_dirs]
    if hit:
        score += DIRECTOR_WEIGHT * sum(top_dirs[d] for d in hit)
        reasons.append(f"+{int(round(DIRECTOR_WEIGHT*sum(top_dirs[d] for d in hit)))} director match ({hit[0]})")
    # Writers
    writers = [w.lower() for w in _listify(it.get("writers"))][:4]
    top_wrs = {k.lower():v for k,v in (model.get("top_writers") or {}).items()}
    hit = [w for w in writers if w in top_wrs]
    if hit:
        score += WRITER_WEIGHT * sum(top_wrs[w] for w in hit)
        reasons.append(f"+{int(round(WRITER_WEIGHT*sum(top_wrs[w] for w in hit)))} writer match")
    # Genres
    genres = [g.lower() for g in _genres_lower(it)]
    top_gen = {k.lower():v for k,v in (model.get("top_genres") or {}).items()}
    hit = [g for g in genres if g in top_gen]
    if hit:
        score += GENRE_WEIGHT * sum(top_gen[g] for g in hit)
        reasons.append(f"+{int(round(GENRE_WEIGHT*sum(top_gen[g] for g in hit)))} genre affinity")
    # Keywords
    kws = [k.lower() for k in (it.get("keywords") or [])][:20]
    top_kw = {k.lower():v for k,v in (model.get("top_keywords") or {}).items()}
    hit = [k for k in kws if k in top_kw]
    if hit:
        score += KEYWORD_WEIGHT * sum(top_kw[k] for k in hit[:5])
        reasons.append(f"+{int(round(KEYWORD_WEIGHT*sum(top_kw[k] for k in hit[:5])))} keyword affinity")
    prev = (it.get("why") or "").strip()
    if reasons:
        it["why"] = (prev + ("; " if prev else "") + "; ".join(reasons))
    return score

def _base_popularity(it: Dict[str, Any]) -> float:
    try: p=float(it.get("popularity") or 0.0)
    except Exception: p=0.0
    return math.log1p(max(0.0, p)) * 0.8

def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    global _ALLOWED_PROVIDERS
    allowed = set()
    for p in (env.get("SUBS_INCLUDE") or []):
        allowed.add(str(p).strip().lower())
    _ALLOWED_PROVIDERS = allowed

    # try to load user model if present
    model_path = env.get("USER_MODEL_PATH") or ""
    model: Dict[str, Any] = {}
    if isinstance(model_path, str) and model_path:
        try:
            import json
            with open(model_path, "r", encoding="utf-8", errors="replace") as fh:
                model = json.load(fh)
        except Exception:
            model={}

    seen_tv_roots = set(env.get("SEEN_TV_TITLE_ROOTS") or [])

    for it in items:
        s = 0.0
        reasons: List[str] = []

        # audience prior
        a = _audience_prior(it); s += a

        # provider preference (small)
        pp = _provider_pref(it); s += pp

        # recency
        rb = _recency_bonus(it); 
        if rb>0: 
            s += rb
            mt=(it.get("media_type") or "").lower()
            if mt=="movie":
                d=_days_since(_parse_ymd(it.get("release_date")))
                if d is not None: reasons.append(f"+{round(rb,1)} new movie ({d}d)")
            else:
                df=_days_since(_parse_ymd(it.get("first_air_date"))); dl=_days_since(_parse_ymd(it.get("last_air_date")))
                if df is not None and df<=REC_TV_FIRST_WINDOW: reasons.append(f"+{round(rb,1)} new series")
                elif dl is not None and dl<=REC_TV_LAST_WINDOW: reasons.append(f"+{round(rb,1)} new season")

        # user affinity
        s += _affinity_contrib(it, model)

        # popularity tie-breaker
        s += _base_popularity(it)

        # penalties
        pen = 0.0
        pen += _anime_kids_penalty(it)
        rp = _romance_penalty(it); 
        if rp>0: reasons.append(f"-{int(rp)} romance de-prioritized")
        pen += rp

        op = _old_bw_penalty(it)
        if op>0:
            y = _to_year(it.get("year") or it.get("release_year") or it.get("first_air_year"))
            if y is not None and y < OLD_CONTENT_YEAR_CUTOFF:
                reasons.append(f"-{OLD_CONTENT_PENALTY} older ({y})")
            if _is_black_and_white(it):
                reasons.append(f"-{BLACK_WHITE_PENALTY} black & white")
        pen += op

        cp = _commitment_penalty(it, seen_tv_roots)
        if cp>0: reasons.append(f"-{int(cp)} long-run TV")
        pen += cp

        s -= pen

        it["score"] = float(max(0.0, min(100.0, s)))
        if reasons:
            prev=(it.get("why") or "").strip()
            it["why"] = (prev + ("; " if prev else "") + "; ".join(reasons))
    return items