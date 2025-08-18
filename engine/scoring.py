# engine/scoring.py
from __future__ import annotations
import json, os, re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Iterable
from datetime import date, datetime

# --- env knobs ---
def _bool(name:str, default:bool)->bool:
    v=(os.getenv(name,"").strip().lower())
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return default
def _int(name:str, default:int)->int:
    try: return int(os.getenv(name,"") or default)
    except Exception: return default
def _float(name:str, default:float)->float:
    try: return float(os.getenv(name,"") or default)
    except Exception: return default
def _csv(name:str, default:str)->List[str]:
    raw=os.getenv(name, default); return [s.strip().lower() for s in raw.split(",") if s.strip()]

# anime/kids penalties
PENALIZE_KIDS = _bool("PENALIZE_KIDS", True)
PENALIZE_ANIME = _bool("PENALIZE_ANIME", True)
KIDS_PENALTY = max(0, _int("KIDS_CARTOON_PENALTY", 25))
ANIME_PENALTY = max(0, _int("ANIME_PENALTY", 20))
KIDS_MOVIE_MIN_RUNTIME = _int("KIDS_MOVIE_MIN_RUNTIME", 70)
KIDS_STUDIO_WHITELIST = set(_csv("KIDS_STUDIO_WHITELIST",
    "pixar animation studios,walt disney animation studios,walt disney pictures,dreamworks animation,sony pictures animation,illumination,laika"))

# TV commitment penalty
COMMITMENT_ENABLED = _bool("COMMITMENT_ENABLED", True)
COMMITMENT_SEASONS_THRESHOLD = _int("COMMITMENT_SEASONS_THRESHOLD", 4)
COMMITMENT_SEASON_PENALTY = _float("COMMITMENT_SEASON_PENALTY", 3.0)
COMMITMENT_MAX_PENALTY = _float("COMMITMENT_MAX_PENALTY", 18.0)

# recency boosts
REC_MOVIE_WINDOW = _int("RECENCY_MOVIE_WINDOW_DAYS", 270)    # ~9 months
REC_MOVIE_MAX = _float("RECENCY_MOVIE_BONUS_MAX", 10.0)
REC_TV_FIRST_WINDOW = _int("RECENCY_TV_FIRST_WINDOW", 180)   # new series
REC_TV_FIRST_MAX = _float("RECENCY_TV_FIRST_BONUS_MAX", 8.0)
REC_TV_LAST_WINDOW = _int("RECENCY_TV_LAST_WINDOW", 120)     # new season (recent air)
REC_TV_LAST_MAX = _float("RECENCY_TV_LAST_BONUS_MAX", 10.0)
REC_TV_FOLLOWUP_MAX = _float("RECENCY_TV_FOLLOWUP_BONUS_MAX", 6.0)  # extra if it's a show you watched
REC_PROVIDER_MULT = _float("RECENCY_PROVIDER_MULTIPLIER", 1.15)     # stronger when on your services

# blend weights
LAMBDA_AUDIENCE = _float("AUDIENCE_PRIOR_LAMBDA", 0.3)
LAMBDA_PROVIDER = _float("PROVIDER_PREF_LAMBDA", 0.5)

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm(s:str)->str:
    return _NON_ALNUM.sub(" ", (s or "").strip().lower()).strip()

def _as_list(x)->List[Any]:
    if x is None: return []
    if isinstance(x, list): return x
    return [x]

def _parse_ymd(s:str|None)->date|None:
    if not s: return None
    s=s.strip()
    for fmt in ("%Y-%m-%d","%Y/%m/%d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    # fallback to year only
    if len(s)>=4 and s[:4].isdigit():
        try: return date(int(s[:4]), 1, 1)
        except Exception: return None
    return None

def _days_since(d:date|None)->int|None:
    if not d: return None
    try: return (date.today()-d).days
    except Exception: return None

def _audience_score(it: Dict[str, Any]) -> float:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0: f *= 10.0
        return max(0.0, min(100.0, f))
    except Exception:
        return 50.0

def _bucket_runtime(it: Dict[str, Any]) -> str:
    mins=None
    if (it.get("media_type") or "").lower()=="movie":
        mins=it.get("runtime")
    else:
        ert=it.get("episode_run_time") or []
        if isinstance(ert, list) and ert: mins=ert[0]
    try:
        m=float(mins)
    except Exception:
        return "unknown"
    if m<=90: return "<=90"
    if m<=120: return "91-120"
    if m<=150: return "121-150"
    return ">150"

def _era(y)->str:
    try: yi=int(str(y)[:4])
    except Exception: return "unknown"
    if 1960<=yi<1980: return "60-79"
    if 1980<=yi<1990: return "80s"
    if 1990<=yi<2000: return "90s"
    if 2000<=yi<2010: return "00s"
    if 2010<=yi<2020: return "10s"
    if 2020<=yi<2030: return "20s"
    return "unknown"

def _kids_penalize(it: Dict[str, Any]) -> Tuple[bool, str]:
    genres=[]
    for g in _as_list(it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"): genres.append(g["name"].lower())
        elif isinstance(g, str): genres.append(g.lower())
    genres=set(genres)
    studios=[str(n).lower() for n in _as_list(it.get("production_companies"))]
    if any(s in KIDS_STUDIO_WHITELIST for s in studios): return False, "kids:whitelist_studio"
    title=_norm(it.get("title") or it.get("name") or "")
    hits=any(k in title for k in ("bluey","peppa","paw patrol","cocomelon","octonauts","dora "))
    kidsish=("animation" in genres) and (("kids" in genres) or ("family" in genres) or hits)
    if not kidsish: return False, ""
    if (it.get("media_type") or "").lower()=="tv":
        return True, "kids:tv"
    mins=0.0
    try: mins=float(it.get("runtime") or 0)
    except Exception: mins=0.0
    if mins and mins < float(KIDS_MOVIE_MIN_RUNTIME): return True, f"kids:movie<{KIDS_MOVIE_MIN_RUNTIME}"
    return False, "kids:feature_ok"

def _anime_flag(it: Dict[str, Any]) -> Tuple[bool, str]:
    title=_norm(it.get("title") or it.get("name") or "")
    genres=set(str(g).lower() for g in _as_list(it.get("genres") or it.get("tmdb_genres") or []))
    lang=(it.get("original_language") or "").lower()
    countries=set(str(c).upper() for c in _as_list(it.get("production_countries") or []))
    if "anime" in genres: return True, "anime:genre"
    if "animation" in genres and (lang=="ja" or "JP" in countries): return True, "anime:lang"
    if any(k in title for k in ("one piece","dandadan","dragon ball","naruto","jujutsu kaisen","attack on titan","my hero academia","chainsaw man","spy x family")):
        return True, "anime:title"
    return False, ""

def _load_model(env: Dict[str, Any]) -> Dict[str, Any]:
    path = env.get("USER_MODEL_PATH") or "data/out/latest/exports/user_model.json"
    p = Path(path)
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"meta": {}, "people": {"director": {}, "writer": {}, "actor": {}},
                "form": {"runtime_bucket": {}, "title_type": {}, "era": {}},
                "genres": {}, "language": {}, "country": {}, "studio": {}, "network": {}, "keywords": {}, "provider": {}}

def _sum(tokens: Iterable[str], table: Dict[str, float]) -> Tuple[float, List[Tuple[str,float]]]:
    contribs=[]; total=0.0
    for t in tokens:
        w=table.get(t)
        if w: contribs.append((t,w)); total+=w
    contribs.sort(key=lambda kv: kv[1], reverse=True)
    return total, contribs

def _recency_boost(it: Dict[str, Any], seen_tv_roots: set[str], allowed_providers: set[str]) -> Tuple[float, str]:
    mt=(it.get("media_type") or "").lower()
    provs=set(str(p).lower() for p in _as_list(it.get("providers") or it.get("providers_slugs")))
    mult = REC_PROVIDER_MULT if (provs & allowed_providers) else 1.0

    if mt=="movie":
        rd=_parse_ymd(it.get("release_date"))
        d=_days_since(rd)
        if d is None: return 0.0, ""
        if d<=REC_MOVIE_WINDOW:
            frac=max(0.0, (REC_MOVIE_WINDOW - d)/REC_MOVIE_WINDOW)
            bonus=frac*REC_MOVIE_MAX*mult
            return bonus, f"+{round(bonus,1)} new movie ({d}d)"
        return 0.0, ""

    if mt=="tv":
        fad=_parse_ymd(it.get("first_air_date")); lad=_parse_ymd(it.get("last_air_date"))
        b=0.0; reasons=[]
        if fad is not None:
            d=_days_since(fad)
            if d is not None and d<=REC_TV_FIRST_WINDOW:
                frac=max(0.0,(REC_TV_FIRST_WINDOW-d)/REC_TV_FIRST_WINDOW)
                x=frac*REC_TV_FIRST_MAX*mult; b+=x; reasons.append(f"+{round(x,1)} new series ({d}d)")
        if lad is not None:
            d=_days_since(lad)
            if d is not None and d<=REC_TV_LAST_WINDOW:
                frac=max(0.0,(REC_TV_LAST_WINDOW-d)/REC_TV_LAST_WINDOW)
                x=frac*REC_TV_LAST_MAX*mult; b+=x; reasons.append(f"+{round(x,1)} new season ({d}d)")
                # follow-up boost if user has watched the show before
                title_root=_norm(it.get("title") or it.get("name") or "")
                if title_root in seen_tv_roots:
                    y=min(REC_TV_FOLLOWUP_MAX, 2.0 + frac*REC_TV_FOLLOWUP_MAX)
                    b+=y; reasons.append(f"+{round(y,1)} follow-up (watched prev)")
        return b, "; ".join(reasons)

    return 0.0, ""

def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    model=_load_model(env)
    meta=model.get("meta", {})
    base_mean=float(meta.get("global_avg", 7.5))*10.0

    people_w=model.get("people", {}) or {}
    directors_w=people_w.get("director", {}) or {}
    writers_w=people_w.get("writer", {}) or {}
    actors_w=people_w.get("actor", {}) or {}

    genres_w=model.get("genres", {}) or {}
    form_w=model.get("form", {}) or {}
    runtime_w=form_w.get("runtime_bucket", {}) or {}
    era_w=form_w.get("era", {}) or {}
    type_w=form_w.get("title_type", {}) or {}
    lang_w=model.get("language", {}) or {}
    country_w=model.get("country", {}) or {}
    studio_w=model.get("studio", {}) or {}
    network_w=model.get("network", {}) or {}
    kw_w=model.get("keywords", {}) or {}
    prov_w=model.get("provider", {}) or {}

    seen_tv_roots=set(_norm(s) for s in env.get("SEEN_TV_TITLE_ROOTS", []) or [])
    allowed_providers=set(_norm(p) for p in (env.get("SUBS_INCLUDE") or []))

    for it in items:
        aud=_audience_score(it)
        s=(1.0 - LAMBDA_AUDIENCE)*base_mean + LAMBDA_AUDIENCE*aud
        reasons: List[str]=[]

        # people
        dirs=[str(n).strip() for n in _as_list(it.get("directors"))]
        t,c=_sum(dirs, directors_w)
        if t: s+=t*2.2; reasons.append(f"+{round(t*2.2,1)} director ({', '.join(n for n,_ in c[:2])})")
        wrs=[str(n).strip() for n in _as_list(it.get("writers"))]
        t,c=_sum(wrs, writers_w)
        if t: s+=t*1.5; reasons.append(f"+{round(t*1.5,1)} writer ({', '.join(n for n,_ in c[:2])})")
        cast=[str(n).strip() for n in _as_list(it.get("cast"))]
        t,c=_sum(cast, actors_w)
        if t: s+=t*1.2; reasons.append(f"+{round(t*1.2,1)} cast ({', '.join(n for n,_ in c[:2])})")

        # keywords
        kws=[str(k).lower() for k in _as_list(it.get("keywords"))]
        t,c=_sum(kws, kw_w)
        if t: s+=t*1.2; reasons.append(f"+{round(t*1.2,1)} keywords ({', '.join(n for n,_ in c[:2])})")

        # studio / network
        studios=[str(n).lower() for n in _as_list(it.get("production_companies"))]
        nets=[str(n).lower() for n in _as_list(it.get("networks"))]
        t,cs=_sum(studios, studio_w)
        if t: s+=t; reasons.append(f"+{round(t,1)} studio ({', '.join(n for n,_ in cs[:1])})")
        t,cn=_sum(nets, network_w)
        if t: s+=t; reasons.append(f"+{round(t,1)} network ({', '.join(n for n,_ in cn[:1])})")

        # genre/form
        gens=[]
        for g in _as_list(it.get("genres") or it.get("tmdb_genres") or []):
            if isinstance(g, dict) and g.get("name"): gens.append(g["name"].lower())
            elif isinstance(g, str): gens.append(g.lower())
        t,_=_sum(gens, genres_w)
        if t: s+=t
        rb=_bucket_runtime(it); t,_=_sum([rb], runtime_w)
        if t: s+=t; reasons.append(f"+{round(t,1)} runtime {rb}")
        era=_era(it.get("year")); t,_=_sum([era], era_w)
        if t: s+=t; reasons.append(f"+{round(t,1)} era {era}")

        # language / country
        lang=(it.get("original_language") or "").lower()
        countries=[str(c).upper() for c in _as_list(it.get("production_countries"))]
        t,_=_sum([lang], lang_w)
        if t: s+=t; reasons.append(f"+{round(t,1)} language {lang}")
        t,_=_sum(countries, country_w)
        if t: s+=t; reasons.append(f"+{round(t,1)} country")

        # provider prior
        provs=[str(p).lower() for p in _as_list(it.get("providers") or it.get("providers_slugs"))]
        if provs:
            t,_=_sum(provs, prov_w)
            if t: s+=LAMBDA_PROVIDER * t

        # kids/anime
        if PENALIZE_KIDS:
            penal, why=_kids_penalize(it)
            if penal: s-=KIDS_PENALTY; reasons.append(f"-{KIDS_PENALTY} kids ({why})")
        if PENALIZE_ANIME:
            ok, why=_anime_flag(it)
            if ok: s-=ANIME_PENALTY; reasons.append(f"-{ANIME_PENALTY} anime ({why})")

        # recency boost (movies, new series, new seasons; extra if you've watched the show)
        rboost, rmsg = _recency_boost(it, seen_tv_roots, allowed_providers)
        if rboost>0:
            s += rboost
            reasons.append(rmsg)

        # commitment penalty (long TV)
        if COMMITMENT_ENABLED and (it.get("media_type") or "").lower()=="tv":
            seasons=0
            try: seasons=int(it.get("number_of_seasons") or 0)
            except Exception: seasons=0
            over=max(0, seasons - COMMITMENT_SEASONS_THRESHOLD)
            if over>0:
                pen=min(COMMITMENT_MAX_PENALTY, over*COMMITMENT_SEASON_PENALTY)
                s-=pen; reasons.append(f"-{int(pen)} long-run ({seasons} seasons)")

        it["score"]=float(max(0.0, min(100.0, s)))
        if reasons:
            prev=(it.get("why") or "").strip()
            it["why"]= (prev + ("; " if prev else "") + "; ".join(reasons))

    return items