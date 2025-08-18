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
                                "attack on titan","