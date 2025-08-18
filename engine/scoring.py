# engine/scoring.py
from __future__ import annotations
import math
import os
from typing import Any, Dict, List

_NON_FAV_ERA_YEAR = int(os.getenv("ERA_CUTOFF_YEAR", "1984") or 1984)

# Penalties / knobs
PENALIZE_ANIME        = (os.getenv("PENALIZE_ANIME","true").lower() in {"1","true","yes","on"})
ANIME_PENALTY         = float(os.getenv("ANIME_PENALTY","20") or 20)

PENALIZE_KIDS         = (os.getenv("PENALIZE_KIDS","true").lower() in {"1","true","yes","on"})
KIDS_CARTOON_PENALTY  = float(os.getenv("KIDS_CARTOON_PENALTY","25") or 25)
KIDS_STUDIO_EXEMPT    = {"pixar","walt disney","disney","waltdisney","disney animation","dreamworks","illumination","blue sky","sony pictures animation","laika","ghibli"}

ROMANCE_PENALTY       = float(os.getenv("ROMANCE_PENALTY","10") or 10)
OLD_BW_PENALTY        = float(os.getenv("OLD_BW_PENALTY","25") or 25)
PRE1984_PENALTY       = float(os.getenv("PRE1984_PENALTY","20") or 20)

TV_COMMIT_PENALTY_PER_SEASON = float(os.getenv("TV_COMMIT_PENALTY_PER_SEASON","3") or 3)
TV_COMMIT_MAX_PENALTY        = float(os.getenv("TV_COMMIT_MAX_PENALTY","18") or 18)

AUDIENCE_PRIOR_LAMBDA = float(os.getenv("AUDIENCE_PRIOR_LAMBDA","0.3") or 0.3)
PROVIDER_PREF_LAMBDA  = float(os.getenv("PROVIDER_PREF_LAMBDA","0.5") or 0.5)

def _norm(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else " " for ch in (s or "")).strip()

def _audience_pct(it: Dict[str, Any]) -> float:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0:
            f *= 10.0
        return max(0.0, min(100.0, f))
    except Exception:
        return 0.0

def _genres(it: Dict[str, Any]) -> List[str]:
    out=[]
    for g in (it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"):
            out.append(g["name"])
        elif isinstance(g, str):
            out.append(g)
    return out

def _keywords(it: Dict[str, Any]) -> List[str]:
    out=[]
    for k in (it.get("keywords") or []):
        if isinstance(k, str):
            out.append(k)
        elif isinstance(k, dict) and k.get("name"):
            out.append(k["name"])
    return out

def _companies(it: Dict[str, Any]) -> List[str]:
    out=[]
    for c in (it.get("production_companies") or []):
        if isinstance(c, str):
            out.append(c)
        elif isinstance(c, dict) and c.get("name"):
            out.append(c["name"])
    return out

def _people_score(names: List[str], pref_map: Dict[str, float], label: str, why: List[str], w: float) -> float:
    score = 0.0
    if not names or not pref_map:
        return 0.0
    hits=[]
    for nm in names:
        if nm in pref_map:
            val = pref_map[nm] * w
            score += val * 100.0
            hits.append(nm)
    if hits:
        why.append(f"{label}: " + ", ".join(hits[:3]))
    return score

def _bag_score(bag: List[str], pref_map: Dict[str, float], label: str, why: List[str], w: float) -> float:
    s=0.0
    hits=[]
    for token in bag:
        k=_norm(token)
        if k in pref_map:
            val = pref_map[k] * w
            s += val * 100.0
            hits.append(token)
    if s>0 and hits:
        why.append(f"{label}: " + ", ".join(hits[:5]))
    return s

def _is_anime_like(it: Dict[str, Any]) -> bool:
    gset = {_norm(g) for g in _genres(it)}
    t = _norm(it.get("title") or it.get("name") or "")
    if "anime" in gset:
        return True
    if "animation" in gset and any(k in t for k in ("one piece","dandadan","dragon ball","naruto","jujutsu kaisen","attack on titan","my hero academia","chainsaw man","spy x family")):
        return True
    if (it.get("original_language") or "").lower() == "ja" and "animation" in gset:
        return True
    return False

def _is_kids_cartoon(it: Dict[str, Any]) -> bool:
    gset = {_norm(g) for g in _genres(it)}
    if "animation" in gset and ("family" in gset or "kids" in gset):
        comps = {_norm(c) for c in _companies(it)}
        if any(ex in comps for ex in KIDS_STUDIO_EXEMPT):
            return False
        return True
    return False

def _is_romance(it: Dict[str, Any]) -> bool:
    gset = {_norm(g) for g in _genres(it)}
    kset = {_norm(k) for k in _keywords(it)}
    if "romance" in gset:
        return True
    if any(k in kset for k in ("romantic comedy","rom com","romcom")):
        return True
    return False

def _is_black_white(it: Dict[str, Any]) -> bool:
    kset = {_norm(k) for k in _keywords(it)}
    return "black and white" in kset or "black-and-white" in kset

def _year(it: Dict[str, Any]) -> int:
    if it.get("year"):
        try: return int(it.get("year"))
        except Exception: pass
    for fld in ("release_date","first_air_date"):
        s = (it.get(fld) or "").strip()
        if len(s) >= 4 and s[:4].isdigit():
            try: return int(s[:4])
            except Exception: pass
    return 0

def _commit_penalty(it: Dict[str, Any]) -> float:
    mt = (it.get("media_type") or "").lower()
    if mt != "tv":
        return 0.0
    seasons = int(it.get("number_of_seasons") or 0)
    if seasons <= 1:
        return 0.0
    pen = (seasons - 1) * TV_COMMIT_PENALTY_PER_SEASON
    return min(pen, TV_COMMIT_MAX_PENALTY)

def score_items(items: List[Dict[str, Any]], model: Dict[str, Any], env: Dict[str, Any]) -> List[Dict[str, Any]]:
    top_actors    = model.get("top_actors") or {}
    top_directors = model.get("top_directors") or {}
    top_writers   = model.get("top_writers") or {}
    top_genres    = model.get("top_genres") or {}
    top_keywords  = model.get("top_keywords") or {}
    top_subgenres = model.get("top_subgenres") or {}

    out: List[Dict[str, Any]] = []
    for it in items:
        why: List[str] = []
        score = 0.0

        # People
        score += _people_score(it.get("cast") or [], top_actors, "cast", why, w=1.0)
        score += _people_score(it.get("directors") or [], top_directors, "director", why, w=1.05)
        score += _people_score(it.get("writers") or [], top_writers, "writer", why, w=0.8)

        # Genres/keywords (subgenres give a tiny extra push)
        score += _bag_score(_genres(it), top_genres, "genre", why, w=0.9)
        score += _bag_score(_keywords(it), top_keywords, "keyword", why, w=0.6)
        if top_subgenres:
            # Create a synthetic bag out of sub-genre pair names (already normalized in profile builder)
            score += _bag_score(list(top_subgenres.keys()), top_subgenres, "sub-genre", why, w=0.5)

        # Audience prior blend
        aud = _audience_pct(it)
        score = (1 - AUDIENCE_PRIOR_LAMBDA) * score + AUDIENCE_PRIOR_LAMBDA * aud

        # Provider preference (mild boost if available on your services)
        provs = it.get("providers") or it.get("providers_slugs") or []
        if provs:
            score = (1 - PROVIDER_PREF_LAMBDA) * score + PROVIDER_PREF_LAMBDA * (score * 1.05)

        # Penalties
        if PENALIZE_ANIME and _is_anime_like(it):
            score -= ANIME_PENALTY; why.append("anime penalty")
        if PENALIZE_KIDS and _is_kids_cartoon(it):
            score -= KIDS_CARTOON_PENALTY; why.append("kids cartoon penalty (studio exceptions apply)")
        if _is_romance(it):
            score -= ROMANCE_PENALTY; why.append("romance penalty")
        yr = _year(it)
        if yr and yr < _NON_FAV_ERA_YEAR:
            score -= PRE1984_PENALTY; why.append(f"pre-{_NON_FAV_ERA_YEAR} penalty")
        if _is_black_white(it):
            score -= OLD_BW_PENALTY; why.append("black & white penalty")
        score -= _commit_penalty(it)

        it2 = dict(it)
        it2["score"] = round(max(0.0, score), 2)
        it2["why"] = "; ".join(why)
        out.append(it2)

    out.sort(key=lambda x: (float(x.get("score", 0.0)), float(x.get("audience", 0.0))), reverse=True)
    return out