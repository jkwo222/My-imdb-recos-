# engine/scoring.py
from __future__ import annotations
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, Iterable

# ---- env knobs ----
def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return default
def _int(name: str, default: int) -> int:
    try: return int(os.getenv(name, "") or default)
    except Exception: return default
def _float(name: str, default: float) -> float:
    try: return float(os.getenv(name, "") or default)
    except Exception: return default
def _csv(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [s.strip().lower() for s in raw.split(",") if s.strip()]

# anime/kids penalties
PENALIZE_KIDS = _bool("PENALIZE_KIDS", True)
PENALIZE_ANIME = _bool("PENALIZE_ANIME", True)
KIDS_PENALTY = max(0, _int("KIDS_CARTOON_PENALTY", 25))
ANIME_PENALTY = max(0, _int("ANIME_PENALTY", 20))

# refined kids gating
KIDS_MOVIE_MIN_RUNTIME = _int("KIDS_MOVIE_MIN_RUNTIME", 70)  # only penalize movies under this runtime
KIDS_STUDIO_WHITELIST = set(_csv(
    "KIDS_STUDIO_WHITELIST",
    "pixar animation studios,walt disney animation studios,walt disney pictures,dreamworks animation,sony pictures animation,illumination,laika"
))

# blend weights
LAMBDA_AUDIENCE = _float("AUDIENCE_PRIOR_LAMBDA", 0.3)
LAMBDA_PROVIDER = _float("PROVIDER_PREF_LAMBDA", 0.5)

# scoring helpers
_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    return _NON_ALNUM.sub(" ", s)

def _as_listish(x) -> List[Any]:
    if x is None: return []
    if isinstance(x, list): return x
    return [x]

def _audience_score(it: Dict[str, Any]) -> float:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0: f *= 10.0
        return max(0.0, min(100.0, f))
    except Exception:
        return 50.0

def _bucket_runtime_for_item(it: Dict[str, Any]) -> str:
    mins = None
    if it.get("media_type") == "movie":
        mins = it.get("runtime")
    else:
        ert = it.get("episode_run_time") or []
        if isinstance(ert, list) and ert:
            mins = ert[0]
    try:
        m = float(mins)
    except Exception:
        return "unknown"
    if m <= 90: return "<=90"
    if m <= 120: return "91-120"
    if m <= 150: return "121-150"
    return ">150"

def _era_for_year(y) -> str:
    try:
        yi = int(str(y)[:4])
    except Exception:
        return "unknown"
    if 1960 <= yi < 1980: return "60-79"
    if 1980 <= yi < 1990: return "80s"
    if 1990 <= yi < 2000: return "90s"
    if 2000 <= yi < 2010: return "00s"
    if 2010 <= yi < 2020: return "10s"
    if 2020 <= yi < 2030: return "20s"
    return "unknown"

# ---- refined kids/anime detection ----
def _kids_should_penalize(it: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Penalize *young children's cartoons*:
      - TV with clear kids signals (genres include animation AND (kids or family), or kid-first networks)
      - Movies only if runtime < KIDS_MOVIE_MIN_RUNTIME (short-form / specials), AND not produced by whitelisted studios.
    Feature animations from Pixar/Disney/DreamWorks/etc. are EXEMPT via studio whitelist.
    """
    # collect signals
    genres = []
    for g in _as_listish(it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"):
            genres.append(g["name"].lower())
        elif isinstance(g, str):
            genres.append(g.lower())
    genres = set(genres)
    media_type = (it.get("media_type") or "").lower()

    studios = [str(n).lower() for n in _as_listish(it.get("production_companies"))]
    if any(s in KIDS_STUDIO_WHITELIST for s in studios):
        return (False, "kids:whitelist_studio")

    # explicit title hints (quick filter)
    title = _norm(it.get("title") or it.get("name") or "")
    title_hits = any(k in title for k in ("bluey","peppa","paw patrol","cocomelon","octonauts","dora "))
    kidsish = ("animation" in genres) and (("kids" in genres) or ("family" in genres) or title_hits)

    if not kidsish:
        return (False, "")

    if media_type == "tv":
        return (True, "kids:tv")

    if media_type == "movie":
        # only penalize short-form movies/specials
        mins = None
        try:
            mins = float(it.get("runtime", 0) or 0)
        except Exception:
            mins = 0
        if mins and mins < float(KIDS_MOVIE_MIN_RUNTIME):
            return (True, f"kids:movie<{KIDS_MOVIE_MIN_RUNTIME}")
        return (False, "kids:feature_ok")

    return (False, "")

def _anime_flag(it: Dict[str, Any]) -> Tuple[bool, str]:
    title = (_norm(it.get("title") or it.get("name") or ""))
    genres = set(str(g).lower() for g in _as_listish(it.get("genres") or it.get("tmdb_genres") or []))
    lang = (it.get("original_language") or "").lower()
    countries = set(c.upper() for c in _as_listish(it.get("production_countries") or []))
    if "anime" in genres: return True, "anime:genre"
    if "animation" in genres and (lang == "ja" or "JP" in countries):
        return True, "anime:lang"
    if any(k in title for k in ("one piece","dandadan","dragon ball","naruto","jujutsu kaisen","attack on titan","my hero academia","chainsaw man","spy x family")):
        return True, "anime:title"
    return False, ""

# ---- model loading / utilities ----
def _load_model(env: Dict[str, Any]) -> Dict[str, Any]:
    path = env.get("USER_MODEL_PATH") or "data/out/latest/exports/user_model.json"
    p = Path(path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"meta": {}, "people": {"director": {}, "writer": {}, "actor": {}},
                "form": {"runtime_bucket": {}, "title_type": {}, "era": {}},
                "genres": {}, "language": {}, "country": {}, "studio": {}, "network": {}, "keywords": {}, "provider": {}}

def _sum_weights(tokens: Iterable[str], table: Dict[str, float]) -> Tuple[float, List[Tuple[str,float]]]:
    contribs: List[Tuple[str,float]] = []
    total = 0.0
    for t in tokens:
        w = table.get(t)
        if w:
            contribs.append((t, w))
            total += w
    contribs.sort(key=lambda kv: kv[1], reverse=True)
    return total, contribs

def _audience_score(it: Dict[str, Any]) -> float:
    v = it.get("audience") or it.get("tmdb_vote")
    try:
        f = float(v)
        if f <= 10.0: f *= 10.0
        return max(0.0, min(100.0, f))
    except Exception:
        return 50.0

# ---- main scoring ----
def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    model = _load_model(env)
    meta = model.get("meta", {})
    base_mean = float(meta.get("global_avg", 7.5)) * 10.0

    # tables
    people_w = model.get("people", {}) or {}
    directors_w = people_w.get("director", {}) or {}
    writers_w   = people_w.get("writer", {}) or {}
    actors_w    = people_w.get("actor", {}) or {}

    genres_w    = model.get("genres", {}) or {}
    form_w      = model.get("form", {}) or {}
    runtime_w   = form_w.get("runtime_bucket", {}) or {}
    era_w       = form_w.get("era", {}) or {}
    type_w      = form_w.get("title_type", {}) or {}
    lang_w      = model.get("language", {}) or {}
    country_w   = model.get("country", {}) or {}
    studio_w    = model.get("studio", {}) or {}
    network_w   = model.get("network", {}) or {}
    kw_w        = model.get("keywords", {}) or {}
    prov_w      = model.get("provider", {}) or {}

    for it in items:
        aud = _audience_score(it)
        s = (1.0 - LAMBDA_AUDIENCE) * base_mean + LAMBDA_AUDIENCE * aud
        reasons: List[str] = []

        # People: directors strongest, then writers, then actors
        dirs = [str(n).strip() for n in _as_listish(it.get("directors"))]
        t_d, c_d = _sum_weights(dirs, directors_w)
        if t_d:
            s += t_d * 2.2
            reasons.append(f"+{round(t_d*2.2,1)} director ({', '.join(n for n,_ in c_d[:2])})")

        wrs = [str(n).strip() for n in _as_listish(it.get("writers"))]
        t_w, c_w = _sum_weights(wrs, writers_w)
        if t_w:
            s += t_w * 1.5
            reasons.append(f"+{round(t_w*1.5,1)} writer ({', '.join(n for n,_ in c_w[:2])})")

        cast = [str(n).strip() for n in _as_listish(it.get("cast"))]
        t_a, c_a = _sum_weights(cast, actors_w)
        if t_a:
            s += t_a * 1.2
            reasons.append(f"+{round(t_a*1.2,1)} cast ({', '.join(n for n,_ in c_a[:2])})")

        # Keywords
        kws = [str(k).lower() for k in _as_listish(it.get("keywords"))]
        t_kw, c_kw = _sum_weights(kws, kw_w)
        if t_kw:
            s += t_kw * 1.2
            reasons.append(f"+{round(t_kw*1.2,1)} keywords ({', '.join(n for n,_ in c_kw[:2])})")

        # Studio / Network
        studios = [str(n).lower() for n in _as_listish(it.get("production_companies"))]
        nets    = [str(n).lower() for n in _as_listish(it.get("networks"))]
        t_st, c_st = _sum_weights(studios, studio_w)
        t_nt, c_nt = _sum_weights(nets, network_w)
        if t_st:
            s += t_st
            reasons.append(f"+{round(t_st,1)} studio ({', '.join(n for n,_ in c_st[:1])})")
        if t_nt:
            s += t_nt
            reasons.append(f"+{round(t_nt,1)} network ({', '.join(n for n,_ in c_nt[:1])})")

        # Genres
        gens = []
        for g in _as_listish(it.get("genres") or it.get("tmdb_genres") or []):
            if isinstance(g, dict) and g.get("name"):
                gens.append(g["name"].lower())
            elif isinstance(g, str):
                gens.append(g.lower())
        t_g, _ = _sum_weights(gens, genres_w)
        if t_g:
            s += t_g

        # Runtime / Era / Type
        rb = _bucket_runtime_for_item(it)
        t_rb, _ = _sum_weights([rb], runtime_w)
        if t_rb:
            s += t_rb
            reasons.append(f"+{round(t_rb,1)} runtime {rb}")
        era = _era_for_year(it.get("year"))
        t_era, _ = _sum_weights([era], era_w)
        if t_era:
            s += t_era
            reasons.append(f"+{round(t_era,1)} era {era}")

        # Language / Country
        lang = (it.get("original_language") or "").lower()
        countries = [str(c).upper() for c in _as_listish(it.get("production_countries"))]
        t_lang, _ = _sum_weights([lang], lang_w)
        t_ctry, _ = _sum_weights(countries, country_w)
        if t_lang:
            s += t_lang
            reasons.append(f"+{round(t_lang,1)} language {lang}")
        if t_ctry:
            s += t_ctry
            reasons.append(f"+{round(t_ctry,1)} country")

        # Provider small prior
        provs = [str(p).lower() for p in _as_listish(it.get("providers") or it.get("providers_slugs"))]
        t_prov, _ = _sum_weights(provs, prov_w)
        if t_prov:
            s += LAMBDA_PROVIDER * t_prov

        # Kids / Anime penalties (refined kids logic)
        if PENALIZE_KIDS:
            penal, why = _kids_should_penalize(it)
            if penal:
                s -= KIDS_PENALTY
                reasons.append(f"-{KIDS_PENALTY} kids ({why})")
        if PENALIZE_ANIME:
            is_anime, why = _anime_flag(it)
            if is_anime:
                s -= ANIME_PENALTY
                reasons.append(f"-{ANIME_PENALTY} anime ({why})")

        it["score"] = float(max(0.0, min(100.0, s)))
        if reasons:
            prev = (it.get("why") or "").strip()
            it["why"] = (prev + ("; " if prev else "") + "; ".join(reasons))

    return items