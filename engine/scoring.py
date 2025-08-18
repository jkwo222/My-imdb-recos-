# engine/scoring.py
from __future__ import annotations
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

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

# anime/kids penalties
PENALIZE_KIDS = _bool("PENALIZE_KIDS", True)
PENALIZE_ANIME = _bool("PENALIZE_ANIME", True)
KIDS_PENALTY = max(0, _int("KIDS_CARTOON_PENALTY", 25))
ANIME_PENALTY = max(0, _int("ANIME_PENALTY", 20))

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
        # episode run time list
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

# anime/kids detection (lightweight)
def _kids_flag(it: Dict[str, Any]) -> Tuple[bool, str]:
    title = (_norm(it.get("title") or it.get("name") or ""))
    genres = set(str(g).lower() for g in _as_listish(it.get("genres") or it.get("tmdb_genres") or []))
    if "animation" in genres and ("kids" in genres or "family" in genres):
        return True, "kids:genres"
    if any(k in title for k in ("bluey","peppa","paw patrol","cocomelon","octonauts","dora ")):
        return True, "kids:title"
    return False, ""
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

# load model
def _load_model(env: Dict[str, Any]) -> Dict[str, Any]:
    # runner writes this path in env["USER_MODEL_PATH"] or we fall back to default
    path = env.get("USER_MODEL_PATH") or "data/out/latest/exports/user_model.json"
    p = Path(path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"meta": {}, "people": {"director": {}}, "form": {"runtime_bucket": {}, "title_type": {}, "era": {}},
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

def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    model = _load_model(env)
    meta = model.get("meta", {})
    base_mean = float(meta.get("global_avg", 7.5)) * 10.0  # convert to 0..100-ish anchor
    # tables
    directors_w = model.get("people", {}).get("director", {}) or {}
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
        # Start from audience prior blended with baseline
        aud = _audience_score(it)
        s = (1.0 - LAMBDA_AUDIENCE) * base_mean + LAMBDA_AUDIENCE * aud
        reasons: List[str] = []

        # People
        dirs = [str(n).strip() for n in _as_listish(it.get("directors"))]
        t_people, c_people = _sum_weights(dirs, directors_w)
        if t_people:
            s += t_people * 2.0  # directors are strong signals
            reasons.append(f"+{round(t_people*2.0,1)} director ({', '.join(n for n,_ in c_people[:2])})")

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
        t_g, c_g = _sum_weights(gens, genres_w)
        if t_g:
            s += t_g
            reasons.append(f"+{round(t_g,1)} genres")

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
        # title type not always present for candidates; skip unless provided

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
            reasons.append(f"+{round(LAMBDA_PROVIDER*t_prov,1)} provider")

        # Kids / Anime penalties (keep last so they're visible)
        if PENALIZE_KIDS:
            is_kids, why = _kids_flag(it)
            if is_kids:
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