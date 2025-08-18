# engine/scoring.py
from __future__ import annotations
import os
import math
import re
from typing import Any, Dict, List, Set, Tuple

"""
Scoring pipeline with penalties for Kids' Cartoons and Anime.

Environment knobs (all optional):
- KIDS_CARTOON_PENALTY=25           # points to subtract (default 25)
- ANIME_PENALTY=20                  # points to subtract (default 20)
- PENALIZE_KIDS=true|false          # default true
- PENALIZE_ANIME=true|false         # default true
- KIDS_TITLE_HINTS="bluey,peppa pig,paw patrol,..."          # CSV of lowercase terms
- ANIME_TITLE_HINTS="one piece,dandadan,dragon ball,..."     # CSV of lowercase terms
- KIDS_NETWORK_HINTS="disney junior,cartoonito,nick jr,cbeebies"     # CSV
- ANIME_STUDIO_HINTS="toei animation,mappa,bones,studio ghibli,wit studio,trigger,cloverworks,a-1 pictures"   # CSV
- KIDS_GENRE_HINTS="family,kids,children,childrens"          # CSV
- ANIME_GENRE_HINTS="animation,anime"                        # CSV
"""

# ------------------ helpers ------------------

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    return _NON_ALNUM.sub(" ", s)

def _tokset(s: str) -> Set[str]:
    return set(t for t in _norm(s).split() if t)

def _as_listish(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        return [x]
    return [x]

def _csv_env(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    vals = [v.strip().lower() for v in raw.split(",") if v.strip()]
    return vals

def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"): return True
    if v in ("0", "false", "no", "n", "off"): return False
    return default

def _int_env(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v else default
    except Exception:
        return default

def _get_title(it: Dict[str, Any]) -> str:
    return (it.get("title") or it.get("name") or it.get("original_title") or it.get("original_name") or "").strip()

def _get_year(it: Dict[str, Any]) -> int | None:
    y = it.get("year")
    try:
        return int(str(y)[:4]) if y is not None else None
    except Exception:
        return None

def _get_genres(it: Dict[str, Any]) -> Set[str]:
    acc: Set[str] = set()
    for key in ("genres", "genre", "tmdb_genres", "tags", "keywords"):
        val = it.get(key)
        for v in _as_listish(val):
            if isinstance(v, str):
                acc.add(v.strip().lower())
            elif isinstance(v, dict):
                # TMDB often supplies [{"id":16,"name":"Animation"}, ...]
                name = v.get("name")
                if isinstance(name, str):
                    acc.add(name.strip().lower())
    # Sometimes we only have IDs; accept common TMDB genre IDs for reference:
    # 16=Animation, 10762=Kids (TV)
    id_list = it.get("tmdb_genre_ids") or it.get("genre_ids")
    if isinstance(id_list, list):
        if 16 in id_list: acc.add("animation")
        if 10762 in id_list: acc.add("kids")
        if 10751 in id_list: acc.add("family")
    return acc

def _get_networks(it: Dict[str, Any]) -> Set[str]:
    acc: Set[str] = set()
    for key in ("networks", "production_companies", "production_networks", "companies"):
        val = it.get(key)
        for v in _as_listish(val):
            if isinstance(v, str):
                acc.add(v.strip().lower())
            elif isinstance(v, dict):
                name = v.get("name")
                if isinstance(name, str):
                    acc.add(name.strip().lower())
    return acc

def _get_studios(it: Dict[str, Any]) -> Set[str]:
    # separate in case caller wants both networks and studios
    acc: Set[str] = set()
    for key in ("studios", "production_companies"):
        val = it.get(key)
        for v in _as_listish(val):
            if isinstance(v, str):
                acc.add(v.strip().lower())
            elif isinstance(v, dict):
                name = v.get("name")
                if isinstance(name, str):
                    acc.add(name.strip().lower())
    return acc

def _origin_lang(it: Dict[str, Any]) -> str:
    return (it.get("original_language") or it.get("language") or "").strip().lower()

def _origin_countries(it: Dict[str, Any]) -> Set[str]:
    acc: Set[str] = set()
    for key in ("origin_country", "production_countries", "countries"):
        val = it.get(key)
        for v in _as_listish(val):
            if isinstance(v, str):
                acc.add(v.strip().upper())
            elif isinstance(v, dict):
                # TMDB production_countries usually: [{"iso_3166_1":"JP","name":"Japan"}]
                code = v.get("iso_3166_1") or v.get("iso3166_1")
                if isinstance(code, str):
                    acc.add(code.strip().upper())
                else:
                    name = v.get("name")
                    if isinstance(name, str):
                        acc.add(name.strip().upper())
    return acc

# ------------------ classification ------------------

_DEF_KIDS_TITLE_HINTS = "bluey,peppa pig,paw patrol,cocomelon,bubble guppies,dora the explorer,octonauts"
_DEF_ANIME_TITLE_HINTS = "one piece,dandadan,dragon ball,bleach,naruto,jujutsu kaisen,attack on titan,my hero academia,spy x family,chainsaw man"

_DEF_KIDS_NETWORK_HINTS = "disney junior,cartoonito,nick jr,cbeebies,pbs kids,universal kids"
_DEF_ANIME_STUDIO_HINTS = "toei animation,mappa,bones,studio ghibli,wit studio,trigger,cloverworks,a-1 pictures,kyoto animation,production i g,studio pierrot,sunrise"

_DEF_KIDS_GENRE_HINTS = "family,kids,children,childrens"
_DEF_ANIME_GENRE_HINTS = "animation,anime"   # 'anime' may appear in keywords/tags

def _kids_cartoon_flag(it: Dict[str, Any]) -> Tuple[bool, str]:
    title = _get_title(it).lower()
    genres = _get_genres(it)
    networks = _get_networks(it)

    title_hints = _csv_env("KIDS_TITLE_HINTS", _DEF_KIDS_TITLE_HINTS)
    network_hints = _csv_env("KIDS_NETWORK_HINTS", _DEF_KIDS_NETWORK_HINTS)
    genre_hints = set(_csv_env("KIDS_GENRE_HINTS", _DEF_KIDS_GENRE_HINTS))

    # Title contains a known kids brand
    for h in title_hints:
        if h in title:
            return True, f"kids:title:{h}"

    # Genres strongly indicate kids cartoon
    if "animation" in genres and (genres & genre_hints):
        return True, "kids:genres"

    # TV networks focused on kids
    for n in networks:
        for h in network_hints:
            if h in n:
                return True, f"kids:network:{h}"

    # TMDB TV "Kids" id 10762 gets mapped to 'kids' above
    if "kids" in genres:
        return True, "kids:tmdb"

    return False, ""

def _anime_flag(it: Dict[str, Any]) -> Tuple[bool, str]:
    title = _get_title(it).lower()
    genres = _get_genres(it)
    studios = _get_studios(it)
    networks = _get_networks(it)
    lang = _origin_lang(it)
    countries = _origin_countries(it)

    title_hints = _csv_env("ANIME_TITLE_HINTS", _DEF_ANIME_TITLE_HINTS)
    studio_hints = _csv_env("ANIME_STUDIO_HINTS", _DEF_ANIME_STUDIO_HINTS)
    genre_hints = set(_csv_env("ANIME_GENRE_HINTS", _DEF_ANIME_GENRE_HINTS))

    # Obvious title brands
    for h in title_hints:
        if h in title:
            return True, f"anime:title:{h}"

    # Language/country + animation
    if ("animation" in genres or "anime" in genres) and (lang == "ja" or "JP" in countries):
        return True, "anime:lang_country"

    # Studios / networks commonly producing anime
    for s in studios | networks:
        for h in studio_hints:
            if h in s:
                return True, f"anime:studio:{h}"

    # Generic genre hint if explicitly present
    if "anime" in genres:
        return True, "anime:genre"

    return False, ""

# ------------------ scoring ------------------

def _base_score(it: Dict[str, Any]) -> float:
    """
    Start from item['score'] or item['match'] if present, else derive from TMDB vote/popularity.
    """
    for k in ("score", "match", "estimated_score"):
        v = it.get(k)
        try:
            return float(v)
        except Exception:
            continue
    vote = it.get("tmdb_vote")
    try:
        vote_f = float(vote)
        # simple uplift: 0..10 -> 0..100
        return max(0.0, min(100.0, vote_f * 10.0))
    except Exception:
        pass
    # fallback on popularity scale (rough)
    pop = it.get("popularity")
    try:
        pop_f = float(pop)
        return max(0.0, min(100.0, 35.0 + math.log1p(pop_f) * 7.0))
    except Exception:
        return 50.0

def _append_why(it: Dict[str, Any], msg: str) -> None:
    prev = (it.get("why") or "").strip()
    it["why"] = (prev + ("; " if prev else "") + msg)

def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    penalize_kids = _bool_env("PENALIZE_KIDS", True)
    penalize_anime = _bool_env("PENALIZE_ANIME", True)
    kids_penalty = max(0, _int_env("KIDS_CARTOON_PENALTY", 25))
    anime_penalty = max(0, _int_env("ANIME_PENALTY", 20))

    for it in items:
        base = _base_score(it)
        final = base
        reasons: List[str] = []

        # Kids' cartoons
        if penalize_kids:
            is_kids, reason = _kids_cartoon_flag(it)
            if is_kids:
                final -= kids_penalty
                reasons.append(f"-{kids_penalty} kids ({reason})")

        # Anime
        if penalize_anime:
            is_anime, reason = _anime_flag(it)
            if is_anime:
                final -= anime_penalty
                reasons.append(f"-{anime_penalty} anime ({reason})")

        # Clamp & write
        final = float(max(0.0, min(100.0, final)))
        it["score"] = final

        if reasons:
            _append_why(it, "; ".join(reasons))

    return items