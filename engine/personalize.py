# engine/personalize.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import math

def _to_list(x) -> List[str]:
    if not x: return []
    if isinstance(x, (list, tuple)): return [str(i) for i in x if i]
    return [str(x)]

def _explode_genre_tags(genres: List[str], tmdb_raw: Dict[str,Any] | None) -> List[str]:
    """
    Expand genre signals using TMDB networks/keywords if present.
    """
    tags = list(genres)
    if not tmdb_raw:
        return tags
    raw = tmdb_raw.get("raw") or {}
    # attach known enrichers
    networks = []
    if "tv_results" in raw and raw["tv_results"]:
        for r in raw["tv_results"]:
            nets = r.get("network") or r.get("networks") or []
            for n in nets:
                nname = n.get("name")
                if nname: networks.append(nname)
    if "movie_results" in raw and raw["movie_results"]:
        # could attach production companies/keywords if present in detail fetch
        pass
    for n in networks:
        tags.append(f"network:{n}")
    return tags

def genre_weights_from_profile(
    items: List[Dict[str,Any]],
    user_profile: Dict[str,Dict],
    imdb_id_field: str="tconst",
    tmdb_index: Dict[str,Dict[str,Any]] | None = None,
) -> Dict[str, float]:
    """
    Weight expanded genre/tags by (my_rating - 6.0) across rated titles.
    """
    acc = defaultdict(float)
    cnt = defaultdict(int)

    by_imdb: Dict[str,Dict[str,Any]] = {}
    for it in items:
        tid = it.get(imdb_id_field) or it.get("imdb_id") or it.get("tconst")
        if not tid: 
            continue
        by_imdb[str(tid)] = it

    for tid, row in user_profile.items():
        it = by_imdb.get(str(tid))
        if not it:
            continue
        genres = _to_list(it.get("genres")) or []
        # add enriched tags from cached tmdb
        tmdb_obj = tmdb_index.get(f"imdb:{tid}") if tmdb_index else None
        tags = _explode_genre_tags(genres, tmdb_obj)

        r = row.get("my_rating")
        if r is None:
            # public list presence == a weak positive signal
            if row.get("from_public_list"):
                r = 6.5
            else:
                continue
        delta = float(r) - 6.0
        for g in tags:
            acc[g] += delta
            cnt[g] += 1

    if not acc:
        return {}
    mx = max(abs(v) for v in acc.values()) or 1.0
    out = {g: (0.5 + 0.5*(v/mx)) for g,v in acc.items()}
    return {g: round(w, 4) for g,w in out.items()}

def apply_personal_score(
    items: List[Dict[str,Any]],
    genre_weights: Dict[str,float],
    base_key: str="imdb_rating",
    downvote_index: Dict[str,Any] | None = None,
) -> None:
    """
    Mutates items: adds 'score' (0–100). Combines base score (IMDb/10) with genre fit and downvote penalty.
    """
    for it in items:
        base = it.get(base_key)
        try:
            base10 = float(base) if base is not None else math.nan
        except Exception:
            base10 = math.nan
        base100 = (base10 * 10.0) if not math.isnan(base10) else 60.0

        g = _to_list(it.get("genres"))
        fit = 0.5
        if genre_weights and g:
            fit = sum(genre_weights.get(x, 0.5) for x in g) / len(g)
        adj = (fit - 0.5) * 30.0

        # Downvote memory — hard penalty if present
        penalty = 0.0
        if downvote_index:
            tconst = str(it.get("tconst") or it.get("imdb_id") or "")
            if tconst and tconst in downvote_index:
                penalty = -40.0  # large drop; effectively hides it

        it["score"] = max(0.0, min(100.0, base100 + adj + penalty))