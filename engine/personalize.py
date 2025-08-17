# engine/personalize.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import math

def _to_list(x) -> List[str]:
    if not x: return []
    if isinstance(x, (list, tuple)): return [str(i) for i in x if i]
    return [str(x)]

def genre_weights_from_profile(
    items: List[Dict[str,Any]],
    user_profile: Dict[str,Dict],
    imdb_id_field: str="tconst",
) -> Dict[str, float]:
    """
    Weight genres by (my_rating - 6.0) across rated titles found in items (where we know the mapping).
    6.0 ~ neutral; >6 favors, <6 disfavors.
    """
    acc = defaultdict(float)
    cnt = defaultdict(int)

    # index items by imdb id for genre lookup
    by_imdb = {}
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
        r = row.get("my_rating")
        if r is None:
            continue
        delta = float(r) - 6.0
        for g in genres:
            acc[g] += delta
            cnt[g] += 1

    if not acc:
        return {}
    mx = max(abs(v) for v in acc.values()) or 1.0
    out = {g: (0.5 + 0.5*(v/mx)) for g,v in acc.items()}  # map [-mx,+mx] -> [0,1]
    return {g: round(w, 4) for g,w in out.items()}

def _best_rating_10(it: Dict[str,Any]) -> float | None:
    """
    Return best available audience-like rating on 0..10:
      - IMDb (imdb_rating) if present and numeric
      - else TMDB vote_average (tmdb_vote)
    """
    def _coerce(x):
        try:
            return float(x)
        except Exception:
            return None
    imdb = _coerce(it.get("imdb_rating"))
    tmdb = _coerce(it.get("tmdb_vote"))
    if imdb is not None and tmdb is not None:
        return max(imdb, tmdb)
    return imdb if imdb is not None else tmdb

def apply_personal_score(
    items: List[Dict[str,Any]],
    genre_weights: Dict[str,float],
) -> None:
    """
    Mutates items: adds:
      - 'score' (0–100)
      - 'match_score' (rounded score, for display)
      - 'why' string ("IMDb 8.7; TMDB 8.5; 2014") if data exists
      - 'audience' (best audience rating, 0..10)
    """
    for it in items:
        base10 = _best_rating_10(it)
        base100 = (base10 * 10.0) if base10 is not None else 60.0  # fallback mid if unrated

        g = _to_list(it.get("genres"))
        if genre_weights and g:
            fit = sum(genre_weights.get(x, 0.5) for x in g) / len(g)
            # center around 0.5 => [-0.5..+0.5], scale to ±15 points
            adj = (fit - 0.5) * 30.0
        else:
            adj = 0.0

        raw = max(0.0, min(100.0, base100 + adj))
        it["score"] = raw
        it["match_score"] = round(raw, 2)

        # audience field (best available)
        it["audience"] = base10 if base10 is not None else None

        # build "why"
        bits = []
        if it.get("imdb_rating") is not None:
            try:
                bits.append(f"IMDb {float(it['imdb_rating']):.1f}")
            except Exception:
                pass
        if it.get("tmdb_vote") is not None:
            try:
                bits.append(f"TMDB {float(it['tmdb_vote']):.1f}")
            except Exception:
                pass
        if it.get("year"):
            bits.append(str(it["year"]))
        it["why"] = "; ".join(bits) if bits else ""