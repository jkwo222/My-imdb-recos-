# engine/personalize.py
from __future__ import annotations
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import math

def _to_list(x) -> List[str]:
    if not x: return []
    if isinstance(x, (list, tuple)): return [str(i) for i in x if i]
    return [str(x)]

def _extra_tags_from_item(it: Dict[str,Any]) -> List[str]:
    """
    Derive more specific tags from available fields. You can add:
      - runtime buckets, year buckets, country, network, etc.
    """
    tags: List[str] = []
    # type tag
    t = it.get("type")
    if t: tags.append(f"type:{t}")
    # year bucket
    y = it.get("year")
    if isinstance(y, int):
        decade = (y//10)*10
        tags.append(f"decade:{decade}s")
        if y >= 2020: tags.append("era:2020s+")
        elif y >= 2010: tags.append("era:2010s")
        elif y >= 2000: tags.append("era:2000s")
        elif y >= 1990: tags.append("era:1990s")
        else: tags.append("era:classic")
    # providers (normalize to slugs)
    for p in _to_list(it.get("providers")):
        slug = p.lower().replace(" ", "_")
        tags.append(f"provider:{slug}")
    return tags

def genre_weights_from_profile(
    items: List[Dict[str,Any]],
    user_profile: Dict[str,Dict],
    imdb_id_field: str="tconst",
) -> Dict[str, float]:
    """
    Weight genres/tags by (my_rating - 6.0).
    If rating is missing but item is present in user's public list, treat as weak +0.5 delta (i.e., 6.5).
    """
    acc = defaultdict(float)
    cnt = defaultdict(int)

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
        tags = genres + _extra_tags_from_item(it)

        r = row.get("my_rating")
        if r is None:
            if row.get("from_public_list") or row.get("from_remote_user"):
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
    out = {g: (0.5 + 0.5*(v/mx)) for g,v in acc.items()}  # [-mx,+mx] -> [0,1]
    return {g: round(w, 4) for g,w in out.items()}

def apply_personal_score(
    items: List[Dict[str,Any]],
    genre_weights: Dict[str,float],
    base_key: str="imdb_rating",
    downvote_index: Dict[str,Any] | None = None,
) -> None:
    """
    Mutates items: adds 'score' (0–100). Combines IMDb base with personalization & downvote penalty.
    """
    for it in items:
        base = it.get(base_key)
        try:
            base10 = float(base) if base is not None else math.nan
        except Exception:
            base10 = math.nan
        base100 = (base10 * 10.0) if not math.isnan(base10) else 60.0  # default mid

        # compute fit across genres + derived tags
        tags = _to_list(it.get("genres")) + _extra_tags_from_item(it)
        if genre_weights and tags:
            fit = sum(genre_weights.get(x, 0.5) for x in tags) / len(tags)
            adj = (fit - 0.5) * 30.0
        else:
            adj = 0.0

        penalty = 0.0
        if downvote_index:
            tconst = str(it.get("tconst") or "")
            if tconst in downvote_index:
                penalty = -40.0  # hide it aggressively

        it["score"] = max(0.0, min(100.0, base100 + adj + penalty))