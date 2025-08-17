# engine/personalize.py
from __future__ import annotations
from typing import Dict, List, Any, Set
import math

def apply_personal_score(
    items: List[Dict[str, Any]],
    genre_weights: Dict[str, float] | None,
    base_key: str = "imdb_rating",
    title_penalties: Dict[str, float] | None = None,
    genre_penalties: Dict[str, float] | None = None,
    hidden_tconsts: Set[str] | None = None,
) -> None:
    """
    Mutates each item in-place to include a 'score' (0-100).
    Also sets 'hidden_reason' when hidden by downvote memory, and
    attaches a 'penalties' dict when any penalty was applied.

    Expected item fields (best-effort):
      - tconst: str (IMDb id)
      - imdb_rating: float (0-10) or None
      - genres: List[str]
    """
    title_penalties = title_penalties or {}
    genre_penalties = { (k or "").lower(): float(v) for k, v in (genre_penalties or {}).items() }
    hidden_tconsts = set([(x or "").lower() for x in (hidden_tconsts or set())])
    gw = genre_weights or {}

    for it in items:
        tconst = (it.get("tconst") or "").lower()

        # Hard-hide based on downvote memory
        if tconst in hidden_tconsts:
            it["score"] = -1.0
            it["hidden_reason"] = "downvoted"
            continue

        # Base from IMDb rating (fallback to a middling baseline)
        base = it.get(base_key)
        try:
            base10 = float(base) if base is not None else math.nan
        except Exception:
            base10 = math.nan
        base100 = (base10 * 10.0) if not math.isnan(base10) else 60.0

        # Personalization bump from genre fit
        g = it.get("genres") or []
        if g and gw:
            # Centered around 0.5 (neutral). Spread of ~±15 (total swing ~30).
            fit = sum(gw.get(x, 0.5) for x in g) / len(g)
            adj = (fit - 0.5) * 30.0
        else:
            adj = 0.0

        # Penalties
        p_title = float(title_penalties.get(tconst, 0.0))
        p_genre = 0.0
        for gg in g:
            p_genre += float(genre_penalties.get((gg or "").lower(), 0.0))
        if g:
            p_genre /= len(g)

        score = max(0.0, min(100.0, base100 + adj - p_title - p_genre))
        it["score"] = score

        if p_title or p_genre:
            it["penalties"] = {"title": round(p_title, 2), "genre": round(p_genre, 2)}