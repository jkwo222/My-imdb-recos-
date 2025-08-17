# engine/scoring.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import math
import datetime

# Optional personalization (persona/taste)
try:
    from .personalize import apply_personalization  # type: ignore
except Exception:
    apply_personalization = None  # type: ignore


def _year_bonus(year: Optional[int]) -> float:
    """Light freshness bonus up to ~8 points."""
    if not isinstance(year, int) or year <= 0:
        return 0.0
    now = datetime.date.today().year
    age = max(0, now - year)
    if age <= 1:
        return 8.0
    if age <= 3:
        return 6.0
    if age <= 7:
        return 4.0
    if age <= 12:
        return 2.0
    return 0.0


def _provider_bonus(item: Dict[str, Any], subs_include: List[str]) -> float:
    """
    Small boost when the item is available on user's subs (max ~6).
    Item providers are slugified (e.g., 'apple_tv_plus'), SUBS_INCLUDE are env slugs.
    """
    if not subs_include:
        return 0.0
    provs = item.get("providers") or item.get("providers_slugs") or []
    if not isinstance(provs, list) or not provs:
        return 0.0
    sset = {p.strip().lower() for p in subs_include if p}
    isect = [p for p in provs if p in sset]
    if not isect:
        return 0.0
    # weight a tiny bit by how many providers match, capped
    return min(6.0, 3.0 + 1.5 * (len(isect) - 1))


def _base_audience(item: Dict[str, Any]) -> float:
    """
    Normalize a base popularity/audience signal to 0..100.
    Prefer 'audience' (already 0..100), else tmdb_vote (0..10 -> *10).
    """
    aud = item.get("audience")
    try:
        a = float(aud)
        if 0.0 <= a <= 100.0:
            return a
    except Exception:
        pass
    vote = item.get("tmdb_vote")
    try:
        v = float(vote)
        if 0.0 <= v <= 10.0:
            return v * 10.0
    except Exception:
        pass
    return 50.0


def _why_reasons(item: Dict[str, Any], parts: List[str]) -> str:
    title = item.get("title") or item.get("name") or ""
    return "; ".join(p for p in parts if p)


def score_items(env: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Produce a 'match' score (0..100) and a brief 'why' for each item.
    If available, applies an optional personalization pass from engine.personalize.
    """
    subs_include = env.get("SUBS_INCLUDE", []) or []

    ranked: List[Dict[str, Any]] = []
    for it in items:
        # base audience signal
        base = _base_audience(it)  # 0..100

        # components
        y = it.get("year")
        yb = _year_bonus(y)
        pb = _provider_bonus(it, subs_include)

        # simple composite
        match = 0.60 * base + yb + pb

        # clamp
        match = max(0.0, min(100.0, match))

        # why reasons
        reasons: List[str] = []
        if base >= 75: reasons.append("high audience rating")
        if yb >= 6:    reasons.append("very recent")
        elif yb >= 4:  reasons.append("recent")
        if pb >= 5.5:  reasons.append("on multiple of your services")
        elif pb >= 3.0: reasons.append("on your service")

        new_it = dict(it)
        new_it["match"] = float(f"{match:.3f}")
        new_it.setdefault("score", new_it["match"])  # keep runner happy if it looks for 'score'
        if reasons and not new_it.get("why"):
            new_it["why"] = _why_reasons(new_it, reasons)
        ranked.append(new_it)

    # Optional personalization hook (profile DNA)
    if callable(apply_personalization):
        try:
            ranked = apply_personalization(
                env, ranked, ratings_csv_path="data/user/ratings.csv", taste=None
            ) or ranked
        except Exception:
            # keep current ranked if personalization fails
            pass

    # Sort by match descending
    ranked.sort(key=lambda r: r.get("match", 0.0), reverse=True)
    return ranked