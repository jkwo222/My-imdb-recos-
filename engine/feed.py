# engine/feed.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ----------------------------
# Public API
# ----------------------------

def write_feed(cfg, ranked: List[dict], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the final feed JSONs:
      - data/out/daily/YYYY-MM-DD/assistant_feed.json
      - data/out/latest/assistant_feed.json

    Inputs
    ------
    cfg:
      - shortlist_size (int) default 50
      - shown_size (int) default 10
      - out_dir (str) default "data/out"
    ranked: list as returned by rank.rank_candidates(...)
    meta: optional metadata dict

    Returns
    -------
    payload (dict) that was written to JSON
    """
    shortlist_size = int(getattr(cfg, "shortlist_size", 50))
    shown_size = int(getattr(cfg, "shown_size", 10))
    out_root = getattr(cfg, "out_dir", "data/out")

    shortlist = ranked[:shortlist_size]
    shown = ranked[:shown_size]

    # Structure a friendly payload for UI/consumption
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload = {
        "generated_at": now,
        "counts": {
            "ranked": len(ranked),
            "shortlist": len(shortlist),
            "shown": len(shown),
        },
        "weights": _extract_weights_from_items(ranked),
        "items": [
            _present_item(it, rank=i + 1)
            for i, it in enumerate(shortlist)
        ],
        "meta": meta or {},
    }

    # Paths
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_dir = os.path.join(out_root, "daily", date_str)
    latest_dir = os.path.join(out_root, "latest")

    os.makedirs(daily_dir, exist_ok=True)
    os.makedirs(latest_dir, exist_ok=True)

    # Write files
    daily_path = os.path.join(daily_dir, "assistant_feed.json")
    latest_path = os.path.join(latest_dir, "assistant_feed.json")

    _write_json(payload, daily_path)
    _write_json(payload, latest_path)

    # Convenience returns
    return {
        "payload": payload,
        "paths": {"daily": daily_path, "latest": latest_path},
    }


# ----------------------------
# Helpers
# ----------------------------

def _write_json(obj: dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _extract_weights_from_items(items: List[dict]) -> Dict[str, float]:
    # Pull any one item's feature keys as hints; not strictly necessary
    if not items:
        return {}
    f = items[0].get("features") or {}
    # We only expose keys; the actual weights live in rank.py and cfg
    return {k: float(f.get(k, 0.0)) for k in sorted(f.keys())}


def _present_item(it: dict, rank: int) -> dict:
    """Flatten an item into a UI-friendly record with reasons."""
    title = it.get("title") or it.get("name") or ""
    year = _year_of(it)
    display_title = f"{title} ({year})" if year else title

    providers = _providers_flat(it)
    genres = _genres_flat(it)
    media_type = it.get("media_type") or ("movie" if "release_date" in it else "tv")

    return {
        "rank": rank,
        "media_type": media_type,
        "tmdb_id": it.get("id"),
        "title": display_title,
        "score": round(float(it.get("score", 0.0)), 2),
        "features": it.get("features", {}),
        "reasons": it.get("reasons", []),
        "genres": genres,
        "providers": providers,
        "popularity": it.get("popularity"),
        "vote_average": it.get("vote_average"),
        "vote_count": it.get("vote_count"),
        "original_language": it.get("original_language"),
        "release_date": it.get("release_date") or it.get("first_air_date"),
        "poster_path": it.get("poster_path"),
        "backdrop_path": it.get("backdrop_path"),
        "tmdb_url": _tmdb_url(media_type, it.get("id")),
    }


def _tmdb_url(kind: str, idv: Any) -> Optional[str]:
    if not idv:
        return None
    base = "https://www.themoviedb.org"
    if kind == "movie":
        return f"{base}/movie/{idv}"
    if kind == "tv":
        return f"{base}/tv/{idv}"
    return None


def _year_of(it: dict) -> Optional[int]:
    date = it.get("release_date") or it.get("first_air_date") or ""
    if not date:
        return None
    try:
        y = int(str(date)[:4])
        return y
    except Exception:
        return None


def _providers_flat(it: dict) -> List[str]:
    names = []
    for key in ("providers", "watch/providers", "watch_providers", "providers_flatrate", "providers_ads", "providers_free"):
        v = it.get(key)
        if isinstance(v, dict):
            for bucket in ("flatrate", "ads", "free"):
                arr = v.get(bucket)
                if isinstance(arr, list):
                    for p in arr:
                        name = (p.get("provider_name") or p.get("name") or "").strip()
                        if name:
                            names.append(name)
        elif isinstance(v, list):
            for p in v:
                name = (p.get("provider_name") or p.get("name") or "").strip()
                if name:
                    names.append(name)
    # dedupe, keep order
    seen = set()
    out = []
    for n in names:
        if n and n not in seen:
            out.append(n)
            seen.add(n)
    return out


def _genres_flat(it: dict) -> List[str]:
    names = []
    if isinstance(it.get("genres"), list) and it["genres"] and isinstance(it["genres"][0], dict):
        names = [str(g.get("name", "")).strip() for g in it["genres"] if g]
    elif isinstance(it.get("genre_ids"), list):
        # We don’t have the mapping here; leave numeric IDs as strings
        names = [str(x) for x in it["genre_ids"]]
    # dedupe, keep order
    seen = set()
    out = []
    for n in names:
        if n and n not in seen:
            out.append(n)
            seen.add(n)
    return out