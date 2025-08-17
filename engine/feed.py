# engine/feed.py
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import List, Dict, Any, Tuple


def _title(it: Dict[str, Any]) -> str:
    return it.get("title") or it.get("name") or ""


def _year(it: Dict[str, Any]) -> int | None:
    return it.get("release_year") or it.get("first_air_year")


def _poster(it: Dict[str, Any]) -> str | None:
    # keep it generic; your catalog builder likely sets poster_path
    p = it.get("poster_path")
    if not p:
        return None
    # TMDB image base is usually added on the consumer side; keep path only
    return p


def _as_feed_item(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": it.get("id"),
        "media_type": it.get("media_type") or ("tv" if it.get("first_air_date") else "movie"),
        "title": _title(it),
        "year": _year(it),
        "overview": it.get("overview"),
        "genres": [g.get("name") for g in (it.get("genres") or []) if isinstance(g, dict) and g.get("name")],
        "vote_average": it.get("vote_average"),
        "vote_count": it.get("vote_count"),
        "poster_path": _poster(it),
        "providers": it.get("watch_providers"),  # if catalog injected this
        "_score": it.get("_score"),
    }


def build_feed(
    ranked: List[Dict[str, Any]],
    cfg,
    catalog_meta: Dict[str, Any],
    rank_meta: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Pick top N ranked as the daily feed and write artifacts to:
      - {out_dir}/latest/assistant_feed.json
      - {out_dir}/daily/YYYY-MM-DD/assistant_feed.json
    """
    n = max(1, int(cfg.show_n))
    top = [_as_feed_item(x) for x in ranked[:n]]

    today = datetime.utcnow().date().isoformat()
    daily_dir = os.path.join(cfg.out_dir, "daily", today)
    os.makedirs(daily_dir, exist_ok=True)

    feed = {
        "date": today,
        "count": len(top),
        "items": top,
        "weights": {"critic": cfg.weight_critic, "audience": cfg.weight_audience},
        "meta": {
            "catalog": catalog_meta or {},
            "ranking": rank_meta or {},
        },
    }

    latest_path = os.path.join(cfg.latest_dir, "assistant_feed.json")
    daily_path = os.path.join(daily_dir, "assistant_feed.json")

    for path in [latest_path, daily_path]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(feed, f, indent=2, ensure_ascii=False)

    return top, {"latest_path": latest_path, "daily_path": daily_path}