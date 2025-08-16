# File: engine/provider_filter.py
from __future__ import annotations
from typing import Dict, List, Tuple

_CANON = {
    "netflix": "netflix",
    "amazon prime video": "prime_video",
    "prime video": "prime_video",
    "amazon video": "prime_video",
    "hulu": "hulu",
    "max": "max",
    "hbo max": "max",
    "disney plus": "disney_plus",
    "disney+": "disney_plus",
    "apple tv plus": "apple_tv_plus",
    "apple tv+": "apple_tv_plus",
    "apple tv": "apple_tv_plus",
    "peacock": "peacock",
    "paramount plus": "paramount_plus",
    "paramount+": "paramount_plus",
}

def _slugify(name: str) -> str:
    return _CANON.get(name.strip().lower(), name.strip().lower().replace(" ", "_"))

def pick_region_data(providers_json: dict, region: str) -> dict:
    if not providers_json or "results" not in providers_json:
        return {}
    return providers_json["results"].get(region.upper(), {}) or {}

def title_has_allowed_provider(
    providers_json: dict,
    allowed_slugs: List[str],
    region: str
) -> Tuple[bool, List[str]]:
    rd = pick_region_data(providers_json, region)
    hits: List[str] = []
    for bucket in ("flatrate", "ads", "free"):
        for entry in rd.get(bucket, []) or []:
            slug = _slugify(entry.get("provider_name", ""))
            if slug in allowed_slugs:
                hits.append(slug)
    return (len(hits) > 0), sorted(set(hits))

def summarize_provider_hits(hit_list: List[List[str]]) -> Dict[str, int]:
    agg: Dict[str, int] = {}
    for row in hit_list:
        for slug in row:
            agg[slug] = agg.get(slug, 0) + 1
    return dict(sorted(agg.items(), key=lambda kv: (-kv[1], kv[0])))