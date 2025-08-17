# engine/provider_filter.py
from __future__ import annotations
from typing import Dict, List, Tuple

# Canonical slugs we use across the project
_CANON = {
    "netflix": "netflix",
    "amazon prime video": "prime_video",
    "prime video": "prime_video",
    "prime video channel": "prime_video",
    "prime video channels": "prime_video",
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

# Allowed baseline services even if user whitelist is empty
_ALLOWED_BASE = set(_CANON.values())

def _norm_name(name: str) -> str:
    return (name or "").strip().lower().replace("_", " ").replace("+", " plus")

def _slugify(name: str) -> str:
    n = _norm_name(name)
    return _CANON.get(n, n.replace(" ", "_"))

def normalize_user_whitelist(user_whitelist: List[str]) -> set[str]:
    """Return canonical slugs for a user-supplied whitelist."""
    out: set[str] = set()
    for x in user_whitelist or []:
        out.add(_slugify(x))
    return out

def is_allowed_provider(name: str, user_whitelist: List[str]) -> bool:
    """True if a single provider is allowed by whitelist or baseline."""
    slug = _slugify(name)
    wl = normalize_user_whitelist(user_whitelist)
    return (slug in wl) or (slug in _ALLOWED_BASE)

def any_allowed(providers: List[str] | None, user_whitelist: List[str]) -> bool:
    """True if ANY provider on the title matches the whitelist/baseline."""
    if not providers:
        return False
    wl = normalize_user_whitelist(user_whitelist)
    for p in providers:
        slug = _slugify(p)
        if (slug in wl) or (slug in _ALLOWED_BASE):
            return True
    return False

# --- TMDB providers JSON helpers (when using TMDB watch/providers blobs) ---

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
    wl = set(allowed_slugs)
    for bucket in ("flatrate", "ads", "free"):
        for entry in rd.get(bucket, []) or []:
            slug = _slugify(entry.get("provider_name", ""))
            if (slug in wl) or (slug in _ALLOWED_BASE):
                hits.append(slug)
    return (len(hits) > 0), sorted(set(hits))

def summarize_provider_hits(hit_list: List[List[str]]) -> Dict[str, int]:
    agg: Dict[str, int] = {}
    for row in hit_list:
        for slug in row:
            agg[slug] = agg.get(slug, 0) + 1
    return dict(sorted(agg.items(), key=lambda kv: (-kv[1], kv[0])))