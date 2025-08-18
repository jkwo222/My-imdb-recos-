# --- TMDB auth helper ---
import os, requests

TMDB_KEY = os.getenv("TMDB_API_KEY")
TMDB_BEARER = os.getenv("TMDB_BEARER")

def _tmdb_get(url, params=None):
    headers = {}
    if TMDB_BEARER:
        headers["Authorization"] = f"Bearer {TMDB_BEARER}"
    params = dict(params or {})
    if TMDB_KEY and "api_key" not in params and not TMDB_BEARER:
        params["api_key"] = TMDB_KEY
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# --- provider name normalization (→ slugs used everywhere else) ---
def _name_to_slug(name: str) -> str:
    n = (name or "").strip().lower()
    if not n:
        return ""
    if "apple tv+" in n or n == "apple tv plus": return "apple_tv_plus"
    if "netflix" in n: return "netflix"
    if n in {"max","hbo max","hbo"}: return "max"
    if "paramount+" in n: return "paramount_plus"
    if "disney+" in n: return "disney_plus"
    if "peacock" in n: return "peacock"
    if "hulu" in n: return "hulu"
    # Others (may appear but will be filtered downstream by summarize.py)
    if "prime video" in n or "amazon" in n: return "prime_video"
    if "starz" in n: return "starz"
    if "showtime" in n: return "showtime"
    if "amc+" in n: return "amc_plus"
    if "criterion" in n: return "criterion_channel"
    if "mubi" in n: return "mubi"
    return n.replace(" ", "_")

# --- subscription-only provider fetch ---
def get_title_watch_providers(kind: str, tmdb_id: int, region: str = "US"):
    """
    Returns a list of provider slugs available via subscription (flatrate/ads) in the region.
    Excludes rent/buy and premium add-ons (channel upsells).
    """
    kind = (kind or "").lower()
    if kind not in ("movie","tv") or not tmdb_id:
        return []
    try:
        data = _tmdb_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/watch/providers")
    except Exception:
        return []
    by_region = (data or {}).get("results", {}).get(region.upper()) or {}
    slugs = set()
    # Only include true subscription buckets
    for bucket in ("flatrate", "ads"):
        for offer in by_region.get(bucket, []) or []:
            slug = _name_to_slug(offer.get("provider_name",""))
            if slug:
                slugs.add(slug)
    # Optional: also include 'free' (ad-supported free) if you want it considered subscription-like:
    # for offer in by_region.get("free", []) or []:
    #     slug = _name_to_slug(offer.get("provider_name",""))
    #     if slug: slugs.add(slug)
    return sorted(slugs)