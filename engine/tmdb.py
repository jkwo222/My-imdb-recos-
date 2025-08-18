# engine/tmdb.py
from __future__ import annotations
import os, time, json, hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import requests

TMDB_KEY = os.getenv("TMDB_API_KEY")
TMDB_BEARER = (
    os.getenv("TMDB_BEARER")
    or os.getenv("TMDB_ACCESS_TOKEN")
    or os.getenv("TMDB_V4_TOKEN")
)
_API_BASE = "https://api.themoviedb.org/3"
_TIMEOUT = 25

# Simple on-disk cache (helps avoid 429s, makes email not empty)
CACHE_DIR = Path("data/cache/tmdb")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
TTL_SECONDS = int(os.getenv("TMDB_CACHE_TTL_SECONDS", "259200"))  # 3 days

def _cache_path(kind: str, key: str) -> Path:
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    p = CACHE_DIR / kind
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{h}.json"

def _cache_read(kind: str, key: str) -> Optional[dict]:
    p = _cache_path(kind, key)
    if not p.exists(): return None
    try:
        if (time.time() - p.stat().st_mtime) > TTL_SECONDS:
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _cache_write(kind: str, key: str, data: dict) -> None:
    try:
        _cache_path(kind, key).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _tmdb_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{_API_BASE}/{path.lstrip('/')}"
    headers = {"Accept": "application/json"}
    if TMDB_BEARER:
        headers["Authorization"] = f"Bearer {TMDB_BEARER}"
    q = dict(params or {})
    if TMDB_KEY and "api_key" not in q and not TMDB_BEARER:
        q["api_key"] = TMDB_KEY

    # tiny retry/backoff for 429/5xx
    for attempt in range(4):
        try:
            r = requests.get(url, headers=headers, params=q, timeout=_TIMEOUT)
            if r.status_code == 429:
                time.sleep(0.5 + attempt * 0.5)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == 3:
                raise
            time.sleep(0.5 + attempt * 0.5)
    return {}

def _slugify_provider_name(name: str) -> str:
    n = (name or "").strip().lower()
    if not n: return ""
    if "apple tv+" in n or n == "apple tv plus": return "apple_tv_plus"
    if "netflix" in n: return "netflix"
    if n in {"hbo","hbo max","hbomax","max"}: return "max"
    if "paramount+" in n: return "paramount_plus"
    if "disney+" in n: return "disney_plus"
    if "peacock" in n: return "peacock"
    if "hulu" in n: return "hulu"
    if "prime video" in n or "amazon" in n: return "prime_video"
    if "starz" in n: return "starz"
    if "showtime" in n: return "showtime"
    if "amc+" in n: return "amc_plus"
    if "criterion" in n: return "criterion_channel"
    if "mubi" in n: return "mubi"
    return n.replace(" ", "_")

def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    if s in {"hbo","hbo_max","hbomax"}: return "max"
    return s

def _fetch_provider_directory(region: str) -> Dict[str, int]:
    region = (region or "US").upper()
    out: Dict[str, int] = {}
    for kind in ("watch/providers/movie", "watch/providers/tv"):
        data = _tmdb_get(kind, {"watch_region": region})
        for rec in (data or {}).get("results", []) or []:
            slug = _slugify_provider_name(rec.get("provider_name", ""))
            pid = rec.get("provider_id")
            if slug and isinstance(pid, int):
                out.setdefault(slug, pid)
    return out

def providers_from_env(subs: List[str], region: str) -> Tuple[List[int], Dict[str, Optional[int]]]:
    subs = [_normalize_slug(s) for s in (subs or [])]
    directory = _fetch_provider_directory(region)
    used_map: Dict[str, Optional[int]] = {}
    ids: List[int] = []
    for s in subs:
        pid = directory.get(s)
        used_map[s] = pid if isinstance(pid, int) else None
        if isinstance(pid, int):
            ids.append(pid)
    ids = sorted({i for i in ids if isinstance(i, int)})
    return ids, used_map

# ---- Common detail fetchers with cache ----
def get_details(kind: str, tmdb_id: int) -> Dict[str, Any]:
    kind = (kind or "").lower()
    if kind not in ("movie","tv") or not tmdb_id: return {}
    key = f"details:{kind}:{int(tmdb_id)}"
    cached = _cache_read("details", key)
    if cached is not None: return cached
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}")
    out: Dict[str, Any] = {
        "original_language": (data.get("original_language") or "").lower(),
        "production_countries": [c.get("iso_3166_1") for c in (data.get("production_countries") or []) if isinstance(c, dict) and c.get("iso_3166_1")],
        "production_companies": [c.get("name") for c in (data.get("production_companies") or []) if isinstance(c, dict) and c.get("name")],
    }
    if kind == "movie":
        out["runtime"] = data.get("runtime")
        out["release_date"] = data.get("release_date")
        out["belongs_to_collection"] = (data.get("belongs_to_collection") or {}).get("name")
    else:
        out["episode_run_time"] = data.get("episode_run_time") or []
        out["networks"] = [n.get("name") for n in (data.get("networks") or []) if isinstance(n, dict) and n.get("name")]
        out["number_of_seasons"] = data.get("number_of_seasons")
        out["number_of_episodes"] = data.get("number_of_episodes")
        out["first_air_date"] = data.get("first_air_date")
        out["last_air_date"] = data.get("last_air_date")
    _cache_write("details", key, out)
    return out

def get_credits(kind: str, tmdb_id: int) -> Dict[str, Any]:
    kind = (kind or "").lower()
    if kind not in ("movie","tv") or not tmdb_id: return {}
    key = f"credits:{kind}:{int(tmdb_id)}"
    cached = _cache_read("credits", key)
    if cached is not None: return cached
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/credits")
    directors = []
    writers = []
    cast = []
    for c in (data.get("crew") or []):
        dep = (c.get("department") or "").lower()
        job = (c.get("job") or "").lower()
        nm = c.get("name")
        if not nm: continue
        if "direct" in job or dep == "directing": directors.append(nm)
        if "writer" in job or dep == "writing": writers.append(nm)
    for a in (data.get("cast") or [])[:12]:
        nm = a.get("name")
        if nm: cast.append(nm)
    out = {"directors": list(dict.fromkeys(directors)),
           "writers": list(dict.fromkeys(writers)),
           "cast": list(dict.fromkeys(cast))}
    _cache_write("credits", key, out)
    return out

def get_external_ids(kind: str, tmdb_id: int) -> Dict[str, Any]:
    kind = (kind or "").lower()
    if kind not in ("movie","tv") or not tmdb_id: return {}
    key = f"external_ids:{kind}:{int(tmdb_id)}"
    cached = _cache_read("xids", key)
    if cached is not None: return cached
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/external_ids")
    out = {"imdb_id": data.get("imdb_id")}
    _cache_write("xids", key, out)
    return out

def get_keywords(kind: str, tmdb_id: int) -> List[str]:
    kind = (kind or "").lower()
    if kind not in ("movie","tv") or not tmdb_id: return []
    key = f"keywords:{kind}:{int(tmdb_id)}"
    cached = _cache_read("keywords", key)
    if cached is not None: return cached.get("keywords", [])
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/keywords")
    raw = data.get("keywords") if kind == "movie" else data.get("results")
    kws = []
    for kw in raw or []:
        name = kw.get("name")
        if isinstance(name, str) and name.strip(): kws.append(name.strip().lower())
    out = {"keywords": list(dict.fromkeys(kws))}
    _cache_write("keywords", key, out)
    return out["keywords"]

def get_title_watch_providers(kind: str, tmdb_id: int, region: str = "US") -> List[str]:
    kind = (kind or "").lower()
    if kind not in ("movie","tv") or not tmdb_id: return []
    region = (region or "US").upper()
    key = f"providers:{kind}:{int(tmdb_id)}:{region}"
    cached = _cache_read("providers", key)
    if cached is not None:
        return cached.get("providers", [])
    # tiny retry/backoff built into _tmdb_get
    data = _tmdb_get(f"{kind}/{int(tmdb_id)}/watch/providers")
    by_region = (data or {}).get("results", {}).get(region) or {}
    slugs = set()
    for bucket in ("flatrate","ads","free"):
        for offer in by_region.get(bucket, []) or []:
            slug = _slugify_provider_name(offer.get("provider_name", ""))
            if slug: slugs.add(slug)
    out = sorted(slugs)
    _cache_write("providers", key, {"providers": out})
    return out