# engine/providers.py
from __future__ import annotations
import json, time
from typing import Dict, List, Any, Tuple
from urllib import request, parse, error
import os

_TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
_BASE = "https://api.themoviedb.org/3"

def _http_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = {k: v for k, v in params.items() if v is not None}
    q = parse.urlencode(params)
    url = f"{_BASE}{path}?{q}"
    req = request.Request(url, headers={"Accept": "application/json"})
    with request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _norm(name: str) -> str:
    n = (name or "").strip().lower()
    if "netflix" in n:
        return "netflix"
    if "prime" in n or "amazon" in n:
        return "prime_video"
    if "hulu" in n:
        return "hulu"
    if n in ("max", "hbo max") or " hbo " in f" {n} ":
        return "max"
    if "disney" in n:
        return "disney_plus"
    if "apple tv" in n:
        return "apple_tv_plus"
    if "peacock" in n:
        return "peacock"
    if "paramount" in n:
        return "paramount_plus"
    return n.replace(" ", "_")

def _providers_for(region_obj: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for bucket in ("flatrate", "ads", "free"):
        for p in region_obj.get(bucket, []) or []:
            n = _norm(p.get("provider_name") or "")
            if n and n not in out:
                out.append(n)
    return out

def annotate_availability(items: List[Dict[str, Any]], region: str = "US") -> List[Dict[str, Any]]:
    if not _TMDB_KEY:
        return items
    cache: Dict[Tuple[str, int], List[str]] = {}
    for it in items:
        typ = it.get("type")
        tid = it.get("tmdb_id")
        if not typ or not tid:
            continue
        key = (typ, tid)
        if key in cache:
            it["providers"] = cache[key]
            continue
        path = f"/{ 'tv' if typ=='tvSeries' else 'movie' }/{tid}/watch/providers"
        try:
            data = _http_get(path, {"api_key": _TMDB_KEY})
            region_obj = (data.get("results") or {}).get(region.upper()) or {}
            provs = _providers_for(region_obj)
            it["providers"] = provs
            cache[key] = provs
        except error.HTTPError as e:
            if e.code == 429:
                time.sleep(0.5)
                continue
            # Don't fail pipeline
        except Exception:
            pass
    return items