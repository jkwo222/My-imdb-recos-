# engine/catalog.py
from __future__ import annotations
import json, math, os, random, time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Tuple
from urllib import request, parse, error
import datetime

from engine import seen_index as seen

_TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
_BASE = "https://api.themoviedb.org/3"

# -------- HTTP helpers --------
def _http_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = {k: v for k, v in params.items() if v is not None}
    q = parse.urlencode(params)
    url = f"{_BASE}{path}?{q}"
    req = request.Request(url, headers={"Accept": "application/json"})
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _sleep_backoff(i: int) -> None:
    time.sleep(min(0.35 + i * 0.05, 1.0))

# -------- Page plan (rotates daily) --------
def _daily_seed(salt: str = "") -> int:
    today = datetime.date.today()
    doy = int(today.strftime("%j"))
    rnd = random.Random(f"{today.isoformat()}::{salt}")
    return rnd.randint(1, 10_000_000) ^ (doy << 11)

def _daily_page_plan(count: int, cap: int = 500, salt: str = "") -> List[int]:
    """
    Deterministic (by date + salt) shuffle of 1..cap, then take first 'count'.
    Ensures pages are different day-to-day and stable within a day.
    """
    rng = random.Random(_daily_seed(salt))
    pages = list(range(1, cap + 1))
    rng.shuffle(pages)
    return pages[:max(0, min(count, cap))]

# -------- Normalize results --------
def _norm_movie(m: Dict[str, Any]) -> Dict[str, Any]:
    title = m.get("title") or m.get("original_title") or ""
    y = (m.get("release_date") or "")[:4]
    return {
        "type": "movie",
        "tmdb_id": int(m.get("id")),
        "title": title,
        "year": int(y) if y.isdigit() else None,
        "original_language": m.get("original_language"),
        "tmdb_vote_average": m.get("vote_average"),
        "tmdb_votes": m.get("vote_count"),
        "region": None,  # set by runner notes
    }

def _norm_tv(tv: Dict[str, Any]) -> Dict[str, Any]:
    title = tv.get("name") or tv.get("original_name") or ""
    y = (tv.get("first_air_date") or "")[:4]
    return {
        "type": "tvSeries",
        "tmdb_id": int(tv.get("id")),
        "title": title,
        "year": int(y) if y.isdigit() else None,
        "original_language": tv.get("original_language"),
        "tmdb_vote_average": tv.get("vote_average"),
        "tmdb_votes": tv.get("vote_count"),
        "region": None,
    }

# -------- Base fetch --------
def fetch_tmdb_base(
    pages_movie: int = 60,
    pages_tv: int = 60,
    region: str = "US",
    langs: List[str] | None = None,
    include_tv_seasons: bool = True,
    max_items: int = 6000,
) -> List[Dict[str, Any]]:
    """Collect a large mixed pool from TMDB discover with a *rotating* page plan."""
    if not _TMDB_KEY:
        return []

    langs = langs or ["en"]
    # Use vote_count sorting to avoid ultra-obscure noise; mix with popularity
    movie_sort = os.environ.get("TMDB_MOVIE_SORT", "vote_count.desc")
    tv_sort = os.environ.get("TMDB_TV_SORT", "vote_count.desc")

    # Cap pages at API max (usually 500)
    movie_pages = _daily_page_plan(pages_movie, cap=500, salt=f"movie::{region}::{','.join(langs)}::{movie_sort}")
    tv_pages    = _daily_page_plan(pages_tv,    cap=500, salt=f"tv::{region}::{','.join(langs)}::{tv_sort}")

    out: List[Dict[str, Any]] = []

    # Movies
    for i, p in enumerate(movie_pages, 1):
        try:
            data = _http_get("/discover/movie", {
                "api_key": _TMDB_KEY,
                "page": p,
                "include_adult": "false",
                "include_video": "false",
                "sort_by": movie_sort,
                "watch_region": region,
                # intentionally not pinning original language here; we filter later
            })
            for m in (data.get("results") or []):
                out.append(_norm_movie(m))
            if len(out) >= max_items:
                return out[:max_items]
        except error.HTTPError as e:
            if e.code == 429:
                _sleep_backoff(i)
                continue
        except Exception:
            continue

    # TV
    for i, p in enumerate(tv_pages, 1):
        try:
            data = _http_get("/discover/tv", {
                "api_key": _TMDB_KEY,
                "page": p,
                "sort_by": tv_sort,
                "include_null_first_air_dates": "false",
                "watch_region": region,
            })
            for tv in (data.get("results") or []):
                out.append(_norm_tv(tv))
            if len(out) >= max_items:
                return out[:max_items]
        except error.HTTPError as e:
            if e.code == 429:
                _sleep_backoff(i)
                continue
        except Exception:
            continue

    return out[:max_items]

# -------- Enrichment --------
def enrich_with_ids(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Hydrate imdb_id via external_ids (bounded to avoid rate blowups)."""
    if not _TMDB_KEY:
        return items
    limit = int(os.environ.get("MAX_ID_HYDRATION", "1200") or "1200")
    count = 0
    for it in items:
        if count >= limit:
            break
        if it.get("imdb_id"):
            continue
        typ = it.get("type")
        tid = it.get("tmdb_id")
        if not typ or not tid:
            continue
        path = f"/{ 'tv' if typ=='tvSeries' else 'movie' }/{tid}/external_ids"
        try:
            data = _http_get(path, {"api_key": _TMDB_KEY})
            imdb_id = data.get("imdb_id")
            if imdb_id:
                it["imdb_id"] = imdb_id
            count += 1
        except error.HTTPError as e:
            if e.code == 429:
                _sleep_backoff(count)
                continue
        except Exception:
            continue
    return items

def enrich_with_votes(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for it in items:
        it["tmdb_vote"] = it.get("tmdb_vote_average")
    return items

def enrich_with_ratings(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Placeholder for OMDb/RT; keep as pass-through to avoid heavy external calls.
    return items

# -------- Filters --------
def filter_seen(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        imdb_id = it.get("imdb_id")
        if imdb_id and seen.is_seen_imdb(imdb_id):
            continue
        out.append(it)
    return out

def filter_by_langs(items: List[Dict[str, Any]], langs: List[str]) -> List[Dict[str, Any]]:
    if not langs:
        return items
    allow = set(x.strip().lower() for x in langs)
    return [it for it in items if (it.get("original_language") or "").lower() in allow]

def filter_by_providers(items: List[Dict[str, Any]], allowed: List[str]) -> List[Dict[str, Any]]:
    if not allowed:
        return items
    allow = set(a.strip().lower() for a in allowed)
    def ok(it: Dict[str, Any]) -> bool:
        provs = [p.lower() for p in (it.get("providers") or [])]
        return bool(allow.intersection(provs))
    return [it for it in items if ok(it)]

# -------- Scoring --------
def _norm01(x: float, lo: float, hi: float) -> float:
    try:
        return max(0.0, min(1.0, (x - lo) / (hi - lo)))
    except Exception:
        return 0.0

def score_and_rank(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    cw = float(os.environ.get("CRITIC_WEIGHT", "0.56"))
    aw = float(os.environ.get("AUDIENCE_WEIGHT", "0.44"))
    novelty = float(os.environ.get("NOVELTY_PRESSURE", "0.15"))
    # Simple heuristic: vote avg (5..9+) and log votes
    for it in items:
        vavg = float(it.get("tmdb_vote_average") or 0.0)
        vcnt = float(it.get("tmdb_votes") or 0.0)
        critic = _norm01(vavg, 5.0, 9.0)
        audience = _norm01(math.log10(vcnt + 1.0), 0.0, 5.0)  # ~0..1 over a wide range
        base = cw * critic + aw * audience
        # Light novelty bump for recent/lower-vote titles
        novelty_bump = novelty * (1.0 - audience) * 0.5
        match = 100.0 * (base + novelty_bump)
        it["match"] = round(match, 1)
    ranked = sorted(items, key=lambda r: (r.get("match") or 0.0, r.get("tmdb_vote_average") or 0.0), reverse=True)
    return ranked, {"critic_weight": cw, "audience_weight": aw, "commitment_cost_scale": 1.0, "novelty_pressure": novelty}