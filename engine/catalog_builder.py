# engine/catalog_builder.py
from __future__ import annotations
import json, os, random, gzip, io, time
from pathlib import Path
from typing import Any, Dict, List, Tuple
import requests

POOL_DIR = Path("data/cache/pool")
POOL_DIR.mkdir(parents=True, exist_ok=True)
POOL_PATH = POOL_DIR / "catalog.ndjson"
STATE_PATH = Path("data/cache/paging_state.json")

TMDB_API = "https://api.themoviedb.org/3"

def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return default

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, "") or default)
    except Exception: return default

def _env_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name, None)
    if not raw: return default
    if raw.strip().startswith("["):
        try:
            import json as _j
            return [str(x).strip().lower() for x in _j.loads(raw)]
        except Exception:
            pass
    return [s.strip().lower() for s in raw.split(",") if s.strip()]

def _tmdb_headers() -> Dict[str, str]:
    bearer = os.getenv("TMDB_BEARER") or os.getenv("TMDB_ACCESS_TOKEN") or os.getenv("TMDB_V4_TOKEN")
    h = {"Accept": "application/json"}
    if bearer:
        h["Authorization"] = f"Bearer {bearer}"
    return h

def _tmdb_params() -> Dict[str, str]:
    # Prefer v3 api_key (query param) if provided
    p: Dict[str, str] = {}
    if os.getenv("TMDB_API_KEY"):
        p["api_key"] = os.getenv("TMDB_API_KEY")  # masked by Actions
    return p

def _tmdb_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # Resilient GET with both api_key and bearer support
    q = {**_tmdb_params(), **params}
    r = requests.get(f"{TMDB_API}{path}", params=q, headers=_tmdb_headers(), timeout=20)
    r.raise_for_status()
    return r.json()

def _normalize_basic(r: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    # Minimal shape; runner later enriches credits/providers/keywords.
    if media_type == "movie":
        title = r.get("title") or r.get("original_title")
        rd = r.get("release_date")
        yr = (rd[:4] if isinstance(rd, str) and len(rd) >= 4 else (r.get("year") or None))
        return {
            "media_type": "movie",
            "tmdb_id": r.get("id"),
            "imdb_id": r.get("imdb_id"),
            "title": title,
            "year": yr,
            "release_date": rd,
            "popularity": r.get("popularity"),
            "tmdb_vote": r.get("vote_average"),
            "original_language": r.get("original_language"),
        }
    else:
        title = r.get("name") or r.get("original_name")
        fad = r.get("first_air_date")
        lad = r.get("last_air_date")
        yr = (fad[:4] if isinstance(fad, str) and len(fad) >= 4 else (r.get("year") or None))
        return {
            "media_type": "tv",
            "tmdb_id": r.get("id"),
            "imdb_id": r.get("imdb_id"),
            "name": title,
            "title": title,
            "year": yr,
            "first_air_date": fad,
            "last_air_date": lad,
            "number_of_seasons": r.get("number_of_seasons"),
            "popularity": r.get("popularity"),
            "tmdb_vote": r.get("vote_average"),
            "original_language": r.get("original_language"),
        }

def _iter_tmdb_pages(kind: str, pages: List[int], region: str, langs: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    # Use discover endpoints so we can filter by language/region.
    path = "/discover/movie" if kind == "movie" else "/discover/tv"
    base = {
        "sort_by": "popularity.desc",
        "region": region,
        "watch_region": region,
        "include_adult": "false",
    }
    # Constrain original language(s) if provided
    if langs:
        # discover supports 'with_original_language' single value; if multiple, do multiple calls
        olangs = langs[:3]  # keep sane
    else:
        olangs = [None]

    for page in pages:
        for ol in olangs:
            params = dict(base)
            params["page"] = page
            if ol:
                params["with_original_language"] = ol
            try:
                data = _tmdb_get(path, params)
                for r in (data.get("results") or []):
                    r["id"] = r.get("id")
                    items.append(_normalize_basic(r, kind))
            except Exception:
                # best-effort; skip on transient errors
                continue
        # brief pause to be friendly
        time.sleep(0.15)
    return items

def _paging_plan(n_pages: int, mode: str, page_max: int) -> List[int]:
    n_pages = max(1, min(50, n_pages))
    page_max = max(n_pages, min(500, page_max or (n_pages * 10)))
    mode = (mode or "first").lower()
    if mode == "first":
        return list(range(1, n_pages + 1))
    if mode == "random":
        start = random.randint(1, max(1, page_max - n_pages + 1))
        return list(range(start, start + n_pages))
    # rolling
    start = 1
    try:
        if STATE_PATH.exists():
            st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            start = int(st.get("start_page") or 1)
        else:
            start = 1
    except Exception:
        start = 1
    pages = list(range(start, start + n_pages))
    # write next start (wrap)
    nxt = start + n_pages
    if nxt > page_max:
        nxt = 1
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps({"start_page": nxt}, indent=2), encoding="utf-8")
    except Exception:
        pass
    return pages

def _load_pool_keys() -> Tuple[int, set]:
    keys = set()
    before = 0
    if POOL_PATH.exists():
        with POOL_PATH.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                before += 1
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                k = (obj.get("media_type"), obj.get("tmdb_id"))
                if k[0] and k[1] is not None:
                    keys.add((k[0], int(k[1])))
    return before, keys

def _append_to_pool(objs: List[Dict[str, Any]], existing_keys: set) -> Tuple[int, int]:
    appended = 0
    unique_now = set(existing_keys)
    with POOL_PATH.open("a", encoding="utf-8") as fh:
        for it in objs:
            k = (it.get("media_type"), it.get("tmdb_id"))
            if not k[0] or k[1] is None:
                continue
            key = (k[0], int(k[1]))
            if key in unique_now:
                continue
            fh.write(json.dumps(it, ensure_ascii=False) + "\n")
            unique_now.add(key)
            appended += 1
    return appended, len(unique_now)

def _read_full_pool() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not POOL_PATH.exists():
        return out
    with POOL_PATH.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out

# ---------- IMDb TSV ingest (hydrated to TMDB) ----------
IMDB_HOST = "https://datasets.imdbws.com"
IMDB_FILES = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
}

def _download_imdb(cache_dir: Path, key: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = IMDB_FILES[key]
    dest = cache_dir / name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    url = f"{IMDB_HOST}/{name}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest

def _iter_tsv_gz(path: Path):
    with gzip.open(path, "rb") as gz:
        data = gz.read()
    text = io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", errors="replace")
    it = iter(text)
    header = next(it).rstrip("\n").split("\t")
    for line in it:
        row = line.rstrip("\n").split("\t")
        yield dict(zip(header, row))

def _hydrate_imdb_to_tmdb(imdb_ids: List[str], limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    mapped = 0
    session = requests.Session()
    headers = _tmdb_headers()
    params = _tmdb_params()
    for tid in imdb_ids:
        if mapped >= limit:
            break
        try:
            data = session.get(
                f"{TMDB_API}/find/{tid}",
                params={**params, "external_source": "imdb_id", "language": "en-US"},
                headers=headers,
                timeout=20,
            ).json()
        except Exception:
            continue
        found = False
        for coll, mtype in (("movie_results", "movie"), ("tv_results", "tv")):
            for r in data.get(coll, []) or []:
                # Normalize minimal record
                out.append(_normalize_basic(r, mtype))
                found = True
                mapped += 1
        # polite pacing
        time.sleep(0.06)
    return out

def _discover_from_imdb(env: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not _env_bool("IMDB_TSV_ENABLE", False):
        return []
    recent_years = _env_int("IMDB_TSV_RECENT_YEARS", 2000)
    max_titles = _env_int("IMDB_TSV_MAX_TITLES", 150000)
    min_votes  = _env_int("IMDB_TSV_MIN_VOTES", 5000)
    max_map    = _env_int("IMDB_TSV_MAX_MAP", 2000)
    langs      = _env_list("ORIGINAL_LANGS", ["en"])

    cache_dir = Path("data/cache/imdb")
    try:
        basics = _download_imdb(cache_dir, "basics")
        ratings = _download_imdb(cache_dir, "ratings")
    except Exception:
        return []

    # Build quick ratings map (tconst -> (avg*10, votes))
    rmap: Dict[str, Tuple[float, int]] = {}
    try:
        for row in _iter_tsv_gz(ratings):
            tconst = row.get("tconst")
            try:
                votes = int(row.get("numVotes") or "0")
                rating = float(row.get("averageRating") or "0.0") * 10.0
            except Exception:
                continue
            if votes >= min_votes:
                rmap[tconst] = (rating, votes)
    except Exception:
        pass

    imdb_ids: List[str] = []
    count = 0
    # Filter by type / recency / language *heuristic* via akas is heavy; rely on TMDB later for language/provider
    ALLOW_TYPES = {"movie", "tvSeries", "tvMiniSeries"}
    try:
        for row in _iter_tsv_gz(basics):
            if count >= max_titles:
                break
            if row.get("isAdult") == "1":
                continue
            tt = row.get("titleType")
            if tt not in ALLOW_TYPES:
                continue
            # Year
            sy = row.get("startYear") or ""
            try:
                if sy != "\\N" and int(sy) < recent_years:
                    continue
            except Exception:
                pass
            tconst = row.get("tconst")
            if not tconst or tconst == "\\N":
                continue
            # ratings threshold
            if tconst not in rmap:
                continue
            # Light language gate via original language in primary title is unreliable; defer to TMDB later
            imdb_ids.append(tconst)
            count += 1
    except Exception:
        pass

    # Hydrate to TMDB via /find/{imdb_id}
    hydrated = _hydrate_imdb_to_tmdb(imdb_ids, max_map)
    return hydrated

# ---------- Public API ----------
def build_catalog(env: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns the *full pool* (previous + new) as a list of items.
    Also updates env['POOL_TELEMETRY'] and env['DISCOVERED_COUNT'].
    """
    region = (env.get("REGION") or "US")
    langs = [str(x).lower() for x in (env.get("ORIGINAL_LANGS") or [])]
    n_pages = int(env.get("DISCOVER_PAGES", 12) or 12)
    paging_mode = os.getenv("DISCOVER_PAGING_MODE", "first")  # 'first'|'rolling'|'random'
    page_max = _env_int("DISCOVER_PAGE_MAX", 200)

    # Plan pages to fetch
    pages = _paging_plan(n_pages, paging_mode, page_max)

    # Gather from TMDB discover
    discovered: List[Dict[str, Any]] = []
    discovered += _iter_tmdb_pages("movie", pages, region, langs)
    discovered += _iter_tmdb_pages("tv", pages, region, langs)

    # Optional: augment from IMDb TSVs (hydrated to TMDB)
    try:
        imdb_aug = _discover_from_imdb(env)
        if imdb_aug:
            discovered += imdb_aug
    except Exception:
        pass

    # Load existing pool keys
    file_before, keys = _load_pool_keys()

    # Append new unique items to pool file
    appended, unique_est = _append_to_pool(discovered, keys)

    # Read the entire pool back (used downstream for filtering/ scoring)
    pool_items = _read_full_pool()

    env["POOL_TELEMETRY"] = {
        "file_lines_before": file_before,
        "file_lines_after": file_before + appended,
        "loaded_unique": len(keys),
        "appended_this_run": appended,
        "unique_keys_est": unique_est,
        "pages": pages,
        "paging_mode": paging_mode,
    }
    env["DISCOVERED_COUNT"] = len(discovered)
    return pool_items