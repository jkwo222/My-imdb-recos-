# engine/imdb_sync.py
from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
STATE_DIR = CACHE_DIR / "state"
IMDB_DIR = CACHE_DIR / "imdb"
USER_DIR = CACHE_DIR / "user"
for p in (CACHE_DIR, STATE_DIR, IMDB_DIR, USER_DIR):
    p.mkdir(parents=True, exist_ok=True)

RATINGS_CSV = DATA_DIR / "user" / "ratings.csv"

_TCONST_RE = re.compile(r"tt\d{7,}")

def _pick(*vals):
    for v in vals:
        if v is not None and str(v).strip():
            return v
    return None

def _to_float(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None

def _parse_tconst_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"(tt\d{7,})", url)
    return m.group(1) if m else None

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _iso_now() -> str:
    return _utcnow().isoformat(timespec="seconds")

def _from_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # accept both with and without 'Z'
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# 1) Load local ratings.csv (rich fields)
# ---------------------------------------------------------------------------

def load_ratings_csv() -> List[Dict[str, Any]]:
    """
    Reads data/user/ratings.csv (if present) and returns rich rows with:
    tconst, my_rating, rated_at, title/titleType, startYear/endYear,
    genres[], directors[], numVotes, imdbRating, url, source='csv'
    """
    rows: List[Dict[str, Any]] = []
    if not RATINGS_CSV.exists():
        return rows

    with RATINGS_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            tconst = _pick(
                r.get("tconst"),
                r.get("imdb_id"),
                _parse_tconst_from_url(r.get("url") or r.get("imdb_url") or r.get("URL") or ""),
                r.get("Title Const"),
            )
            if not tconst or not _TCONST_RE.fullmatch(str(tconst)):
                continue

            genres = []
            if r.get("genres"):
                genres = [g.strip() for g in str(r["genres"]).split(",") if g.strip()]

            directors = []
            if r.get("directors"):
                directors = [d.strip() for d in str(r["directors"]).split(",") if d.strip()]

            row = {
                "tconst": str(tconst),
                "my_rating": _to_float(_pick(r.get("my_rating"), r.get("rating"), r.get("Your Rating"))),
                "rated_at": _pick(r.get("rated_at"), r.get("date_rated"), r.get("Rated At")),
                "title": _pick(r.get("primaryTitle"), r.get("originalTitle"), r.get("Title")),
                "titleType": r.get("titleType"),
                "startYear": _pick(r.get("startYear"), r.get("year")),
                "endYear": r.get("endYear"),
                "genres": genres,
                "directors": directors,
                "numVotes": _to_float(r.get("numVotes")),
                "imdbRating": _to_float(_pick(r.get("averageRating"), r.get("imdbRating"))),
                "url": _pick(r.get("url"), r.get("imdb_url")),
                "source": "csv",
            }
            rows.append(row)
    return rows

# ---------------------------------------------------------------------------
# 2) Optional remote (IMDb web) — used only as an EXCLUSION signal
# We avoid brittle scraping; we only read a cached file if the workflow or a
# separate script has populated it. Otherwise returns [].
# ---------------------------------------------------------------------------

def fetch_user_ratings_web(user_id: str, ttl_days: int = 3) -> List[Dict[str, Any]]:
    """
    Returns cached remote ratings if present and not stale; otherwise [].
    This avoids scraping in Actions. You can populate the cache file externally.
    Cache file: data/cache/imdb/user_<id>_ratings.json
    """
    path = IMDB_DIR / f"user_{user_id}_ratings.json"
    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    cached_at = _from_iso(data.get("cached_at", ""))
    if cached_at is None:
        return []
    if _utcnow() - cached_at > timedelta(days=ttl_days):
        return []

    rows = data.get("rows") or []
    # Normalize tconst + shape
    out: List[Dict[str, Any]] = []
    for r in rows:
        t = str(r.get("tconst") or "")
        if not _TCONST_RE.fullmatch(t):
            continue
        out.append({
            "tconst": t,
            "my_rating": _to_float(r.get("my_rating")),
            "rated_at": r.get("rated_at"),
            "title": r.get("title"),
            "titleType": r.get("titleType"),
            "genres": r.get("genres") or [],
            "directors": r.get("directors") or [],
            "numVotes": _to_float(r.get("numVotes")),
            "imdbRating": _to_float(r.get("imdbRating")),
            "url": r.get("url"),
            "source": "imdb_web_cache",
        })
    return out

# ---------------------------------------------------------------------------
# 3) Merge + profile
# ---------------------------------------------------------------------------

def merge_user_sources(local: List[Dict[str, Any]], remote: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge local and remote rows by tconst. Local CSV takes precedence.
    """
    by_t: Dict[str, Dict[str, Any]] = {}
    for r in remote:
        t = str(r["tconst"])
        by_t[t] = r
    for r in local:
        t = str(r["tconst"])
        by_t[t] = {**by_t.get(t, {}), **r}
    return list(by_t.values())

def to_user_profile(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Convert rows -> { tconst: {...} } and also persist snapshots for telemetry.
    """
    prof: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        t = str(r["tconst"])
        prof[t] = {
            "tconst": t,
            "my_rating": r.get("my_rating"),
            "rated_at": r.get("rated_at"),
            "genres": r.get("genres") or [],
            "directors": r.get("directors") or [],
            "titleType": r.get("titleType"),
            "title": r.get("title"),
        }

    # Save for inspection / history
    state = {
        "updated_at": _iso_now(),
        "count": len(prof),
        "sample_titles": [v.get("title") for _, v in list(prof.items())[:10]],
    }
    (STATE_DIR / "personal_history.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    # Also write a canonical state file (runner/summarize can read)
    (STATE_DIR / "personal_state.json").write_text(
        json.dumps({"profile_size": len(prof), "updated_at": state["updated_at"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return prof