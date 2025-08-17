from __future__ import annotations
from typing import List, Dict, Any, Tuple, Set
from pathlib import Path
import csv, json, re
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
USER_DIR = DATA_DIR / "user"
CACHE_DIR = DATA_DIR / "cache"
STATE_DIR = CACHE_DIR / "state"
USER_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

RATINGS_CSV = USER_DIR / "ratings.csv"
REMOTE_CACHE = STATE_DIR / "imdb_remote_user_ratings.json"
PERSONAL_STATE = STATE_DIR / "personal_state.json"
PERSONAL_HISTORY = STATE_DIR / "personal_history.json"

def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k: (v or "").strip() for k, v in r.items()})
    return rows

def load_ratings_csv() -> List[Dict[str, Any]]:
    """
    Returns rows with normalized fields we care about.
    Supported columns (best-effort): imdb_id, tconst, url, title, year, my_rating, genres, directors, titleType, numVotes, rated_at
    """
    rows = _read_csv_rows(RATINGS_CSV)
    out: List[Dict[str, Any]] = []
    for r in rows:
        imdb_id = r.get("imdb_id") or r.get("tconst")
        if not imdb_id and r.get("url"):
            m = re.search(r"(tt\d+)", r["url"])
            if m: imdb_id = m.group(1)
        title = r.get("title") or r.get("primaryTitle") or r.get("originalTitle")
        year = None
        for key in ("year","startYear","release_year"):
            v = r.get(key)
            if v and v.isdigit():
                year = int(v); break
        my_rating = None
        for key in ("my_rating","rating","yourRating"):
            v = r.get(key)
            try:
                if v:
                    my_rating = float(v)
                    break
            except Exception:
                pass
        genres = []
        graw = r.get("genres") or ""
        if graw:
            genres = [g.strip() for g in graw.replace("|", ",").split(",") if g.strip()]
        directors = []
        draw = r.get("directors") or r.get("director") or ""
        if draw:
            directors = [d.strip() for d in draw.split(",") if d.strip()]
        rated_at = r.get("rated_at") or r.get("date") or r.get("created") or ""
        out.append({
            "imdb_id": imdb_id,
            "title": title,
            "year": year,
            "my_rating": my_rating,
            "genres": genres,
            "directors": directors,
            "titleType": r.get("titleType") or r.get("type"),
            "numVotes": r.get("numVotes"),
            "url": r.get("url"),
            "rated_at": rated_at,
        })
    return out

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _parse_ts(s: str) -> datetime | None:
    if not s: return None
    try:
        # accept “...Z” and +00:00
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _is_stale(ts_iso: str, ttl_days: int) -> bool:
    ts = _parse_ts(ts_iso)
    if not ts: 
        return True
    return datetime.now(timezone.utc) - ts > timedelta(days=ttl_days)

def fetch_user_ratings_web(imdb_user_id: str, ttl_days: int = 2) -> List[Dict[str, Any]]:
    """
    Placeholder: if you later add a proper web fetcher, keep the same file shape:
    { "cached_at": "...", "rows": [...] }
    For now we just read the cache if present (or return empty).
    """
    if REMOTE_CACHE.exists():
        cached = json.loads(REMOTE_CACHE.read_text(encoding="utf-8"))
        if isinstance(cached, dict) and not _is_stale(cached.get("cached_at",""), ttl_days):
            return cached.get("rows", [])
    # If you implement fetching, put rows under `rows` and timestamp `cached_at`.
    # For now, keep empty to rely on ratings.csv only.
    REMOTE_CACHE.write_text(json.dumps({"cached_at": _now_iso(), "rows": []}, indent=2), encoding="utf-8")
    return []

def merge_user_sources(local: List[Dict[str, Any]], remote: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    def put(row: Dict[str,Any]):
        key = row.get("imdb_id") or ""
        if not key: return
        prev = by_id.get(key, {})
        merged = {**row, **prev, **row}  # prefer row values
        by_id[key] = merged
    for r in local: put(r)
    for r in remote: put(r)
    return list(by_id.values())

def to_user_profile(rows: List[Dict[str, Any]]) -> Dict[str, Dict]:
    """
    Returns mapping imdb_id -> evidence.
    """
    profile: Dict[str, Dict] = {}
    for r in rows:
        imdb_id = r.get("imdb_id")
        if not imdb_id: 
            continue
        profile[imdb_id] = {
            "my_rating": r.get("my_rating"),
            "title": r.get("title"),
            "year": r.get("year"),
            "genres": r.get("genres") or [],
            "directors": r.get("directors") or [],
            "titleType": r.get("titleType"),
            "rated_at": r.get("rated_at"),
        }
    return profile

def compute_genre_weights(profile: Dict[str, Dict]) -> Dict[str, float]:
    """
    Score genres by (my_rating - 6), normalize to 0..1.
    """
    acc = defaultdict(float)
    cnt = defaultdict(int)
    for _, row in profile.items():
        r = row.get("my_rating")
        if r is None: 
            continue
        delta = float(r) - 6.0
        for g in (row.get("genres") or []):
            acc[g] += delta
            cnt[g] += 1
    if not acc:
        return {}
    mx = max(abs(v) for v in acc.values()) or 1.0
    out = {g: 0.5 + 0.5*(v/mx) for g, v in acc.items()}
    return {g: round(w, 4) for g, w in out.items()}

def build_exclusion_set(rows: List[Dict[str, Any]]) -> Tuple[Set[str], Set[Tuple[str,int|None]]]:
    """
    Returns:
      - imdb_ids: set of 'tt...' you’ve already rated
      - title_year: set of (title_norm, year) pairs for backstop matching
    """
    ids: Set[str] = set()
    pairs: Set[Tuple[str,int|None]] = set()
    def norm_title(t: str) -> str:
        return re.sub(r"\s+", " ", (t or "").strip().lower())
    for r in rows:
        ttid = r.get("imdb_id")
        if ttid: ids.add(ttid)
        title = r.get("title")
        year = r.get("year")
        if title:
            pairs.add((norm_title(title), year if isinstance(year, int) else None))
    return ids, pairs

def save_personal_state(genre_weights: Dict[str,float], profile_rows: List[Dict[str,Any]]) -> None:
    state = {
        "cached_at": _now_iso(),
        "genre_weights": genre_weights,
        "profile_size": len(profile_rows),
    }
    PERSONAL_STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    # Append a small history snapshot for trend analysis
    snap = {
        "at": _now_iso(),
        "genres": genre_weights,
        "n_profile": len(profile_rows),
    }
    history: List[Dict[str,Any]] = []
    if PERSONAL_HISTORY.exists():
        try:
            history = json.loads(PERSONAL_HISTORY.read_text(encoding="utf-8"))
        except Exception:
            history = []
    history.append(snap)
    PERSONAL_HISTORY.write_text(json.dumps(history[-100:], indent=2), encoding="utf-8")