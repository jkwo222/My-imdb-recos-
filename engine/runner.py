from __future__ import annotations

import csv
import json
import os
import random
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple

# Local modules
from .config import Config  # your existing tolerant config wrapper
from .catalog import build_pool  # returns (pool: List[dict], meta: Dict)
from .exclusions import build_exclusion_index, filter_excluded  # you already added these


# -------------------- small env helpers --------------------

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# -------------------- strong normalization for titles --------------------

def _norm_title(s: str) -> str:
    """
    Normalize for robust title comparison:
    - lowercase
    - strip spaces
    - collapse punctuation/diacritics to ASCII-ish core (cheap version)
    - remove non-alnum
    """
    if not s:
        return ""
    import unicodedata
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii", "ignore")
    s = s.lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        # keep spaces to avoid accidental collisions like "up" vs "u p"
        elif ch.isspace():
            out.append(" ")
    # collapse spaces
    joined = " ".join("".join(out).split())
    return joined


# -------------------- secondary local safety-net exclusion pass --------------------

def _load_seen_from_csv(ratings_csv_path: str) -> Tuple[set, set, Dict[str, set]]:
    """
    Parse ratings.csv (IMDb export or your CSV) to extract:
      - imdb_ids: e.g., tt1234567
      - titles_norm: normalized titles
      - titles_by_year: norm title -> {years}
    We keep it very forgiving about headers/columns.
    """
    imdb_ids = set()
    titles_norm = set()
    titles_by_year: Dict[str, set] = {}

    if not ratings_csv_path or not os.path.exists(ratings_csv_path):
        return imdb_ids, titles_norm, titles_by_year

    def add_title(t: str, y: int | None):
        nt = _norm_title(t)
        if not nt:
            return
        titles_norm.add(nt)
        if y:
            titles_by_year.setdefault(nt, set()).add(int(y))

    with open(ratings_csv_path, "r", encoding="utf-8", errors="ignore") as f:
        rdr = csv.DictReader(f)
        # common IMDb export fields: const (ttid), title, year / your custom columns may vary
        for row in rdr:
            # IMDb id
            for key in ("const", "imdb_id", "imdbid", "tconst", "id"):
                val = row.get(key)
                if val and val.startswith("tt"):
                    imdb_ids.add(val.strip())
                    break
            # titles
            t = None
            for key in ("title", "primaryTitle", "originalTitle", "name"):
                if row.get(key):
                    t = row[key]
                    break
            # year
            y = None
            for key in ("year", "startYear", "releaseYear"):
                if row.get(key) and str(row[key]).strip().isdigit():
                    y = int(str(row[key]).strip())
                    break
            if t:
                add_title(t, y)
    return imdb_ids, titles_norm, titles_by_year


def _extra_exclusion_pass(
    items: List[Dict[str, Any]],
    ratings_csv_path: str,
) -> List[Dict[str, Any]]:
    """
    Belt-and-suspenders pass AFTER exclusions.filter_excluded():
    - Drop anything whose normalized title matches a title in ratings.csv
    - If year is available, also match by (title_norm + year)
    - If an item carries imdb_id in its dict, drop if it matches
    """
    imdb_ids, titles_norm, titles_by_year = _load_seen_from_csv(ratings_csv_path)
    if not imdb_ids and not titles_norm:
        return items

    out: List[Dict[str, Any]] = []
    for it in items:
        title = it.get("title") or it.get("name") or ""
        year = it.get("year")
        imdb_id = (it.get("imdb_id") or it.get("imdbId") or it.get("imdb") or "").strip()

        # imdb id check
        if imdb_id and imdb_id in imdb_ids:
            continue

        nt = _norm_title(title)
        if nt in titles_norm:
            # if we also have year scoping in ratings, respect it
            if not year:
                # seen without year context — exclude
                continue
            yrs = titles_by_year.get(nt)
            if not yrs or (year in yrs):
                continue

        out.append(it)
    return out


# -------------------- simple rank (mirrors catalog scoring logic) --------------------

def _rank(items: List[Dict[str, Any]], critic_weight: float, audience_weight: float) -> List[Dict[str, Any]]:
    ranked = []
    for it in items:
        va = float(it.get("vote_average", 0.0))
        pop = float(it.get("popularity", 0.0))
        score = (critic_weight * va * 10.0) + (audience_weight * min(pop, 100.0) * 0.1)
        it2 = dict(it)
        it2["match"] = round(score, 1)
        ranked.append(it2)
    ranked.sort(key=lambda x: x.get("match", 0.0), reverse=True)
    return ranked


# -------------------- output helpers --------------------

def _utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _utc_timestamp_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def _write_json(path: str, data: Any) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _make_feed(top: List[Dict[str, Any]], pool_size: int, unseen: int, shortlist_size: int, shown_size: int) -> Dict[str, Any]:
    """
    Minimal but stable JSON schema for your assistant_feed.json.
    """
    return {
        "generated_at": _utc_timestamp_str(),
        "pool": pool_size,
        "unseen": unseen,
        "shortlist": shortlist_size,
        "shown": shown_size,
        "top": [
            {
                "title": it.get("title"),
                "year": it.get("year"),
                "type": it.get("type"),
                "match": it.get("match", 0.0),
                "tmdb_id": it.get("tmdb_id"),
                # pass through any IDs we might have attached earlier
                "imdb_id": it.get("imdb_id"),
            }
            for it in top
        ],
    }


# -------------------- main workflow --------------------

def main() -> None:
    print("[bootstrap] {} — workflow started".format(_utc_timestamp_str().replace(" ", "T")), flush=True)

    # 1) Load config (safe; catalog._cfg_get reads env with defaults if absent)
    cfg = Config.from_env_and_files()

    # Sizing knobs
    shortlist_size = _env_int("SHORTLIST_SIZE", 50)
    shown_size     = _env_int("SHOWN_SIZE", 10)

    # Optional explicit weights (fallback to catalog meta if not provided)
    critic_w   = os.getenv("CRITIC_WEIGHT")
    audience_w = os.getenv("AUDIENCE_WEIGHT")

    # Files/paths
    ratings_csv_path = _env_str("RATINGS_CSV", "data/ratings.csv")
    out_root = _env_str("OUT_DIR", "data/out")
    out_latest_dir = os.path.join(out_root, "latest")
    out_daily_dir = os.path.join(out_root, "daily", _utc_date_str())

    print("[hb] | catalog:begin", flush=True)
    pool, meta = build_pool(cfg)
    print("[hb] | catalog:end pool={} movie={} tv={}".format(
        meta.get("counts", {}).get("cumulative", len(pool)),
        meta.get("counts", {}).get("movie_pages_fetched", 0) * 20,
        meta.get("counts", {}).get("tv_pages_fetched", 0) * 20,
    ), flush=True)

    # 2) Build exclusion index from your CSV(s)
    exclusion_index = build_exclusion_index(
        ratings_csv=ratings_csv_path,
        extra_csv=_env_str("EXTRA_EXCLUDE_CSV", ""),   # optional, if you maintain one
        cache_dir=_env_str("EXCLUSION_CACHE_DIR", "data/cache/exclusions"),
        # You can expose more knobs in exclusions.py; keeping this generic.
    )

    # 3) First-pass exclusions using your dedicated module (IDs + fuzzy titles)
    pool_after = filter_excluded(pool, exclusion_index)

    # 4) Second-pass local safety net (title/year/IMDb ID from ratings.csv)
    pool_after = _extra_exclusion_pass(pool_after, ratings_csv_path)

    # The "unseen" set is whatever is left after exclusions
    unseen_count = len(pool_after)

    # 5) Rank
    # Prefer weights from meta (catalog sets defaults), unless overridden by env
    mw = meta.get("weights", {}) if isinstance(meta, dict) else {}
    c_w = float(critic_w) if critic_w not in (None, "") else float(mw.get("critic_weight", 0.6))
    a_w = float(audience_w) if audience_w not in (None, "") else float(mw.get("audience_weight", 0.4))
    ranked = _rank(pool_after, c_w, a_w)

    # 6) Shortlist + shown
    shortlist = ranked[:max(0, shortlist_size)]
    # You can pick simply the top N, or randomly from top K. We'll keep it deterministic: top N -> shown top M.
    shown = shortlist[:max(0, shown_size)]

    # 7) One last guard: ensure shown/shortlist still clean (paranoid)
    shortlist = _extra_exclusion_pass(shortlist, ratings_csv_path)
    shown = _extra_exclusion_pass(shown, ratings_csv_path)

    # 8) Write outputs
    feed = _make_feed(
        top=shown,
        pool_size=len(pool),
        unseen=unseen_count,
        shortlist_size=len(shortlist),
        shown_size=len(shown),
    )

    # latest
    _write_json(os.path.join(out_latest_dir, "assistant_feed.json"), feed)
    # dated
    _write_json(os.path.join(out_daily_dir, "assistant_feed.json"), feed)

    # 9) Telemetry similar to your logs
    print("Weights: critic={}, audience={}".format(c_w, a_w), flush=True)
    print("Counts: tmdb_pool={}, eligible_unseen={}, shortlist={}, shown={}".format(
        meta.get("counts", {}).get("tmdb_pool", 0),
        unseen_count,
        len(shortlist),
        len(shown),
    ), flush=True)
    print("Output: {}".format(out_daily_dir), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Always flush a stacktrace for Actions logs
        import traceback
        traceback.print_exc()
        sys.exit(1)