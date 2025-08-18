# engine/filtering.py
from __future__ import annotations
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

_NON = re.compile(r"[^a-z0-9]+")

def _norm_title(s: str) -> str:
    return _NON.sub(" ", (s or "").lower()).strip()

class SeenIndex:
    def __init__(self) -> None:
        self.imdb_ids: Set[str] = set()
        self.tmdb_ids: Set[str] = set()
        self.title_year: Set[Tuple[str, int]] = set()
        # For series-level suppression (TV roots)
        self.tv_roots: Set[str] = set()

def _split_multi(s: str) -> List[str]:
    if not s:
        return []
    out=[]
    for tok in re.split(r"[|,]", s):
        t=tok.strip()
        if t:
            out.append(t)
    return out

def _maybe_int(s: Any) -> Optional[int]:
    try:
        v = int(str(s).strip())
        return v
    except Exception:
        return None

def build_seen_index(csv_path: Path, imdb_public_json: Optional[Path] = None) -> SeenIndex:
    """
    Build a strict seen index from your CSV (primary) and optional IMDb public export JSON.
    We check IMDb ids, title+year pairs, and TV root names to avoid repeats across seasons.
    """
    idx = SeenIndex()

    # Ratings CSV (primary)
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
            rd = csv.DictReader(fh)
            fields = [f.lower() for f in (rd.fieldnames or [])]
            # Guess columns
            col_const = next((f for f in rd.fieldnames or [] if f.lower() in {"const","imdb id","imdb_id"}), None)
            col_title = next((f for f in rd.fieldnames or [] if "title" in f.lower() and "original" not in f.lower()), None)
            col_year  = next((f for f in rd.fieldnames or [] if "year" in f.lower()), None)
            col_type  = next((f for f in rd.fieldnames or [] if f.lower() in {"title type","type"}), None)
            col_series= next((f for f in rd.fieldnames or [] if "series" in f.lower()), None)

            for row in rd:
                # imdb id
                if col_const:
                    v = (row.get(col_const) or "").strip()
                    if v.startswith("tt"):
                        idx.imdb_ids.add(v)
                # title-year pair
                t = _norm_title(row.get(col_series) or row.get(col_title) or "")
                y = _maybe_int(row.get(col_year))
                if t and y:
                    idx.title_year.add((t, y))
                # tv roots
                tt = (row.get(col_type) or "").lower()
                if "tv" in tt or "episode" in tt or "series" in tt:
                    if t:
                        idx.tv_roots.add(t)

    # IMDb public list (optional JSON: { "imdb_ids": [...], "title_year": [[title, year], ...], "tv_roots": [...] })
    if imdb_public_json and imdb_public_json.exists():
        try:
            obj = json.loads(imdb_public_json.read_text(encoding="utf-8", errors="replace"))
            for v in obj.get("imdb_ids", []):
                if isinstance(v, str) and v.startswith("tt"):
                    idx.imdb_ids.add(v)
            for pair in obj.get("title_year", []):
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    t = _norm_title(pair[0]); y = _maybe_int(pair[1])
                    if t and y:
                        idx.title_year.add((t, y))
            for t in obj.get("tv_roots", []):
                if isinstance(t, str):
                    idx.tv_roots.add(_norm_title(t))
        except Exception:
            pass

    return idx

def _item_title_year(it: Dict[str, Any]) -> Optional[Tuple[str, int]]:
    title = _norm_title(it.get("title") or it.get("name") or "")
    year = None
    if it.get("year"):
        year = _maybe_int(it.get("year"))
    else:
        rd = (it.get("release_date") or it.get("first_air_date") or "").strip()
        if len(rd) >= 4 and rd[:4].isdigit():
            try:
                year = int(rd[:4])
            except Exception:
                year = None
    if title and year:
        return (title, year)
    return None

def _tv_root_key(it: Dict[str, Any]) -> Optional[str]:
    if (it.get("media_type") or it.get("type") or "").lower() != "tv":
        return None
    # Prefer "name", else "title"
    t = _norm_title(it.get("name") or it.get("title") or "")
    return t or None

def filter_seen(items: List[Dict[str, Any]], idx: SeenIndex) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    kept: List[Dict[str, Any]] = []
    excluded = 0
    for it in items:
        # imdb
        imdb_id = (it.get("imdb_id") or "").strip()
        if imdb_id and imdb_id in idx.imdb_ids:
            excluded += 1
            continue
        # tmdb (optional if you ever capture)
        tid = it.get("tmdb_id") or it.get("id")
        if tid and str(tid) in idx.tmdb_ids:
            excluded += 1
            continue
        # title-year
        ty = _item_title_year(it)
        if ty and ty in idx.title_year:
            excluded += 1
            continue
        # tv root (suppress whole series)
        root = _tv_root_key(it)
        if root and root in idx.tv_roots:
            excluded += 1
            continue
        kept.append(it)
    return kept, {"excluded": excluded}