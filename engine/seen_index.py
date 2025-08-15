# engine/seen_index.py
from __future__ import annotations
import csv
import json
import os
import re
import unicodedata
from typing import Dict, Iterable, List, Optional, Set, Tuple

try:
    from bloom_filter2 import BloomFilter  # optional accel
except Exception:  # pragma: no cover
    BloomFilter = None

CACHE_DIR = os.environ.get("CACHE_DIR", "data/cache")
SEEN_KEYS_JSON = os.path.join(CACHE_DIR, "seen_keys.json")
SEEN_BLOOM_BIN = os.path.join(CACHE_DIR, "seen_keys.bloom")

IMDB_ID_COLS = ("Const", "imdb_id")
TITLE_COLS = ("Title", "title")
YEAR_COLS = ("Year", "year", "Release Year")
TYPE_COLS = ("Title Type", "title_type")

ARTICLE_START = re.compile(r"^(the|a|an)\s+", re.IGNORECASE)
NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _ensure_dirs():
    os.makedirs(CACHE_DIR, exist_ok=True)

def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")

def _norm_title(s: str) -> str:
    s = _ascii_fold(s).lower().strip()
    s = ARTICLE_START.sub("", s)
    s = NON_ALNUM.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _year_str(y: Optional[int | str]) -> str:
    try:
        yi = int(y) if y is not None and str(y).strip() != "" else None
    except Exception:
        yi = None
    return str(yi) if yi else ""

def _token_key(s: str) -> str:
    toks = _norm_title(s).split()
    toks = [t for t in toks if len(t) >= 3][:5]
    return "tok:" + "-".join(toks)

def _iter_first_present(row: dict, candidates: Tuple[str, ...]) -> Optional[str]:
    for c in candidates:
        if c in row and row[c]:
            return str(row[c]).strip()
    return None

def _guess_type(row: dict) -> str:
    t = (_iter_first_present(row, TYPE_COLS) or "").lower()
    if "movie" in t or t == "film":
        return "movie"
    if "tv" in t or "series" in t or "mini" in t or "episode" in t:
        return "tv"
    return "unknown"

def _keys_for_imdb_record(tt: Optional[str], title: str, year: Optional[str], typ: str) -> List[str]:
    keys: List[str] = []
    if tt and tt.startswith("tt"):
        keys.append(f"imdb:{tt}")
    y = _year_str(year)
    nt = _norm_title(title)
    if nt:
        if y:
            keys.append(f"title:{nt}:{y}")
        keys.append(f"title:{nt}")
        keys.append(_token_key(title))
        if y:
            keys.append(f"{typ}:{nt}:{y}")
        keys.append(f"{typ}:{nt}")
    return list(dict.fromkeys(keys))

class SeenIndex:
    def __init__(self, keys: Optional[Set[str]] = None):
        self.keys: Set[str] = keys or set()
        self.bloom = None
        if BloomFilter is not None:
            capacity = max(10000, len(self.keys) * 10 or 10000)
            self.bloom = BloomFilter(max_elements=capacity, error_rate=1e-4)
            for k in self.keys:
                self.bloom.add(k)

    def add_many(self, ks: Iterable[str]) -> None:
        for k in ks:
            if not k:
                continue
            if k not in self.keys:
                self.keys.add(k)
                if self.bloom is not None:
                    self.bloom.add(k)

    def contains(self, k: str) -> bool:
        if not k:
            return False
        if k in self.keys:
            return True
        if self.bloom is not None:
            return k in self.bloom
        return False

    def save(self) -> None:
        _ensure_dirs()
        with open(SEEN_KEYS_JSON, "w", encoding="utf-8") as f:
            json.dump(sorted(self.keys), f, ensure_ascii=False)
        if self.bloom is not None:
            try:
                import pickle
                with open(SEEN_BLOOM_BIN, "wb") as bf:
                    pickle.dump(self.bloom, bf, protocol=pickle.HIGHEST_PROTOCOL)
            except Exception:
                pass

def load_seen() -> SeenIndex:
    _ensure_dirs()
    keys: Set[str] = set()
    if os.path.exists(SEEN_KEYS_JSON):
        try:
            with open(SEEN_KEYS_JSON, "r", encoding="utf-8") as f:
                arr = json.load(f)
                keys.update(str(x) for x in arr if x)
        except Exception:
            pass
    return SeenIndex(keys)

# ---- Ratings loading (auto-path) -------------------------------------------

def resolve_ratings_path() -> Optional[str]:
    """
    Try multiple locations; the first existing CSV wins.
    No blanks required from the user.
    """
    candidates = [
        os.environ.get("IMDB_RATINGS_CSV_PATH", "").strip(),
        "/mnt/data/ratings.csv",
        "data/ratings.csv",
        os.path.join(os.getcwd(), "ratings.csv"),
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None

def load_imdb_ratings_csv_auto() -> Tuple[List[dict], Optional[str]]:
    path = resolve_ratings_path()
    if not path:
        return [], None
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append(r)
    return rows, path

def update_seen_from_ratings(rows: List[dict]) -> Tuple[SeenIndex, int]:
    seen = load_seen()
    added = 0
    for r in rows:
        tt = _iter_first_present(r, IMDB_ID_COLS)
        title = _iter_first_present(r, TITLE_COLS) or ""
        year = _iter_first_present(r, YEAR_COLS)
        typ = _guess_type(r)
        ks = _keys_for_imdb_record(tt, title, year, typ)
        for k in ks:
            if k and k not in seen.keys:
                added += 1
        seen.add_many(ks)
    seen.save()
    return seen, added

# ---- Exports ---------------------------------------------------------------

def normalized_title(s: str) -> str:
    return _norm_title(s)

def title_keys(title: str, year: Optional[int], typ: str = "any") -> List[str]:
    y = _year_str(year)
    nt = _norm_title(title)
    out = []
    if nt:
        if y:
            out.append(f"title:{nt}:{y}")
        out.append(f"title:{nt}")
        out.append(_token_key(title))
        if y:
            out.append(f"{typ}:{nt}:{y}")
        out.append(f"{typ}:{nt}")
    return list(dict.fromkeys(out))