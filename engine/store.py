# FILE: engine/store.py
from __future__ import annotations
import json, pathlib
from typing import Dict, List

class PersistentPool:
    """
    Keeps an accumulated dictionary of items across runs.
    Keyed by imdb_id if present, else by tmdb_id.
    Merge policy keeps most useful fields and updates providers/ratings.
    """
    def __init__(self, path: pathlib.Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, Dict] = {}
        if self.path.exists():
            try:
                self._data = json.load(open(self.path, "r", encoding="utf-8"))
            except Exception:
                self._data = {}

    def _key(self, it: Dict) -> str:
        iid = (it.get("imdb_id") or "").strip()
        if iid:
            return f"imdb:{iid}"
        tid = it.get("tmdb_id")
        return f"tmdb:{tid}"

    def merge_and_save(self, items: List[Dict]) -> Dict[str, Dict]:
        for it in items:
            k = self._key(it)
            if not k: 
                continue
            prev = self._data.get(k, {})
            merged = dict(prev)
            # Always update the basics from the latest crawl
            for fld in ("tmdb_id","imdb_id","title","year","type","seasons"):
                if it.get(fld) is not None:
                    merged[fld] = it.get(fld)
            # Ratings: keep max imdb_rating / tmdb_vote seen
            for fld in ("imdb_rating","tmdb_vote"):
                v = it.get(fld)
                if isinstance(v, (int,float)):
                    merged[fld] = max(float(v), float(merged.get(fld, 0.0)))
            # Providers: union
            prov = set(merged.get("providers") or []) | set(it.get("providers") or [])
            merged["providers"] = sorted(prov)
            self._data[k] = merged

        json.dump(self._data, open(self.path,"w",encoding="utf-8"), indent=2)
        return self._data