# engine/tmdb_client.py
from __future__ import annotations
import os, json, time
from typing import Dict, Iterable, List, Literal, Tuple
import requests

MediaType = Literal["movie", "tv"]

class TMDB:
    def __init__(self, api_key: str, cache_dir: str = "data/cache/tmdb"):
        self.base = "https://api.themoviedb.org/3"
        self.api_key = api_key
        self.session = requests.Session()
        self.session.params = {"api_key": api_key}
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _cache_path(self, *parts: str) -> str:
        safe = "_".join(p.replace("/", "_") for p in parts)
        return os.path.join(self.cache_dir, safe + ".json")

    def _get(self, path: str, params: Dict = None, cache_key: Tuple[str, ...] = None, ttl: int = 0) -> dict:
        # very light cache (opt-in with ttl>0)
        if cache_key and ttl > 0:
            fp = self._cache_path(*cache_key)
            if os.path.exists(fp):
                try:
                    if int(time.time()) - int(os.path.getmtime(fp)) <= ttl:
                        with open(fp, "r", encoding="utf-8") as f:
                            return json.load(f)
                except Exception:
                    pass
        url = f"{self.base}{path}"
        resp = self.session.get(url, params=params or {}, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if cache_key and ttl > 0:
            try:
                with open(self._cache_path(*cache_key), "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except Exception:
                pass
        return data

    def discover_page(self, media: MediaType, page: int, sort_by: str, original_langs: List[str]) -> List[dict]:
        params = {
            "sort_by": sort_by,
            "page": page,
            "include_adult": "false",
            "with_original_language": "|".join(original_langs) if original_langs else None
        }
        data = self._get(f"/discover/{media}", params=params, cache_key=("discover", media, sort_by, str(page)), ttl=0)
        return list(data.get("results") or [])

    def details(self, media: MediaType, tmdb_id: int) -> dict:
        return self._get(f"/{media}/{tmdb_id}", params={"append_to_response": "external_ids"}, cache_key=(media, "details", str(tmdb_id)), ttl=86400)

    def watch_providers(self, media: MediaType, tmdb_id: int) -> dict:
        return self._get(f"/{media}/{tmdb_id}/watch/providers", cache_key=(media, "providers", str(tmdb_id)), ttl=86400)

    def external_ids(self, media: MediaType, tmdb_id: int) -> dict:
        return self._get(f"/{media}/{tmdb_id}/external_ids", cache_key=(media, "xids", str(tmdb_id)), ttl=86400)

def collect_discover(t: TMDB, media: MediaType, pages: Iterable[int], sort_by: str, langs: List[str]) -> List[dict]:
    pool: List[dict] = []
    seen_keys = set()
    for p in pages:
        items = t.discover_page(media, p, sort_by, langs)
        for it in items:
            key = (media, it.get("id"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            it["media_type"] = media
            pool.append(it)
    return pool

def hydrate_items(
    t: TMDB,
    items: List[dict],
    limit: int,
    heartbeat_every: int = 50,
    heartbeat_fn = None
) -> List[dict]:
    hydrated: List[dict] = []
    n = min(limit, len(items))
    for i in range(n):
        it = items[i]
        media: MediaType = it.get("media_type", "movie")  # default
        tmdb_id = it.get("id")
        try:
            det = t.details(media, tmdb_id) or {}
            prov = t.watch_providers(media, tmdb_id) or {}
            xids = det.get("external_ids") or {}
            out = {
                "type": "tvSeries" if media == "tv" else "movie",
                "tmdb_id": tmdb_id,
                "title": det.get("name") if media == "tv" else det.get("title"),
                "year": (det.get("first_air_date") or det.get("release_date") or "")[:4] or None,
                "original_language": det.get("original_language"),
                "tmdb_vote": float(det.get("vote_average") or 0.0),
                "tmdb_votes": int(det.get("vote_count") or 0),
                "seasons": int(det.get("number_of_seasons") or 0) if media == "tv" else None,
                "imdb_id": xids.get("imdb_id"),
                "providers": prov,
            }
            hydrated.append(out)
        except Exception as e:
            # skip item on error
            pass
        if heartbeat_every and (i+1) % heartbeat_every == 0:
            if heartbeat_fn:
                heartbeat_fn(i+1, n)
    return hydrated