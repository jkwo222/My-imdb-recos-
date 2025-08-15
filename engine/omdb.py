import os, json, time, requests, hashlib

def _cache_dir():
    p = "data/cache/omdb"; os.makedirs(p, exist_ok=True); return p

def _cache_path(imdb_id: str) -> str:
    h = hashlib.md5(imdb_id.encode("utf-8")).hexdigest()
    return os.path.join(_cache_dir(), f"{h}.json")

def fetch_omdb(imdb_id: str, api_key: str) -> dict:
    if not imdb_id or not api_key: return {}
    cp = _cache_path(imdb_id)
    if os.path.exists(cp):
        try:
            return json.load(open(cp,"r",encoding="utf-8"))
        except Exception:
            pass
    url = f"http://www.omdbapi.com/?apikey={api_key}&i={imdb_id}&tomatoes=true"
    for _ in range(2):
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            try:
                data = r.json()
                json.dump(data, open(cp,"w"), indent=2)
                return data
            except Exception:
                pass
        time.sleep(0.5)
    return {}