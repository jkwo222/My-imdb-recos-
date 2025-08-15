# tools/ratings.py
import os, re, csv, json, time, pathlib, requests
from typing import List, Dict, Any, Optional

UA = {"User-Agent": "RecoEngine/2.13 (+github actions)"}
OMDB_CACHE_DIR = pathlib.Path("data/cache/omdb")
OMDB_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- IMDb ratings CSV loader ----------------

def _parse_int(x: Any, default: int = 0) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return default

def _parse_float(x: Any, default: float = 0.0) -> float:
    try:
        s = str(x).strip()
        if not s or s.lower() == "nan":
            return default
        return float(s)
    except Exception:
        return default

def _from_url_get_tconst(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"/title/(tt\d+)/", url)
    return m.group(1) if m else ""

def load_imdb_ratings_csv(path: str) -> List[Dict[str, Any]]:
    """
    Reads IMDb's ratings export CSV (or a similar CSV).
    Accepts columns:
      - imdb_id OR const OR URL
      - title OR Title
      - year OR Year
      - Title type / Title Type (movie, tvSeries, tvMiniSeries, etc.)
      - Your Rating (optional)
    Returns list[ {imdb_id,title,year,type,your_rating} ].
    """
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            title = r.get("title") or r.get("Title") or ""
            year = _parse_int(r.get("year") or r.get("Year") or 0)
            ttype = (r.get("type") or r.get("Type") or r.get("Title type") or r.get("Title Type") or "").strip() or "movie"
            your_rating = _parse_float(r.get("Your Rating") or r.get("your_rating") or 0)

            imdb_id = (r.get("imdb_id") or r.get("const") or "").strip()
            if not imdb_id:
                imdb_id = _from_url_get_tconst(r.get("URL") or r.get("url") or "")

            rows.append({
                "imdb_id": imdb_id,
                "title": title,
                "year": year,
                "type": ttype,
                "your_rating": your_rating
            })
    return rows

# ---------------- OMDb enrichment ----------------

_LANG_MAP = {
    "english":"en","eng":"en",
    "spanish":"es","español":"es",
    "french":"fr","français":"fr",
    "german":"de","deutsch":"de",
    "italian":"it","italiano":"it",
    "portuguese":"pt","português":"pt",
    "russian":"ru",
    "japanese":"ja",
    "korean":"ko",
    "chinese":"zh","mandarin":"zh","cantonese":"zh",
    "hindi":"hi",
    "arabic":"ar",
    "dutch":"nl",
    "swedish":"sv",
    "norwegian":"no",
    "danish":"da",
    "finnish":"fi",
    "polish":"pl",
    "turkish":"tr",
    "thai":"th",
    "vietnamese":"vi",
    "czech":"cs",
    "greek":"el",
    "hebrew":"he",
}

def _norm_langs(s: str) -> Dict[str, Any]:
    """
    OMDb 'Language' is a comma-separated names string.
    Return {'lang_names': [...], 'langs': ['en', ...], 'lang_is_english': bool}
    """
    names = []
    codes = []
    if s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            names.append(p)
            k = p.lower()
            codes.append(_LANG_MAP.get(k, ""))  # may be ""
    codes = [c for c in codes if c]
    return {
        "lang_names": names,
        "langs": codes,
        "lang_is_english": ("en" in codes) or any(n.lower() == "english" for n in names)
    }

def _norm_countries(s: str) -> List[str]:
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]

def _omdb_key() -> Optional[str]:
    k = os.environ.get("OMDB_API_KEY", "").strip()
    return k or None

def _omdb_cache_key(item: Dict[str, Any]) -> str:
    iid = (item.get("imdb_id") or "").strip().lower()
    if iid:
        return f"byid_{iid}.json"
    title = (item.get("title") or "").strip().lower()
    year = str(item.get("year") or 0)
    itype = (item.get("type") or "").strip().lower()
    safe = re.sub(r"[^a-z0-9]+", "_", f"{title}_{year}_{itype}").strip("_")
    return f"bytitle_{safe}.json" if safe else f"bytitle_{int(time.time()*1000)}.json"

def _load_cache(p: pathlib.Path) -> Optional[Dict[str, Any]]:
    if p.exists():
        try:
            return json.load(p.open("r"))
        except Exception:
            return None
    return None

def _save_cache(p: pathlib.Path, data: Dict[str, Any]) -> None:
    try:
        json.dump(data, p.open("w"))
    except Exception:
        pass

def _omdb_fetch(item: Dict[str, Any]) -> Dict[str, Any]:
    key = _omdb_key()
    if not key:
        return {"__error__": "OMDB_API_KEY missing"}

    params = {"apikey": key}
    iid = (item.get("imdb_id") or "").strip()
    if iid:
        params["i"] = iid
    else:
        title = (item.get("title") or "").strip()
        if not title:
            return {"Response":"False","Error":"Missing title"}
        params["t"] = title
        y = int(item.get("year") or 0)
        if y:
            params["y"] = str(y)
        itype = (item.get("type") or "").strip()
        if itype in ("tvSeries", "tvMiniSeries"):
            params["type"] = "series"
        elif itype in ("movie", "tvMovie"):
            params["type"] = "movie"

    url = "http://www.omdbapi.com/"
    r = requests.get(url, params=params, headers=UA, timeout=30)
    if r.status_code != 200:
        return {"Response":"False","Error":f"HTTP {r.status_code}"}
    try:
        return r.json()
    except Exception:
        return {"Response":"False","Error":"bad json"}

def _extract_rt_pct(ratings_list: Any) -> int:
    try:
        for rr in ratings_list or []:
            if (rr.get("Source") or "").lower() == "rotten tomatoes":
                v = rr.get("Value") or ""
                m = re.match(r"(\d+)\s*%", v)
                if m:
                    return int(m.group(1))
    except Exception:
        pass
    return 0

def _merge_omdb_fields(target: Dict[str, Any], om: Dict[str, Any]) -> None:
    if not om or om.get("Response") != "True":
        return
    iid = (om.get("imdbID") or "").strip()
    if iid and not target.get("imdb_id"):
        target["imdb_id"] = iid

    # ratings (used for scoring)
    try:
        ir = float(om.get("imdbRating")) if om.get("imdbRating") not in (None, "N/A") else 0.0
        if ir > 0:
            target["imdb_rating"] = ir
    except Exception:
        pass
    target["rt_pct"] = _extract_rt_pct(om.get("Ratings"))

    # certification & meta
    rated = (om.get("Rated") or "").strip()
    if rated:
        target["cert"] = rated

    # genres/runtime/country/language — store & normalize for later filters
    target.setdefault("omdb", {})
    target["omdb"]["genres"] = (om.get("Genre") or "").strip()
    target["omdb"]["runtime"] = (om.get("Runtime") or "").strip()
    target["omdb"]["votes"] = (om.get("imdbVotes") or "").strip()
    target["omdb"]["country_raw"] = (om.get("Country") or "").strip()
    target["omdb"]["language_raw"] = (om.get("Language") or "").strip()

    # normalized helpers
    lang_info = _norm_langs(target["omdb"]["language_raw"])
    target["lang_names"] = lang_info["lang_names"]
    target["langs"] = lang_info["langs"]
    target["lang_is_english"] = lang_info["lang_is_english"]

    target["countries"] = _norm_countries(target["omdb"]["country_raw"])

def enrich_with_omdb(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fills in imdb_id, imdb_rating, rt_pct, cert, langs/lang_names/lang_is_english,
    countries, and stores OMDb raw meta in item['omdb'].
    Uses on-disk cache at data/cache/omdb to avoid re-fetching.
    """
    out: List[Dict[str, Any]] = []
    for it in items:
        cache_name = _omdb_cache_key(it)
        p = OMDB_CACHE_DIR / cache_name
        data = _load_cache(p)
        if data is None:
            data = _omdb_fetch(it)
            _save_cache(p, data)
            time.sleep(0.12)  # be nice to OMDb
        _merge_omdb_fields(it, data)
        out.append(it)
    return out

# convenience for filtering
def is_english_from_item(item: Dict[str, Any]) -> bool:
    if item.get("lang_is_english"):
        return True
    # fallback to TMDB original_language if OMDb missing
    if (item.get("original_language") or "").lower() == "en":
        return True
    # last resort: title language hints
    return False