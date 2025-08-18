# engine/profile.py
from __future__ import annotations
import csv
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any, Iterable

from . import tmdb

# ---------- env knobs ----------
def _int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v else default
    except Exception:
        return default

def _float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        return float(v) if v else default
    except Exception:
        return default

def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return default

AFFINITY_K = _float("AFFINITY_K", 5.0)
DECAY_HALF_LIFE_DAYS = _float("DECAY_HALF_LIFE_DAYS", 270.0)
PROFILE_ENRICH_CREDITS = _bool("PROFILE_ENRICH_CREDITS", True)
PROFILE_ENRICH_MAX_TITLES = _int("PROFILE_ENRICH_MAX_TITLES", 250)   # cap TMDB calls
PROFILE_ENRICH_MIN_RATING = _float("PROFILE_ENRICH_MIN_RATING", 8.0) # only enrich high-rated

# ---------- utils ----------
def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%m/%d/%Y", "%b %d %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _decay_weight(d: datetime | None) -> float:
    if d is None:
        return 1.0
    age_days = max(0.0, (_now_utc() - d).total_seconds() / 86400.0)
    if DECAY_HALF_LIFE_DAYS <= 0:
        return 1.0
    tau = DECAY_HALF_LIFE_DAYS / math.log(2.0)
    return math.exp(-age_days / tau)

def _bucket_runtime(mins: float | int | str | None) -> str:
    try:
        m = float(str(mins).strip())
    except Exception:
        return "unknown"
    if m <= 90:   return "<=90"
    if m <= 120:  return "91-120"
    if m <= 150:  return "121-150"
    return ">150"

def _bucket_era(year: int | str | None) -> str:
    try:
        y = int(str(year)[:4])
    except Exception:
        return "unknown"
    if 1960 <= y < 1980: return "60-79"
    if 1980 <= y < 1990: return "80s"
    if 1990 <= y < 2000: return "90s"
    if 2000 <= y < 2010: return "00s"
    if 2010 <= y < 2020: return "10s"
    if 2020 <= y < 2030: return "20s"
    return "unknown"

def _split_csv(s: str | None) -> List[str]:
    if not s:
        return []
    return [t.strip() for t in str(s).split(",") if t.strip()]

def _extract_imdb_id(row: Dict[str, Any]) -> str | None:
    # common headers
    for k in ("imdb_id","const","IMDb Const","Const","IMDB_ID","ID"):
        v = row.get(k)
        if isinstance(v,str) and v.startswith("tt"):
            return v.strip()
    url = row.get("URL") or row.get("Url") or row.get("url")
    if isinstance(url,str):
        m = re.search(r"/title/(tt\d{7,8})", url)
        if m: return m.group(1)
    return None

@dataclass
class Obs:
    rating: float
    w: float

def _weighted_avg(observations: List[Obs]) -> Tuple[float, float]:
    if not observations:
        return 0.0, 0.0
    sw = sum(o.w for o in observations)
    if sw <= 0:
        return 0.0, 0.0
    avg = sum(o.rating * o.w for o in observations) / sw
    return avg, sw

# ---------- cache for TMDB lookups ----------
def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            return {}
    return {}

def _save_cache(cache_path: Path, data: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- model builder ----------
def build_user_model(ratings_csv: Path, out_dir: Path) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    # read CSV rows
    rows: List[Dict[str, Any]] = []
    if ratings_csv.exists():
        with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                try:
                    rating = float(r.get("Your Rating") or r.get("YourRating") or r.get("Rating") or 0.0)
                except Exception:
                    rating = 0.0
                date_rated = _parse_date(r.get("Date Rated") or r.get("Date Added"))
                w = _decay_weight(date_rated)
                title_type = (r.get("Title Type") or "").strip()
                year = r.get("Year")
                runtime = r.get("Runtime (mins)")
                directors = _split_csv(r.get("Directors") or r.get("Director"))
                genres = _split_csv(r.get("Genres") or r.get("Genre"))
                imdb_id = _extract_imdb_id(r)

                rows.append({
                    "rating": rating, "w": w, "title_type": title_type,
                    "year": year, "runtime": runtime,
                    "directors": directors, "genres": genres,
                    "imdb_id": imdb_id,
                })
    else:
        (out_dir / "user_model.json").write_text("{}", encoding="utf-8")
        return {"meta": {"count": 0, "note": "ratings.csv missing"}}

    # baselines
    global_avg, global_n = _weighted_avg([Obs(r["rating"], r["w"]) for r in rows])
    by_type: Dict[str, Tuple[float, float]] = {}
    for t in set(r["title_type"] for r in rows):
        obs = [Obs(r["rating"], r["w"]) for r in rows if r["title_type"] == t]
        by_type[t] = _weighted_avg(obs)

    # token observation collectors
    tok_people = {"director": {}, "writer": {}, "actor": {}}  # type: ignore
    tok_genres: Dict[str, List[Obs]] = {}
    tok_runtime: Dict[str, List[Obs]] = {}
    tok_era: Dict[str, List[Obs]] = {}
    tok_type: Dict[str, List[Obs]] = {}
    tok_language: Dict[str, List[Obs]] = {}
    tok_country: Dict[str, List[Obs]] = {}
    tok_studio: Dict[str, List[Obs]] = {}
    tok_network: Dict[str, List[Obs]] = {}
    tok_keywords: Dict[str, List[Obs]] = {}

    # 1) From CSV: directors/genres/runtime/era/title_type
    for r in rows:
        obs = Obs(r["rating"], r["w"])
        for d in r["directors"]:
            tok_people["director"].setdefault(d, []).append(obs)
        for g in r["genres"]:
            tok_genres.setdefault(g.lower(), []).append(obs)
        tok_runtime.setdefault(_bucket_runtime(r["runtime"]), []).append(obs)
        tok_era.setdefault(_bucket_era(r["year"]), []).append(obs)
        tok_type.setdefault((r["title_type"] or "unknown"), []).append(obs)

    # 2) Optional TMDB enrichment for high-rated titles: actors/writers/studios/networks/keywords/lang/country
    cache_path = Path("data/cache/tmdb_profile.json")
    cache = _load_cache(cache_path)
    enriched = 0
    if PROFILE_ENRICH_CREDITS:
        # Sort by rating * w (importance), highest first
        ranked = sorted(rows, key=lambda rr: rr["rating"] * rr["w"], reverse=True)
        for r in ranked:
            if enriched >= PROFILE_ENRICH_MAX_TITLES:
                break
            if r["rating"] < PROFILE_ENRICH_MIN_RATING:
                continue
            imdb_id = r.get("imdb_id")
            if not imdb_id:
                continue

            # map to tmdb id & kind
            key = f"map:{imdb_id}"
            if key in cache:
                maprec = cache[key]
            else:
                maprec = tmdb.find_by_imdb(imdb_id)
                cache[key] = maprec

            kind = (maprec or {}).get("media_type")
            tid = (maprec or {}).get("tmdb_id")
            if not kind or not tid:
                continue
            obs = Obs(r["rating"], r["w"])

            # credits
            ckey = f"cred:{kind}:{tid}"
            if ckey in cache:
                cred = cache[ckey]
            else:
                cred = tmdb.get_credits(kind, int(tid))
                cache[ckey] = cred
            for d in (cred.get("directors") or []):
                tok_people["director"].setdefault(d, []).append(obs)
            for w in (cred.get("writers") or []):
                tok_people["writer"].setdefault(w, []).append(obs)
            for a in (cred.get("cast") or [])[:8]:
                tok_people["actor"].setdefault(a, []).append(obs)

            # keywords
            kkey = f"kw:{kind}:{tid}"
            if kkey in cache:
                kws = cache[kkey]
            else:
                kws = tmdb.get_keywords(kind, int(tid))
                cache[kkey] = kws
            for kw in (kws or [])[:20]:
                tok_keywords.setdefault(kw, []).append(obs)

            # details (lang/country/studio/network)
            dkey = f"det:{kind}:{tid}"
            if dkey in cache:
                det = cache[dkey]
            else:
                det = tmdb.get_details(kind, int(tid))
                cache[dkey] = det

            lang = (det.get("original_language") or "").lower()
            if lang:
                tok_language.setdefault(lang, []).append(obs)
            for c in (det.get("production_countries") or []):
                tok_country.setdefault(str(c).upper(), []).append(obs)
            for s in (det.get("production_companies") or []):
                tok_studio.setdefault(s.lower(), []).append(obs)
            for n in (det.get("networks") or []):
                tok_network.setdefault(n.lower(), []).append(obs)

            enriched += 1

    # save cache back
    try:
        _save_cache(cache_path, cache)
    except Exception:
        pass

    # convert obs -> weights
    def to_weights(tok_map: Dict[str, List[Obs]]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for tok, obs in tok_map.items():
            avg, n_eff = _weighted_avg(obs)
            shrink = n_eff / (n_eff + AFFINITY_K)
            out[tok] = (avg - global_avg) * shrink
        return out

    model: Dict[str, Any] = {
        "meta": {
            "count": len(rows),
            "global_avg": round(global_avg, 4),
            "AFFINITY_K": AFFINITY_K,
            "DECAY_HALF_LIFE_DAYS": DECAY_HALF_LIFE_DAYS,
            "by_type": {k: {"avg": round(v[0], 4), "n_eff": round(v[1], 3)} for k, v in by_type.items()},
            "enriched_titles": enriched if PROFILE_ENRICH_CREDITS else 0,
            "PROFILE_ENRICH_MAX_TITLES": PROFILE_ENRICH_MAX_TITLES,
            "PROFILE_ENRICH_MIN_RATING": PROFILE_ENRICH_MIN_RATING,
        },
        "people": {
            "director": to_weights(tok_people["director"]),
            "writer":   to_weights(tok_people["writer"]),
            "actor":    to_weights(tok_people["actor"]),
        },
        "form": {
            "runtime_bucket": to_weights(tok_runtime),
            "title_type":     to_weights(tok_type),
            "era":            to_weights(tok_era),
        },
        "genres":   to_weights(tok_genres),
        "language": to_weights(tok_language),
        "country":  to_weights(tok_country),
        "studio":   to_weights(tok_studio),
        "network":  to_weights(tok_network),
        "keywords": to_weights(tok_keywords),
        "provider": {},  # could be learned later
    }

    # write artifacts
    (out_dir / "user_model.json").write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")

    def topn(d: Dict[str, float], n=12):
        return "\n".join(f"- {k}: {round(v,2)}" for k, v in sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n])

    report = [
        "# User model",
        f"- rows: {len(rows)}",
        f"- global_avg: {round(global_avg,2)}",
        f"- enriched_titles: {enriched if PROFILE_ENRICH_CREDITS else 0}",
        "## Directors (top +)",
        topn(model["people"]["director"]),
        "## Writers (top +)",
        topn(model["people"]["writer"]),
        "## Actors (top +)",
        topn(model["people"]["actor"]),
        "## Genres (top +)",
        topn(model["genres"]),
        "## Runtime bucket",
        topn(model["form"]["runtime_bucket"]),
        "## Era",
        topn(model["form"]["era"]),
        "## Studios (top +)",
        topn(model["studio"]),
        "## Networks (top +)",
        topn(model["network"]),
        "## Keywords (top +)",
        topn(model["keywords"]),
    ]
    (out_dir / "profile_report.md").write_text("\n".join(report), encoding="utf-8")
    return model