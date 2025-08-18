# engine/profile.py
from __future__ import annotations
import csv, json, math, os, re
from pathlib import Path
from typing import Dict, Any, List, Tuple
from collections import Counter, defaultdict

_NON = re.compile(r"[^a-z0-9]+")
def _norm(s: str) -> str:
    return _NON.sub(" ", (s or "").strip().lower()).strip()

def _float(v, d=0.0):
    try: return float(v)
    except Exception: return d

def _pair_key(a: str, b: str) -> str:
    a, b = a.lower(), b.lower()
    return " & ".join(sorted([a,b]))

FAV_RATING_THRESHOLD = 8.0
DECAY_HALF_LIFE_DAYS = int(os.getenv("DECAY_HALF_LIFE_DAYS","270") or 270)

def _decay_weight(days_ago: float) -> float:
    # exponential half-life decay
    if not days_ago or days_ago <= 0: return 1.0
    hl = DECAY_HALF_LIFE_DAYS
    return math.pow(0.5, float(days_ago)/float(hl))

def build_user_model(csv_path: Path, exports_dir: Path) -> Dict[str, Any]:
    """
    Build a light-weight user profile from ratings.csv:
    - top actors/directors/writers/genres/keywords (decayed)
    - sub-genre pairs (e.g., 'crime & thriller', 'sci-fi & horror')
    Exports to exports_dir/user_model.json
    """
    actors=Counter()
    directors=Counter()
    writers=Counter()
    genres=Counter()
    keywords=Counter()
    subgenres=Counter()

    if not csv_path.exists():
        model = {
            "top_actors":{}, "top_directors":{}, "top_writers":{},
            "top_genres":{}, "top_keywords":{}, "top_subgenres":{}
        }
        (exports_dir / "user_model.json").write_text(json.dumps(model, indent=2), encoding="utf-8")
        return model

    with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
        rd = csv.DictReader(fh)
        for row in rd:
            try:
                rating = _float(row.get("Your Rating") or row.get("Rating"))
                if rating < FAV_RATING_THRESHOLD: 
                    continue
                days_ago = _float(row.get("Days Since Rated") or row.get("Days Since Added") or 0)
                w = _decay_weight(days_ago)
                # People
                for a in (row.get("Actors") or "").split("|"):
                    a=a.strip()
                    if a: actors[a] += w
                for d in (row.get("Directors") or "").split("|"):
                    d=d.strip()
                    if d: directors[d] += w
                for wr in (row.get("Writers") or "").split("|"):
                    wr=wr.strip()
                    if wr: writers[wr] += w
                # Genres & sub-genres
                g = [ _norm(g) for g in (row.get("Genres") or "").split(",") if g.strip() ]
                for gg in g: genres[gg] += w
                # pairwise co-occurrence for sub-genre signals
                for i in range(len(g)):
                    for j in range(i+1, len(g)):
                        subgenres[_pair_key(g[i], g[j])] += w
                # Keywords (if present)
                for kw in (row.get("Keywords") or "").split("|"):
                    kw = _norm(kw)
                    if kw: keywords[kw] += w
            except Exception:
                continue

    # normalize and keep top-k-ish
    def top_map(c: Counter, k: int = 200) -> Dict[str, float]:
        total = sum(c.values()) or 1.0
        return {k: round(v/total, 4) for k, v in c.most_common(k)}

    model = {
        "top_actors": top_map(actors, 150),
        "top_directors": top_map(directors, 120),
        "top_writers": top_map(writers, 120),
        "top_genres": top_map(genres, 60),
        "top_keywords": top_map(keywords, 200),
        "top_subgenres": top_map(subgenres, 120),
    }
    exports_dir.mkdir(parents=True, exist_ok=True)
    (exports_dir / "user_model.json").write_text(json.dumps(model, indent=2), encoding="utf-8")
    return model