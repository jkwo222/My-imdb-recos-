# engine/profile.py
from __future__ import annotations
import csv, json, math, os, re
from pathlib import Path
from typing import Dict, Any, List
from collections import Counter

_NON = re.compile(r"[^a-z0-9]+")
def _norm(s: str) -> str: return _NON.sub(" ", (s or "").strip().lower()).strip()
def _float(v, d=0.0):
    try: return float(v)
    except Exception: return d

FAV_RATING_THRESHOLD = 8.0
DECAY_HALF_LIFE_DAYS = int(os.getenv("DECAY_HALF_LIFE_DAYS","270") or 270)

def _decay_weight(days_ago: float) -> float:
    if not days_ago or days_ago <= 0: return 1.0
    return math.pow(0.5, float(days_ago)/float(DECAY_HALF_LIFE_DAYS))

def _split_multi(s: str) -> List[str]:
    if not s: return []
    # handle "A|B|C" and "A, B, C"
    parts = []
    for tok in re.split(r"[|,]", s):
        t = tok.strip()
        if t: parts.append(t)
    return parts

def _find_cols(fieldnames: List[str], needles: List[str]) -> List[str]:
    hits=[]
    lowers=[(f, f.lower()) for f in fieldnames]
    for f, fl in lowers:
        for n in needles:
            if n in fl:
                hits.append(f); break
    # remove duplicates preserving order
    seen=set(); out=[]
    for h in hits:
        if h not in seen:
            seen.add(h); out.append(h)
    return out

def build_user_model(csv_path: Path, exports_dir: Path) -> Dict[str, Any]:
    actors=Counter(); directors=Counter(); writers=Counter()
    genres=Counter(); keywords=Counter(); subpairs=Counter()

    if not csv_path.exists():
        model = {"top_actors":{}, "top_directors":{}, "top_writers":{}, "top_genres":{}, "top_keywords":{}, "top_subgenres":{}}
        exports_dir.mkdir(parents=True, exist_ok=True)
        (exports_dir / "user_model.json").write_text(json.dumps(model, indent=2), encoding="utf-8")
        return model

    with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
        rd = csv.DictReader(fh)
        fields = rd.fieldnames or []
        actor_cols   = _find_cols(fields, ["actor", "cast", "stars", "principal"])
        director_cols= _find_cols(fields, ["director"])
        writer_cols  = _find_cols(fields, ["writer", "screenplay"])
        genre_cols   = _find_cols(fields, ["genre"])
        keyword_cols = _find_cols(fields, ["keyword", "tag"])
        date_cols    = _find_cols(fields, ["days since rated","days since added","days since"])

        for row in rd:
            try:
                rating = _float(row.get("Your Rating") or row.get("Rating"))
                if rating < FAV_RATING_THRESHOLD: 
                    continue
                days_ago = None
                for dc in date_cols:
                    v = _float(row.get(dc), None)
                    if v is not None: days_ago = v; break
                w = _decay_weight(days_ago if days_ago is not None else 0)

                # People
                for col in actor_cols:
                    for a in _split_multi(row.get(col, "")):
                        if a: actors[a] += w
                for col in director_cols:
                    for d in _split_multi(row.get(col, "")):
                        if d: directors[d] += w
                for col in writer_cols:
                    for wr in _split_multi(row.get(col, "")):
                        if wr: writers[wr] += w

                # Genres + sub-genre pairs
                gset=[]
                for col in genre_cols:
                    for g in _split_multi(row.get(col, "")):
                        ng=_norm(g)
                        if ng: genres[ng] += w; gset.append(ng)
                for i in range(len(gset)):
                    for j in range(i+1, len(gset)):
                        a,b = gset[i], gset[j]
                        pair = " & ".join(sorted([a,b]))
                        subpairs[pair] += w

                # Keywords
                for col in keyword_cols:
                    for kw in _split_multi(row.get(col, "")):
                        k=_norm(kw)
                        if k: keywords[k] += w
            except Exception:
                continue

    def top_map(c: Counter, k: int) -> Dict[str, float]:
        total = float(sum(c.values()) or 1.0)
        return {k: round(v/total, 4) for k, v in c.most_common(k)}

    model = {
        "top_actors":    top_map(actors,    150),
        "top_directors": top_map(directors, 120),
        "top_writers":   top_map(writers,   120),
        "top_genres":    top_map(genres,     60),
        "top_keywords":  top_map(keywords,  200),
        "top_subgenres": top_map(subpairs,  120),
    }
    exports_dir.mkdir(parents=True, exist_ok=True)
    (exports_dir / "user_model.json").write_text(json.dumps(model, indent=2), encoding="utf-8")
    return model