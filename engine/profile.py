# engine/profile.py
from __future__ import annotations
import csv
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any, Iterable

# ---------- config knobs (from env) ----------

def _int_env(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v else default
    except Exception:
        return default

def _float_env(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        return float(v) if v else default
    except Exception:
        return default

AFFINITY_K = _float_env("AFFINITY_K", 5.0)                # Bayesian shrinkage K
DECAY_HALF_LIFE_DAYS = _float_env("DECAY_HALF_LIFE_DAYS", 270.0)  # ~9 months

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
    tau = DECAY_HALF_LIFE_DAYS / math.log(2.0)  # half-life -> exp time constant
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

@dataclass
class Obs:
    rating: float
    w: float   # decay weight

def _weighted_avg(observations: List[Obs]) -> Tuple[float, float]:
    if not observations:
        return 0.0, 0.0
    sw = sum(o.w for o in observations)
    if sw <= 0:
        return 0.0, 0.0
    avg = sum(o.rating * o.w for o in observations) / sw
    return avg, sw  # sw = effective n

# ---------- model builder ----------

def build_user_model(ratings_csv: Path, out_dir: Path) -> Dict[str, Any]:
    """
    Build a compact user model:
      - baseline averages (global, by media type)
      - affinities for: directors, genres, title_type, runtime_bucket, era
    Writes: out_dir/user_model.json and out_dir/profile_report.md
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    if not ratings_csv.exists():
        model = {"meta": {"count": 0, "note": "ratings.csv missing"}}
        (out_dir / "user_model.json").write_text("{}", encoding="utf-8")
        return model

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

            rows.append({
                "rating": rating, "w": w, "title_type": title_type,
                "year": year, "runtime": runtime,
                "directors": directors, "genres": genres,
            })

    # baselines
    global_avg, global_n = _weighted_avg([Obs(r["rating"], r["w"]) for r in rows])
    by_type: Dict[str, Tuple[float, float]] = {}
    for t in set(r["title_type"] for r in rows):
        obs = [Obs(r["rating"], r["w"]) for r in rows if r["title_type"] == t]
        by_type[t] = _weighted_avg(obs)

    # collect token observations
    def collect_token_affinity(get_tokens, label: str) -> Dict[str, float]:
        tok_obs: Dict[str, List[Obs]] = {}
        for r in rows:
            toks = get_tokens(r)
            if not toks:
                continue
            for t in toks:
                tok_obs.setdefault(t, []).append(Obs(r["rating"], r["w"]))

        weights: Dict[str, float] = {}
        for tok, obs in tok_obs.items():
            avg, n_eff = _weighted_avg(obs)
            shrink = n_eff / (n_eff + AFFINITY_K)
            weights[tok] = (avg - global_avg) * shrink
        return weights

    directors_w = collect_token_affinity(lambda r: r["directors"], "directors")
    genres_w    = collect_token_affinity(lambda r: r["genres"], "genres")
    runtime_w   = collect_token_affinity(lambda r: [_bucket_runtime(r["runtime"])], "runtime")
    era_w       = collect_token_affinity(lambda r: [_bucket_era(r["year"])], "era")
    type_w      = collect_token_affinity(lambda r: [r["title_type"].strip() or "unknown"], "title_type")

    model: Dict[str, Any] = {
        "meta": {
            "count": len(rows),
            "global_avg": round(global_avg, 4),
            "AFFINITY_K": AFFINITY_K,
            "DECAY_HALF_LIFE_DAYS": DECAY_HALF_LIFE_DAYS,
            "by_type": {k: {"avg": round(v[0], 4), "n_eff": round(v[1], 3)} for k, v in by_type.items()},
        },
        "people": {"director": directors_w},   # you can add writer/actor later if present
        "form": {
            "runtime_bucket": runtime_w,
            "title_type": type_w,
            "era": era_w,
        },
        "genres": genres_w,
        # stubs for future signals; scoring handles missing gracefully:
        "language": {},
        "country": {},
        "studio": {},
        "network": {},
        "keywords": {},
        "provider": {},  # small priors can be learned later
    }

    # Write artifacts
    (out_dir / "user_model.json").write_text(
        __import__("json").dumps(model, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # human summary
    def topn(d: Dict[str, float], n=12):
        return "\n".join(f"- {k}: {round(v,2)}" for k, v in sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n])

    report = [
        "# User model",
        f"- rows: {len(rows)}",
        f"- global_avg: {round(global_avg,2)}",
        "## Directors (top +)",
        topn(directors_w),
        "## Genres (top +)",
        topn(genres_w),
        "## Runtime bucket (signal)",
        topn(runtime_w),
        "## Era (signal)",
        topn(era_w),
        "## Title type (signal)",
        topn(type_w),
    ]
    (out_dir / "profile_report.md").write_text("\n".join(report), encoding="utf-8")

    return model