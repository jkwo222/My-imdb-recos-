# FILE: engine/feed.py
from __future__ import annotations
from typing import Dict, List, Tuple

_SLUGS = {
    "netflix": "netflix",
    "prime video": "prime_video",
    "amazon prime video": "prime_video",
    "amazon video": "prime_video",
    "hulu": "hulu",
    "max": "max",
    "hbo max": "max",
    "disney plus": "disney_plus",
    "disney+": "disney_plus",
    "apple tv plus": "apple_tv_plus",
    "apple tv+": "apple_tv_plus",
    "apple tv": "apple_tv_plus",
    "peacock": "peacock",
    "paramount plus": "paramount_plus",
    "paramount+": "paramount_plus",
}

def _slugify(name: str) -> str:
    if not name: return ""
    k = name.strip().lower()
    return _SLUGS.get(k, k.replace(" ", "_"))

def _providers_set(item) -> set:
    prov = item.get("providers") or []
    return { _slugify(p) for p in prov }

def filter_by_providers(pool: List[Dict], allowed: List[str]) -> List[Dict]:
    if not allowed: return []
    allowed_set = { _slugify(s) for s in allowed }
    out = []
    for it in pool:
        if _providers_set(it) & allowed_set:
            out.append(it)
    return out

def _pick_critic(item: Dict) -> float:
    if isinstance(item.get("rt_rating"), (int, float)) and item["rt_rating"] > 0:
        return float(item["rt_rating"]) / 100.0
    if isinstance(item.get("critic"), (int, float)) and 0 < item["critic"] <= 1.0:
        return float(item["critic"])
    if isinstance(item.get("tmdb_vote"), (int, float)) and item["tmdb_vote"] > 0:
        return float(item["tmdb_vote"]) / 10.0
    return 0.0

def _pick_audience(item: Dict) -> float:
    if isinstance(item.get("imdb_rating"), (int, float)) and item["imdb_rating"] > 0:
        return float(item["imdb_rating"]) / 10.0
    if isinstance(item.get("audience"), (int, float)) and 0 < item["audience"] <= 1.0:
        return float(item["audience"])
    if isinstance(item.get("tmdb_vote"), (int, float)) and item["tmdb_vote"] > 0:
        return float(item["tmdb_vote"]) / 10.0
    return 0.0

def _commitment_penalty(item: Dict, scale: float) -> float:
    t = (item.get("type") or "").strip()
    if t != "tvSeries": 
        return 0.0
    seasons = int(item.get("seasons") or 1)
    if seasons >= 3: return 0.09 * scale
    if seasons == 2: return 0.04 * scale
    return 0.0

def score_items(items: List[Dict], weights: Dict[str, float]) -> List[Dict]:
    cw = float(weights.get("critic_weight", 0.35))
    aw = float(weights.get("audience_weight", 0.65))
    cc = float(weights.get("commitment_cost_scale", 1.0))
    base_anchor = 0.60
    out = []
    for it in items:
        critic = _pick_critic(it)
        audience = _pick_audience(it)
        raw = cw * critic + aw * audience
        penalty = _commitment_penalty(it, cc)
        s = base_anchor + 0.20 * max(0.0, raw - penalty)
        match = round(max(55.0, min(98.0, s * 100.0)), 1)
        x = dict(it)
        x["match"] = match
        x["_score_parts"] = {"critic": critic, "audience": audience, "penalty": penalty}
        out.append(x)
    out.sort(key=lambda r: r["match"], reverse=True)
    return out

def _why(item: Dict) -> str:
    sp = item.get("_score_parts") or {}
    prov = ", ".join(sorted(_providers_set(item))) or "—"
    critic_pct = int(round(100.0 * float(sp.get("critic", 0.0))))
    audience_10 = round(10.0 * float(sp.get("audience", 0.0)), 1)
    bits = []
    if critic_pct > 0: bits.append(f"Critic ~{critic_pct}%")
    if audience_10 > 0: bits.append(f"IMDb ~{audience_10}/10")
    bits.append(f"On: {prov}")
    return "; ".join(bits)

def _split_top(items: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    movies = [r for r in items if (r.get("type") or "movie") == "movie"]
    series = [r for r in items if (r.get("type") or "") in ("tvSeries", "tvMiniSeries")]
    return movies[:10], series[:10]

def top10_by_type(scored: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    return _split_top(scored)

def to_markdown(movies: List[Dict], series: List[Dict], weights: Dict[str, float], meta: Dict) -> str:
    def rowfmt(r: Dict) -> str:
        yr = f" ({r.get('year')})" if r.get("year") else ""
        return f"- **{r.get('title','?')}{yr}** — {r.get('match',0)}\n  - {_why(r)}"
    lines = []
    lines.append("# Daily Recommendations")
    lines.append("")
    lines.append("## Top 10 Movies")
    lines.extend([rowfmt(r) for r in movies] or ["_No movie picks today._"])
    lines.append("")
    lines.append("## Top 10 Series")
    lines.extend([rowfmt(r) for r in series] or ["_No series picks today._"])
    lines.append("")
    lines.append("## Telemetry")
    lines.append(f"- Pool sizes: {meta.get('pool_sizes')}")
    lines.append(f"- Weights: {weights}")
    lines.append(f"- Subs: {', '.join(meta.get('subs') or [])}")
    return "\n".join(lines)