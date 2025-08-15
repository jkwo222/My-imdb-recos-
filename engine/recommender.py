# engine/recommender.py
from typing import List, Dict, Any

def _to_float(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

def score(item: Dict[str, Any], w: Dict[str, Any]) -> float:
    """
    Personalized score driven by OMDb + your weights.
    Range-bounded to 60–98.
    """
    rt = _to_float(item.get("rt_pct"), 0.0) / 100.0          # 0..1
    imdb = _to_float(item.get("imdb_rating"), 0.0) / 10.0    # 0..1

    # if either missing, backfill lightly so everything isn't flat 62.
    if rt == 0 and imdb == 0:
        rt = 0.50
        imdb = 0.55

    critic_w = float(w.get("critic_weight", 0.5))
    aud_w = float(w.get("audience_weight", 0.5))
    base = 60.0
    blended = critic_w * rt + aud_w * imdb
    s = base + 36.0 * blended   # 60..96 typically

    # commitment cost (multi-season series you've not seen)
    if item.get("type") == "tvSeries":
        seasons = int(item.get("seasons") or 1)
        if seasons >= 3:
            s -= 9.0 * float(w.get("commitment_cost_scale", 1.0))
        elif seasons == 2:
            s -= 4.0 * float(w.get("commitment_cost_scale", 1.0))

    # tiny quality boost for big RT (signal)
    if item.get("rt_pct", 0) >= 90:
        s += 1.0

    # clamp
    if s < 60.0: s = 60.0
    if s > 98.0: s = 98.0
    return round(s, 1)

def reason(item: Dict[str, Any]) -> str:
    bits = []
    if item.get("lang_is_english"): bits.append("English-language")
    if item.get("rt_pct"): bits.append(f"RT {int(item['rt_pct'])}%")
    if item.get("imdb_rating"): bits.append(f"IMDb {item['imdb_rating']}/10")
    g = (item.get("omdb", {}) or {}).get("genres")
    if g: bits.append(g)
    cert = item.get("cert")
    if cert: bits.append(cert)
    if item.get("type") == "tvSeries" and item.get("seasons"):
        bits.append(f"{int(item['seasons'])} seasons")
    return " • ".join(bits) or "Signals matched your taste profile"

def recommend(catalog: List[Dict[str, Any]], w: Dict[str, Any], seen_checker) -> List[Dict[str, Any]]:
    out = []
    for c in catalog:
        # final filter on type (allow movies, series, miniseries, TV movies/specials)
        if c.get("type") not in ("movie","tvSeries","tvMiniSeries","tvMovie","tvSpecial"):
            continue
        # skip seen
        if seen_checker(c):
            continue
        x = dict(c)
        x["match"] = score(x, w)
        x["why"] = reason(x)
        out.append(x)
    out.sort(key=lambda z: z["match"], reverse=True)
    return out[:50]