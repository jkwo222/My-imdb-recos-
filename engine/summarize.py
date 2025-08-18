# engine/summarize.py
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ALLOWED_SUBS = {
    "apple_tv_plus", "netflix", "max", "paramount_plus",
    "disney_plus", "peacock", "hulu",
}

FRIENDLY = {
    "apple_tv_plus": "Apple TV+",
    "netflix": "Netflix",
    "max": "Max",
    "paramount_plus": "Paramount+",
    "disney_plus": "Disney+",
    "peacock": "Peacock",
    "hulu": "Hulu",
    # extra slugs
    "prime_video": "Prime Video",
}

def _load_json(p: Optional[Path]) -> Any:
    if not p:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

def _as_list(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v) for v in x]
    if isinstance(x, str):
        return [s.strip() for s in x.split(",") if s.strip()]
    return [str(x)]

def _normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    if s in {"hbo_max", "hbomax", "hbo"}:
        return "max"
    return s

def _score(it: Dict[str, Any]) -> float:
    try:
        return float(it.get("match", it.get("score", 0.0)) or 0.0)
    except Exception:
        return 0.0

def _aud_0_100(it: Dict[str, Any]) -> float:
    for k in ("audience", "tmdb_vote"):
        v = it.get(k)
        try:
            f = float(v)
        except Exception:
            continue
        if f <= 10.0:
            f *= 10.0
        return max(0.0, min(100.0, f))
    return 50.0

def _providers_slugs(it: Dict[str, Any]) -> List[str]:
    p = it.get("providers") or it.get("providers_slugs") or []
    return [s for s in p if isinstance(s, str)]

def _allowed_from_env(diag_env: Dict[str, Any]) -> List[str]:
    subs = [_normalize_slug(s) for s in _as_list(diag_env.get("SUBS_INCLUDE"))]
    if not subs:
        subs = list(ALLOWED_SUBS)
    allowed = [s for s in subs if s in ALLOWED_SUBS]
    return allowed or list(ALLOWED_SUBS)

def _eligible_item(it: Dict[str, Any], allowed: List[str]) -> bool:
    provs = [_normalize_slug(p) for p in _providers_slugs(it)]
    if not provs:
        return False
    allowed_set = set(allowed)
    return any(p in allowed_set for p in provs)

def _fmt_providers_bold(it: Dict[str, Any], allowed: List[str], maxn: int = 3) -> Optional[str]:
    provs = [_normalize_slug(p) for p in _providers_slugs(it)]
    if not provs:
        return None
    kept = [p for p in provs if p in set(allowed)]
    if not kept:
        return None
    names = [FRIENDLY.get(p, " ".join(w.capitalize() for w in p.split("_"))) for p in kept]
    names = [f"**{n}**" for n in names]
    short = names[:maxn]
    return ", ".join(short) + ("…" if len(names) > maxn else "")

def _imdb_link(it: Dict[str, Any]) -> Optional[str]:
    imdb = it.get("imdb_id")
    return f"https://www.imdb.com/title/{imdb}/" if isinstance(imdb, str) and imdb else None

def _tmdb_link(it: Dict[str, Any]) -> Optional[str]:
    tid = it.get("tmdb_id")
    kind = (it.get("media_type") or "").lower()
    if not tid or not kind:
        return None
    try:
        tid = int(tid)
    except Exception:
        return None
    return f"https://www.themoviedb.org/{'movie' if kind=='movie' else 'tv'}/{tid}"

def _emoji_for_kind(kind: str) -> str:
    return "🍿" if (kind or "").lower() == "movie" else "📺"

def _pick_top(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(items, key=_score, reverse=True)[:n]

# ---- FINAL GUARD: consult exported seen_index.json ---------------------------

def _load_seen_guard(diag: Dict[str, Any]) -> Dict[str, set]:
    paths = (diag or {}).get("paths", {}) if isinstance(diag, dict) else {}
    exp = paths.get("seen_index_json")
    if not exp:
        return {"ids": set(), "tkeys": set()}
    p = Path(exp)
    data = _load_json(p) or {}
    ids = set(data.get("imdb_ids") or [])
    tkeys = set(data.get("title_year_keys") or [])
    return {"ids": ids, "tkeys": tkeys}

def _title_year_key_norm(it: Dict[str, Any]) -> str:
    t = (it.get("title") or it.get("name") or "").strip().lower()
    t = "".join(ch if ch.isalnum() else " " for ch in t)
    t = " ".join(w for w in t.split() if w not in {"the","a","an","and","of","part"})
    y = str(it.get("year") or "").strip()
    return f"{t}::{y}"

def _guard_seen(it: Dict[str, Any], guard: Dict[str, set]) -> bool:
    imdb = it.get("imdb_id")
    if imdb and imdb in guard["ids"]:
        return True
    key = _title_year_key_norm(it)
    if key in guard["tkeys"]:
        return True
    # year ±1 tolerance
    if "::" in key and key.endswith("::"):
        # no usable year, skip tol
        return False
    base, y = key.rsplit("::", 1)
    try:
        yi = int(y)
    except Exception:
        return False
    for dy in (-1, 1):
        if f"{base}::{yi+dy}" in guard["tkeys"]:
            return True
    return False

def _bullet(it: Dict[str, Any], allowed: List[str]) -> Optional[str]:
    title = it.get("title") or it.get("name") or "—"
    year = it.get("year") or ""
    sc = _score(it)
    aud = _aud_0_100(it)
    kind = it.get("media_type") or ""
    emoji = _emoji_for_kind(kind)

    prov_bold = _fmt_providers_bold(it, allowed, maxn=3)
    if not prov_bold:
        return None

    why = (it.get("why") or "").strip()
    links = []
    imdb = _imdb_link(it)
    tmdb = _tmdb_link(it)
    if imdb: links.append(f"[IMDb]({imdb})")
    if tmdb: links.append(f"[TMDB]({tmdb})")
    link_s = " • ".join(links)

    main = f"{emoji} **{title}** ({year}) — **Match {sc:.0f}** | Audience {aud:.0f} | {prov_bold}"
    if why:
        main += f" — _{why}_"
    if link_s:
        main += f" — {link_s}"
    return f"- {main}"

def build_digest(items: List[Dict[str, Any]], diag: Dict[str, Any], ratings_csv: Optional[Path], top_n: int = 12) -> str:
    env = (diag or {}).get("env", {}) if isinstance(diag, dict) else {}
    allowed = _allowed_from_env(env)

    region = env.get("REGION", "US")
    langs = _as_list(env.get("ORIGINAL_LANGS"))
    pages = env.get("DISCOVER_PAGES", 0)
    prov_map = env.get("PROVIDER_MAP", {})
    prov_unmatched = env.get("PROVIDER_UNMATCHED", [])
    pool_t = env.get("POOL_TELEMETRY", {}) or {}
    ran_at = (diag or {}).get("ran_at_utc")
    run_sec = (diag or {}).get("run_seconds")
    discovered = env.get("DISCOVERED_COUNT", None)
    eligible_pre = env.get("ELIGIBLE_COUNT", None)

    # 1) service filter
    filtered = [it for it in items if _eligible_item(it, allowed)]

    # 2) take top N
    picks = _pick_top(filtered, top_n)

    # 3) final guard: drop anything in seen_index.json (ids or title/year ±1)
    guard = _load_seen_guard(diag)
    guard_removed = 0
    guarded_picks: List[Dict[str, Any]] = []
    for it in picks:
        if _guard_seen(it, guard):
            guard_removed += 1
            continue
        guarded_picks.append(it)

    # 4) taste profile (optional)
    ratings_rows, genre_counter = (0, {})
    if ratings_csv and ratings_csv.exists():
        try:
            import re
            sep = re.compile(r"[|,/;+]")
            with ratings_csv.open("r", encoding="utf-8", errors="replace") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    ratings_rows += 1
                    gs = row.get("genres") or row.get("Genres") or ""
                    for tok in (t.strip() for t in sep.split(gs)):
                        if tok:
                            genre_counter[tok] = genre_counter.get(tok, 0) + 1
        except Exception:
            pass

    lines: List[str] = []
    lines.append(f"### 🎬 Top Picks ({region})\n")
    if ratings_rows:
        top_gen = ", ".join([f"{g}×{c}" for g, c in sorted(genre_counter.items(), key=lambda kv: kv[1], reverse=True)[:6]])
        lines.append(f"_Taste profile (from your ratings.csv, {ratings_rows} rows):_ {top_gen}\n")

    if guarded_picks:
        for it in guarded_picks:
            b = _bullet(it, allowed)
            if b:
                lines.append(b)
        lines.append("")
    else:
        lines.append("_No items to show on your services right now._\n")

    # Telemetry
    lines.append("### 📊 Telemetry")
    if ran_at is not None:
        lines.append(f"- Ran at (UTC): **{ran_at}**" + (f" — {run_sec:.1f}s" if isinstance(run_sec, (int, float)) else ""))
    lines.append(f"- Region: **{region}**")
    if langs:
        lines.append(f"- Original languages: `{', '.join(langs)}`")
    lines.append(f"- SUBS_INCLUDE (effective): `{', '.join(allowed)}`")
    lines.append(f"- Discover pages: **{pages}**")
    if discovered is not None:
        lines.append(f"- Discovered (raw): **{discovered}**")
    if eligible_pre is not None:
        lines.append(f"- Eligible before service filter: **{eligible_pre}**")
    lines.append(f"- Eligible after service filter: **{len(filtered)}**")
    lines.append(f"- Provider map: `{json.dumps(prov_map, ensure_ascii=False)}`")
    if prov_unmatched:
        lines.append(f"- Provider slugs not matched: `{prov_unmatched}`")

    # exclusions summary from runner
    excl = (diag or {}).get("env", {}).get("EXCLUSIONS", {})
    if excl:
        lines.append(f"- Excluded as seen (runner): **{excl.get('excluded_count', 0)}** "
                     f"(ratings_ids~{excl.get('ratings_rows', 0)}, public_ids={excl.get('public_ids', 0)})")
    # guard summary
    lines.append(f"- Guard removed in summary: **{guard_removed}**")

    if pool_t:
        before = pool_t.get("file_lines_before")
        after = pool_t.get("file_lines_after")
        appended = pool_t.get("appended_this_run")
        delta = (after - before) if isinstance(before, int) and isinstance(after, int) else appended
        lines.append(f"- Pool growth: **{('+'+str(delta)) if isinstance(delta, int) else '—'} this run**")
        lines.append(f"- Pool size (lines): **{before} → {after}**")
        lines.append(f"- Appended records this run: **{appended}**")
        lines.append(f"- Loaded unique from pool: **{pool_t.get('loaded_unique')}** / cap **{pool_t.get('pool_max_items')}**")
        if pool_t.get("unique_keys_est") is not None:
            lines.append(f"- Unique keys (est): **{pool_t.get('unique_keys_est')}**")
        if pool_t.get("prune_at"):
            lines.append(f"- Prune policy: prune_at={pool_t.get('prune_at')}, keep={pool_t.get('prune_keep')}")

    return "\n".join(lines).strip() + "\n"

def main() -> None:
    ap = argparse.ArgumentParser(description="Produce a compact Top Picks digest into summary.md")
    ap.add_argument("--in", dest="inp", required=True, help="items.enriched.json (or assistant_feed.json)")
    ap.add_argument("--diag", dest="diag", required=False, help="diag.json (runner telemetry)")
    ap.add_argument("--ratings", dest="ratings", required=False, help="data/user/ratings.csv (optional)")
    ap.add_argument("--out", dest="out", required=True, help="output markdown (summary.md)")
    ap.add_argument("--top", dest="top", type=int, default=12, help="Top-N picks to list")
    args = ap.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)
    diagp = Path(args.diag) if args.diag else None
    ratingsp = Path(args.ratings) if args.ratings else None

    items = _load_json(inp) or []
    if not isinstance(items, list):
        items = []
    diag = _load_json(diagp) if (diagp and diagp.exists()) else {}
    body = build_digest(items, diag, ratingsp if (ratingsp and ratingsp.exists()) else None, top_n=args.top)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(body, encoding="utf-8")

if __name__ == "__main__":
    main()