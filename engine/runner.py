from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

# =============================================================================
# External engine pieces
# =============================================================================
# Expect these to exist in your repo as before.
# - build_catalog(env) returns a list[dict] of items discovered+enriched
#   (we treat whatever it returns as "enriched items").
try:
    from .catalog_builder import build_catalog  # type: ignore
except Exception as e:
    print(f"[runner] Failed to import catalog_builder.build_catalog: {e}", file=sys.stderr)
    raise

# =============================================================================
# Environment, paths, and small utilities
# =============================================================================

@dataclass
class Env:
    tmdb_api_key: Optional[str]
    tmdb_access_token: Optional[str]
    imdb_user_id: Optional[str]
    region: str
    original_langs: List[str]
    subs_include: List[str]
    min_match_cut: float
    discover_pages: int

    out_dir: Path
    out_latest: Path

    # Where we try to read local ratings (both supported)
    ratings_user_csv: Path   # data/user/ratings.csv
    ratings_csv: Path        # data/ratings.csv


def _get_env() -> Env:
    region = os.environ.get("REGION", "US").strip() or "US"
    langs = os.environ.get("ORIGINAL_LANGS", "en").strip()
    langs_list = [s.strip() for s in langs.split(",") if s.strip()]
    subs = os.environ.get(
        "SUBS_INCLUDE",
        "netflix,prime_video,hulu,max,disney_plus,apple_tv_plus,peacock,paramount_plus",
    )
    subs_list = [s.strip() for s in subs.split(",") if s.strip()]
    min_cut = float(os.environ.get("MIN_MATCH_CUT", "58"))
    pages = int(os.environ.get("DISCOVER_PAGES", "3"))

    out_dir = Path("data/out")
    out_latest = out_dir / "latest"

    env = Env(
        tmdb_api_key=os.environ.get("TMDB_API_KEY"),
        tmdb_access_token=os.environ.get("TMDB_ACCESS_TOKEN"),
        imdb_user_id=os.environ.get("IMDB_USER_ID"),
        region=region,
        original_langs=langs_list,
        subs_include=subs_list,
        min_match_cut=min_cut,
        discover_pages=pages,
        out_dir=out_dir,
        out_latest=out_latest,
        ratings_user_csv=Path("data/user/ratings.csv"),
        ratings_csv=Path("data/ratings.csv"),
    )
    return env


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Ratings loader / exclusion set (robust against different schemas)
# =============================================================================

IMDB_TCONST_RE = re.compile(r"(tt\d{7,})", re.IGNORECASE)

def _normalize_imdb_id_series(s: Optional[pd.Series]) -> pd.Series:
    """Extract/normalize IMDb tconsts from a variety of possible values or URLs."""
    if s is None:
        return pd.Series(dtype="string")
    return (
        s.astype(str)
         .str.extract(IMDB_TCONST_RE, expand=False)
         .str.lower()
    )

def _load_local_ratings_any(csv_path: Path) -> pd.DataFrame:
    """Load a single CSV path if present; return normalized df with 'tconst' and optional 'my_rating'."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return pd.DataFrame(columns=["tconst"])
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame(columns=["tconst"])

    colmap = {c.lower().strip(): c for c in df.columns}

    # Direct ID candidates
    direct_candidates = ["const", "tconst", "imdb_id", "id", "titleid"]
    direct_col = next((colmap[k] for k in direct_candidates if k in colmap), None)

    # URL candidates
    url_candidates = ["imdb_url", "url", "link", "permalink"]
    url_col = next((colmap[k] for k in url_candidates if k in colmap), None)

    tconst_series = None
    if direct_col:
        tconst_series = _normalize_imdb_id_series(df[direct_col])
    elif url_col:
        tconst_series = _normalize_imdb_id_series(df[url_col])
    else:
        # last resort: scan all columns for a tt-id
        for c in df.columns:
            tmp = _normalize_imdb_id_series(df[c])
            if tmp.notna().any():
                tconst_series = tmp
                break

    if tconst_series is None:
        return pd.DataFrame(columns=["tconst"])

    out = pd.DataFrame({"tconst": tconst_series})
    # adopt a rating column if present
    rating_col = next(
        (colmap[k] for k in ["my rating", "rating", "user_rating", "score"] if k in colmap),
        None,
    )
    if rating_col:
        out["my_rating"] = pd.to_numeric(df[rating_col], errors="coerce")

    return out.dropna(subset=["tconst"]).drop_duplicates(subset=["tconst"])


def load_local_ratings(ratings_user_csv: Path, ratings_csv: Path) -> pd.DataFrame:
    """Try user CSV first, then fallback to data/ratings.csv; merge & de-dup."""
    a = _load_local_ratings_any(ratings_user_csv)
    b = _load_local_ratings_any(ratings_csv)
    if a.empty and b.empty:
        return pd.DataFrame(columns=["tconst"])
    if a.empty:
        return b
    if b.empty:
        return a
    # merge and de-dup by tconst; prefer ratings from 'a' when both exist
    out = pd.concat([a, b], ignore_index=True)
    out = out.drop_duplicates(subset=["tconst"], keep="first")
    return out


def _exclusion_set(local_csv_a: Path, local_csv_b: Path, imdb_web_remote: Optional[pd.DataFrame] = None) -> set[str]:
    """
    Build a set of IMDb tconsts that should be excluded (already seen/rated).
    - Local CSVs: normalize and add their tconsts
    - imdb_web_remote (optional): normalize if provided
    """
    seen: set[str] = set()

    # Local ratings
    local_df = load_local_ratings(local_csv_a, local_csv_b)
    if not local_df.empty and "tconst" in local_df.columns:
        seen.update(x for x in local_df["tconst"].dropna().astype(str))

    # IMDb web scrape (optional)
    if imdb_web_remote is not None and not imdb_web_remote.empty:
        candidates = [
            c for c in imdb_web_remote.columns
            if c.lower() in {"tconst", "const", "imdb_id", "id", "url", "imdb_url", "titleid"}
        ]
        series = None
        for col in (candidates or imdb_web_remote.columns.tolist()):
            s = _normalize_imdb_id_series(imdb_web_remote[col])
            if s.notna().any():
                series = s
                break
        if series is not None:
            seen.update(x for x in series.dropna().astype(str))

    return seen


# =============================================================================
# Scoring / selection
# =============================================================================

def _tmdb_vote_score(item: Dict[str, Any]) -> float:
    """Simple, transparent baseline score on 0..100 based on TMDB vote average."""
    try:
        v = float(item.get("tmdb_vote", 0.0))
    except Exception:
        v = 0.0
    # Scale 0..10 → 0..100; clamp to [0, 100]
    return max(0.0, min(100.0, v * 10.0))


def _calc_scores(items: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], float]]:
    return [(it, _tmdb_vote_score(it)) for it in items]


def _top_by_cut(items_with_scores: List[Tuple[Dict[str, Any], float]], cut: float) -> List[Tuple[Dict[str, Any], float]]:
    return [(it, s) for (it, s) in items_with_scores if s >= cut]


# =============================================================================
# Telemetry / outputs
# =============================================================================

def _write_summary_md(
    env: Env,
    discovered_raw: int,
    enriched_count: int,
    enrich_errors: int,
    exclusion_size: int,
    excluded_seen: int,
    eligible_after_exclusions: int,
    above_cut_count: int,
    out_path: Path,
) -> None:
    lines = []
    lines.append("# Daily recommendations\n")
    lines.append("## Telemetry\n")
    lines.append(f"- Region: **{env.region}**")
    lines.append(f"- SUBS_INCLUDE: `{','.join(env.subs_include)}`")
    lines.append(f"- Discover pages: **{env.discover_pages}**")
    lines.append(f"- Discovered (raw): **{discovered_raw}**")
    lines.append(f"- Enriched (details fetched): **{enriched_count}**; errors: **{enrich_errors}**")
    lines.append(f"- Exclusion list size (ratings + IMDb web): **{exclusion_size}**")
    lines.append(f"- Excluded for being seen: **{excluded_seen}**")
    lines.append(f"- Eligible after exclusions: **{eligible_after_exclusions}**")
    lines.append(f"- Above match cut (≥ {env.min_match_cut:.1f}): **{above_cut_count}**\n")

    # If we want to show whether we computed genre weights, we can only infer by presence of ratings
    local = load_local_ratings(env.ratings_user_csv, env.ratings_csv)
    if local.empty:
        lines.append("## Your profile: genre weights\n_No genre weights computed (no ratings.csv?)._\n")
    else:
        lines.append("## Your profile: genre weights\n_(not shown in this summary; ratings file detected and used for exclusions.)_\n")

    if above_cut_count == 0:
        lines.append("_No items above cut today._\n")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def _write_assistant_feed(feed_items: List[Dict[str, Any]], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(feed_items, f, ensure_ascii=False, indent=2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    env = _get_env()
    _ensure_dirs(env.out_dir, env.out_latest)

    # -----------------------------------------------------------------------------
    # 1) Discover + enrich catalog
    # -----------------------------------------------------------------------------
    print(" | catalog:begin")
    items: List[Dict[str, Any]] = build_catalog(env)  # expected to use env.* (region, langs, providers, etc.)
    # We don’t know separate “raw vs enriched” from build_catalog, so we assume
    # items returned == enriched set and 0 errors unless your builder tells us otherwise.
    discovered_raw = len(items)
    enriched_count = len(items)
    enrich_errors = 0
    kept_initial = len(items)
    print(f" | catalog:end kept={kept_initial}")

    # -----------------------------------------------------------------------------
    # 2) Exclusions (local ratings + optional IMDb web)
    # -----------------------------------------------------------------------------
    imdb_web_df = None  # if you have a scraper, plug it here; normalization is handled
    seen_tconst = _exclusion_set(env.ratings_user_csv, env.ratings_csv, imdb_web_df)
    exclusion_size = len(seen_tconst)

    # Build a quick index on item imdb ids (normalize to tconst)
    def item_tconst(it: Dict[str, Any]) -> Optional[str]:
        raw = it.get("imdb_id") or it.get("imdbId") or it.get("imdb") or it.get("url")
        if not raw:
            return None
        m = IMDB_TCONST_RE.search(str(raw))
        return m.group(1).lower() if m else None

    before = len(items)
    filtered: List[Dict[str, Any]] = []
    for it in items:
        t = item_tconst(it)
        if t and t in seen_tconst:
            continue
        filtered.append(it)
    excluded_seen = before - len(filtered)
    eligible_after_exclusions = len(filtered)

    # -----------------------------------------------------------------------------
    # 3) Score + select
    # -----------------------------------------------------------------------------
    with_scores = _calc_scores(filtered)
    above_cut = _top_by_cut(with_scores, env.min_match_cut)

    # Sort by score desc, then tmdb_vote desc, then title asc for stability
    above_cut_sorted = sorted(
        above_cut,
        key=lambda x: (x[1], float(x[0].get("tmdb_vote", 0.0)), str(x[0].get("title", ""))),
        reverse=True,
    )
    above_cut_items = [it for (it, s) in above_cut_sorted]
    above_cut_count = len(above_cut_items)

    # -----------------------------------------------------------------------------
    # 4) Outputs
    # -----------------------------------------------------------------------------
    summary_md = env.out_latest / "summary.md"
    _write_summary_md(
        env=env,
        discovered_raw=discovered_raw,
        enriched_count=enriched_count,
        enrich_errors=enrich_errors,
        exclusion_size=exclusion_size,
        excluded_seen=excluded_seen,
        eligible_after_exclusions=eligible_after_exclusions,
        above_cut_count=above_cut_count,
        out_path=summary_md,
    )

    # assistant_feed.json: include the items above cut; if none, include the entire eligible
    feed = above_cut_items if above_cut_items else filtered
    # Trim to a sensible size (e.g., 120 max)
    if len(feed) > 120:
        feed = feed[:120]

    assistant_feed = env.out_latest / "assistant_feed.json"
    _write_assistant_feed(feed, assistant_feed)

    # Helpful stdout diagnostics
    print(f"[runner] time={_now_iso()} region={env.region} pages={env.discover_pages} min_cut={env.min_match_cut}")
    print(f"[runner] discovered={discovered_raw} enriched={enriched_count} errors={enrich_errors}")
    print(f"[runner] exclusions_size={exclusion_size} excluded_seen={excluded_seen} eligible={eligible_after_exclusions}")
    print(f"[runner] above_cut={above_cut_count} written: summary={summary_md} feed={assistant_feed}")


if __name__ == "__main__":
    # align with the style in your logs (tracebacks already show correct lines)
    main()