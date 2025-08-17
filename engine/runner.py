from __future__ import annotations
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

from .env import from_os_env, Env
from .catalog_builder import build_catalog

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY")  # GH Actions sets this
OUT_DIR = Path("data/out/latest")
CACHE_DIR = Path("data/cache")
RATINGS_PATHS = [
    Path("data/ratings.csv"),
    Path("data/user/ratings.csv"),
]  # we’ll try these in order


# ---------- ratings loader (flexible) ----------

_TCONST_RE = re.compile(r"(tt\d+)")
_URL_TCONST_RE = re.compile(r"tt\d+")

def _extract_tt_from_url(s: str) -> Optional[str]:
    if not isinstance(s, str):
        return None
    m = _URL_TCONST_RE.search(s)
    return m.group(0) if m else None

def _normalize_imdb_id(x: Any) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    m = _TCONST_RE.search(s)
    return m.group(1) if m else None

POSSIBLE_TCONST_COLUMNS = [
    "const", "tconst", "imdb_id", "IMDbID", "imdbId", "titleId", "TitleId",
    "url", "URL", "IMDb URL",
]

def load_local_ratings() -> Tuple[Set[str], Dict[str, Any]]:
    """
    Return: (seen_imdb_ids, diagnostics)
    - supports many column names
    - logs helpful diagnostics for GH summary
    """
    diags: Dict[str, Any] = {"path_checked": [], "found": False}
    for p in RATINGS_PATHS:
        diags["path_checked"].append(str(p))
        if p.exists() and p.is_file():
            try:
                df = pd.read_csv(p)
            except Exception as ex:
                diags["error"] = f"Failed to read {p}: {ex!r}"
                continue

            diags["found"] = True
            diags["columns"] = list(df.columns)
            diags["n_rows"] = int(len(df))

            # Try to find a tconst column
            col: Optional[str] = None
            for c in POSSIBLE_TCONST_COLUMNS:
                if c in df.columns:
                    col = c
                    break

            ids: Set[str] = set()
            if col:
                diags["chosen_column"] = col
                series = df[col].astype(str)
                # If the chosen column looks like a URL, extract tt
                if col.lower() in {"url", "imdb url"}:
                    series = series.map(_extract_tt_from_url)
                else:
                    series = series.map(_normalize_imdb_id)
                ids = {s for s in series.dropna().astype(str) if s.startswith("tt")}
            else:
                # last-ditch: scan all columns, pick tt-like tokens
                diags["chosen_column"] = None
                for c in df.columns:
                    cand = df[c].astype(str).map(_normalize_imdb_id)
                    ids.update(s for s in cand.dropna().astype(str) if s.startswith("tt"))

            diags["seen_count"] = len(ids)
            diags["sample_first_5"] = sorted(list(ids))[:5]
            diags["sample_last_5"] = sorted(list(ids))[-5:]
            return ids, diags

    # Not found anywhere
    diags["seen_count"] = 0
    return set(), diags


# ---------- summary & debug helpers ----------

def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _append_step_summary(md: str) -> None:
    if not STEP_SUMMARY:
        return
    with open(STEP_SUMMARY, "a", encoding="utf-8") as f:
        f.write(md.rstrip() + "\n")

def _int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


# ---------- main ----------

def main() -> None:
    print(" | catalog:begin")
    env: Env = from_os_env()

    # build discovery pool (raw list of items)
    items: List[Dict[str, Any]] = build_catalog(env)
    print(f" | catalog:end kept={len(items)}")

    # exclusions – local ratings first
    seen_local, ratings_diag = load_local_ratings()

    # (Optional) extend with imdb web history if you have a fetcher; left as no-op.
    seen_all = set(seen_local)

    # Apply exclusions by imdb_id if available, otherwise by tmdb_id as fallback
    def _is_seen(it: Dict[str, Any]) -> bool:
        imdb_id = it.get("imdb_id")
        if imdb_id and imdb_id in seen_all:
            return True
        # no imdb_id? treat as unseen
        return False

    eligible = [it for it in items if not _is_seen(it)]

    # Score / match-cut gate — assume a precomputed `match` if your pipeline adds it,
    # else neutral 0. We don’t invent a model here—just pass-through.
    # (If you do have a scorer elsewhere, this will still preserve the threshold.)
    min_cut = float(getattr(env, "MIN_MATCH_CUT", 58.0) or 58.0)
    above_cut = [it for it in eligible if float(it.get("match", 0)) >= min_cut]

    # telemetry object for summary/debug
    telemetry = {
        "Region": env.get("REGION", "US"),
        "SUBS_INCLUDE": env.get("SUBS_INCLUDE", []),
        "Discover pages": env.get("DISCOVER_PAGES", 3),
        "Discovered (raw)": len(items),
        "Excluded for being seen": len(items) - len(eligible),
        "Eligible after exclusions": len(eligible),
        "Above match cut (≥ {:.1f})".format(min_cut): len(above_cut),
    }

    # write assistant_feed.json (for your debugging)
    _write_json(OUT_DIR / "assistant_feed.json", eligible)

    # write diagnostics pack (small, separate from big “out.zip”)
    debug_payload = {
        "telemetry": telemetry,
        "ratings_diagnostics": ratings_diag,
        "min_match_cut": min_cut,
        "eligible_sample_first_5": eligible[:5],
    }
    _write_json(OUT_DIR / "debug/diagnostics.json", debug_payload)

    # GH step summary
    _append_step_summary("# Daily recommendations\n")
    _append_step_summary("## Telemetry\n")
    _append_step_summary(f"- Region: **{telemetry['Region']}**")
    _append_step_summary(f"- SUBS_INCLUDE: `{','.join(telemetry['SUBS_INCLUDE'])}`")
    _append_step_summary(f"- Discover pages: **{telemetry['Discover pages']}**")
    _append_step_summary(f"- Discovered (raw): **{telemetry['Discovered (raw)']}**")
    _append_step_summary(f"- Excluded for being seen: **{telemetry['Excluded for being seen']}**")
    _append_step_summary(f"- Eligible after exclusions: **{telemetry['Eligible after exclusions']}**")
    _append_step_summary(f"- Above match cut (≥ {min_cut:.1f}): **{telemetry[f'Above match cut (≥ {min_cut:.1f})']}**")

    # Also surface ratings.csv diagnostics so you can confirm the column being used
    _append_step_summary("\n## ratings.csv diagnostics\n")
    if ratings_diag.get("found"):
        _append_step_summary(f"- Found: **True**")
        _append_step_summary(f"- Path tried: `{', '.join(ratings_diag.get('path_checked', []))}`")
        _append_step_summary(f"- Columns: `{', '.join(ratings_diag.get('columns', []))}`")
        _append_step_summary(f"- Rows: **{ratings_diag.get('n_rows', 0)}**")
        _append_step_summary(f"- Chosen column: `{ratings_diag.get('chosen_column')}`")
        _append_step_summary(f"- Seen (unique) count: **{ratings_diag.get('seen_count', 0)}**")
        _append_step_summary(f"- Samples: `{ratings_diag.get('sample_first_5', [])}` … `{ratings_diag.get('sample_last_5', [])}`")
    else:
        _append_step_summary(f"- Found: **False**")
        _append_step_summary(f"- Paths tried: `{', '.join(ratings_diag.get('path_checked', []))}`")

    # Write a tiny summary.md for artifact viewers
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with (OUT_DIR / "summary.md").open("w", encoding="utf-8") as f:
        f.write("# Daily recommendations\n\n")
        f.write(json.dumps(telemetry, indent=2))
        f.write("\n")

    # Exit code 0 even if none above cut; the job should succeed as long as the pipeline ran.
    # If you want to fail on empty results, flip this condition.
    if False and not above_cut:
        raise SystemExit(2)


if __name__ == "__main__":
    main()