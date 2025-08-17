#!/usr/bin/env bash
set -euo pipefail

OUT_ZIP="debug-data.zip"
BUNDLE_DIR="debug-bundle"

rm -f "$OUT_ZIP"
rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR"

# -------- figure out the latest run dir robustly --------
resolve_run_dir() {
  local run_src=""
  if [[ -e "data/out/latest" ]]; then
    run_src="data/out/latest"
  fi
  if [[ -z "${run_src}" && -f "data/out/last_run_dir.txt" ]]; then
    run_src="$(cat data/out/last_run_dir.txt || true)"
  fi
  if [[ -z "${run_src}" ]]; then
    # fallback: newest run_*
    local newest
    newest=$(ls -dt data/out/run_* 2>/dev/null | head -n1 || true)
    if [[ -n "${newest}" ]]; then
      run_src="${newest}"
    fi
  fi
  echo "${run_src}"
}

RUN_DIR="$(resolve_run_dir)"
echo "Detected run dir: ${RUN_DIR:-<none>}"

# -------- copy last run artifacts --------
if [[ -n "${RUN_DIR}" && -d "${RUN_DIR}" ]]; then
  for f in runner.log assistant_feed.json items.discovered.json items.enriched.json summary.md diag.json; do
    cp -f "${RUN_DIR}/${f}" "$BUNDLE_DIR/" 2>/dev/null || true
  done
else
  echo "WARN: No run directory found; bundle will be minimal" | tee "$BUNDLE_DIR/_warning.txt"
fi

# -------- env capture (no secret values) --------
{
  echo "REGION=${REGION:-}"
  echo "SUBS_INCLUDE=${SUBS_INCLUDE:-}"
  echo "ORIGINAL_LANGS=${ORIGINAL_LANGS:-}"
  echo "DISCOVER_PAGES=${DISCOVER_PAGES:-}"
} > "$BUNDLE_DIR/env.txt"

{
  for k in TMDB_API_KEY TMDB_BEARER IMDB_USER_ID IMDB_RATINGS_CSV_PATH; do
    v="${!k:-}"
    if [[ -n "${v}" ]]; then echo "$k=<set>"; else echo "$k=<missing>"; fi
  done
  echo "REGION=${REGION:-}"
  echo "SUBS_INCLUDE=${SUBS_INCLUDE:-}"
  echo "ORIGINAL_LANGS=${ORIGINAL_LANGS:-}"
  echo "DISCOVER_PAGES=${DISCOVER_PAGES:-}"
} > "$BUNDLE_DIR/env-sanitized.txt"

# -------- git + listings + symlink diag --------
{
  echo "== git status -sb =="; git status -sb || true
  echo; echo "== git log -1 =="; git log -1 --oneline --decorate || true
  echo; echo "== git remote -v =="; git remote -v || true
} > "$BUNDLE_DIR/git.txt"

{
  echo "# data/out (top)"; ls -alh "data/out" 2>/dev/null || echo "<no data/out>"
  echo; echo "# data/out/latest (top)"; ls -alh "data/out/latest" 2>/dev/null || echo "<no latest>"
  echo; echo "# data/cache (top)"; ls -alh "data/cache" 2>/dev/null || echo "<no cache>"
} > "$BUNDLE_DIR/listings.txt"

{
  echo "# SYMLINK TARGETS"
  if [[ -e "data/out/latest" ]]; then
    if [[ -L "data/out/latest" ]]; then
      echo -n "latest -> "; readlink "data/out/latest" || true
      echo -n "latest realpath -> "; realpath "data/out/latest" || true
    else
      echo "data/out/latest is not a symlink"
    fi
  else
    echo "data/out/latest missing"
  fi
} > "$BUNDLE_DIR/links.txt"

# -------- pool snapshot (if present) --------
if [[ -f "data/cache/pool/pool.jsonl" ]]; then
  head -n 100 "data/cache/pool/pool.jsonl" > "$BUNDLE_DIR/pool.head.jsonl" || true
  wc -l "data/cache/pool/pool.jsonl" > "$BUNDLE_DIR/pool.count.txt" || true
fi

# -------- goal-focused diagnostics via Python --------
python - <<'PY' || true
import json, csv, os, re, statistics
from pathlib import Path

B = Path("debug-bundle")
RUN = Path(os.environ.get("RUN_DIR","")) if os.environ.get("RUN_DIR") else None
# Try to locate run dir similarly here too
if not RUN or not RUN.exists():
    cand = ["data/out/latest"]
    if Path("data/out/last_run_dir.txt").exists():
        try: cand.append(Path(Path("data/out/last_run_dir.txt").read_text().strip()))
        except: pass
    # newest run_*
    import glob
    runs = sorted(glob.glob("data/out/run_*"))
    if runs:
        cand.append(Path(runs[0]))
    for c in cand:
        p = Path(c)
        if p.exists():
            RUN = p; break

def load_json(p):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

def try_file(name):
    if RUN and (RUN / name).exists(): return (RUN / name)
    if (B / name).exists(): return (B / name)
    return None

diag = load_json(try_file("diag.json")) or {}
items_enriched = load_json(try_file("items.enriched.json")) or []
items_discovered = load_json(try_file("items.discovered.json")) or []
assistant_feed = load_json(try_file("assistant_feed.json")) or []

dataset = items_enriched or assistant_feed or items_discovered or []

# IMDb coverage and year/vote stats
imdb_have = sum(1 for it in dataset if isinstance(it, dict) and it.get("imdb_id"))
votes = [float(it.get("tmdb_vote", 0.0)) for it in dataset if isinstance(it, dict) and it.get("tmdb_vote") is not None]
vote_stats = {
  "count": len(votes),
  "min": min(votes) if votes else None,
  "median": statistics.median(votes) if votes else None,
  "max": max(votes) if votes else None,
}

# Ratings CSV presence
ratings_csv = Path("data/user/ratings.csv")
ratings_head = ""
ratings_rows = 0
if ratings_csv.exists():
    txt = ratings_csv.read_text(encoding="utf-8", errors="replace").splitlines()
    ratings_head = "\n".join(txt[:6])
    try:
        rows = csv.DictReader(txt)
        ratings_rows = sum(1 for _ in rows)
    except Exception:
        ratings_rows = 0

# Pool size
pool_path = Path("data/cache/pool/pool.jsonl")
pool_lines = sum(1 for _ in pool_path.open("r", encoding="utf-8")) if pool_path.exists() else 0

# Provider map & discover page telemetry (if present)
env_diag = (diag or {}).get("env", {})
provider_map = env_diag.get("PROVIDER_MAP", {})
discover_pages = (diag or {}).get("discover_pages", [])

report = {
  "dataset_len": len(dataset),
  "enriched_len": len(items_enriched),
  "discovered_len": len(items_discovered),
  "imdb_id_coverage": {"have": imdb_have, "total": len(dataset)},
  "vote_stats": vote_stats,
  "ratings_csv_rows": ratings_rows,
  "pool_size_lines": pool_lines,
  "provider_map": provider_map,
  "discover_pages_sample": discover_pages[:3],
  "auth_mode": "TMDB_API_KEY" if os.getenv("TMDB_API_KEY") else ("TMDB_BEARER" if os.getenv("TMDB_BEARER") else "unknown"),
}

(B / "analysis.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

md = []
md.append("# Diagnostics")
md.append(f"- Dataset len: **{len(dataset)}**, Enriched: **{len(items_enriched)}**, Discovered: **{len(items_discovered)}**")
md.append(f"- IMDb ID coverage: **{imdb_have}/{len(dataset)}**")
md.append(f"- TMDB vote stats: {vote_stats}")
md.append(f"- Ratings CSV rows (data/user/ratings.csv): **{ratings_rows}**")
md.append(f"- Pool size (lines): **{pool_lines}**")
md.append(f"- Auth mode used (by env presence): **{report['auth_mode']}**")
if provider_map: md.append(f"- Provider map (env slug → TMDB id): `{json.dumps(provider_map)}`")
if discover_pages:
    md.append(f"- Discover pages captured: {len(discover_pages)}; first page:")
    from pprint import pformat
    md.append("```json\n" + json.dumps(discover_pages[0], indent=2) + "\n```")
if ratings_head:
    md.append("\n## ratings.csv (first lines)\n```\n" + ratings_head + "\n```")

(B / "analysis.md").write_text("\n".join(md), encoding="utf-8")
print("Wrote goal-focused diagnostics into debug-bundle/analysis.{md,json}")
PY

# -------- zip the bundle --------
( cd "$BUNDLE_DIR" && zip -q -r "../${OUT_ZIP}" . ) || true
ls -lh "$OUT_ZIP" || true

# -------- create a compact repo snapshot --------
# Exclude caches, git, venv, and large dirs
SNAP="repo-snapshot.zip"
zip -q -r "$SNAP" . \
  -x ".git/*" \
  -x "data/out/*" \
  -x "data/cache/*" \
  -x "__pycache__/*" \
  -x "*.pyc" \
  -x ".venv/*" "venv/*" \
  -x "node_modules/*" \
  -x ".mypy_cache/*" ".pytest_cache/*" || true
ls -lh "$SNAP" || true