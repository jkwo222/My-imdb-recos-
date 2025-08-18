#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

OUT_DIR="data/out"
RUN_DIR_FILE="$OUT_DIR/last_run_dir.txt"

# --- locate latest run dir ---
if [[ -f "$RUN_DIR_FILE" ]]; then
  RUN_DIR="$(cat "$RUN_DIR_FILE")"
else
  # fallback to newest run_* dir
  RUN_DIR="$(ls -1dt "$OUT_DIR"/run_* 2>/dev/null | head -n1 || true)"
fi

if [[ -z "${RUN_DIR:-}" || ! -d "$RUN_DIR" ]]; then
  echo "No run directory found. Exiting."
  exit 0
fi

echo "Using run dir: $RUN_DIR"

# --- staging ---
STAGE=".debug_stage"
FULL="$STAGE/full"
SNAP="$STAGE/snapshot"
rm -rf "$STAGE"
mkdir -p "$FULL" "$SNAP"

# --- copy core files into FULL bundle (verbose, everything we might inspect) ---
mkdir -p "$FULL/data/out/latest" "$FULL/data/cache" "$FULL/exports"

# prefer run_dir; mirror key outputs
cp -f "$RUN_DIR/runner.log" "$FULL/runner.log" || true
cp -f "$RUN_DIR/diag.json" "$FULL/diag.json" || true
cp -f "$RUN_DIR/items.discovered.json" "$FULL/items.discovered.json" || true
cp -f "$RUN_DIR/items.enriched.json" "$FULL/items.enriched.json" || true
cp -f "$RUN_DIR/assistant_feed.json" "$FULL/assistant_feed.json" || true
cp -f "$RUN_DIR/summary.md" "$FULL/summary.md" || true

# exports (model + guards)
mkdir -p "$FULL/exports"
cp -f "$RUN_DIR/exports/"*.json "$FULL/exports/" 2>/dev/null || true
cp -f "$RUN_DIR/exports/"*.md   "$FULL/exports/" 2>/dev/null || true

# cache (pool – optional but helpful to check growth)
mkdir -p "$FULL/data/cache/pool"
cp -f data/cache/pool/* "$FULL/data/cache/pool/" 2>/dev/null || true

# also keep a copy of the last_run_dir pointer for convenience
mkdir -p "$FULL/data/out"
cp -f "$OUT_DIR/last_run_dir.txt" "$FULL/data/out/last_run_dir.txt" || true

# --- build SNAPSHOT (compact, analysis-ready) ---
# Keep only essentials + computed diagnostics
cp -f "$RUN_DIR/runner.log" "$SNAP/runner.log" || true
cp -f "$RUN_DIR/diag.json" "$SNAP/diag.json" || true
cp -f "$RUN_DIR/summary.md" "$SNAP/summary.md" || true

mkdir -p "$SNAP/exports"
cp -f "$RUN_DIR/exports/seen_index.json" "$SNAP/exports/seen_index.json" 2>/dev/null || true
cp -f "$RUN_DIR/exports/user_model.json" "$SNAP/exports/user_model.json" 2>/dev/null || true
cp -f "$RUN_DIR/exports/seen_tv_roots.json" "$SNAP/exports/seen_tv_roots.json" 2>/dev/null || true

# Trim items.enriched.json to top 200 by score for compactness (we’ll also store the top 10 list and metrics)
python3 - <<'PY'
import json, sys, os, pathlib
root = pathlib.Path(".")
run_dir = pathlib.Path(os.environ.get("RUN_DIR", "")) if os.environ.get("RUN_DIR") else None
snap = pathlib.Path(".debug_stage/snapshot")
src = (run_dir / "items.enriched.json") if run_dir and (run_dir / "items.enriched.json").exists() \
      else None
if src:
    data = json.loads(src.read_text(encoding="utf-8", errors="replace"))
    safe = sorted(data, key=lambda x: float(x.get("score", x.get("tmdb_vote", 0.0)) or 0.0), reverse=True)
    top200 = safe[:200]
    (snap / "items.top200.json").write_text(json.dumps(top200, ensure_ascii=False, indent=2), encoding="utf-8")
PY

# --- add computed rich diagnostics (metrics, violations, provider coverage, recency, penalties, pool delta) ---
python3 .github/scripts/collect_diag.py \
  --run-dir "$RUN_DIR" \
  --out-dir "$SNAP" \
  || echo "collect_diag.py failed (continuing)"

# --- version fingerprint (sha + run id if on Actions) ---
{
  echo "git_sha=${GITHUB_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
  echo "workflow=${GITHUB_WORKFLOW:-local}"
  echo "run_id=${GITHUB_RUN_ID:-local}"
  echo "run_number=${GITHUB_RUN_NUMBER:-local}"
} > "$SNAP/VERSION.txt"

# --- pack zips in repo root (avoid absolute paths) ---
DEBUG_FULL_ZIP="debug-data.zip"
DEBUG_SNAP_ZIP="debug-snapshot.zip"
rm -f "$DEBUG_FULL_ZIP" "$DEBUG_SNAP_ZIP"

( cd "$FULL"  && zip -qr "../$DEBUG_FULL_ZIP" . )
( cd "$SNAP"  && zip -qr "../$DEBUG_SNAP_ZIP" . )
mv "$STAGE/$DEBUG_FULL_ZIP" "$ROOT/$DEBUG_FULL_ZIP"
mv "$STAGE/$DEBUG_SNAP_ZIP" "$ROOT/$DEBUG_SNAP_ZIP"

# cleanup stage
rm -rf "$STAGE"

# sizes
echo "Created $DEBUG_FULL_ZIP ($(du -h "$DEBUG_FULL_ZIP" | awk '{print $1}'))"
echo "Created $DEBUG_SNAP_ZIP ($(du -h "$DEBUG_SNAP_ZIP" | awk '{print $1}'))"