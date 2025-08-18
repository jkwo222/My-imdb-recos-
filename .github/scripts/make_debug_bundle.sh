#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

OUT_DIR="data/out"
RUN_DIR_FILE="$OUT_DIR/last_run_dir.txt"

# --- locate latest run dir (supports last_run_dir.txt, latest/, or newest run_*) ---
RUN_DIR=""
if [[ -f "$RUN_DIR_FILE" ]]; then
  RUN_DIR="$(cat "$RUN_DIR_FILE" || true)"
fi
if [[ -z "${RUN_DIR:-}" || ! -d "$RUN_DIR" ]]; then
  if [[ -d "$OUT_DIR/latest" ]]; then
    RUN_DIR="$OUT_DIR/latest"
  else
    RUN_DIR="$(ls -1dt "$OUT_DIR"/run_* 2>/dev/null | head -n1 || true)"
  fi
fi
if [[ -z "${RUN_DIR:-}" || ! -d "$RUN_DIR" ]]; then
  echo "WARN: could not locate run directory; using $OUT_DIR"
  RUN_DIR="$OUT_DIR"
fi

STAGE="$(mktemp -d ./.dbg_stage_XXXXXX)"
FULL="$STAGE/full"
SNAP="$STAGE/snapshot"
mkdir -p "$FULL" "$SNAP"

# --- copy core files into FULL bundle (everything we might inspect) ---
mkdir -p "$FULL/$RUN_DIR" "$FULL/data/cache" "$FULL/data/user" "$FULL/.github/workflows"

# copy run outputs
cp -f "$RUN_DIR/runner.log"               "$FULL/runner.log"               || true
cp -f "$RUN_DIR/diag.json"                "$FULL/diag.json"                || true
cp -f "$RUN_DIR/items.discovered.json"    "$FULL/items.discovered.json"    || true
cp -f "$RUN_DIR/items.enriched.json"      "$FULL/items.enriched.json"      || true
cp -f "$RUN_DIR/assistant_feed.json"      "$FULL/assistant_feed.json"      || true
cp -f "$RUN_DIR/summary.md"               "$FULL/summary.md"               || true
cp -f "$RUN_DIR/exports/selection_breakdown.json" "$FULL/selection_breakdown.json" || true
cp -f "$RUN_DIR/exports/feedback_targets.json"   "$FULL/feedback_targets.json"   || true

# copy inputs & caches
cp -f data/user/ratings.csv               "$FULL/data/user/ratings.csv"    || true
cp -f data/cache/pool/pool.jsonl          "$FULL/pool.jsonl"               || true
cp -f data/cache/rotation.json            "$FULL/rotation.json"            || true

# copy workflow files for context
cp -f .github/workflows/nightly.yml       "$FULL/.github/workflows/nightly.yml" || true
cp -f .github/workflows/feedback.yml      "$FULL/.github/workflows/feedback.yml" || true
cp -f .github/workflows/ci.yml            "$FULL/.github/workflows/ci.yml" || true

# copy engine modules (as-is source snapshot for debugging)
mkdir -p "$FULL/engine"
cp -f engine/*.py "$FULL/engine/" || true

# --- compact snapshot (SNAP) with just the highlights ---
mkdir -p "$SNAP"
cp -f "$FULL/summary.md"                  "$SNAP/summary.md"               || true
cp -f "$FULL/selection_breakdown.json"    "$SNAP/selection_breakdown.json" || true
cp -f "$FULL/feedback_targets.json"       "$SNAP/feedback_targets.json"    || true
cp -f "$FULL/diag.json"                   "$SNAP/diag.json"                || true

# --- zip outputs at repo root ---
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