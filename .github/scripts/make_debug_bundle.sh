#!/usr/bin/env bash
# .github/scripts/make_debug_bundle.sh
set -euo pipefail

# Where to emit
OUT_ZIP="debug-data.zip"
BUNDLE_DIR="debug-bundle"

rm -f "$OUT_ZIP"
rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR"

# 1) Include most recent run outputs if present
if [[ -d "data/out/latest" ]]; then
  cp -f data/out/latest/runner.log            "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/assistant_feed.json   "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/items.discovered.json "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/items.enriched.json   "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/summary.md            "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/diag.json             "$BUNDLE_DIR/" 2>/dev/null || true
fi

# 2) Environment capture (minimal + sanitized full)
{
  echo "REGION=${REGION:-}"
  echo "SUBS_INCLUDE=${SUBS_INCLUDE:-}"
  echo "ORIGINAL_LANGS=${ORIGINAL_LANGS:-}"
  echo "DISCOVER_PAGES=${DISCOVER_PAGES:-}"
} > "$BUNDLE_DIR/env.txt"

{
  # redact secrets but show presence
  for k in TMDB_API_KEY OMDB_API_KEY IMDB_USER_ID IMDB_RATINGS_CSV_PATH; do
    v="${!k:-}"
    if [[ -n "${v}" ]]; then
      echo "$k=<set>"
    else
      echo "$k=<missing>"
    fi
  done
  # dump a few useful vars verbatim
  echo "REGION=${REGION:-}"
  echo "SUBS_INCLUDE=${SUBS_INCLUDE:-}"
  echo "ORIGINAL_LANGS=${ORIGINAL_LANGS:-}"
  echo "DISCOVER_PAGES=${DISCOVER_PAGES:-}"
} > "$BUNDLE_DIR/env-sanitized.txt"

# 3) Git state
{
  echo "== git status -sb =="
  git status -sb || true
  echo
  echo "== git log -1 =="
  git log -1 --oneline --decorate || true
  echo
  echo "== git remote -v =="
  git remote -v || true
} > "$BUNDLE_DIR/git.txt"

# 4) Tree + listings
{
  echo "# REPO TREE (find .)"
  find . -maxdepth 3 -printf "%y %M %u %g %8s %TY-%Tm-%Td %TH:%TM:%TS %p\n" 2>/dev/null || true
  echo
  echo "# data/out (top)"
  ls -alh "data/out" 2>/dev/null || echo "<no data/out>"
  echo
  echo "# data/out/latest (top)"
  ls -alh "data/out/latest" 2>/dev/null || echo "<no latest>"
  echo
  echo "# data/cache (top)"
  ls -alh "data/cache" 2>/dev/null || echo "<no cache>"
} > "$BUNDLE_DIR/listings.txt"

# 5) Symlink diagnostics
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

# 6) Disk usage snapshot
{
  echo "== du -h data =="
  du -h -d 2 data 2>/dev/null || du -h --max-depth=2 data 2>/dev/null || true
  echo
  echo "== df -h =="
  df -h || true
} > "$BUNDLE_DIR/dirs.txt"

# 7) Python info
{
  echo "# Python info"
  which python || true
  python -V 2>&1 || true
  echo
  echo "# pip freeze"
  pip freeze || true
} > "$BUNDLE_DIR/python.txt"

# 8) Runner latest sanity (double-check latest points correctly)
{
  echo "== runner latest sanity =="
  if [[ -L "data/out/latest" ]]; then
    LRP="$(realpath data/out/latest || true)"
    echo "latest is symlink -> ${LRP}"
    if [[ -n "${LRP}" && -d "${LRP}" && "${LRP}" == *"/data/out/run_"* ]]; then
      echo "OK: latest resolves to a run directory."
    else
      echo "WARN: latest does not resolve to a run_ directory. Please inspect."
    fi
  else
    if [[ -d "data/out/latest" ]]; then
      echo "latest is a real directory (not symlink)."
    else
      echo "latest missing."
    fi
  fi
} > "$BUNDLE_DIR/runner.sanity.txt"

# 9) Zip it up
( cd "$BUNDLE_DIR" && zip -q -r "../${OUT_ZIP}" . ) || true
ls -lh "$OUT_ZIP" || true