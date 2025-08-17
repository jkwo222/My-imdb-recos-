#!/usr/bin/env bash
set -euo pipefail

OUT_ZIP="debug-data.zip"
BUNDLE_DIR="debug-bundle"

rm -f "$OUT_ZIP"
rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR"

# 1) Latest outputs (if present)
if [[ -d "data/out/latest" ]]; then
  cp -f data/out/latest/runner.log            "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/assistant_feed.json   "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/items.discovered.json "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/items.enriched.json   "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/summary.md            "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/diag.json             "$BUNDLE_DIR/" 2>/dev/null || true
fi

# 2) Env capture (no secrets printed)
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

# 3) Git state
{
  echo "== git status -sb =="; git status -sb || true
  echo; echo "== git log -1 =="; git log -1 --oneline --decorate || true
  echo; echo "== git remote -v =="; git remote -v || true
} > "$BUNDLE_DIR/git.txt"

# 4) Listings + symlink diag
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

# 5) Python info
{
  echo "# Python info"; which python || true; python -V 2>&1 || true
  echo; echo "# pip freeze"; pip freeze || true
} > "$BUNDLE_DIR/python.txt"

# 6) Zip it
( cd "$BUNDLE_DIR" && zip -q -r "../${OUT_ZIP}" . ) || true
ls -lh "$OUT_ZIP" || true