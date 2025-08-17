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
  cp -f data/out/latest/runner.log         "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/assistant_feed.json "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/items.discovered.json "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/items.enriched.json   "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/summary.md         "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/options.sanity.json "$BUNDLE_DIR/" 2>/dev/null || true
  cp -f data/out/latest/links.sanity.json   "$BUNDLE_DIR/" 2>/dev/null || true
fi

# 2) Environment capture
{
  echo "REGION=${REGION:-}"
  echo "SUBS_INCLUDE=${SUBS_INCLUDE:-}"
  echo "ORIGINAL_LANGS=${ORIGINAL_LANGS:-}"
  echo "DISCOVER_PAGES=${DISCOVER_PAGES:-}"
} > "$BUNDLE_DIR/env.txt"

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
  echo
  echo "== modified/untracked (head) =="
  git ls-files -m -o --exclude-standard | sed -n '1,200p' || true
} > "$BUNDLE_DIR/git.txt"

# 4) Symlink + paths info
{
  echo "last_run_dir.txt:"
  if [[ -f "data/out/last_run_dir.txt" ]]; then
    cat data/out/last_run_dir.txt
  else
    echo "<missing>"
  fi
  echo
  echo "data/out listing:"
  ls -alh "data/out" 2>/dev/null || echo "<no data/out>"
  echo
  echo "latest details:"
  if [[ -e "data/out/latest" ]]; then
    echo "exists: yes"
    if [[ -L "data/out/latest" ]]; then
      echo "is_symlink: yes"
      echo -n "readlink: "; readlink "data/out/latest" || true
      echo -n "realpath: "; realpath "data/out/latest" || true
    else
      echo "is_symlink: no"
      echo "type: $(stat -c %F data/out/latest || echo '?')"
    fi
  else
    echo "exists: no"
  fi
} > "$BUNDLE_DIR/links.txt"

# 5) File tree (compact)
{
  if command -v tree >/dev/null 2>&1; then
    echo "== tree -a -L 3 . =="
    tree -a -L 3 .
  else
    echo "== find (tree not available) =="
    find . -maxdepth 3 -print
  fi
} > "$BUNDLE_DIR/listings.txt"

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
  echo "== python -V =="
  python -V 2>&1 || true
  echo
  echo "== which python =="
  which python || true
  echo
  echo "== pip freeze (top 100) =="
  pip freeze | sed -n '1,100p' || true
} > "$BUNDLE_DIR/python.txt"

# 8) Runner sanity (double-check latest points correctly)
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