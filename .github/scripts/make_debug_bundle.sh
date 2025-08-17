#!/usr/bin/env bash
set -euo pipefail

OUT_ZIP="debug-data.zip"
BUNDLE_DIR="debug-bundle"

rm -f "$OUT_ZIP"
rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR"

resolve_run_dir() {
  local run_src=""
  if [[ -e "data/out/latest" ]]; then
    run_src="data/out/latest"
  fi
  if [[ -z "${run_src}" && -f "data/out/last_run_dir.txt" ]]; then
    run_src="$(cat data/out/last_run_dir.txt || true)"
  fi
  if [[ -z "${run_src}" ]]; then
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

# Copy run artifacts
if [[ -n "${RUN_DIR}" && -d "${RUN_DIR}" ]]; then
  for f in runner.log assistant_feed.json items.discovered.json items.enriched.json summary.md diag.json; do
    cp -f "${RUN_DIR}/${f}" "$BUNDLE_DIR/" 2>/dev/null || true
  done
  if [[ -d "${RUN_DIR}/exports" ]]; then
    mkdir -p "$BUNDLE_DIR/exports"
    cp -rf "${RUN_DIR}/exports/." "$BUNDLE_DIR/exports/" 2>/dev/null || true
  fi
else
  echo "WARN: No run directory found; bundle will be minimal" | tee "$BUNDLE_DIR/_warning.txt"
fi

# Env capture (no secrets)
{
  echo "REGION=${REGION:-}"
  echo "SUBS_INCLUDE=${SUBS_INCLUDE:-}"
  echo "ORIGINAL_LANGS=${ORIGINAL_LANGS:-}"
  echo "DISCOVER_PAGES=${DISCOVER_PAGES:-}"
  echo "POOL_MAX_ITEMS=${POOL_MAX_ITEMS:-}"
  echo "POOL_PRUNE_AT=${POOL_PRUNE_AT:-}"
  echo "POOL_PRUNE_KEEP=${POOL_PRUNE_KEEP:-}"
} > "$BUNDLE_DIR/env.txt"

{
  for k in TMDB_API_KEY TMDB_BEARER IMDB_USER_ID; do
    v="${!k:-}"
    if [[ -n "${v}" ]]; then echo "$k=<set>"; else echo "$k=<missing>"; fi
  done
} > "$BUNDLE_DIR/env-sanitized.txt"

# Git + listings
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

# Pool snapshot (head, tail, count)
if [[ -f "data/cache/pool/pool.jsonl" ]]; then
  head -n 100 "data/cache/pool/pool.jsonl" > "$BUNDLE_DIR/pool.head.jsonl" || true
  tail -n 100 "data/cache/pool/pool.jsonl" > "$BUNDLE_DIR/pool.tail.jsonl" || true
  wc -l "data/cache/pool/pool.jsonl" > "$BUNDLE_DIR/pool.count.txt" || true
fi

# Quick diagnostics (JSON + MD)
python - <<'PY' || true
import json, statistics
from pathlib import Path
B = Path("debug-bundle")

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

items = load_json(B/"items.enriched.json") or load_json(B/"assistant_feed.json") or []
matches = []
for it in items:
    try:
        matches.append(float(it.get("match", it.get("score", 0.0)) or 0.0))
    except Exception:
        pass

diag = load_json(B/"diag.json") or {}
env_diag = (diag or {}).get("env", {})
pool_t = env_diag.get("POOL_TELEMETRY", {})

report = {
  "items": len(items),
  "match_stats": {
    "count": len(matches),
    "min": min(matches) if matches else None,
    "median": statistics.median(matches) if matches else None,
    "max": max(matches) if matches else None
  },
  "pool": pool_t,
}

(B/"analysis.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
(B/"analysis.md").write_text("# Diagnostics\n\n````json\n"+json.dumps(report, indent=2)+"\n````\n", encoding="utf-8")
print("Wrote analysis into debug-bundle/analysis.{md,json}")
PY

# Zip the debug bundle
( cd "$BUNDLE_DIR" && zip -q -r "../${OUT_ZIP}" . ) || true
ls -lh "$OUT_ZIP" || true

# Compact repo snapshot
SNAP="repo-snapshot.zip"
zip -q -r "$SNAP" . \
  -x ".git/*" "data/out/*" "data/cache/*" \
  -x "__pycache__/*" "*.pyc" ".venv/*" "venv/*" \
  -x "node_modules/*" ".mypy_cache/*" ".pytest_cache/*" || true
ls -lh "$SNAP" || true