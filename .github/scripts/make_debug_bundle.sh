#!/usr/bin/env bash
set -euo pipefail

OUT_ZIP="debug-data.zip"
BUNDLE_DIR="debug-bundle"

rm -f "$OUT_ZIP"
rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR"

# -------- resolve latest run directory robustly --------
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

# -------- copy run artifacts --------
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

# -------- env capture (no secrets) --------
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
import json, csv, os, statistics
from pathlib import Path
B = Path("debug-bundle")

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

items = load_json(B/"items.enriched.json") or load_json(B/"assistant_feed.json") or []
imdb_have = sum(1 for it in items if isinstance(it, dict) and it.get("imdb_id"))
votes = [float(it.get("tmdb_vote") or 0.0) for it in items if isinstance(it, dict) and it.get("tmdb_vote") is not None]
matches = [float(it.get("score", it.get("match", 0.0)) or 0.0) for it in items if isinstance(it, dict)]
env_diag = {}
diag = load_json(B/"diag.json") or {}
env_diag = (diag or {}).get("env", {})

report = {
  "dataset_len": len(items),
  "imdb_id_coverage": {"have": imdb_have, "total": len(items)},
  "vote_stats": {"count": len(votes), "min": min(votes) if votes else None, "median": (statistics.median(votes) if votes else None), "max": max(votes) if votes else None},
  "match_stats": {"count": len(matches), "min": min(matches) if matches else None, "median": (statistics.median(matches) if matches else None), "max": max(matches) if matches else None},
  "provider_map": (env_diag or {}).get("PROVIDER_MAP", {}),
}
(B/"analysis.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
(B/"analysis.md").write_text("# Diagnostics\\n\\n````json\\n"+json.dumps(report, indent=2)+"\\n````\\n", encoding="utf-8")
print("Wrote analysis into debug-bundle/analysis.{md,json}")
PY

# -------- zip the debug bundle --------
( cd "$BUNDLE_DIR" && zip -q -r "../${OUT_ZIP}" . ) || true
ls -lh "$OUT_ZIP" || true

# -------- compact repo snapshot --------
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