from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from .env import Env
from .catalog_builder import build_catalog
from .scoring import load_seen_index, filter_unseen, score_items
from .self_check import run_self_check

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "out"

def _install_safe_unraisable_hook() -> None:
    """
    Some libs raise unraisable exceptions during interpreter teardown
    (e.g., when sys.stderr has already been detached). Make the hook
    resilient so it never propagates or crashes.
    """
    def _safe_hook(unraisable):
        try:
            # Best-effort log; swallow any error if stderr is gone.
            msg = f"[shutdown] Unraisable: {getattr(unraisable, 'exc_type', type(None)).__name__}: {getattr(unraisable, 'exc_value', '')}"
            try:
                print(msg, file=sys.stderr, flush=True)
            except Exception:
                pass
        except Exception:
            # Never let this raise.
            pass
    try:
        sys.unraisablehook = _safe_hook  # type: ignore[attr-defined]
    except Exception:
        # Python <3.8 or restricted env; ignore.
        pass

def _env_from_os() -> Env:
    # Allow ORIGINAL_LANGS as JSON-ish (["en","es"]) or CSV ("en,es").
    langs_raw = os.getenv("ORIGINAL_LANGS", "").strip()
    langs: List[str]
    if langs_raw.startswith("[") and langs_raw.endswith("]"):
        try:
            parsed = json.loads(langs_raw)
            langs = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            langs = ["en"]
    elif "," in langs_raw:
        langs = [t.strip() for t in langs_raw.split(",") if t.strip()]
    else:
        langs = [langs_raw] if langs_raw else ["en"]

    return Env.from_mapping({
        "REGION": os.getenv("REGION", "US").strip() or "US",
        "ORIGINAL_LANGS": langs,
        "SUBS_INCLUDE": os.getenv("SUBS_INCLUDE", ""),
        # default to 12 pages; can be overridden in workflow env
        "DISCOVER_PAGES": int(os.getenv("DISCOVER_PAGES", "12") or "12"),
    })

def _write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _timestamp_dir() -> Path:
    ts = time.strftime("run_%Y%m%d_%H%M%S", time.gmtime())
    return OUT_DIR / ts

def main() -> None:
    _install_safe_unraisable_hook()
    run_self_check()

    env = _env_from_os()

    print(" | catalog:begin", flush=True)
    t0 = time.time()
    items = build_catalog(env)
    kept = len(items)
    print(f" | catalog:end kept={kept}", flush=True)

    # Load "seen" index (ratings.csv and/or public IMDb if IMDB_USER_ID is set)
    seen_idx = load_seen_index(str(ROOT / "data" / "ratings.csv"))
    eligible = filter_unseen(items, seen_idx)

    # Score
    scored = score_items(env, eligible)

    # Basic counters for the logs
    discovered = len(items)
    elig_cnt = len(eligible)
    above_cut = sum(1 for r in scored if r.get("match", 0) >= 58.0)
    print(f" | results: discovered={discovered} eligible={elig_cnt} above_cut={above_cut}", flush=True)

    # Persist run artifacts
    run_dir = _timestamp_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "items.discovered.json", items)
    _write_json(run_dir / "items.enriched.json", eligible)  # after exclusion but before scoring details
    _write_json(run_dir / "assistant_feed.json", scored)

    # Tiny markdown summary for the site step (compat with your previous format)
    summary_lines = [
        "# Daily recommendations",
        "",
        "## Telemetry",
        f"- Region: **{env.get('REGION', 'US')}**",
        f"- SUBS_INCLUDE: `{env.get('SUBS_INCLUDE', '')}`",
        f"- Discover pages: **{env.get('DISCOVER_PAGES', 0)}**",
        f"- Discovered (raw): **{discovered}**",
        f"- Eligible after exclusions: **{elig_cnt}**",
        f"- Above match cut (≥ 58.0): **{above_cut}**",
        "",
        "## Your profile: genre weights",
        "_No genre weights computed (no ratings.csv?)._",
        "",
        ("_No items above cut today._" if above_cut == 0 else ""),
        "",
    ]
    (run_dir / "summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    # Maintain 'latest' copy for upload step
    latest = OUT_DIR / "latest"
    if latest.exists():
        try:
            if latest.is_symlink() or latest.is_file():
                latest.unlink()
            else:
                import shutil
                shutil.rmtree(latest)
        except Exception:
            pass
    import shutil
    shutil.copytree(run_dir, latest)

    # Done; explicit clean exit for CI
    elapsed = time.time() - t0
    try:
        print(f" | done in {elapsed:.2f}s", flush=True)
    except Exception:
        pass

    # On GitHub Actions, force a clean zero even if some atexit/unraisable nonsense fires later.
    if os.getenv("GITHUB_ACTIONS", "").lower() == "true" or os.getenv("RUNNER_HARD_EXIT", "1") == "1":
        os._exit(0)  # noqa: PLE1142
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()