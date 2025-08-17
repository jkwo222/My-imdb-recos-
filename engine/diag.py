# engine/diag.py
from __future__ import annotations
import json, os, time
from pathlib import Path
from typing import Any, Dict, Optional

def _safe(obj: Any) -> Any:
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)

def write_diag(run_dir: Path,
               *,
               discovered: int = 0,
               eligible: int = 0,
               above_cut: int = 0,
               provider_ids: Optional[list[int]] = None,
               env_snapshot: Optional[Dict[str, Any]] = None,
               started_ts: Optional[float] = None,
               finished_ts: Optional[float] = None,
               notes: Optional[str] = None) -> None:
    """
    Writes a compact JSON with useful diagnostics for each run.
    Safe no-op on exceptions.
    """
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        out = {
            "version": 1,
            "timestamps": {
                "started": started_ts,
                "finished": finished_ts,
                "duration_sec": (None if (started_ts is None or finished_ts is None)
                                 else max(0.0, float(finished_ts - started_ts)))
            },
            "counts": {
                "discovered": int(discovered),
                "eligible": int(eligible),
                "above_cut": int(above_cut),
            },
            "env": {
                "REGION": os.environ.get("REGION", "US"),
                "SUBS_INCLUDE": os.environ.get("SUBS_INCLUDE", ""),
                "ORIGINAL_LANGS": os.environ.get("ORIGINAL_LANGS", '["en"]'),
                "DISCOVER_PAGES": os.environ.get("DISCOVER_PAGES", ""),
            },
            "provider_ids_used": list(provider_ids or []),
            "extra": _safe(env_snapshot or {}),
            "host": {
                "python": os.environ.get("PYTHON_VERSION", ""),
                "platform": os.environ.get("ImageOS", ""),
            },
            "notes": notes or "",
        }
        with (run_dir / "diag.json").open("w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    except Exception:
        # never fail the run because of diag writing
        pass