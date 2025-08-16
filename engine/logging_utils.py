# File: engine/logging_utils.py
from __future__ import annotations
import json, sys, time
from pathlib import Path
from typing import Any, Dict

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")

class HeartbeatLogger:
    """NDJSON heartbeat to file + concise stdout breadcrumbs."""
    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.file = self.run_dir / "heartbeat.log"

    def ping(self, stage: str, **kv: Any) -> None:
        rec: Dict[str, Any] = {"ts": _now_iso(), "stage": stage, **kv}
        try:
            with self.file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"[heartbeat] write failed: {e}", file=sys.stderr)
        parts = " ".join(f"{k}={v}" for k, v in kv.items() if v is not None)
        print(f"[HB] {stage} {parts}".strip())

def make_heartbeat(run_dir: Path) -> HeartbeatLogger:
    return HeartbeatLogger(run_dir)