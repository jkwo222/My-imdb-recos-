# engine/telemetry.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List

@dataclass
class Telemetry:
    counts: Dict[str, int] = field(default_factory=dict)
    provider_breakdown: Dict[str, int] = field(default_factory=dict)
    notes: Dict[str, Any] = field(default_factory=dict)

    def mark(self, stage: str, n: int | None = None) -> None:
        if n is not None:
            self.counts[stage] = int(n)

    def add_note(self, key: str, value: Any) -> None:
        self.notes[key] = value

    def set_provider_breakdown(self, counts: Dict[str, int]) -> None:
        self.provider_breakdown = dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"counts": self.counts}
        if self.provider_breakdown:
            out["providers"] = self.provider_breakdown
        if self.notes:
            out["notes"] = self.notes
        return out

def provider_histogram(items: List[dict], field: str = "providers") -> Dict[str, int]:
    hist: Dict[str, int] = {}
    for o in items or []:
        for p in (o.get(field) or []):
            if not isinstance(p, str):
                continue
            hist[p] = hist.get(p, 0) + 1
    return hist