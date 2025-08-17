from __future__ import annotations
import csv
import re
from typing import Any, Dict, Iterable, List, Optional

__all__ = [
    "clamp01",
    "try_float",
    "normalize_title",
    "safe_read_csv_dicts",
]

def clamp01(x: float) -> float:
    if x != x:
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x

def try_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

_ROMAN = {
    " i ": " 1 ", " ii ": " 2 ", " iii ": " 3 ", " iv ": " 4 ", " v ": " 5 ",
    " vi ": " 6 ", " vii ": " 7 ", " viii ": " 8 ", " ix ": " 9 ", " x ": " 10 ",
}

def normalize_title(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    out, depth = [], 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    s = "".join(out)
    s = s.replace("&", " and ")
    s = re.sub(r"[-—–_:/,.'!?;]", " ", s)
    s = f" {s} "
    for k, v in _ROMAN.items():
        s = s.replace(k, v)
    s = re.sub(r"^\s*the\s+", "", s)
    s = " ".join(t for t in s.split() if t)
    return s

def safe_read_csv_dicts(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                rows.append(dict(row))
    except Exception:
        return []
    return rows