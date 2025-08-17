from __future__ import annotations
import re
from datetime import date
from typing import Optional

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

def parse_year(s: str) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    if len(s) >= 4 and s[:4].isdigit():
        try:
            y = int(s[:4])
            if 1870 <= y <= 2100:
                return y
        except Exception:
            return None
    return None

def parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip().split("T", 1)[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x