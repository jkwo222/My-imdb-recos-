from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, Iterable, List

def file_exists(p: str) -> bool:
    try:
        return Path(p).exists()
    except Exception:
        return False

def safe_read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if not path.exists():
        return out
    try:
        with path.open("r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                out.append({k: (v if isinstance(v, str) else str(v)) for k, v in row.items()})
    except UnicodeDecodeError:
        with path.open("r", encoding="latin-1") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                out.append({k: (v if isinstance(v, str) else str(v)) for k, v in row.items()})
    return out