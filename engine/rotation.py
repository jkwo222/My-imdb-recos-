# engine/rotation.py
from __future__ import annotations
import time
from typing import List

def _window_start(step: int, minutes: int, cap: int) -> int:
    # deterministic rotation keyed to current time bucket
    bucket = int(time.time() // 60) // max(1, minutes)
    return (bucket * max(1, step)) % max(1, cap)

def plan_pages(pages_requested: int, step: int, rotate_minutes: int, cap: int) -> List[int]:
    """
    Return 1-based TMDB page numbers for this run, with wrap-around and
    deterministic rotation so pages change every `rotate_minutes`.
    """
    cap = max(1, cap)
    pages_requested = max(1, min(pages_requested, cap))
    start0 = _window_start(step=step, minutes=rotate_minutes, cap=cap)  # 0..cap-1
    seq = []
    for i in range(pages_requested):
        # convert 0-based index to 1-based TMDB page, wrapping around
        p0 = (start0 + i) % cap
        seq.append(p0 + 1)
    return seq