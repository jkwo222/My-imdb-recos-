# engine/env.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Union


def _parse_int(val: Optional[str], default: int) -> int:
    try:
        if val is None:
            return default
        return int(str(val).strip())
    except Exception:
        return default


def _parse_list(val: Optional[Union[str, List[str]]]) -> List[str]:
    """
    Accepts:
      - JSON-ish strings: '["en","fr"]'
      - comma-separated:  "en,fr"
      - already-a-list:   ["en","fr"]
    Returns a clean list of lowercased, trimmed strings.
    """
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    s = str(val).strip()
    if not s:
        return []
    # Try JSON first
    if s.startswith("[") and s.endswith("]"):
        try:
            data = json.loads(s)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
        except Exception:
            pass
    # Fallback: comma-separated
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


@dataclass
class Env:
    """
    Canonical env container, but also supports dict-like .get() access
    for compatibility with older code.
    """
    REGION: str = "US"
    ORIGINAL_LANGS: List[str] = None  # type: ignore[assignment]
    SUBS_INCLUDE: List[str] = None    # type: ignore[assignment]
    DISCOVER_PAGES: int = 3

    # Optional knobs (used by rotation/planning or future tuning)
    DISCOVER_STEP: int = 1
    DISCOVER_CAP: int = 20
    CATALOG_ROTATE_MINUTES: int = 60

    # Raw store for dict-like compatibility
    _raw: Dict[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.ORIGINAL_LANGS is None:
            self.ORIGINAL_LANGS = ["en"]
        if self.SUBS_INCLUDE is None:
            self.SUBS_INCLUDE = []
        if self._raw is None:
            # seed _raw with dataclass values
            self._raw = asdict(self)

    # ---------- dict-like compatibility ----------
    def get(self, key: str, default: Any = None) -> Any:
        """
        Allow code to call env.get("REGION", "US") etc.
        Looks at attributes first, then _raw, then returns default.
        """
        if hasattr(self, key):
            return getattr(self, key)
        if isinstance(self._raw, dict) and key in self._raw:
            return self._raw[key]
        return default

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # _raw may include itself; ensure sane output
        d.pop("_raw", None)
        return d

    # ---------- construction ----------
    @classmethod
    def from_os_environ(cls, src: Optional[Dict[str, str]] = None) -> "Env":
        """
        Build Env from os.environ (or a provided dict).
        Handles list-ish strings and safe int parsing.
        """
        env = src if src is not None else os.environ

        region = env.get("REGION", "US").strip() or "US"

        # Accept ORIGINAL_LANGS='["en","fr"]' or 'en,fr'
        orig_langs = _parse_list(env.get("ORIGINAL_LANGS"))
        if not orig_langs:
            orig_langs = ["en"]

        # Accept SUBS_INCLUDE='netflix,prime_video,...'
        subs = _parse_list(env.get("SUBS_INCLUDE"))

        # How many TMDB pages per run
        discover_pages = _parse_int(env.get("DISCOVER_PAGES"), 3)

        # Optional rotation knobs (won’t break anything if unused)
        step = _parse_int(env.get("DISCOVER_STEP"), 1)
        cap = _parse_int(env.get("DISCOVER_CAP"), 20)
        rotate_mins = _parse_int(env.get("CATALOG_ROTATE_MINUTES"), 60)

        inst = cls(
            REGION=region,
            ORIGINAL_LANGS=orig_langs,
            SUBS_INCLUDE=subs,
            DISCOVER_PAGES=discover_pages,
            DISCOVER_STEP=step,
            DISCOVER_CAP=cap,
            CATALOG_ROTATE_MINUTES=rotate_mins,
            _raw=dict(env),  # keep a copy to satisfy env.get() style access
        )

        # Also mirror common keys into _raw so code reading from env.get works consistently
        inst._raw.update({
            "REGION": inst.REGION,
            "ORIGINAL_LANGS": inst.ORIGINAL_LANGS,
            "SUBS_INCLUDE": inst.SUBS_INCLUDE,
            "DISCOVER_PAGES": inst.DISCOVER_PAGES,
            "DISCOVER_STEP": inst.DISCOVER_STEP,
            "DISCOVER_CAP": inst.DISCOVER_CAP,
            "CATALOG_ROTATE_MINUTES": inst.CATALOG_ROTATE_MINUTES,
        })
        return inst