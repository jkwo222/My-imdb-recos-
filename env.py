from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


def _split_csv_env(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [x.strip() for x in val.split(",") if x.strip()]


@dataclass
class Env:
    # core knobs
    REGION: str = "US"
    ORIGINAL_LANGS: List[str] = field(default_factory=lambda: ["en"])
    SUBS_INCLUDE: List[str] = field(default_factory=list)
    MIN_MATCH_CUT: float = 58.0
    DISCOVER_PAGES: int = 3

    # secrets (names only; values read from OS env)
    TMDB_API_KEY: Optional[str] = None
    TMDB_ACCESS_TOKEN: Optional[str] = None
    IMDB_USER_ID: Optional[str] = None

    # Allow dict-like access for backwards compatibility
    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)


def from_os_env(environ: Optional[Dict[str, str]] = None) -> Env:
    """
    Build Env from OS environment variables (or a provided mapping).
    Provides sane defaults and trims/normalizes list-like fields.
    """
    e = environ or os.environ

    region = e.get("REGION", "US").strip() or "US"
    langs = _split_csv_env(e.get("ORIGINAL_LANGS", "en"))
    subs = _split_csv_env(e.get("SUBS_INCLUDE", ""))

    def _float(name: str, default: float) -> float:
        try:
            return float(e.get(name, default))
        except Exception:
            return default

    def _int(name: str, default: int) -> int:
        try:
            return int(e.get(name, default))
        except Exception:
            return default

    return Env(
        REGION=region,
        ORIGINAL_LANGS=langs or ["en"],
        SUBS_INCLUDE=subs,
        MIN_MATCH_CUT=_float("MIN_MATCH_CUT", 58.0),
        DISCOVER_PAGES=_int("DISCOVER_PAGES", 3),
        TMDB_API_KEY=e.get("TMDB_API_KEY"),
        TMDB_ACCESS_TOKEN=e.get("TMDB_ACCESS_TOKEN"),
        IMDB_USER_ID=e.get("IMDB_USER_ID"),
    )