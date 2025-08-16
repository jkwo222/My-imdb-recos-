# engine/self_check.py
from __future__ import annotations
import importlib
import sys

def _has_attr(mod_name: str, symbol: str) -> bool:
    try:
        mod = importlib.import_module(mod_name)
        return hasattr(mod, symbol)
    except Exception:
        return False

def run_self_check() -> None:
    ok = True
    if not _has_attr("engine.tmdb", "discover_movie_page"):
        print("SELF-CHECK: engine.tmdb.discover_movie_page MISSING", file=sys.stderr, flush=True)
        ok = False
    if not _has_attr("engine.tmdb", "discover_tv_page"):
        print("SELF-CHECK: engine.tmdb.discover_tv_page MISSING", file=sys.stderr, flush=True)
        ok = False
    if not ok:
        raise SystemExit("Repo self-check failed. See messages above.")