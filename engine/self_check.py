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

def _exists(mod_name: str) -> bool:
    try:
        importlib.import_module(mod_name)
        return True
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

    # Soft checks (do not fail run): optional modules
    optional = [
        "engine.persona",
        "engine.taste",
        "engine.personalization",
        "engine.util",
    ]
    for name in optional:
        present = _exists(name)
        print(f"SELF-CHECK: optional {name}: {'present' if present else 'absent'}", file=sys.stderr, flush=True)

    if not ok:
        raise SystemExit("Repo self-check failed. See messages above.")