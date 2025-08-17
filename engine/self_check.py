# engine/self_check.py
from __future__ import annotations
import importlib
import sys

def _exists(mod_name: str) -> bool:
    try:
        importlib.import_module(mod_name)
        return True
    except Exception:
        return False

def run_self_check() -> None:
    ok = True

    # Required core
    required = [
        "engine.env",
        "engine.catalog_builder",
        "engine.tmdb",
        "engine.runner",
    ]
    for name in required:
        if _exists(name):
            print(f"SELF-CHECK: required {name}: present", file=sys.stderr, flush=True)
        else:
            print(f"SELF-CHECK: required {name}: MISSING", file=sys.stderr, flush=True)
            ok = False

    # Optional modules actually present in repo
    optional = [
        "engine.persona",
        "engine.taste",
        "engine.personalize",      # (not 'personalization')
        "engine.util",
        "engine.rank",
        "engine.feed",
    ]
    for name in optional:
        present = _exists(name)
        print(f"SELF-CHECK: optional {name}: {'present' if present else 'absent'}", file=sys.stderr, flush=True)

    if not ok:
        raise SystemExit("Repo self-check failed. See messages above.")