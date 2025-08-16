from __future__ import annotations
from typing import List

def _norm(s: str) -> str:
    return s.strip().lower().replace("_", " ").replace("+", " plus").replace("  ", " ")

# Common aliases normalized.
_ALLOWED = {
    "netflix",
    "prime video", "amazon prime video", "prime video channel", "prime video channels",
    "hulu",
    "max", "hbo max",
    "disney plus",
    "apple tv plus",
    "peacock",
    "paramount plus",
}

def normalize_user_whitelist(user_whitelist: List[str]) -> set[str]:
    wl = set()
    for x in user_whitelist:
        wl.add(_norm(x))
    return wl

def is_allowed_provider(name: str, user_whitelist: List[str]) -> bool:
    n = _norm(name)
    wl = normalize_user_whitelist(user_whitelist)
    return (n in wl) or (n in _ALLOWED)

def any_allowed(providers: List[str] | None, user_whitelist: List[str]) -> bool:
    if not providers:
        return False
    wl = normalize_user_whitelist(user_whitelist)
    for p in providers:
        if _norm(p) in wl or _norm(p) in _ALLOWED:
            return True
    return False