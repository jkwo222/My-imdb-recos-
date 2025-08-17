from __future__ import annotations
import os
from typing import Any, Dict, Optional

def _as_bool(s: Optional[str]) -> Optional[bool]:
    if s is None:
        return None
    v = s.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return None

def get_persona(env: Dict[str, Any]) -> Dict[str, Any]:
    preset = os.getenv("PERSONA_PRESET", "").strip().lower()
    mood = os.getenv("PERSONA_MOOD", "").strip().lower() or None
    runtime_limit = os.getenv("PERSONA_RUNTIME_LIMIT", "").strip()
    family_mode = _as_bool(os.getenv("PERSONA_FAMILY_MODE"))
    try:
        runtime_limit_mins = int(runtime_limit) if runtime_limit else None
    except Exception:
        runtime_limit_mins = None

    persona = {
        "preset": preset or "default",
        "mood": mood or "open",
        "runtime_limit_mins": runtime_limit_mins,
        "family_mode": family_mode if family_mode is not None else False,
        "region": env.get("REGION", "US"),
        "langs": env.get("ORIGINAL_LANGS", ["en"]),
        "subs_include": env.get("SUBS_INCLUDE", []),
    }
    return persona

# Back-compat alias
load_persona = get_persona