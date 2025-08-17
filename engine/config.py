from __future__ import annotations

import json
import os
from typing import Any, Dict


class Config:
    """
    Lightweight config wrapper that reads (in order of precedence):
      1) explicit kwargs passed to the constructor
      2) a JSON file (path via CONFIG_JSON or default: data/config.json)
      3) environment variables (UPPER_SNAKE)
    Accessing a missing attribute raises AttributeError (so callers can
    choose their own defaults cleanly).
    """

    def __init__(self, **overrides: Any) -> None:
        self._env = dict(os.environ)
        cfg_path = self._env.get("CONFIG_JSON", "data/config.json")
        self._file: Dict[str, Any] = {}
        if os.path.exists(cfg_path):
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    self._file = loaded
            except Exception:
                # Non-fatal; just run with env/overrides
                pass
        self._overrides = overrides

    def __getattr__(self, key: str) -> Any:
        # 1) explicit overrides
        if key in self._overrides:
            return self._overrides[key]
        # 2) file
        if key in self._file:
            return self._file[key]
        # 3) environment
        env_key = key.upper()
        if env_key in self._env:
            return self._env[env_key]
        # Not found -> let caller decide defaulting
        raise AttributeError(key)