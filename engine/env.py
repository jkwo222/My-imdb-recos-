# engine/env.py
from __future__ import annotations
import json
import os
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping


class Env(MutableMapping[str, Any]):
    """
    Tiny wrapper around a dict so the rest of the code can use BOTH:
      - mapping-style access: env["REGION"], env.get("REGION")
      - attribute-style access: env.REGION

    Also provides helpers to build from OS env or a plain mapping.
    """

    # ---------- constructors ----------

    def __init__(self, data: Mapping[str, Any] | None = None) -> None:
        self._data: Dict[str, Any] = dict(data or {})

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "Env":
        """Create Env from a plain mapping (dict)."""
        return cls(mapping)

    @classmethod
    def from_os_environ(cls) -> "Env":
        """
        Build an Env from process environment variables.
        Handles ORIGINAL_LANGS given as JSON list (e.g. '["en","es"]')
        or CSV (e.g. 'en,es').
        """
        region = os.getenv("REGION", "US").strip() or "US"

        langs_raw = os.getenv("ORIGINAL_LANGS", "").strip()
        if langs_raw.startswith("[") and langs_raw.endswith("]"):
            try:
                langs = [str(x).strip() for x in json.loads(langs_raw) if str(x).strip()]
            except Exception:
                langs = ["en"]
        elif "," in langs_raw:
            langs = [t.strip() for t in langs_raw.split(",") if t.strip()]
        else:
            langs = [langs_raw] if langs_raw else ["en"]

        subs_include = os.getenv("SUBS_INCLUDE", "").strip()
        pages_raw = os.getenv("DISCOVER_PAGES", "").strip()
        try:
            pages = int(pages_raw) if pages_raw else 3
        except Exception:
            pages = 3

        return cls({
            "REGION": region,
            "ORIGINAL_LANGS": langs,
            "SUBS_INCLUDE": subs_include,
            "DISCOVER_PAGES": pages,
        })

    # ---------- mapping protocol ----------

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    # ---------- convenience ----------

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        return self._data.get(key, default)

    def as_dict(self) -> Dict[str, Any]:
        """Return a plain dict copy."""
        return dict(self._data)

    # Allow attribute-style access for known keys
    def __getattr__(self, name: str) -> Any:
        # Only called if normal attribute lookup fails
        try:
            return self._data[name]
        except KeyError as e:
            raise AttributeError(name) from e

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self._data[name] = value

    def __repr__(self) -> str:
        return f"Env({self._data!r})"