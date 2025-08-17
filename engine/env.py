from __future__ import annotations
import os
from typing import Any, Dict, Iterable, List, Optional


def _split_csv_env(val: Optional[str]) -> List[str]:
    if not val:
        return []
    # split by comma/space and keep non-empty
    parts = [p.strip() for p in val.replace(" ", ",").split(",")]
    return [p for p in parts if p]


class Env(dict):
    """
    A tiny env/config helper that behaves like both:
      - a dict (supports .get, indexing)
      - an object with attributes (env.REGION, etc.)
    Values are normalized to the types the engine expects.
    """

    # allow attribute access to dict keys
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as ex:
            raise AttributeError(name) from ex

    def __setattr__(self, name: str, value: Any) -> None:
        # store everything in the dict
        self[name] = value

    # explicitly keep dict.get so older code paths work
    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        return super().get(key, default)

    @staticmethod
    def _bool(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in ("1", "true", "yes", "y", "on")

    @classmethod
    def from_os_and_defaults(cls) -> "Env":
        """
        Build Env from process env vars with safe defaults. These names
        intentionally match what the rest of the engine expects.
        """
        e = cls()

        # Region & languages
        e["REGION"] = os.getenv("REGION", "US").strip() or "US"
        e["ORIGINAL_LANGS"] = _split_csv_env(os.getenv("ORIGINAL_LANGS") or "en")

        # Subscriptions / providers
        # CSV of slugs like: netflix,prime_video,hulu,...
        e["SUBS_INCLUDE"] = _split_csv_env(os.getenv("SUBS_INCLUDE") or "")

        # Discovery paging/rotation
        e["DISCOVER_PAGES"] = int(os.getenv("DISCOVER_PAGES", "3"))
        e["ROTATE_MINUTES"] = int(os.getenv("ROTATE_MINUTES", "60"))
        e["DISCOVER_STEP"] = int(os.getenv("DISCOVER_STEP", "3"))
        e["DISCOVER_CAP"] = int(os.getenv("DISCOVER_CAP", "30"))

        # Data directory (for outputs, caches, ratings.csv, etc.)
        e["DATA_DIR"] = os.getenv("DATA_DIR", "data")

        # Paths commonly used elsewhere
        data_dir = e["DATA_DIR"]
        e["RATINGS_CSV"] = os.getenv("RATINGS_CSV", os.path.join(data_dir, "ratings.csv"))
        e["OUT_DIR"] = os.path.join(data_dir, "out")
        e["CACHE_DIR"] = os.path.join(data_dir, "cache")

        # Optional external identifiers
        e["TMDB_ACCESS_TOKEN"] = os.getenv("TMDB_ACCESS_TOKEN", "").strip()
        e["IMDB_USER_ID"] = os.getenv("IMDB_USER_ID", "").strip()

        # Scoring knobs (safe defaults)
        e["CRITIC_WEIGHT"] = float(os.getenv("CRITIC_WEIGHT", "0.0"))
        e["AUDIENCE_WEIGHT"] = float(os.getenv("AUDIENCE_WEIGHT", "1.0"))
        e["COMMITMENT_COST_SCALE"] = float(os.getenv("COMMITMENT_COST_SCALE", "1.0"))

        # Match cut
        e["MATCH_CUT"] = float(os.getenv("MATCH_CUT", "58.0"))

        return e

    def asdict(self) -> Dict[str, Any]:
        return dict(self)
