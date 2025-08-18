# engine/feedback.py
from __future__ import annotations
import json, os, time
from pathlib import Path
from typing import Dict, Any, Tuple, List, Set, Optional
from datetime import datetime, timezone

from .recency import key_for_item  # uses imdb_id -> tmdb_id -> title::year

# -------- env knobs (read by runner; safe defaults here too) --------
def _int(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v else default
    except Exception:
        return default

def _float(name: str, default: float) -> float:
    try:
        v = os.getenv(name, "")
        return float(v) if v else default
    except Exception:
        return default

FEEDBACK_JSON_PATH_DEFAULT = "data/user/feedback.json"
FEATURE_BANK_PATH = Path(os.getenv("FEEDBACK_FEATURE_BANK_PATH", "data/cache/feedback/features.json"))
FEATURE_BANK_PATH.parent.mkdir(parents=True, exist_ok=True)

FEEDBACK_DOWN_COOLDOWN_DAYS = _int("FEEDBACK_DOWN_COOLDOWN_DAYS", 14)
FEEDBACK_DECAY = _float("FEEDBACK_DECAY", 0.98)  # multiplicative decay applied to prior bank each run

# Similarity weights (how much a liked/disliked feature nudges scoring)
FB_SIM_ACTOR_W    = _float("FEEDBACK_SIMILAR_ACTOR_W",    1.4)
FB_SIM_DIRECTOR_W = _float("FEEDBACK_SIMILAR_DIRECTOR_W", 0.8)
FB_SIM_WRITER_W   = _float("FEEDBACK_SIMILAR_WRITER_W",   0.6)
FB_SIM_GENRE_W    = _float("FEEDBACK_SIMILAR_GENRE_W",    0.6)
FB_SIM_KEYWORD_W  = _float("FEEDBACK_SIMILAR_KEYWORD_W",  0.2)

def _now_ts() -> float:
    return time.time()

def _iso_to_ts(s: Optional[str]) -> Optional[float]:
    if not s: return None
    try:
        # “2025-08-18T07:22:01Z” or with offset
        return datetime.fromisoformat(s.replace("Z","+00:00")).timestamp()
    except Exception:
        return None

# ------------ Feedback persistence ------------

def load_feedback(path: Path) -> Dict[str, Any]:
    """
    feedback.json schema (written by feedback.yml workflow):
    {
      "items": {
         "tt123...": {"up": 2, "down": 0, "last_reaction": "+1", "last_at": "ISO"},
         "tm:456":   {"up": 0, "down": 3, "last_reaction": "-1", "last_at": "ISO"}
      }
    }
    """
    if not path.exists():
        return {"items": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        if not isinstance(data, dict):
            return {"items": {}}
        data.setdefault("items", {})
        return data
    except Exception:
        return {"items": {}}

def _load_bank() -> Dict[str, Any]:
    if not FEATURE_BANK_PATH.exists():
        return {
            "version": 1,
            "last_updated": None,
            "liked": {"actors":{}, "directors":{}, "writers":{}, "genres":{}, "keywords":{}},
            "disliked":{"actors":{}, "directors":{}, "writers":{}, "genres":{}, "keywords":{}},
        }
    try:
        return json.loads(FEATURE_BANK_PATH.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {
            "version": 1,
            "last_updated": None,
            "liked": {"actors":{}, "directors":{}, "writers":{}, "genres":{}, "keywords":{}},
            "disliked":{"actors":{}, "directors":{}, "writers":{}, "genres":{}, "keywords":{}},
        }

def _save_bank(bank: Dict[str, Any]) -> None:
    FEATURE_BANK_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FEATURE_BANK_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(bank, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(FEATURE_BANK_PATH)

def _decay_bank(bank: Dict[str, Any], gamma: float) -> None:
    try:
        for mood in ("liked","disliked"):
            for bucket in ("actors","directors","writers","genres","keywords"):
                d = bank[mood][bucket]
                for k in list(d.keys()):
                    d[k] = float(d[k]) * gamma
                    if abs(d[k]) < 1e-4:
                        d.pop(k, None)
    except Exception:
        pass

# ------------ Building feature bank from current items + feedback ------------

def _listify_names(x) -> List[str]:
    out: List[str] = []
    if not x: return out
    if isinstance(x, list):
        for it in x:
            if isinstance(it, dict) and it.get("name"):
                out.append(str(it["name"]).strip())
            else:
                out.append(str(it).strip())
    else:
        out.append(str(x))
    return [s for s in out if s]

def _genres_lower(it: Dict[str, Any]) -> List[str]:
    out=[]
    for g in (it.get("genres") or it.get("tmdb_genres") or []):
        if isinstance(g, dict) and g.get("name"): out.append(g["name"].lower())
        elif isinstance(g, str): out.append(g.lower())
    return out

def _keywords_lower(it: Dict[str, Any]) -> List[str]:
    return [str(k).lower() for k in (it.get("keywords") or []) if str(k).strip()]

def update_feature_bank(
    items: List[Dict[str, Any]],
    feedback: Dict[str, Any],
    *,
    cooldown_days: int = FEEDBACK_DOWN_COOLDOWN_DAYS,
    decay: float = FEEDBACK_DECAY,
) -> Tuple[Dict[str, Any], Set[str], Dict[str, Any]]:
    """
    Returns:
      - feature_bank (dict) {liked/disliked -> actors/directors/writers/genres/keywords -> weight}
      - suppress_keys (set) keys to hide due to recent 👎 within cooldown_days
      - stats (dict) telemetry
    """
    # Build index from items by stable key
    index: Dict[str, Dict[str, Any]] = {}
    for it in items:
        k = key_for_item(it)
        if k:
            index[k] = it

    fb_items: Dict[str, Any] = feedback.get("items") or {}
    suppress: Set[str] = set()
    now = _now_ts()
    cooldown_sec = max(0, cooldown_days) * 86400

    # Load and decay existing bank
    bank = _load_bank()
    _decay_bank(bank, decay)

    up_ct = down_ct = found_ct = 0

    for key, entry in fb_items.items():
        up = int(entry.get("up") or 0)
        down = int(entry.get("down") or 0)
        last = _iso_to_ts(entry.get("last_at"))
        last_reaction = (entry.get("last_reaction") or "").strip()

        if down > 0 and last and (now - last) <= cooldown_sec and last_reaction == "-1":
            suppress.add(key)

        it = index.get(key)
        if not it:
            # We still use direct key boosts/penalties later even if we can't extract features now.
            continue

        found_ct += 1
        weight = float(up - down)
        if abs(weight) < 1e-9:
            continue

        def add(bucket: str, name: str, liked: bool):
            if not name: return
            target = bank["liked" if liked else "disliked"][bucket]
            target[name] = float(target.get(name, 0.0)) + weight

        if up > 0: up_ct += 1
        if down > 0: down_ct += 1
        actors    = _listify_names(it.get("cast"))[:8]
        directors = _listify_names(it.get("directors"))[:4]
        writers   = _listify_names(it.get("writers"))[:4]
        genres    = _genres_lower(it)
        keywords  = _keywords_lower(it)

        liked = (weight > 0)
        for a in actors:    add("actors", a,    liked)
        for d in directors: add("directors", d, liked)
        for w in writers:   add("writers", w,   liked)
        for g in genres:    add("genres", g,    liked)
        for k in keywords:  add("keywords", k,  liked)

    bank["last_updated"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    _save_bank(bank)

    stats = {
        "feedback_items": len(fb_items),
        "feedback_items_in_pool": found_ct,
        "thumbs_up_keys": up_ct,
        "thumbs_down_keys": down_ct,
        "suppress_keys": len(suppress),
    }
    return bank, suppress, stats