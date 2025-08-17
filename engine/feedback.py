# engine/feedback.py
from __future__ import annotations
import json, re, time
from pathlib import Path
from typing import Dict, List, Any, Tuple, Set
from rapidfuzz import process, fuzz

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "data" / "cache" / "state"
INBOX_DIR = ROOT / "data" / "inbox"
STATE_DIR.mkdir(parents=True, exist_ok=True)
INBOX_DIR.mkdir(parents=True, exist_ok=True)

DOWNVOTES_JSON = STATE_DIR / "downvotes.json"   # persistent memory
COMMENTS_JSON = INBOX_DIR / "gh_comments.json"  # fetched by workflow (optional)
DOWNVOTES_TXT  = INBOX_DIR / "downvotes.txt"    # optional manual input

# -------- persistence --------
def load_downvote_state() -> Dict[str, Any]:
    if DOWNVOTES_JSON.exists():
        try:
            return json.loads(DOWNVOTES_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"titles": {}, "genres": {}, "hidden": set()}

def save_downvote_state(state: Dict[str, Any]) -> None:
    # sets aren’t JSON serializable
    st = dict(state)
    if isinstance(st.get("hidden"), set):
        st["hidden"] = sorted(st["hidden"])
    DOWNVOTES_JSON.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

# -------- parsing helpers --------
TC_RE = re.compile(r"\btt\d{7,8}\b", re.I)
SKIP_GENRE_RE = re.compile(r"skip\s+genre\s*:\s*(.+)", re.I)
HIDE_RE = re.compile(r"hide\s*:\s*(.+)", re.I)
DOWNVOTE_WORD_RE = re.compile(r"\b(downvote|👎)\b", re.I)

def _read_issue_comments_dump() -> List[Dict[str, Any]]:
    if not COMMENTS_JSON.exists():
        return []
    try:
        return json.loads(COMMENTS_JSON.read_text(encoding="utf-8"))
    except Exception:
        return []

def _read_manual_lines() -> List[str]:
    if not DOWNVOTES_TXT.exists():
        return []
    return [ln.strip() for ln in DOWNVOTES_TXT.read_text(encoding="utf-8").splitlines() if ln.strip()]

def _normalize_title(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()

def _title_key(it: Dict[str, Any]) -> str:
    t = f'{it.get("title","")}'.strip()
    y = it.get("year")
    return _normalize_title(f"{t} ({y})" if y else t)

# -------- main API --------
def collect_downvote_events(items: List[Dict[str,Any]]) -> Dict[str, Any]:
    """
    Returns a dict:
      { "tconsts": set([...]),
        "title_keys": set([... "the godfather (1972)"]),
        "genres": set([...]),
        "hide_title_keys": set([...]) }
    """
    events = {"tconsts": set(), "title_keys": set(), "genres": set(), "hide_title_keys": set()}
    sources: List[str] = []

    # 1) From issue comments JSON (array of GH comment objects)
    for c in _read_issue_comments_dump():
        body = (c.get("body") or "").strip()
        if not body:
            continue
        sources.append(body)

    # 2) From manual file
    sources.extend(_read_manual_lines())

    # parse tokens from all sources
    for body in sources:
        # imdb ids
        for m in TC_RE.findall(body):
            events["tconsts"].add(m.lower())

        # explicit downvote by title/year (requires "downvote" or 👎 present)
        if DOWNVOTE_WORD_RE.search(body):
            # try to extract lines after the marker
            for part in re.split(r"[,\n;/]+", body):
                part = part.strip()
                if not part or TC_RE.search(part) or SKIP_GENRE_RE.search(part) or HIDE_RE.search(part):
                    continue
                # treat as possible "Title (Year)" or just Title
                events["title_keys"].add(_normalize_title(part))

        # skip genre:
        mg = SKIP_GENRE_RE.findall(body)
        for g in mg:
            events["genres"].add(g.strip().lower())

        # hide:
        mh = HIDE_RE.findall(body)
        for h in mh:
            events["hide_title_keys"].add(_normalize_title(h))

    # fuzzy-map freeform titles to today's items to produce tconst hits
    if events["title_keys"]:
        choices = { _title_key(it): it.get("tconst") for it in items }
        for key in list(events["title_keys"]):
            match = process.extractOne(key, choices.keys(), scorer=fuzz.WRatio)
            if match and match[1] >= 90:  # pretty strict
                tconst = choices[match[0]]
                if tconst:
                    events["tconsts"].add(tconst.lower())

    # also map hide_title_keys to today’s tconsts (for immediate effect)
    mapped_hide = set()
    if events["hide_title_keys"]:
        choices = { _title_key(it): it.get("tconst") for it in items }
        for key in list(events["hide_title_keys"]):
            match = process.extractOne(key, choices.keys(), scorer=fuzz.WRatio)
            if match and match[1] >= 90:
                tc = choices[match[0]]
                if tc:
                    mapped_hide.add(tc.lower())

    events["hide_tconsts"] = mapped_hide
    return events

def update_downvote_state(state: Dict[str,Any], events: Dict[str,Any], now: float|None=None) -> None:
    """
    Mutates state with new events.
    state schema:
      {
        "titles": { "tt123": [{"ts": 1699999999, "weight": 1.0}, ...], ... },
        "genres": { "western": [{"ts": ..., "weight": 1.0}], ... },
        "hidden": ["tt...", ...]
      }
    """
    ts = now if now is not None else time.time()
    titles = state.setdefault("titles", {})
    genres = state.setdefault("genres", {})
    hidden = set(state.get("hidden") or [])

    for tc in events.get("tconsts", []):
        titles.setdefault(tc, []).append({"ts": ts, "weight": 1.0})

    for g in events.get("genres", []):
        genres.setdefault(g.lower(), []).append({"ts": ts, "weight": 1.0})

    hidden |= set([x.lower() for x in events.get("hide_tconsts", set())])
    state["hidden"] = sorted(hidden)

def compute_penalties(
    items: List[Dict[str,Any]],
    state: Dict[str,Any],
    half_life_days: float = 60.0,
    base_title_penalty: float = 25.0,
    base_genre_penalty: float = 8.0,
    hide_threshold: int = 2,
) -> Tuple[Dict[str,float], Set[str], Dict[str,float]]:
    """
    Returns (title_penalties, hidden_tconsts, genre_penalties)
    - title penalty decays over time; multiple downvotes add up.
    - genre penalty decays likewise.
    - if a title has >= hide_threshold non-decayed downvotes, hide it.
    """
    title_pen = {}
    genre_pen = {}
    hidden = set(map(str.lower, state.get("hidden") or []))

    def decayed(weight: float, age_days: float) -> float:
        # exponential decay: w * 0.5^(age/half_life)
        return weight * (0.5 ** (age_days / max(half_life_days, 1e-6)))

    now = time.time()
    # title penalties
    for tc, events in (state.get("titles") or {}).items():
        s = 0.0
        active_count = 0.0
        for ev in events:
            age_days = (now - float(ev.get("ts", now))) / 86400.0
            w = decayed(float(ev.get("weight", 1.0)), age_days)
            if w > 0.2:  # counts as “active” downvote
                active_count += 1.0
            s += w
        if s > 0:
            title_pen[tc.lower()] = base_title_penalty * s
        if active_count >= hide_threshold:
            hidden.add(tc.lower())

    # genre penalties
    for g, events in (state.get("genres") or {}).items():
        s = 0.0
        for ev in events:
            age_days = (now - float(ev.get("ts", now))) / 86400.0
            s += decayed(float(ev.get("weight", 1.0)), age_days)
        if s > 0:
            genre_pen[g.lower()] = base_genre_penalty * s

    # Map penalties to today’s set (only for titles present today)
    today_ids = { (it.get("tconst") or "").lower() for it in items if it.get("tconst") }
    title_pen = { tc: p for tc, p in title_pen.items() if tc in today_ids }
    hidden_today = { tc for tc in hidden if tc in today_ids }

    return title_pen, hidden_today, genre_pen