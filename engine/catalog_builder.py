# engine/catalog_builder.py
from __future__ import annotations
import os, json, random, time
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple

from . import tmdb

POOL_DIR  = Path("data/cache/pool")
POOL_FILE = POOL_DIR / "pool.jsonl"   # line-delimited JSON
KEY = lambda it: f"{(it.get('media_type') or '').lower()}:{it.get('tmdb_id') or ''}" or (it.get('imdb_id') or "")

def _i(n: str, d: int) -> int:
    try: v=os.getenv(n,""); return int(v) if v else d
    except Exception: return d
def _jsonl_read(path: Path) -> Dict[str, Dict[str, Any]]:
    idx={}
    if not path.exists(): return idx
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line=line.strip()
            if not line: continue
            try:
                it=json.loads(line)
                k=KEY(it)
                if k: idx[k]=it
            except Exception:
                continue
    return idx
def _jsonl_append(path: Path, items: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n=0
    with path.open("a", encoding="utf-8") as fh:
        for it in items:
            try:
                fh.write(json.dumps(it, ensure_ascii=False)+"\n")
                n+=1
            except Exception:
                continue
    return n

def _pick_pages(mode: str, count: int, max_page: int) -> List[int]:
    count=max(1, min(100, count))
    max_page=max(1, min(1000, max_page))
    if mode == "random":
        pages=set()
        while len(pages) < count:
            pages.add(random.randint(1, max_page))
        return sorted(pages)
    # default rolling (first N pages)
    return list(range(1, count+1))

def _collect_for_page(kind: str, page: int, region: str, langs: List[str]) -> List[Dict[str, Any]]:
    """
    Collect multiple TMDB lists per page to widen sources.
    We defensively try several endpoints; missing ones are skipped.
    """
    out: List[Dict[str, Any]] = []
    try:
        fn = getattr(tmdb, f"discover_{kind}", None)
        if fn:
            out += (fn(page=page, region=region, langs=langs) or [])
    except Exception:
        pass
    # Popular / Top rated
    for name in ("popular", "top_rated"):
        try:
            fn = getattr(tmdb, f"{name}_{kind}", None)
            if fn:
                out += (fn(page=page, region=region) or [])
        except Exception:
            pass
    # Now-playing / Upcoming (movies) | Airing-today / On-the-air (tv)
    alt = ("now_playing","upcoming") if kind=="movie" else ("airing_today","on_the_air")
    for name in alt:
        try:
            fn = getattr(tmdb, f"{name}_{kind}", None)
            if fn:
                out += (fn(page=page, region=region) or [])
        except Exception:
            pass
    # Trending
    try:
        fn = getattr(tmdb, f"trending_{kind}", None)
        if fn:
            out += (fn(page=page, region=region) or [])
    except Exception:
        pass
    # Unify media_type + tiny normalize
    for it in out:
        it.setdefault("media_type", kind)
        # normalize basic fields used downstream
        if it.get("id") and not it.get("tmdb_id"):
            it["tmdb_id"] = it["id"]
    return out

def build_catalog(env: Dict[str, Any]) -> List[Dict[str, Any]]:
    region = env.get("REGION","US")
    langs  = env.get("ORIGINAL_LANGS",["en"])
    mode   = os.getenv("DISCOVER_PAGING_MODE","rolling").strip().lower() or "rolling"
    pages  = _pick_pages(mode, _i("DISCOVER_PAGES", 12), _i("DISCOVER_PAGE_MAX", 200))

    # Load existing pool
    pool_idx = _jsonl_read(POOL_FILE)
    before = len(pool_idx)

    # Collect across pages for movies + tv
    collected: Dict[str, Dict[str, Any]] = {}
    for page in pages:
        for kind in ("movie","tv"):
            items = _collect_for_page(kind, page, region, langs)
            for it in items:
                k=KEY(it)
                if not k: continue
                collected[k]=it

    # Append only *new* entries
    new_items: List[Dict[str, Any]] = []
    for k, it in collected.items():
        if k not in pool_idx:
            new_items.append(it)
            pool_idx[k]=it
    appended = _jsonl_append(POOL_FILE, new_items) if new_items else 0

    # Telemetry clarity
    env["POOL_TELEMETRY"] = {
        "pages_mode": mode,
        "pages_used": pages,
        "pool_size_before": before,
        "pool_size_after": len(pool_idx),
        "pool_appended_this_run": appended,
        "source_kinds": ["movie","tv"],
    }
    # legacy field for downstream (kept for compatibility)
    env["DISCOVERED_COUNT"] = appended

    # Return full pool snapshot (bounded)
    max_items = int(env.get("POOL_MAX_ITEMS", 20000) or 20000)
    items = list(pool_idx.values())
    if len(items) > max_items:
        items = items[-max_items:]
    return items