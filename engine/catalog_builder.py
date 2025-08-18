# engine/catalog_builder.py
from __future__ import annotations
import os, json, random
from pathlib import Path
from typing import Dict, Any, Iterable, List

from . import tmdb

POOL_DIR  = Path("data/cache/pool")
POOL_FILE = POOL_DIR / "pool.jsonl"         # primary
LEGACY_ND = POOL_DIR / "catalog.ndjson"     # fallback/legacy
KEY       = lambda it: f"{(it.get('media_type') or '').lower()}:{it.get('tmdb_id') or ''}" or (it.get('imdb_id') or "")

def _i(n: str, d: int) -> int:
    try:
        v = os.getenv(n, "")
        return int(v) if v else d
    except Exception:
        return d

def _read_lines_json(path: Path) -> Dict[str, Dict[str, Any]]:
    idx={}
    if not path.exists(): return idx
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line=line.strip()
            if not line: continue
            try:
                it=json.loads(line)
                # normalize
                if it.get("id") and not it.get("tmdb_id"):
                    it["tmdb_id"] = it["id"]
                if it.get("media_type") and not isinstance(it["media_type"], str):
                    it["media_type"] = str(it["media_type"])
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
    return list(range(1, count+1))

def _collect_for_page(kind: str, page: int, region: str, langs: List[str], tel: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    def call(fn_name: str):
        fn = getattr(tmdb, fn_name, None)
        if not fn: return
        try:
            res = fn(page=page, region=region, langs=langs) if "discover" in fn_name else fn(page=page, region=region)
            if res: out.extend(res)
        except Exception:
            tel.setdefault("errors", {}).setdefault("tmdb_calls_failed", 0)
            tel["errors"]["tmdb_calls_failed"] += 1

    # primary discover
    call(f"discover_{kind}")
    # popular/top rated
    for name in ("popular", "top_rated"):
        fn = f"{name}_{kind}"
        call(fn)
    # now-playing/upcoming or tv airing/on-the-air
    alt = ("now_playing","upcoming") if kind=="movie" else ("airing_today","on_the_air")
    for name in alt:
        call(f"{name}_{kind}")
    # trending
    call(f"trending_{kind}")

    for it in out:
        it.setdefault("media_type", kind)
        if it.get("id") and not it.get("tmdb_id"):
            it["tmdb_id"] = it["id"]
    return out

def build_catalog(env: Dict[str, Any]) -> List[Dict[str, Any]]:
    region = env.get("REGION","US")
    langs  = env.get("ORIGINAL_LANGS",["en"])
    mode   = (os.getenv("DISCOVER_PAGING_MODE","rolling") or "rolling").strip().lower()
    pages  = _pick_pages(mode, _i("DISCOVER_PAGES", 12), _i("DISCOVER_PAGE_MAX", 200))

    # Load existing pool from either format (merge if both present)
    pool_idx = {}
    srcs=[]
    j1 = _read_lines_json(POOL_FILE)
    if j1:
        pool_idx.update(j1); srcs.append(POOL_FILE.name)
    j2 = _read_lines_json(LEGACY_ND)
    if j2:
        for k,v in j2.items():
            pool_idx.setdefault(k, v)
        srcs.append(LEGACY_ND.name)
    before = len(pool_idx)

    tel = {"pages_mode": mode, "pages_used": pages, "source_kinds": ["movie","tv"], "loaded_from": srcs}
    collected: Dict[str, Dict[str, Any]] = {}
    for page in pages:
        for kind in ("movie","tv"):
            items = _collect_for_page(kind, page, region, langs, tel)
            for it in items:
                k=KEY(it)
                if k: collected[k]=it

    # Append only new entries
    new_items=[]
    for k, it in collected.items():
        if k not in pool_idx:
            pool_idx[k]=it
            new_items.append(it)

    appended = _jsonl_append(POOL_FILE, new_items) if new_items else 0

    env["POOL_TELEMETRY"] = {
        **tel,
        "pool_size_before": before,
        "pool_size_after": len(pool_idx),
        "pool_appended_this_run": appended,
    }
    env["DISCOVERED_COUNT"] = appended

    max_items = int(env.get("POOL_MAX_ITEMS", 20000) or 20000)
    items = list(pool_idx.values())
    if len(items) > max_items:
        items = items[-max_items:]
    return items