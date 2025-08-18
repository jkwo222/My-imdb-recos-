# engine/enrich.py
from __future__ import annotations
import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from difflib import SequenceMatcher

from . import tmdb

def _bool(n: str, d: bool) -> bool:
    v = (os.getenv(n, "") or "").strip().lower()
    if v in {"1","true","yes","on"}: return True
    if v in {"0","false","no","off"}: return False
    return d
def _int(n: str, d: int) -> int:
    try: return int(os.getenv(n, "") or d)
    except Exception: return d

REGION = os.getenv("REGION", "US")
SEARCH_MULTI_ON_EMPTY_DETAILS = _bool("SEARCH_MULTI_ON_EMPTY_DETAILS", True)
SEARCH_MULTI_ON_MISSING_ID   = _bool("SEARCH_MULTI_ON_MISSING_ID", True)
SEARCH_MULTI_TITLE_SIM_TH    = float(os.getenv("SEARCH_MULTI_TITLE_SIM_TH", "0.62") or 0.62)
SEARCH_MULTI_YEAR_WEIGHT     = float(os.getenv("SEARCH_MULTI_YEAR_WEIGHT", "0.35") or 0.35)
SEARCH_MULTI_TYPE_BONUS      = float(os.getenv("SEARCH_MULTI_TYPE_BONUS", "0.25") or 0.25)
ENRICH_SCORING_TOP_N         = _int("ENRICH_SCORING_TOP_N", 220)

@dataclass
class Telemetry:
    items_in: int = 0
    items_out: int = 0
    details_ok: int = 0
    credits_ok: int = 0
    keywords_ok: int = 0
    externals_ok: int = 0
    providers_ok: int = 0
    used_search_multi: int = 0
    search_multi_no_match: int = 0
    empty_after_all: int = 0

def _title_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

def _pick_title_year(mt: str, d: Dict[str, Any]) -> Tuple[str, Optional[int]]:
    if (mt or "").lower() == "movie":
        title = d.get("title") or d.get("name") or d.get("original_title") or d.get("original_name") or ""
        y=None; rd=(d.get("release_date") or "").strip()
        if len(rd)>=4 and rd[:4].isdigit(): y=int(rd[:4])
        return title, y
    else:
        title = d.get("name") or d.get("title") or d.get("original_name") or d.get("original_title") or ""
        y=None; fd=(d.get("first_air_date") or "").strip()
        if len(fd)>=4 and fd[:4].isdigit(): y=int(fd[:4])
        return title, y

def _choose_search_hit(mt: str, title: str, year: Optional[int], hits: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best=None; best_score=-1e9
    for h in hits:
        h_mt=(h.get("media_type") or "").lower()
        h_title, h_year = _pick_title_year(h_mt, h)
        sim = _title_sim(title, h_title)
        type_bonus = SEARCH_MULTI_TYPE_BONUS if (mt and h_mt == mt.lower()) else 0.0
        year_pen = 0.0
        if year and h_year:
            year_pen = min(1.0, abs(year-h_year) * SEARCH_MULTI_YEAR_WEIGHT / 10.0)
        score = type_bonus + sim - year_pen
        if score > best_score:
            best_score = score; best = h
    if not best:
        return None
    h_title, _ = _pick_title_year((best.get("media_type") or ""), best)
    if _title_sim(title, h_title) < SEARCH_MULTI_TITLE_SIM_TH:
        return None
    return best

def _apply_details(mt: str, base: Dict[str, Any], details: Dict[str, Any]) -> None:
    if not details: return
    for k, v in details.items():
        if k in {"title","name"} and base.get(k): continue
        base[k] = v

def _enrich_one(item: Dict[str, Any], tel: Telemetry) -> Optional[Dict[str, Any]]:
    it = dict(item)
    mt = (it.get("media_type") or it.get("type") or "movie").lower()
    if mt not in {"movie","tv"}:
        mt = "movie"; it["media_type"] = "movie"

    tmdb_id = it.get("tmdb_id") or it.get("id")
    title   = it.get("title") or it.get("name") or ""
    _, year = _pick_title_year(mt, it)

    details = {}
    if tmdb_id:
        details = tmdb.get_details(mt, int(tmdb_id))
        if details: tel.details_ok += 1

    need_search = (SEARCH_MULTI_ON_MISSING_ID and not tmdb_id) or (SEARCH_MULTI_ON_EMPTY_DETAILS and not details)
    if need_search and title.strip():
        hits = tmdb.search_multi(f"{title} {year}" if year else title, page=1, region=REGION)
        best = _choose_search_hit(mt, title, year, hits)
        if best:
            tmdb_id = best.get("tmdb_id") or best.get("id")
            it["tmdb_id"] = tmdb_id
            it["media_type"] = (best.get("media_type") or mt).lower()
            mt = it["media_type"]
            b_title, b_year = _pick_title_year(mt, best)
            if b_title: it.setdefault("title", b_title)
            if b_year and not it.get("year"): it["year"] = b_year
            details = tmdb.get_details(mt, int(tmdb_id))
            if details: tel.details_ok += 1
            tel.used_search_multi += 1
        else:
            tel.search_multi_no_match += 1

    if not tmdb_id or not details:
        tel.empty_after_all += 1
        return None

    _apply_details(mt, it, details)

    try:
        credits = tmdb.get_credits(mt, int(tmdb_id))
        if credits:
            it.update({k: v for k, v in credits.items() if v})
            tel.credits_ok += 1
    except Exception: pass

    try:
        kws = tmdb.get_keywords(mt, int(tmdb_id))
        if kws:
            it["keywords"] = kws
            tel.keywords_ok += 1
    except Exception: pass

    try:
        ex = tmdb.get_external_ids(mt, int(tmdb_id))
        if ex:
            it.update(ex)
            tel.externals_ok += 1
    except Exception: pass

    try:
        provs = tmdb.get_title_watch_providers(mt, int(tmdb_id), region=REGION)
        if provs:
            it["providers"] = provs
            tel.providers_ok += 1
    except Exception: pass

    it["media_type"] = mt
    if not it.get("year"):
        _, y = _pick_title_year(mt, it)
        if y: it["year"] = y
    return it

def enrich_items(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Telemetry]:
    tel = Telemetry(items_in=len(items))
    out: List[Dict[str, Any]] = []
    work = items[:ENRICH_SCORING_TOP_N] if ENRICH_SCORING_TOP_N > 0 else items
    for it in work:
        e = _enrich_one(it, tel)
        if e: out.append(e)
    tel.items_out = len(out)
    return out, tel

def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def _append_tel_to_diag(run_dir: Path, tel: Telemetry) -> None:
    diag_path = run_dir / "diag.json"
    diag = _read_json(diag_path) or {}
    counts = diag.get("counts") or {}
    counts.update({
        "enrich_items_in": tel.items_in,
        "enrich_items_out": tel.items_out,
        "enrich_details_ok": tel.details_ok,
        "enrich_credits_ok": tel.credits_ok,
        "enrich_keywords_ok": tel.keywords_ok,
        "enrich_externals_ok": tel.externals_ok,
        "enrich_providers_ok": tel.providers_ok,
        "enrich_used_search_multi": tel.used_search_multi,
        "enrich_search_multi_no_match": tel.search_multi_no_match,
        "enrich_empty_after_all": tel.empty_after_all,
    })
    diag["counts"] = counts
    _write_json(diag_path, diag)

def write_enriched(*, items_in_path: Path, out_path: Path, run_dir: Optional[Path] = None) -> Path:
    raw = _read_json(items_in_path) or []
    enriched, tel = enrich_items(list(raw))
    _write_json(out_path, enriched)
    if run_dir:
        _append_tel_to_diag(run_dir, tel)
    return out_path

def _parse_args():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="items.discovered.json")
    ap.add_argument("--out", dest="out", required=True, help="items.enriched.json")
    ap.add_argument("--run-dir", dest="run_dir", default=None, help="run directory for diag.json")
    return ap.parse_args()

def main():
    args = _parse_args()
    run_dir = Path(args.run_dir) if args.run_dir else None
    write_enriched(items_in_path=Path(args.inp), out_path=Path(args.out), run_dir=run_dir)

if __name__ == "__main__":
    main()