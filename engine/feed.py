from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple


def _out_dir_daily(meta: Dict[str, Any]) -> str:
    # caller can set this; otherwise default into YYYY-MM-DD under /data/out/daily
    day = meta.get("day") or meta.get("date") or ""
    d = os.path.join("data", "out", "daily", str(day) if day else "")
    os.makedirs(d, exist_ok=True)
    return d


def _out_dir_latest() -> str:
    d = os.path.join("data", "out", "latest")
    os.makedirs(d, exist_ok=True)
    return d


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _mk_table(top: List[Dict[str, Any]], n: int) -> str:
    lines = ["| # | Title | Year | Type | Match |",
             "|---:|---|---:|---|---:|"]
    for i, it in enumerate(top[:n], 1):
        lines.append(f"| {i} | {it.get('title','')} | {it.get('year') or ''} | {it.get('type')} | {round(float(it.get('match',0.0)),2)} |")
    return "\n".join(lines)


def build_feed_document(
    ranked: List[Dict[str, Any]],
    *,
    shortlist_size: int,
    shown_count: int,
    pool_size: int,
    unseen_count: int,
    day_stamp: str,
) -> Dict[str, Any]:
    shortlist = ranked[:shortlist_size]
    shown = shortlist[:shown_count]

    md = []
    md.append(f"# Nightly Recommendations — {day_stamp}")
    md.append("")
    md.append(f"**Pool:** {pool_size}  •  **Unseen:** {unseen_count}  •  **Shortlist:** {shortlist_size}  •  **Shown:** {shown_count}")
    md.append("")
    md.append("## Top 10")
    md.append("")
    md.append(_mk_table(shortlist, min(shown_count, 10)))
    md_str = "\n".join(md)

    return {
        "meta": {
            "pool": pool_size,
            "unseen": unseen_count,
            "shortlist": shortlist_size,
            "shown": shown_count,
            "day": day_stamp,
        },
        "shortlist": shortlist,
        "shown": shown,
        "top_markdown": md_str,
    }


def write_feed(feed_doc: Dict[str, Any], meta: Dict[str, Any]) -> None:
    latest_dir = _out_dir_latest()
    _write_json(os.path.join(latest_dir, "assistant_feed.json"), feed_doc)
    _write_text(os.path.join(latest_dir, "assistant_feed.md"), feed_doc.get("top_markdown") or "")

    # Optional dated output
    daily_dir = _out_dir_daily(feed_doc.get("meta", {}))
    _write_json(os.path.join(daily_dir, "assistant_feed.json"), feed_doc)
    _write_text(os.path.join(daily_dir, "assistant_feed.md"), feed_doc.get("top_markdown") or "")