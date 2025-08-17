# tools/check_exclusions.py
from __future__ import annotations
import json, os, sys
from engine.exclusions import build_exclusion_index, is_excluded

def main():
    csv_path = os.getenv("RATINGS_CSV", "data/ratings.csv")
    pool_path = "data/catalog_store.json"

    if not os.path.exists(csv_path):
        print(f"[warn] ratings CSV not found at {csv_path} — skipping.", flush=True)
        return

    if not os.path.exists(pool_path):
        print(f"[warn] pool not found at {pool_path} — skipping.", flush=True)
        return

    idx = build_exclusion_index(csv_path)
    with open(pool_path, "r", encoding="utf-8") as f:
        try:
            items = json.load(f)
        except Exception:
            print("[error] could not parse catalog_store.json", flush=True)
            sys.exit(2)

    bad = [it for it in items if is_excluded(it, idx)]
    print(f"Pool size={len(items)}  blocked_in_pool={len(bad)}", flush=True)
    if bad:
        for it in bad[:25]:
            print(" -", it.get("title"), it.get("year"))
        sys.exit(1)

if __name__ == "__main__":
    main()