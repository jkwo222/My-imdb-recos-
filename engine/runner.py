# engine/runner.py  (only the exclusions block changed + diag additions)
# ... (imports and earlier code unchanged) ...
from .exclusions import load_seen_index as _load_seen_index, filter_unseen as _filter_unseen, merge_with_public as _merge_seen_public

# inside main(), replace the exclusions section with this:

    # Exclusions — STRICT "seen = out"
    excl_info = {"ratings_rows": 0, "public_ids": 0, "excluded_count": 0}
    try:
        seen_idx: Dict[str, bool] = {}
        ratings_csv = Path("data/user/ratings.csv")
        if ratings_csv.exists():
            seen_idx = _load_seen_index(ratings_csv)
            # Count approx rows via unique title/year or ids
            excl_info["ratings_rows"] = sum(1 for k in seen_idx.keys() if k.startswith("tt"))
        # Augment with public IMDb
        before_pub = len(seen_idx)
        seen_idx = _merge_seen_public(seen_idx)
        excl_info["public_ids"] = max(0, len(seen_idx) - before_pub)

        pre_ct = len(items)
        items = _filter_unseen(items, seen_idx)
        excl_info["excluded_count"] = pre_ct - len(items)
        _log(f"[exclusions] applied strict filter: removed={excl_info['excluded_count']} (ratings_ids~{excl_info['ratings_rows']}, public_ids={excl_info['public_ids']})")
    except Exception as ex:
        _log(f"[exclusions] FAILED: {ex!r}")
        traceback.print_exc()
# ... keep the rest as in your current runner (scoring, enrichment, etc.) ...

# When writing diag, add EXCLUSIONS into env:
    diag = {
        "ran_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "run_seconds": round(time.time() - t0, 3),
        "env": {
            "REGION": env.get("REGION", "US"),
            "ORIGINAL_LANGS": env.get("ORIGINAL_LANGS", []),
            "SUBS_INCLUDE": env.get("SUBS_INCLUDE", []),
            "DISCOVER_PAGES": env.get("DISCOVER_PAGES", 0),
            "PROVIDER_MAP": env.get("PROVIDER_MAP", {}),
            "PROVIDER_UNMATCHED": env.get("PROVIDER_UNMATCHED", []),
            "POOL_TELEMETRY": env.get("POOL_TELEMETRY", {}),
            "EXCLUSIONS": excl_info,  # <-- NEW
        },
        # ...
    }