import os, json, glob, datetime

def latest_out_dir():
    base = "data/out/daily"
    if not os.path.isdir(base): return None
    dirs = sorted([d for d in glob.glob(os.path.join(base, "*")) if os.path.isdir(d)])
    return dirs[-1] if dirs else None

def main():
    out = latest_out_dir()
    if not out:
        print("Top 10\n(no results)\nTelemetry: pool=0, eligible=0, after_skip=0, shown=0")
        return
    recs = {}
    tel = {}
    wts = {}
    try:
        recs = json.load(open(os.path.join(out,"recs.json"), "r"))
    except Exception:
        pass
    try:
        tel = json.load(open(os.path.join(out,"telemetry.json"), "r"))
    except Exception:
        pass

    picks = (recs.get("recs") if isinstance(recs, dict) else [])[:10]
    weights = (recs.get("weights") if isinstance(recs, dict) else {}) or {}

    print("Top 10")
    if not picks:
        print("(no results)")
    else:
        for i, r in enumerate(picks, 1):
            line = f"\t{i}\t{r.get('match',0)} — {r.get('title','?')} ({r.get('year','?')}) [{r.get('type','?')}]"
            extras = []
            if r.get("rt") is not None: extras.append(f"RT {int(r['rt'])}%")
            if r.get("imdb_rating") is not None: extras.append(f"IMDb {r['imdb_rating']}/10")
            if extras: line += "  (" + ", ".join(extras) + ")"
            print(line)

    tel_line = f'Telemetry: pool={tel.get("pool",0)}, eligible={tel.get("eligible_unseen",0)}, after_skip={tel.get("after_skip",0)}, shown={tel.get("shown",0)}'
    print(tel_line)
    if weights:
        print(f"Weights: critic={weights.get('critic_weight',0):.2f}, audience={weights.get('audience_weight',0):.2f}")
    print("This product uses the TMDB and OMDb APIs but is not endorsed or certified by them.")

if __name__ == "__main__":
    main()