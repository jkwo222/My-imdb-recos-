# tools/render_issue.py
import json, glob, os

today = os.environ.get("today") or ""
if not today:
    # allow reading the newest folder
    paths = sorted(glob.glob("data/out/daily/*/recs.json"))
    path = paths[-1] if paths else ""
else:
    path = f"data/out/daily/{today}/recs.json"

if not os.path.exists(path):
    print("No results")
    raise SystemExit(0)

data = json.load(open(path,"r"))
recs = data.get("recs",[])
meta = data.get("meta",{})

print(f"Run: https://github.com/{os.environ.get('GITHUB_REPOSITORY','<repo>')}/actions/runs/{os.environ.get('GITHUB_RUN_ID','')}")
print("Top 10")
for i, r in enumerate(recs[:10], start=1):
    print(f"\t{i}\t{r['match']} — {r['title']} ({r.get('year','')}) [{r.get('type','')}]")
    if r.get("why"):
        print(f"\t   {r['why']}")
print(f"Telemetry: pool={meta.get('pool',0)}, eligible={meta.get('eligible',0)}, after_skip={meta.get('after_skip',0)}, shown={meta.get('shown',0)}")
w = meta.get("weights", {})
if w:
    print(f"Weights: critic={w.get('critic',0):.2f}, audience={w.get('audience',0):.2f}")
print("This product uses the TMDB API but is not endorsed or certified by TMDB.")