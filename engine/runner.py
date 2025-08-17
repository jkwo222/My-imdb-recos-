# engine/runner.py (near the top)
import os, requests

def _probe_omdb_once():
    key = os.environ.get("OMDB_API_KEY","").strip()
    if not key:
        print("[probe] OMDB_API_KEY not set in env.")
        return
    try:
        # Fast, cheap known title
        r = requests.get(f"https://www.omdbapi.com/?apikey={key}&t=The+Matrix&y=1999&r=json", timeout=10)
        if r.status_code != 200:
            print(f"[probe] OMDb status {r.status_code}: {(r.text or '').strip()[:160]}")
            return
        data = r.json()
        if str(data.get("Response","")).lower() == "false":
            print(f"[probe] OMDb JSON error: {(data.get('Error') or '').strip()}")
        else:
            # Give one-liner success to confirm key is good
            print("[probe] OMDb ok — key works.")
    except Exception as e:
        print(f"[probe] OMDb exception: {e}")

# call it once at start
if __name__ == "__main__" or True:
    _probe_omdb_once()