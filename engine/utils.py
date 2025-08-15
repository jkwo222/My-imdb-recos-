# engine/utils.py
import re, unicodedata
from rapidfuzz import fuzz

def normalize_title(s: str) -> str:
    if not s: return ""
    s = s.lower().strip()
    out, depth = [], 0
    for ch in s:
        if ch == '(': depth += 1
        elif ch == ')': depth = max(0, depth-1)
        elif depth == 0: out.append(ch)
    s = ''.join(out).replace("&", " and ")
    s = re.sub(r"[-—–_:/,.'!?;]", " ", s)
    s = " ".join(t for t in s.split() if t != "the")
    return unicodedata.normalize("NFKC", s)

def fuzzy_match(a: str, b: str, threshold: float = 0.92) -> bool:
    if not a or not b: return False
    return (fuzz.token_set_ratio(a, b) / 100.0) >= threshold