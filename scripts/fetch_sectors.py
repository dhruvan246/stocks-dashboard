#!/usr/bin/env python3
"""Fetch sector/industry metadata for every BSE-listed stock from BSE's
   ComHeadernew endpoint, enrich scripts/stock_data.json in place."""
import json, subprocess, concurrent.futures, time, re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "scripts" / "stock_data.json"
BSE_JSON = "/tmp/bse.json"
UA  = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

bse = json.load(open(BSE_JSON))
scrip_list = []
for b in bse:
    if b.get("Status") != "Active" or b.get("Segment") != "Equity": continue
    code = (b.get("SCRIP_CD") or "").strip()
    sid  = (b.get("scrip_id") or "").strip()
    name = (b.get("Scrip_Name") or "").strip()
    if not code: continue
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    referer = f"https://www.bseindia.com/stock-share-price/{slug}/{sid.lower()}/{code}/"
    scrip_list.append((code, sid, referer))
print(f"BSE scrips to enrich: {len(scrip_list)}")

def fetch(entry):
    code, sid, ref = entry
    url = f"https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w?quotetype=EQ&scripcode={code}"
    try:
        r = subprocess.run(
            ["curl", "-s", "--max-time", "8",
             "-A", UA,
             "-H", f"Referer: {ref}",
             "-H", "Origin: https://www.bseindia.com",
             "-H", "Accept: application/json, text/plain, */*",
             url],
            capture_output=True, timeout=10)
        d = json.loads(r.stdout)
        if d.get("Sector"):
            return code, {"sector": d.get("Sector") or "", "industry": d.get("IndustryNew") or ""}
    except Exception:
        pass
    return code, None

# Two passes to mop up transient failures
sectors = {}
for attempt in range(2):
    todo = [s for s in scrip_list if s[0] not in sectors]
    if not todo: break
    print(f"Pass {attempt+1}: fetching {len(todo)}")
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for code, info in ex.map(fetch, todo):
            if info: sectors[code] = info
    print(f"  got {sum(1 for c in todo if c[0] in sectors)} more in {time.time()-t0:.0f}s")

print(f"\nTotal sector rows: {len(sectors)} / {len(scrip_list)}")

# Merge into stock_data.json
data = json.loads(DATA.read_text())
sid_to_code = {}
for b in bse:
    sid = (b.get("scrip_id") or "").strip()
    code = (b.get("SCRIP_CD") or "").strip()
    if sid and code: sid_to_code[sid] = code

for ticker, meta in data["meta"].items():
    sym, suffix = ticker.rsplit(".", 1)
    code = sym if suffix == "BO" else sid_to_code.get(sym)
    info = sectors.get(code or "")
    if info and info.get("sector"):
        meta["sector"] = info["sector"]
        meta["industry"] = info.get("industry") or ""
    else:
        meta["sector"] = "Uncategorized"
        meta["industry"] = ""
DATA.write_text(json.dumps(data, separators=(",", ":")))
print(f"Updated {DATA}")
