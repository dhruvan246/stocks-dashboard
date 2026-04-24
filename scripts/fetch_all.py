#!/usr/bin/env python3
"""Fetch historical price data for the full NSE + BSE universe via curl.
Runs in CI — reads the NSE CSV and BSE JSON from /tmp (downloaded by the workflow).
Writes stock_data.json next to this script."""
import json, csv, time, subprocess, concurrent.futures, datetime as _dt
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
NSE_CSV  = "/tmp/nse.csv"
BSE_JSON = "/tmp/bse.json"
OUT_JSON = ROOT / "scripts" / "stock_data.json"

# --- Build universe -----------------------------------------------------
nse_symbols = set()
with open(NSE_CSV) as f:
    for row in csv.DictReader(f):
        row = {k.strip(): (v or '').strip() for k, v in row.items()}
        if row.get("SERIES") == "EQ" and row.get("SYMBOL"):
            nse_symbols.add(row["SYMBOL"])
print(f"NSE EQ symbols: {len(nse_symbols)}")

bse_scrips = json.load(open(BSE_JSON))
universe, seen = [], set()
for b in bse_scrips:
    if b.get("Status") != "Active" or b.get("Segment") != "Equity":
        continue
    sid  = (b.get("scrip_id") or "").strip()
    code = (b.get("SCRIP_CD") or "").strip()
    name = (b.get("Scrip_Name") or "").strip()
    try: mcap = float(b.get("Mktcap") or 0)
    except: mcap = 0
    grp  = (b.get("GROUP") or "").strip() or "Other"
    if not sid and not code: continue
    if sid and sid in nse_symbols:
        ticker, display = f"{sid}.NS", sid
        key = ("NS", sid)
    else:
        ticker, display = f"{code}.BO", sid or code
        key = ("BO", code)
    if key in seen: continue
    seen.add(key)
    universe.append({"ticker": ticker, "display": display, "name": name, "group": grp, "mcap": round(mcap, 2)})

bse_nse_syms = {u["display"] for u in universe if u["ticker"].endswith(".NS")}
for sym in sorted(nse_symbols - bse_nse_syms):
    universe.append({"ticker": f"{sym}.NS", "display": sym, "name": sym, "group": "NSE-only", "mcap": 0})

print(f"Total universe: {len(universe)}")

# --- Fetch via curl ----------------------------------------------------
END_TS   = int(time.time())
START_TS = int(_dt.datetime(2020, 3, 1).timestamp())
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

def fetch_one(entry):
    t = entry["ticker"]
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?period1={START_TS}&period2={END_TS}&interval=1d"
    try:
        res = subprocess.run(
            ["curl", "-s", "--max-time", "12", "-A", UA, url],
            capture_output=True, timeout=15
        )
        body = res.stdout
        if not body: return t, None
        data = json.loads(body)
        result = data["chart"]["result"][0]
        ts = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        pairs = [[t2, round(c, 2)] for t2, c in zip(ts, closes) if c is not None]
        if len(pairs) < 2: return t, None
        return t, pairs
    except Exception:
        return t, None

series = {}
start = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
    futures = {ex.submit(fetch_one, u): u for u in universe}
    done = 0
    for fut in concurrent.futures.as_completed(futures):
        done += 1
        t, pairs = fut.result()
        if pairs is not None:
            series[t] = pairs
        if done % 250 == 0:
            ok = 100 * len(series) / done
            print(f"  {done}/{len(universe)}  ok={len(series)} ({ok:.0f}%)  elapsed={time.time()-start:.0f}s", flush=True)

print(f"\nDone: {len(series)}/{len(universe)} in {time.time()-start:.0f}s")

kept = [u for u in universe if u["ticker"] in series]
kept.sort(key=lambda u: -u["mcap"])

payload = {
    "generatedAt": END_TS,
    "startTs": START_TS,
    "endTs":   END_TS,
    "meta":   {u["ticker"]: {
        "symbol": u["display"], "name": u["name"],
        "sector": u["group"],   "mcap": u["mcap"],
    } for u in kept},
    "series": {u["ticker"]: series[u["ticker"]] for u in kept},
}
OUT_JSON.write_text(json.dumps(payload, separators=(",", ":")))
print(f"Wrote {OUT_JSON} ({OUT_JSON.stat().st_size/1024/1024:.2f} MB)")
