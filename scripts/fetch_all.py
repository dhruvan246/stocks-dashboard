#!/usr/bin/env python3
"""Fetch historical price data for the full NSE + BSE universe via curl.

Recovery passes:
  Pass 1 — primary ticker (NSE if symbol exists in NSE master, else BSE).
  Pass 2 — for misses, try the alternate exchange (.BO if .NS failed, vice
           versa). Recovers names like INA where one of the two listings
           is too new on Yahoo.
Stocks where BOTH passes return no data are still included in the output —
their `series` is empty and the dashboard shows "—" for prices/change.
"""
import json, csv, time, subprocess, concurrent.futures, datetime as _dt
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
NSE_CSV  = "/tmp/nse.csv"
BSE_JSON = "/tmp/bse.json"
OUT_JSON = ROOT / "scripts" / "stock_data.json"

# --- Build universe ----------------------------------------------------
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
    if b.get("Status") != "Active" or b.get("Segment") != "Equity": continue
    sid  = (b.get("scrip_id") or "").strip()
    code = (b.get("SCRIP_CD") or "").strip()
    name = (b.get("Scrip_Name") or "").strip()
    try: mcap = float(b.get("Mktcap") or 0)
    except: mcap = 0
    grp  = (b.get("GROUP") or "").strip() or "Other"
    if not sid and not code: continue
    if sid and sid in nse_symbols:
        primary = f"{sid}.NS"
        alts    = [f"{sid}.BO"] + ([f"{code}.BO"] if code else [])
        display = sid
        key = ("NS", sid)
    else:
        primary = f"{code}.BO" if code else f"{sid}.BO"
        alts    = ([f"{sid}.BO"] if sid and code else []) + ([f"{sid}.NS"] if sid else [])
        display = sid or code
        key = ("BO", code or sid)
    if key in seen: continue
    seen.add(key)
    universe.append({
        "primary": primary, "alts": alts,
        "display": display, "name": name, "group": grp, "mcap": round(mcap, 2),
    })

bse_nse_syms = {u["display"] for u in universe if u["primary"].endswith(".NS")}
for sym in sorted(nse_symbols - bse_nse_syms):
    universe.append({
        "primary": f"{sym}.NS", "alts": [f"{sym}.BO"],
        "display": sym, "name": sym, "group": "NSE-only", "mcap": 0,
    })

print(f"Total universe: {len(universe)}")

# --- Fetch via curl ----------------------------------------------------
END_TS   = int(time.time())
START_TS = int(_dt.datetime(2020, 3, 1).timestamp())
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

def fetch_one(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={START_TS}&period2={END_TS}&interval=1d"
    try:
        res = subprocess.run(["curl","-s","--max-time","12","-A",UA,url],
                             capture_output=True, timeout=15)
        body = res.stdout
        if not body: return None
        data = json.loads(body)
        result = data.get("chart", {}).get("result")
        if not result: return None
        result = result[0]
        # Skip if Yahoo reports it as MUTUALFUND or other non-equity
        if result.get("meta", {}).get("instrumentType") and \
           result["meta"]["instrumentType"] != "EQUITY":
            return None
        ts = result.get("timestamp") or []
        closes = (result.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
        pairs = [[t2, round(c, 2)] for t2, c in zip(ts, closes) if c is not None]
        return pairs if len(pairs) >= 2 else None
    except Exception:
        return None

def fetch_with_fallback(entry):
    """Try primary, then each alt. Return (winning_ticker, pairs) or (primary, None)."""
    series = fetch_one(entry["primary"])
    if series: return entry["primary"], series
    for alt in entry["alts"]:
        series = fetch_one(alt)
        if series: return alt, series
    return entry["primary"], None

# Pass 1 + Pass 2 in one go (each entry tries primary then alts internally)
results = {}  # ticker -> pairs
empty   = {}  # ticker -> None (still in universe)
start   = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
    futures = {ex.submit(fetch_with_fallback, u): u for u in universe}
    done = 0
    for fut in concurrent.futures.as_completed(futures):
        done += 1
        entry  = futures[fut]
        winning_ticker, pairs = fut.result()
        entry["ticker"] = winning_ticker  # remember which one paid off
        if pairs is not None:
            results[winning_ticker] = pairs
        else:
            empty[winning_ticker]   = None
        if done % 250 == 0:
            ok = 100 * len(results) / done
            print(f"  {done}/{len(universe)}  with_data={len(results)} ({ok:.0f}%)  empty={len(empty)}  elapsed={time.time()-start:.0f}s", flush=True)

elapsed = time.time() - start
print(f"\nDone: {len(results)} with data, {len(empty)} without, total {len(universe)} ({elapsed:.0f}s)")

# Sort universe by mcap desc so the heaviest names are first in the payload
universe.sort(key=lambda u: -u["mcap"])

payload = {
    "generatedAt": END_TS, "startTs": START_TS, "endTs": END_TS,
    "meta":   {u["ticker"]: {
        "symbol": u["display"], "name": u["name"],
        "sector": u["group"],   "mcap": u["mcap"],
    } for u in universe},
    "series": results,   # only contains tickers that had data
}
OUT_JSON.write_text(json.dumps(payload, separators=(",", ":")))
print(f"Wrote {OUT_JSON} ({OUT_JSON.stat().st_size/1024/1024:.2f} MB)")
print(f"Universe in dashboard: {len(payload['meta'])} stocks")
print(f"  with price history:  {len(payload['series'])}")
print(f"  metadata-only:       {len(payload['meta']) - len(payload['series'])}")
