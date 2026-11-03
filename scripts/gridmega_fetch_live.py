# -*- coding: utf-8 -*-
"""Stage the LIVE data the mega grid reads from `scripts/_live/` (DATA_RUNBOOK §7.0/§7.4).

grid_search_mega.js loads ONLY from scripts/_live/ so a grid run analyses exactly the
bytes the site serves, never the stale committed snapshot. This script rebuilds that
directory from the live GitHub Pages origins:

  stock_data_live.bin  <- stocks-dashboard/stock_data.bin   (indicesHistory/fnoHistory/startTs)
  p1_new.bin/p2_new.bin <- sf-data parts, merged per-symbol (deep first, recent appended),
                          symbols split in half across the two (the engine merges p1+p2)
  fund_live.json       <- stocks-dashboard/sf_fundamentals.json
  fund_months_live.json <- stocks-dashboard/fund_months.json (proven half-year rows, runbook §198)
  shp_live.json        <- stocks-dashboard/shp_engine.json
  nifty_live.json      <- stocks-dashboard/nifty.json
  nifty500_live.json   <- stocks-dashboard/nifty500.json

Run: python3 scripts/gridmega_fetch_live.py
"""
import json, gzip, os, sys, urllib.request, time

SF   = "https://dhruvan246.github.io/sf-data/"
SITE = "https://dhruvan246.github.io/stocks-dashboard/"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT  = os.path.join(HERE, "_live")


def get(url):
    # Yahoo refuses non-browser agents
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0" if "yahoo" in url else "gridmega_fetch_live"})
    with urllib.request.urlopen(req) as r:
        return r.read()


def main():
    os.makedirs(OUT, exist_ok=True)
    M = json.loads(get(SF + "sf_meta.json?t=%d" % time.time()))
    print("live sf meta:", M, flush=True)

    # ---- sf price/turnover parts: deep first so recent arrays APPEND after (browser order)
    parts = []
    for i in range(M.get("deep", 0)):
        print("fetching sf_deep_%d.bin…" % (i + 1), flush=True)
        parts.append(json.loads(gzip.decompress(get(SF + "sf_deep_%d.bin?v=%s" % (i + 1, M["end"])))))
    for i in range(M.get("recent", 1)):
        print("fetching sf_recent_%d.bin…" % (i + 1), flush=True)
        parts.append(json.loads(gzip.decompress(get(SF + "sf_recent_%d.bin?v=%s" % (i + 1, M["end"])))))

    full = {k: v for k, v in parts[-1].items() if k not in ("data", "meta")}
    full["data"], full["meta"] = {}, {}
    for dp in parts:
        full["meta"].update(dp.get("meta", {}))
        for sym, o in dp["data"].items():
            tgt = full["data"].setdefault(sym, {})
            for key, arr in o.items():
                if isinstance(arr, list):
                    tgt.setdefault(key, []).extend(arr)
                else:
                    tgt[key] = arr
    full["end"] = M["end"]
    full.setdefault("start", M.get("fullStart"))

    # rename guard — the engine aborts on this, catch it here with a clearer message
    if "ZOMATO" in full["data"] or "ETERNAL" not in full["data"]:
        sys.exit("rename sanity failed: ZOMATO present or ETERNAL missing in live sf-data")

    nbars = sum(len(o.get("d", [])) for o in full["data"].values())
    print("merged: %d symbols, %d bars, end=%s" % (len(full["data"]), nbars, full["end"]), flush=True)

    # Split the symbols across p1/p2 (the grid merges {...p1.data, ...p2.data}, each symbol whole in
    # one file). One file stopped fitting by Nov-2026: 606 MB of JSON is past Node's 512 MB string
    # limit, and every grid job died at load with ERR_STRING_TOO_LONG.
    syms = sorted(full["data"])
    half = len(syms) // 2
    for name, chunk in (("p1_new.bin", syms[:half]), ("p2_new.bin", syms[half:])):
        part = {k: v for k, v in full.items() if k not in ("data", "meta")}
        part["data"] = {s: full["data"][s] for s in chunk}
        part["meta"] = full["meta"] if name == "p1_new.bin" else {}   # merged by the grid; keep it whole
        blob = json.dumps(part, separators=(",", ":")).encode()
        if len(blob) >= 0x1fffffe8:
            sys.exit("%s is %d bytes of JSON — over Node's string limit; split into more parts" % (name, len(blob)))
        with open(os.path.join(OUT, name), "wb") as f:
            f.write(gzip.compress(blob, 6))
        print("  %s  %d symbols  %.0f MB JSON" % (name, len(chunk), len(blob) / 1e6), flush=True)

    # ---- site files (stock_data.bin is already gzip; copy the bytes through)
    for src, dst in [("stock_data.bin", "stock_data_live.bin"),
                     ("sf_fundamentals.json", "fund_live.json"),
                     ("fund_months.json", "fund_months_live.json"),
                     ("shp_engine.json", "shp_live.json"),
                     ("nifty.json", "nifty_live.json"),
                     ("nifty500.json", "nifty500_live.json")]:
        print("fetching %s…" % src, flush=True)
        b = get(SITE + src + "?t=%d" % time.time())
        with open(os.path.join(OUT, dst), "wb") as f:
            f.write(b)
        print("  %s  %.1f MB" % (dst, len(b) / 1e6), flush=True)

    # ---- Nifty 100 benchmark: the site has no daily Nifty 100 file, so take Yahoo's ^CNX100 closes
    # (daily from 2005-11-30; matched docs/index_monthly.json NIFTY 100 month-ends 2026-11-03 except 3
    # months where Yahoo lacks the last trading day). Earlier phases fall back to index_monthly.json.
    print("fetching ^CNX100 (Nifty 100 benchmark)…", flush=True)
    yj = json.loads(get("https://query1.finance.yahoo.com/v8/finance/chart/%%5ECNX100?period1=1041379200&period2=%d&interval=1d" % time.time()))
    yr = yj["chart"]["result"][0]
    px = {time.strftime("%Y-%m-%d", time.gmtime(t + 19800)): round(c, 2)
          for t, c in zip(yr["timestamp"], yr["indicators"]["quote"][0]["close"]) if c}
    px = {d: v for d, v in px.items() if d <= full["end"]}
    with open(os.path.join(OUT, "nifty100_live.json"), "w") as f:
        json.dump({"src": "yahoo ^CNX100", "px": px}, f)
    print("  nifty100_live.json  %d days %s..%s" % (len(px), min(px), max(px)), flush=True)

    print("\n_live/ staged. sf end = %s" % full["end"])


if __name__ == "__main__":
    main()
