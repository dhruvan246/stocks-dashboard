# -*- coding: utf-8 -*-
"""
Daily INCREMENTAL updater for docs/sf_stock_data.bin (the survivorship-free
backtest dataset: close/turnover/high/low/open/volume/delivery%/VWAP).

Run daily (GitHub Actions): appends only the trading days missing since the
file's `end` — no 30-year refetch, no git bloat (the workflow publishes the
bin as a GitHub Release asset instead of committing it).

Base file: tries the release asset first, falls back to docs/sf_stock_data.bin.
Touches docs/.sf_updated when (and only when) new data was appended.

Run: python -X utf8 update_sf_data.py
"""
import os, sys, json, gzip, datetime, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, "docs", "sf_stock_data.bin")
MARK = os.path.join(ROOT, "docs", ".sf_updated")
RELEASE_URL = "https://github.com/dhruvan246/stocks-dashboard/releases/download/data/sf_stock_data.bin"

sys.path.insert(0, HERE)
import build_sf_data as B   # reuse fetch_day / parse_rows / jar (module-level code is harmless)

CA_FRACS = [1/2, 1/3, 2/3, 1/4, 3/4, 1/5, 2/5, 3/5, 1/6, 5/6, 1/8, 1/10, 1/20, 1/50,
            2.0, 3.0, 4.0, 5.0, 10.0]
def ca_factor(r):
    if 0.75 <= r <= 1.30: return 1.0
    for f in CA_FRACS:
        if abs(r / f - 1) <= 0.08: return f
    return 1.0

def load_base():
    try:
        raw = urllib.request.urlopen(urllib.request.Request(RELEASE_URL, headers={"User-Agent": "Mozilla/5.0"}), timeout=120).read()
        print("Base: release asset (%.1f MB)" % (len(raw) / 1048576))
        return json.loads(gzip.decompress(raw))
    except Exception as e:
        print("Base: release unavailable (%s) — using local docs copy" % e)
        return json.loads(gzip.decompress(open(OUT, "rb").read()))

def main():
    if os.path.exists(MARK): os.remove(MARK)
    D = load_base()
    last = datetime.datetime.strptime(D["end"], "%Y-%m-%d").date()
    today = datetime.date.today()
    days = []
    d = last + datetime.timedelta(days=1)
    while d <= today:
        if d.weekday() < 5: days.append(d)
        d += datetime.timedelta(days=1)
    if not days:
        print("Up to date (end=%s)" % D["end"]); return
    print("Missing trading-day candidates: %s" % ", ".join(x.isoformat() for x in days))

    data = D["data"]; meta = D["meta"]; j = B.jar(); appended = 0
    for day in days:
        rows = B.fetch_day(day, j)
        if not rows:
            print("  %s: no file (holiday or not yet published)" % day); continue
        # stale-file guard: NSE sometimes serves the prior day's file — if almost every
        # symbol's close equals its current last close, this is a duplicate; skip it.
        same = tot = 0
        for r in rows:
            o = data.get(r[0])
            if o and o["c"]:
                tot += 1
                if abs(o["c"][-1] - r[1]) < 0.005: same += 1
        if tot > 500 and same / tot > 0.99:
            print("  %s: duplicate of previous day (%d/%d identical) — skipped" % (day, same, tot)); continue

        ymd = int(day.strftime("%Y%m%d"))
        for r in rows:
            sym, c, p, t = r[0], r[1], r[2], r[3]
            h = r[4] if len(r) > 4 else c; l = r[5] if len(r) > 5 else c
            o_ = r[6] if len(r) > 6 else c; v = r[7] if len(r) > 7 else 0
            dlv = r[8] if len(r) > 8 else 0; vw = r[9] if len(r) > 9 else 0
            hb = max(0, round((h / c - 1) * 1000)) if h >= c else 0
            lb = max(0, round((1 - l / c) * 1000)) if l <= c else 0
            ob = round((o_ / c - 1) * 1000) if o_ > 0 else 0
            dv = round(dlv * 10) if dlv else 0
            vwb = round((vw / c - 1) * 1000) if vw > 0 else 0
            e = data.get(sym)
            if e is None:   # new listing (IPO / relist) — start a fresh series
                data[sym] = {"d": [ymd], "c": [round(c, 2)], "t": [round(t, 1)], "hb": [hb], "lb": [lb],
                             "ob": [ob], "v": [int(v)], "dv": [dv], "vw": [vwb]}
                meta.setdefault(sym, {"name": sym, "ind": "Unknown", "alive": True})
                continue
            if e["d"] and e["d"][-1] >= ymd: continue   # already have this day
            prev_raw = e["c"][-1]   # series is re-anchored: last value == last RAW close
            ratio = (c / prev_raw) if prev_raw else 1.0
            f = ca_factor(ratio)
            if f != 1.0:   # corporate action: re-anchor the whole history so last stays = raw
                e["c"] = [round(x * f, 2) for x in e["c"]]
                print("  %s: %s corporate action f=%s (history re-anchored)" % (day, sym, f))
            e["d"].append(ymd); e["c"].append(round(c, 2)); e["t"].append(round(t, 1))
            e["hb"].append(hb); e["lb"].append(lb); e["ob"].append(ob)
            e["v"].append(int(v)); e["dv"].append(dv); e["vw"].append(vwb)
            if sym in meta: meta[sym]["raw"] = round(c, 2)
        D["end"] = day.isoformat(); appended += 1
        print("  %s: appended %d rows" % (day, len(rows)))

    if not appended:
        print("No new trading days appended."); return
    blob = gzip.compress(json.dumps(D, separators=(",", ":")).encode(), 6)
    open(OUT, "wb").write(blob)
    open(MARK, "w").write(D["end"])
    # tiny version marker — committed daily, lets the browser cache the big bin in IndexedDB
    # keyed to this `end` and skip re-downloading 80 MB until the data actually changes.
    json.dump({"end": D["end"]}, open(os.path.join(ROOT, "docs", "sf_meta.json"), "w"))
    print("Wrote %s (%.2f MB) + docs/sf_meta.json, end=%s" % (OUT, len(blob) / 1048576, D["end"]))

if __name__ == "__main__":
    main()
