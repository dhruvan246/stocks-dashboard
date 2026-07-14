# -*- coding: utf-8 -*-
"""Backfill / update BSE daily closes for the BSE-ONLY universe → docs/bse_prices.bin.

BSE-only stocks aren't in the NSE sf_stock_data.bin, so they have no price series → no last price,
no result-day reaction, no chart. This pulls BSE's UDiFF equity bhavcopy (one CSV per trading day,
~4,900 scrips, has ISIN/TckrSymb/ClsPric/TtlTradgVol) and keeps ONLY the BSE-only scrips.

Store: gzipped JSON  {"end": YYYYMMDD, "px": { "<scripcode>": {"d":[YYYYMMDD…], "c":[close…], "v":[vol…]} }}
       dates ascending, per scrip. Compact: closes rounded to 2dp, vols as ints.

Resumable + self-healing: appends every missing trading day between the file's `end` and today; a day
whose bhavcopy 404s (holiday/not-published-yet) is skipped. Weekend/holiday fetches just no-op.

Run:  python -X utf8 scripts/fetch_bse_bhav.py [--since YYYYMMDD] [--days N]
      (default: from existing end+1, else last 400 calendar days, up to today)
"""
import os, sys, json, gzip, io, csv, time, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "docs", "bse_prices.bin")
UNIV = os.path.join(HERE, "..", "docs", "bse_universe.json")
BHAV = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_%s_F_0000.CSV"

def load_prices():
    if os.path.exists(OUT):
        try: return json.loads(gzip.decompress(open(OUT, "rb").read()).decode("utf8"))
        except Exception: pass
    return {"end": 0, "px": {}}

def save_prices(d):
    # keep every scrip's series in ascending date order (backfill may add OLDER dates than existing)
    for s in d["px"].values():
        if len(s["d"]) > 1 and any(s["d"][i] > s["d"][i + 1] for i in range(len(s["d"]) - 1)):
            order = sorted(range(len(s["d"])), key=lambda i: s["d"][i])
            s["d"] = [s["d"][i] for i in order]
            s["c"] = [s["c"][i] for i in order]
            s["v"] = [s["v"][i] for i in order]
    open(OUT, "wb").write(gzip.compress(json.dumps(d, separators=(",", ":")).encode("utf8"), 6))

def bse_only_codes():
    u = json.load(open(UNIV, encoding="utf-8"))
    # rows: [scrip_cd, ticker, name, isin, group, faceval, mcap, sector]
    return {str(r[0]) for r in u["rows"]}

def day_closes(op, d):
    """Return {scripcode: (close, vol)} for one trading day, or None if no bhavcopy."""
    ymd = d.strftime("%Y%m%d")
    try:
        raw = B.get(op, BHAV % ymd, b=True)
    except Exception:
        return None
    if not raw or raw[:2] == b"<!" or raw[:2] == b"PK" and b"html" in raw[:200].lower():
        return None
    try:
        rd = csv.DictReader(io.StringIO(raw.decode("utf8", "ignore")))
    except Exception:
        return None
    out = {}
    for r in rd:
        code = (r.get("FinInstrmId") or "").strip()
        cls = r.get("ClsPric")
        if not code or not cls: continue
        try: c = round(float(cls), 2)
        except Exception: continue
        if c <= 0: continue
        try: v = int(float(r.get("TtlTradgVol") or 0))
        except Exception: v = 0
        out[code] = (c, v)
    return out if len(out) > 500 else None

def main():
    codes = bse_only_codes()
    data = load_prices()
    px = data["px"]
    today = datetime.date.today()
    if "--since" in sys.argv:
        start = datetime.datetime.strptime(sys.argv[sys.argv.index("--since") + 1], "%Y%m%d").date()
    elif data["end"]:
        start = datetime.datetime.strptime(str(data["end"]), "%Y%m%d").date() + datetime.timedelta(days=1)
    else:
        ndays = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 400
        start = today - datetime.timedelta(days=ndays)

    have = {code: set(s["d"]) for code, s in px.items()}   # dates already stored, per scrip
    op = B.session(); time.sleep(1)
    d = start; got = 0; last = data["end"]
    while d <= today:
        if d.weekday() < 5:                       # skip weekends outright
            cl = day_closes(op, d)
            if cl:
                di = int(d.strftime("%Y%m%d")); got += 1; last = max(last, di)
                for code in codes:
                    t = cl.get(code)
                    if not t: continue
                    s = px.get(code)
                    if s is None: s = px[code] = {"d": [], "c": [], "v": []}; have[code] = set()
                    if di in have.get(code, ()): continue      # already have this date (any order)
                    s["d"].append(di); s["c"].append(t[0]); s["v"].append(t[1]); have.setdefault(code, set()).add(di)
                if got % 20 == 0:
                    data["end"] = last; save_prices(data)
                    print("  …%d days, through %d" % (got, last)); time.sleep(0.2)
            time.sleep(0.15)
        d += datetime.timedelta(days=1)
    data["end"] = last
    save_prices(data)
    ncov = sum(1 for c in codes if c in px and px[c]["d"])
    print("WROTE %s: %d trading days added; %d/%d BSE-only scrips have prices; end=%d"
          % (os.path.normpath(OUT), got, ncov, len(codes), last))

if __name__ == "__main__":
    main()
