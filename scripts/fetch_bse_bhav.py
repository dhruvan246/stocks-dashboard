# -*- coding: utf-8 -*-
"""Backfill / update BSE daily closes + delivery% for the BSE-ONLY universe → docs/bse_prices.bin.

BSE-only stocks aren't in the NSE sf_stock_data.bin, so they have no price series → no last price,
no result-day reaction, no chart. This pulls BSE's UDiFF equity bhavcopy (one CSV per trading day,
~4,900 scrips, has ISIN/TckrSymb/ClsPric/TtlTradgVol) and keeps ONLY the BSE-only scrips.

Store: gzipped JSON  {"end": YYYYMMDD, "px": { "<scripcode>": {"d":[YYYYMMDD…], "c":[close…], "v":[vol…],
       "dv":[deliv%…]} }}  dates ascending, per scrip. Compact: closes 2dp, vols ints, deliv% 2dp
       (0 = unavailable; a TRUE 0.00% delivery day is stored as 0.01 so it stays distinguishable).

Delivery source: BSE gross-delivery file  bseindia.com/BSEDATA/gross/<YYYY>/SCBSEALL<DDMM>.zip
(pipe-separated TXT: DATE|SCRIP CODE|DELIVERY QTY|DELIVERY VAL|DAY'S VOLUME|DAY'S TURNOVER|DELV. PER.;
BSE T2T groups print their natural 100.00). Every run also SELF-HEALS delivery: any stored date where
no scrip has dv>0 (bounded by --dv-budget, default 600) gets its SCBSEALL fetched and applied — so the
first run after this feature lands backfills the whole store, and later runs converge to ~yesterday only.

Resumable + self-healing: appends every missing trading day between the file's `end` and today; a day
whose bhavcopy 404s (holiday/not-published-yet) is skipped. Weekend/holiday fetches just no-op.

Run:  python -X utf8 scripts/fetch_bse_bhav.py [--since YYYYMMDD] [--days N] [--dv-budget N]
      (default: from existing end+1, else last 400 calendar days, up to today)
"""
import os, sys, json, gzip, io, csv, time, datetime, bisect
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "docs", "bse_prices.bin")
UNIV = os.path.join(HERE, "..", "docs", "bse_universe.json")
BHAV = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_%s_F_0000.CSV"
GROSS = "https://www.bseindia.com/BSEDATA/gross/%d/SCBSEALL%s.zip"   # % (year, DDMM)

def load_prices():
    if os.path.exists(OUT):
        try: return json.loads(gzip.decompress(open(OUT, "rb").read()).decode("utf8"))
        except Exception: pass
    return {"end": 0, "px": {}}

def ensure_dv(s):
    """Pad a series' dv list to the length of d (0 = delivery unavailable for that date)."""
    dvl = s.setdefault("dv", [])
    if len(dvl) < len(s["d"]): dvl.extend([0] * (len(s["d"]) - len(dvl)))
    return dvl

def save_prices(d):
    # keep every scrip's series in ascending date order (backfill may add OLDER dates than existing)
    for s in d["px"].values():
        ensure_dv(s)
        if len(s["d"]) > 1 and any(s["d"][i] > s["d"][i + 1] for i in range(len(s["d"]) - 1)):
            order = sorted(range(len(s["d"])), key=lambda i: s["d"][i])
            for f in ("d", "c", "v", "dv"):
                s[f] = [s[f][i] for i in order]
    open(OUT, "wb").write(gzip.compress(json.dumps(d, separators=(",", ":")).encode("utf8"), 6))

def bse_only_codes():
    u = json.load(open(UNIV, encoding="utf-8"))
    # rows: [scrip_cd, ticker, name, isin, group, faceval, mcap, sector]
    return {str(r[0]) for r in u["rows"]}


def apply_delivery(px, di, dl):
    """Set dv for date di from {code: pct}; fill-only (never overwrites a non-zero). Returns cells set."""
    n = 0
    for code, s in px.items():
        pct = dl.get(code)
        if pct is None or not s["d"]: continue
        i = bisect.bisect_left(s["d"], di)
        if i >= len(s["d"]) or s["d"][i] != di: continue
        dvl = ensure_dv(s)
        if not dvl[i]: dvl[i] = pct; n += 1
    return n


def heal_delivery(op, px, budget):
    """Backfill dv for stored dates that have no delivery yet (oldest first, up to budget days).
    Converges to ~yesterday-only once the store is filled; a day whose SCBSEALL never existed
    is retried each run but costs one cheap 404."""
    covered = set()
    alldates = set()
    for s in px.values():
        dvl = ensure_dv(s)
        for i, di in enumerate(s["d"]):
            alldates.add(di)
            if dvl[i]: covered.add(di)
    todo = sorted(alldates - covered)
    if not todo: return 0, 0
    done = 0; cells = 0
    for di in todo[:budget]:
        d = datetime.date(di // 10000, di // 100 % 100, di % 100)
        dl = day_delivery(op, d)
        if dl:
            cells += apply_delivery(px, di, dl); done += 1
        time.sleep(0.12)
        if done and done % 25 == 0:
            print("  …dv healed %d days (%d cells so far)" % (done, cells))
    print("dv heal: %d/%d pending days fetched, %d cells set" % (done, len(todo), cells))
    return done, cells

def day_delivery(op, d):
    """Return {scripcode: deliv%} for one trading day from the SCBSEALL gross-delivery zip,
    or None when the file isn't published (holiday / too early). TRUE 0.00%-delivery rows
    are returned as 0.01 so 0 can stay the 'unavailable' sentinel in the store."""
    import zipfile
    try:
        raw = B.get(op, GROSS % (d.year, d.strftime("%d%m")), b=True)
    except Exception:
        return None
    if not raw or raw[:2] != b"PK":
        return None
    try:
        z = zipfile.ZipFile(io.BytesIO(raw))
        text = z.read(z.namelist()[0]).decode("utf8", "ignore")
    except Exception:
        return None
    out = {}
    for line in text.split("\n"):
        p = line.strip().split("|")
        # DATE|SCRIP CODE|DELIVERY QTY|DELIVERY VAL|DAY'S VOLUME|DAY'S TURNOVER|DELV. PER.
        if len(p) < 7 or not p[1].strip().isdigit(): continue
        try: pct = float(p[6])
        except ValueError: continue
        out[p[1].strip().lstrip("0") or "0"] = round(pct, 2) if pct > 0 else 0.01
    return out if len(out) > 500 else None


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
    save_prices(data)   # closes are safe on disk before the delivery pass touches anything
    # delivery layer: heal every stored date that still lacks dv (first run backfills the
    # whole store from SCBSEALL, later runs converge to just the freshly-appended days).
    # Shielded: a delivery-side failure must never cost the day's price append.
    dvb = int(sys.argv[sys.argv.index("--dv-budget") + 1]) if "--dv-budget" in sys.argv else 600
    try:
        heal_delivery(op, px, dvb)
    except Exception as ex:
        print("dv heal errored (%s) — prices unaffected, next run retries" % ex)
    save_prices(data)
    ncov = sum(1 for c in codes if c in px and px[c]["d"])
    ndv = sum(1 for c in codes if c in px and any(x > 0 for x in px[c].get("dv", ())))
    print("WROTE %s: %d trading days added; %d/%d BSE-only scrips have prices (%d with delivery); end=%d"
          % (os.path.normpath(OUT), got, ncov, len(codes), ndv, last))

if __name__ == "__main__":
    main()
