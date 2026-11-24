# -*- coding: utf-8 -*-
"""Extra chart series for docs/chart.html that no CI job can fetch: BSE sector indices, MCX commodity
futures (continuous front / second month), NSE index futures (continuous front month) and one delisted
BSE-only stock. Output: docs/xseries/<FILE>.json + docs/xseries/_index.json.

SOURCES (each measured 2026-11-24; runbook §249)
  BSE_<NAME>.txt      BSE index archive, api.bseindia.com/BseIndiaAPI/api/IndexArchDaily — answers only a
                      page-context request from bseindia.com in a real browser (a plain client is refused).
                      rows  YYYYMMDD_open_high_low_close   ('x' or 0 = BSE did not publish that field)
  MCXIDX_<CODE>.txt   mcxindia.com/market-data/mcx-icomdex-indices → GetMCXIComdexIndicesHistoryFilter (browser-only);
                      same row shape as the BSE index files. MCXCOPRDEX: closes from 2015-12-31, open/high/low from 2019-12-20.
  MCX_<SYM>.txt       mcxindia.com/market-data/bhavcopy → GetCommoditywiseBhavCopy, one call per contract,
                      same browser-only constraint.
                      rows  YYYYMMDD_EXPIRYYYYMMDD_open_high_low_close_volume_openinterest
  BSESTK_<SYM>.txt    BSE StockpricesearchData (browser-only). UNADJUSTED prices:
                      rows  YYYYMMDD_open_high_low_close_shares
  NSE index futures   ~/stocks-wt/fo_raw_cache (scripts/fetch_fo_bhavcopy.py — NSE's own F&O bhavcopy).

RULES (stated on the page too — nothing here is estimated)
  Continuous futures  "1!" = on each day, the contract with the nearest expiry on or after that day that
                      has a row that day; "2!" = the next expiry after it. Prices are the contract's own —
                      NOT back-adjusted, so there is a step on each roll day, as on TradingView's 1!/2!.
  MCX: a day on which that contract shows volume 0 gets NO bar — MCX still prints a "close" on such rows (a carried
                      price on Saturdays, a base price on a contract's first listed day: COPPER 31JUL2019 on 2019-06-17
                      shows 2157.5 with volume 0 and open interest 0 between 406 and 446), which is not a traded price.
  BSE index / NSE futures: a bar whose open/high/low are 0 or absent keeps its close and gets no open/high/low.
  Corporate actions   only for BSESTK files, from the ADJ table below; each entry names the BSE record read.

Values are stored x100 as integers, like docs/idx_ohlc (null = not published).

Run: python3 scripts/build_xseries.py [--raw DIR] [--fo DIR] [--out DIR]
     Files that are absent are skipped and their series left as they are on disk (never deleted).
"""
import argparse, datetime as dt, glob, gzip, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = 1
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

BSE_IDX = {   # raw file key -> (chart code, label, BSE archive code)
    "BSE_METAL": ("BSE:METAL", "BSE Metal", "SI1200"),
    "BSE_MIDCAP": ("BSE:MIDCAP", "BSE MidCap", "BSEMID"),
    "BSE_SMLCAP": ("BSE:SMLCAP", "BSE SmallCap", "BSESML"),
    "BSE_POWER": ("BSE:POWER", "BSE Power", "SIPOWE"),
    "BSE_PSU": ("BSE:PSU", "BSE PSU", "SIBPSU"),
}
MCX = {       # raw file key -> (symbol, label, unit)
    "MCX_COPPER": ("COPPER", "MCX Copper", "Rs/kg"),
    "MCX_ALUMINIUM": ("ALUMINIUM", "MCX Aluminium", "Rs/kg"),
    "MCX_CRUDEOIL": ("CRUDEOIL", "MCX Crude oil", "Rs/barrel"),
    "MCX_NATURALGAS": ("NATURALGAS", "MCX Natural gas", "Rs/mmBtu"),
}
MCX_IDX = {"MCXIDX_MCXCOPRDEX": ("MCX:MCXCOPRDEX", "MCX iCOMDEX Copper index", "575")}   # raw key -> (code, label, MCX instrument id)
NSE_FUT = {"NIFTY": "Nifty 50", "BANKNIFTY": "Nifty Bank"}
# Corporate actions for the delisted stock files. Each: (ex-date, divide earlier prices by, BSE CorporateAction row read 2026-11-24)
ADJ = {
    "APOLLOTRI": [("20141231", 5.0, "Stock Split From Rs.10/- to Rs.2/-, RD 01/01/2015"),
                  ("20210916", 2.0, "Bonus issue 1:1, RD 18/09/2021")],
}
STK = {"APOLLOTRI": ("Apollo Tricoat Tubes", "538566")}


def x100(v):
    return int(round(float(v) * 100))


def num(s):
    if s in ("x", "", None):
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    return v if v > 0 else None


def pack(rows):
    """rows: sorted [(yyyymmdd, o, h, l, c)] with o/h/l None when not published -> column arrays."""
    d0 = rows[0][0]
    days = [dt.date(int(r[0][:4]), int(r[0][4:6]), int(r[0][6:])) for r in rows]
    dd = [(days[i] - days[i - 1]).days for i in range(1, len(days))]
    o, h, l, c = [], [], [], []
    for _, ro, rh, rl, rc in rows:
        full = ro is not None and rh is not None and rl is not None
        if full and (rh < max(ro, rc) - 1e-9 or rl > min(ro, rc) + 1e-9):
            # the source's own bar is inconsistent: keep it as published, the page widens high/low itself
            pass
        o.append(x100(ro) if full else None); h.append(x100(rh) if full else None)
        l.append(x100(rl) if full else None); c.append(x100(rc))
    iso = lambda s: "%s-%s-%s" % (s[:4], s[4:6], s[6:])
    return {"d0": iso(d0), "dd": dd, "o": o, "h": h, "l": l, "c": c}, iso(rows[0][0]), iso(rows[-1][0])


def read_rows(path):
    return [r for r in open(path, encoding="utf-8").read().split("n") if r.strip()]


def bse_index(path):
    out = {}
    for r in read_rows(path):
        p = r.split("_")
        if len(p) != 5:
            continue
        c = num(p[4])
        if c is None:
            continue
        o, h, l = num(p[1]), num(p[2]), num(p[3])
        if o is None or h is None or l is None or (o == h == l == c):
            o = h = l = None                     # all four equal = BSE repeated the close (early years)
        out[p[0]] = (p[0], o, h, l, c)
    return [out[k] for k in sorted(out)]


def mcx_cont(path, nth):
    by = {}
    for r in read_rows(path):
        p = r.split("_")
        if len(p) != 8:
            continue
        c = num(p[5])
        if c is None or p[1] < p[0]:
            continue
        by.setdefault(p[0], {})[p[1]] = p
    out = []
    for d in sorted(by):
        exps = sorted(by[d])
        if len(exps) < nth:
            continue
        p = by[d][exps[nth - 1]]
        if not float(p[6] or 0) > 0:
            continue                             # that contract did not trade that day: no bar (see RULES)
        o, h, l, c = num(p[2]), num(p[3]), num(p[4]), num(p[5])
        if o is None or h is None or l is None:
            o = h = l = None
        out.append((d, o, h, l, c))
    return out


def stock(path, sym):
    rows = []
    for r in read_rows(path):
        p = r.split("_")
        if len(p) < 5:
            continue
        o, h, l, c = num(p[1]), num(p[2]), num(p[3]), num(p[4])
        if c is None:
            continue
        f = 1.0
        for ex, div, _ in ADJ.get(sym, []):
            if p[0] < ex:
                f *= div
        if o is None or h is None or l is None:
            rows.append((p[0], None, None, None, c / f))
        else:
            rows.append((p[0], o / f, h / f, l / f, c / f))
    return sorted(rows)


def nse_fut(fo_dir, sym):
    out = []
    for fp in sorted(glob.glob(os.path.join(fo_dir, "*", "*.json.gz"))):
        try:
            j = json.load(gzip.open(fp, "rt"))
        except Exception as e:
            print("  unreadable %s: %s" % (fp, e), file=sys.stderr); continue
        day = j["date"]
        cand = [r for r in j["rows"] if r["sym"] == sym and r["ins"] == "FUT" and r["exp"] >= day and (r["c"] or 0) > 0]
        if not cand:
            continue
        r = min(cand, key=lambda r: r["exp"])
        o, h, l = r["o"] or None, r["h"] or None, r["l"] or None
        if not r["v"] or o is None or h is None or l is None:
            o = h = l = None
        out.append((day.replace("-", ""), o, h, l, r["c"]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=os.path.expanduser("~/stocks-backups/index_hist_ni"))
    ap.add_argument("--fo", default=os.path.expanduser("~/stocks-wt/fo_raw_cache"))
    ap.add_argument("--out", default=os.path.join(HERE, "..", "docs", "xseries"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    ip = os.path.join(args.out, "_index.json")
    ser = (json.load(open(ip)).get("ser") or {}) if os.path.exists(ip) else {}

    def put(code, fname, label, kind, src, unit, rows, alias, note=""):
        if len(rows) < 2:
            print("  %-18s SKIPPED: %d rows" % (code, len(rows))); return
        body, first, last = pack(rows)
        body = dict({"sv": SCHEMA, "key": code, "label": label}, **body)
        with open(os.path.join(args.out, fname + ".json"), "w", encoding="utf-8") as fh:
            json.dump(body, fh, separators=(",", ":"))
        nohl = sum(1 for r in rows if r[1] is None)
        ser[code] = {"f": fname, "label": label, "kind": kind, "src": src, "unit": unit, "first": first, "last": last,
                     "n": len(rows), "ohl_from": next(("%s-%s-%s" % (r[0][:4], r[0][4:6], r[0][6:]) for r in rows if r[1] is not None), None),
                     "alias": alias, "note": note}
        print("  %-18s %5d bars %s -> %s, %d without open/high/low" % (code, len(rows), first, last, nohl))

    for key, (code, label, _) in BSE_IDX.items():
        p = os.path.join(args.raw, key + ".txt")
        if os.path.exists(p):
            put(code, key, label, "Index", "BSE index archive", "", bse_index(p), [key.replace("_", "")])
    for key, (code, label, _) in MCX_IDX.items():
        p = os.path.join(args.raw, key + ".txt")
        if os.path.exists(p):
            put(code, key, label, "Index", "MCX index history", "", bse_index(p), [code.split(":")[1]])
    for key, (sym, label, unit) in MCX.items():
        p = os.path.join(args.raw, key + ".txt")
        if os.path.exists(p):
            for nth in (1, 2):
                put("MCX:%s%d!" % (sym, nth), "%s%d" % (key, nth), "%s futures, %s month" % (label, "front" if nth == 1 else "second"),
                    "Futures", "MCX bhavcopy", unit, mcx_cont(p, nth), ["%s%d!" % (sym, nth), "MCX%s%d!" % (sym, nth)],
                    "Continuous contract, not back-adjusted: the price steps on each roll day.")
    for sym, (name, code) in STK.items():
        p = os.path.join(args.raw, "BSESTK_%s.txt" % sym)
        if os.path.exists(p):
            put("BSE:" + sym, "BSESTK_" + sym, name, "Not trading", "BSE price archive, scrip %s" % code, "", stock(p, sym), [sym],
                "Delisted. Adjusted for: " + "; ".join("%s (ex %s-%s-%s)" % (t, e[:4], e[4:6], e[6:]) for e, _, t in ADJ.get(sym, [])) + ".")
    if os.path.isdir(args.fo):
        for sym, name in NSE_FUT.items():
            rows = nse_fut(args.fo, sym)
            if rows:
                put("NSE:%s1!" % sym, "NSEFUT_%s1" % sym, "%s futures, front month (NSE)" % name, "Futures", "NSE F&O bhavcopy", "", rows,
                    ["%s1!" % sym], "Continuous contract, not back-adjusted: the price steps on each roll day.")
    json.dump({"sv": SCHEMA, "updated": dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"), "ser": ser},
              open(ip, "w", encoding="utf-8"), separators=(",", ":"), sort_keys=True)
    print("xseries: %d series in %s" % (len(ser), os.path.normpath(args.out)))


if __name__ == "__main__":
    main()
