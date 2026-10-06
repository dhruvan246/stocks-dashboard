# -*- coding: utf-8 -*-
"""Fill a multi-year HOLE inside a Nifty-500 series with BSE's own daily prices (runbook §179g; user 2026-10-07:
"go ahead with option A, fill from BSE").

KENNAMET has no bar from 2002-09-30 to 2019-08-19 and SPICEJET none from 1999-07-19 to 2019-08-19: NSE printed neither
company in between (NSE's symbol-change file: WIDIA -> KENNAMET and MODILUFT -> SPICEJET, both on 19-Aug-2019, when NSE
trading resumed), while BSE traded both throughout. Across the hole the engine's "last 200 sessions" and its 3/6/12-month
look-backs reached 17-20 years back (KENNAMET ret12m +1,310 % on 31-Dec-2019 from a 2002 bar; quantmac reply #5, 12 cells).

Source: BSE's per-scrip daily history (api.bseindia.com .../StockPriceCSVDownload, ONE request per scrip, bse_headers),
cached in ~/stocks-cache/bse_hist (env BSE_HIST_CACHE). Output: standard `prepend` blocks in scripts/bse_sme_prepend.json.gz
for update_sf_data.insert_sme_history's HOLE mode (§171a — applied only while the bin's next bar after the block start IS
the anchor; a re-run no-ops):
  bars    [d, c, t(Rs lakh), h, l, op, v, dv=0, vw=turnover/volume] for BSE sessions strictly inside the hole: daily-era
          days (>= dailyFrom) only where they are sessions of the bin's own calendar (update_sf_data.session_calendar);
          before dailyFrom one bar per week — the week's first BSE session, the store's pre-2002 convention — skipping
          the week of the last stored bar;
  anchor  {ymd: the NSE bar that ends the hole, raw: stored close / product of the series' corporate actions after it}
          -> the consumer's rescale (stored / raw) equals that product, so BSE's own level is kept (§171 gate 2).
Gates (all must pass or the block is refused): the bin still has exactly this hole; same ISIN on NSE and BSE; median
NSE-stored / BSE-raw close over the first <= 10 common sessions within 3 % of the CA product; no split / bonus /
consolidation / rights / scheme on BSE's corporate-action list inside the hole; every bar 0 < low <= open, close <= high.
Usage: python3 scripts/build_bse_hole_fill.py --tape <release sf_stock_data.bin> [--dry]"""
import os, sys, io, csv, json, gzip, time, datetime, statistics, argparse, urllib.request
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bse_headers as BH
BH.install()
CACHE = os.path.expanduser(os.environ.get("BSE_HIST_CACHE", "~/stocks-cache/bse_hist"))
OUT = os.path.join(HERE, "bse_sme_prepend.json.gz")
HIST = "https://api.bseindia.com/BseIndiaAPI/api/StockPriceCSVDownload/w?pageType=0&rbType=D&Scode=%s&FDates=%s&TDates=%s"
CAURL = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w?Fdate=19900101&Purposecode=&ScripCode=%s&segment=0&strSearch=S&TDate=%s"
# sym -> BSE code, the ISIN both exchanges carry, the last stored bar before the hole, the NSE bar that ends it
HOLES = {"KENNAMET": dict(code="505890", isin="INE717A01029", last=20020930, anchor=20190819),
         "SPICEJET": dict(code="500285", isin="INE285B01017", last=19990719, anchor=20190819)}
CA_WORDS = ("split", "bonus", "consolidat", "right", "arrangement", "amalgamation", "demerger", "reduction")


def ymd(d): return datetime.date(d // 10000, d // 100 % 100, d % 100)
def dmy(d): return ymd(d).strftime("%d/%m/%Y")


def fetch(url, path):
    if not os.path.exists(path):
        body = urllib.request.urlopen(urllib.request.Request(url), timeout=180).read()   # one request at a time (BSE pacing)
        open(path, "wb").write(body); time.sleep(3)
    return open(path, "rb").read()


def bse_days(code, a, b):
    """BSE's daily rows for scrip `code`, a..b inclusive: {ymd: (o, h, l, c, shares, turnover_rs)}."""
    raw = fetch(HIST % (code, dmy(a), dmy(b)), os.path.join(CACHE, "%s_%s_%s.csv" % (code, dmy(a).replace("/", ""), dmy(b).replace("/", ""))))
    out = {}
    for r in list(csv.reader(io.StringIO(raw.decode("utf-8", "replace"))))[1:]:
        if not r or not r[0]: continue
        d = int(datetime.datetime.strptime(r[0].strip(), "%d-%B-%Y").strftime("%Y%m%d"))
        row = (float(r[1]), float(r[2]), float(r[3]), float(r[4]), int(float(r[6])), float(r[8]))
        # 2000-2001 prints two rows on some dates (BSE ran two settlement segments then; SPICEJET 102 dates): keep the
        # main one = more shares traded
        if d not in out or row[4] > out[d][4]: out[d] = row
    return out


def ca_rows(code):
    raw = fetch(CAURL % (code, datetime.date.today().strftime("%Y%m%d")), os.path.join(CACHE, "ca_%s.json" % code))
    rows = json.loads(raw or b"[]")
    return rows if isinstance(rows, list) else (rows.get("Table") or [])


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--tape", required=True); ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()
    T = json.loads(gzip.open(a.tape).read()); data = T["data"]
    import update_sf_data as U
    lo, hi = int((T.get("dailyFrom") or "2002-01-02").replace("-", "")), int(T["end"].replace("-", ""))
    cal = U.session_calendar(data, lo, hi)
    raw_tape = U.B if hasattr(U, "B") else None
    cfac = json.load(open(os.path.join(HERE, "corp_actions.json"))).get("factors", {})
    led = json.load(gzip.open(OUT, "rt", encoding="utf-8"))
    made = []
    for sym, h in HOLES.items():
        e = data.get(sym); ds = e["d"] if e else []
        if h["last"] not in ds or ds.index(h["last"]) + 1 >= len(ds) or ds[ds.index(h["last"]) + 1] != h["anchor"]:
            print("REFUSED %s: the bin no longer holds the hole %d -> %d" % (sym, h["last"], h["anchor"])); continue
        first, end = (ymd(h["last"]) + datetime.timedelta(days=1)).strftime("%Y%m%d"), (ymd(h["anchor"]) - datetime.timedelta(days=1)).strftime("%Y%m%d")
        B = bse_days(h["code"], int(first), int(end))
        # identity 1: the ISIN NSE prints on the anchor day == BSE's master ISIN for the code (checked by hand: bse_scrip_master)
        meta_isin = (T["meta"].get(sym) or {}).get("isin")
        if meta_isin and meta_isin != h["isin"]:
            print("REFUSED %s: bin ISIN %s != %s" % (sym, meta_isin, h["isin"])); continue
        # identity 2: NSE-stored / BSE-raw over the first <= 10 common sessions after the anchor == CA product (3 %)
        after = [d for d in ds if d >= h["anchor"]][:40]
        Bn = bse_days(h["code"], after[0], after[-1])
        prod = 1.0
        for d_, f_ in cfac.get(sym, []):
            if int(d_) > h["anchor"]: prod *= float(f_)
        com = [d for d in after if d in Bn][:10]
        rat = [e["c"][ds.index(d)] / Bn[d][3] for d in com]
        med = statistics.median(rat)
        if not com or abs(med / prod - 1) > 0.03:
            print("REFUSED %s: median NSE/BSE %.4f vs CA product %.4f" % (sym, med, prod)); continue
        # no capital action inside the hole on BSE's own list
        bad = []
        for r in ca_rows(h["code"]):
            ex = r.get("Ex_date") or r.get("BCRD_from") or ""
            try: exd = int(datetime.datetime.strptime(ex.strip(), "%d %b %Y").strftime("%Y%m%d"))
            except ValueError: continue
            if h["last"] < exd < h["anchor"] and any(w in (r.get("Purpose") or "").lower() for w in CA_WORDS): bad.append((exd, r.get("Purpose")))
        if bad:
            print("REFUSED %s: capital action(s) inside the hole %s" % (sym, bad)); continue
        bars = []; seen_wk = {ymd(h["last"]).isocalendar()[:2]}; dropped = []
        for d in sorted(B):
            o, hh, ll, c, v, t = B[d]
            if d < lo:
                wk = ymd(d).isocalendar()[:2]
                if wk in seen_wk: continue
                seen_wk.add(wk)
            elif d not in cal[0]:
                dropped.append(d); continue
            if not (0 < ll <= min(o, c) and max(o, c) <= hh):
                print("REFUSED %s: insane bar %d %s" % (sym, d, B[d])); bars = None; break
            bars.append([d, round(c, 2), round(t / 1e5, 2), round(hh, 2), round(ll, 2), round(o, 2), v, 0, round(t / v, 2) if v else round(c, 2)])
        if not bars: continue
        ia = ds.index(h["anchor"])
        anchor = {"ymd": h["anchor"], "raw": round(e["c"][ia] / prod, 4), "bse_raw": Bn.get(h["anchor"], (None,) * 4)[3], "median_ratio": round(med, 4), "n": len(com)}
        note = ("BSE main board %s: %d bars %d -> %d (BSE per-scrip daily history, %d daily-era sessions + %d pre-%d weekly) filling the hole "
                "%d -> %d; splits none (BSE corporate actions inside the hole: none of %s); rescale %.4f; §179g"
                % (h["code"], len(bars), bars[0][0], bars[-1][0], sum(1 for b in bars if b[0] >= lo), sum(1 for b in bars if b[0] < lo), lo,
                   h["last"], h["anchor"], "/".join(CA_WORDS), prod))
        led["prepend"][sym] = {"target": sym, "bars": bars, "anchor": anchor, "meta": {"isin": h["isin"]}, "note": note}
        made.append(sym)
        print("ADD  %-9s %s" % (sym, note))
        print("     anchor %s | BSE rows in the hole %d, daily-era non-sessions dropped %d %s" % (anchor, len(B), len(dropped), dropped[:8]))
    if made and not a.dry:
        json.dump(led, gzip.open(OUT, "wt", encoding="utf-8")); print("wrote", OUT, "| blocks", len(led["prepend"]))


if __name__ == "__main__":
    main()
