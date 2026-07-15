# -*- coding: utf-8 -*-
"""Build docs/bse_results.json — a SLIM BSE-only payload in the SAME shape as quarterly_results.json's
`co`, so the Quarterly Results page merges BSE-listed-only companies (Cella Space, NSDL, …) in one line.

Joins three BSE side-files (all produced separately):
  - docs/bse_universe.json     — name, sector, mcap per BSE-only scrip
  - docs/bse_fundamentals.json — quarterly {rev,pat,ann,basis} per scrip (OCR of the co's own filing)
  - docs/bse_prices.bin        — daily closes per scrip (BSE bhavcopy) → result-day reaction + since-drift

Quarters list is READ FROM quarterly_results.json so the q-array indices line up exactly. Each emitted
co record mirrors the NSE payload:  {n,s,i,m,f,x,e, bse:1, q:[[revS,opS,patS,revC,opC,patC,ann,rx,sr]|null,…]}
BSE small-caps are standalone → we put the same value in the std AND con slots (op left null); `bse:1`
flags the row so the page can show a BSE badge. Keyed by BSE ticker; skips any ticker already on NSE.

Run: python -X utf8 scripts/build_bse_results.py
"""
import os, sys, json, gzip, datetime
HERE = os.path.dirname(os.path.abspath(__file__))
D = os.path.join(HERE, "..", "docs")
QR = os.path.join(D, "quarterly_results.json")
UNIV = os.path.join(D, "bse_universe.json")
FUND = os.path.join(D, "bse_fundamentals.json")
PX = os.path.join(D, "bse_prices.bin")
FAILS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_bse_fund_fail.json")
OUT = os.path.join(D, "bse_results.json")
PDF_ONLY_MIN = 2                 # fail attempts before a filed co is flagged "PDF only" (not parseable)

FIN_SECTORS = {"Finance", "Banks", "Bank", "Financial Services", "NBFC"}

def load_prices():
    if os.path.exists(PX):
        try: return json.loads(gzip.decompress(open(PX, "rb").read()).decode("utf8"))
        except Exception: pass
    return {"px": {}, "end": 0}

def reaction(series, ann):
    """(result-day move %, since-result drift %) from a scrip's close series and an ann date int."""
    if not series or not ann: return None, None
    d, c = series["d"], series["c"]
    j = next((i for i, x in enumerate(d) if x >= ann), None)
    if j is None or j == 0: return None, None
    rd = (c[j] / c[j - 1] - 1) * 100 if c[j - 1] else None       # result-day vs prior close
    sr = (c[-1] / c[j] - 1) * 100 if c[j] else None              # drift to latest close
    return (round(rd, 2) if rd is not None else None,
            round(sr, 2) if sr is not None else None)

def main():
    qr = json.load(open(QR, encoding="utf-8"))
    quarters = qr["quarters"]                         # e.g. [20260630, 20260331, …] newest-first
    qidx = {int(q): i for i, q in enumerate(quarters)}
    nse_syms = set(qr["co"].keys())

    univ = {str(r[0]): r for r in json.load(open(UNIV, encoding="utf-8"))["rows"]}   # scrip -> row
    fund = json.load(open(FUND, encoding="utf-8"))["px"] if os.path.exists(FUND) else {}
    prices = load_prices()["px"]

    co = {}
    for code, qs in fund.items():
        u = univ.get(code)
        if not u: continue
        scrip, tkr, name, isin, grp, fv, mc, sec = u
        tkr = (tkr or "").upper()
        if not tkr or tkr in nse_syms or tkr in co: continue    # never shadow an NSE company
        q = [None] * len(quarters)
        series = prices.get(code)
        any_num = False
        for qe, rec in qs.items():
            qi = qidx.get(int(qe))
            if qi is None: continue
            rev = rec.get("rev"); pat = rec.get("pat"); op = rec.get("op"); ann = rec.get("ann") or 0
            rx, sr = reaction(series, ann) if ann else (None, None)
            # std and con slots both carry the (standalone) value; op filled when the filing reports it
            q[qi] = [rev, op, pat, rev, op, pat, ann or None, rx, sr]
            any_num = True
        if not any_num: continue
        f = 1 if (sec in FIN_SECTORS) else 0
        co[tkr] = {"n": name, "s": sec or "Other", "i": sec or "Other",
                   "m": mc, "f": f, "x": 0, "e": "", "bse": 1,
                   "cd": int(scrip), "q": q}

    # "PDF only": BSE tickers that FILED a result but resisted OCR ≥ PDF_ONLY_MIN times — their number
    # isn't merely queued (scanned/odd-layout PDF), so the page labels them honestly instead of "coming".
    pdf_only = []
    try:
        fails = json.load(open(FAILS))
        for code, n in fails.items():
            if n >= PDF_ONLY_MIN and str(code) in univ:
                tkr = (univ[str(code)][1] or "").upper()
                if tkr and tkr not in co and tkr not in nse_syms: pdf_only.append(tkr)
    except Exception:
        pass

    ist = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "asof": qr.get("asof"),
           "quarters": quarters, "count": len(co), "co": co, "pdf_only": sorted(set(pdf_only))}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("WROTE %s: %d BSE-only companies with numbers, %d PDF-only" % (os.path.normpath(OUT), len(co), len(pdf_only)))

if __name__ == "__main__":
    main()
