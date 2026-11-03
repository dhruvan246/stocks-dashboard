# -*- coding: utf-8 -*-
"""Market cap on every quarter-end, per stock -> docs/hist_mcap.json (All Picks rewind, 2026-11-03).

    mcap(QE) [Rs Cr] = shares(QE) x RAW close on the quarter's last trading day / 1e7

shares(QE) = scripts/shares_history.json (the company's own SHP filing, §180). The raw close is NSE's
bhavcopy for that session — never the split/bonus-adjusted series, because the share count is the
count as filed then (Reliance Mar-2023: 676.6 cr shares, before its 2024 1:1 bonus).

The page turns this into the cap on any rewind date D by moving the nearest quarter-end at or before
D with the page's OWN adjusted price: mcap(D) = mcap(QE) x adj(D) / adj(QE). A split or bonus
between QE and D cancels out of that ratio, and so does any later re-adjustment of the price
history, so this file only needs rebuilding when a new quarter's share counts land.

Identity (one ticker can be two companies, §203/§207): a bhavcopy row is used only when its ISIN
issuer prefix (first 7 chars, e.g. INE758T — stable across face-value changes) equals the stock's
current ISIN prefix (EQUITY_L / SME list). Old tickers are found through scripts/_rename_map.json.
A symbol with no current ISIN on file is used only when every matched quarter carries ONE issuer.
Every cell not written is counted by reason in the output's _meta.

Run: python3 scripts/build_hist_mcap.py [--bhav DIR] [--bhav-sec DIR] [--univ DIR]
"""
import argparse, csv, io, json, os, sys, zipfile, datetime as dt, collections

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
C = os.path.expanduser("~/stocks-cache")
ap = argparse.ArgumentParser()
ap.add_argument("--bhav", default=f"{C}/fii_mcap_threshold/nse_full_bhav")   # YYYYMMDD.zip, cm…bhav / UDiFF (with ISIN)
ap.add_argument("--bhav-sec", default=f"{C}/nse-bhav")                       # sec_bhavdata_full_DDMMYYYY.csv (no ISIN)
ap.add_argument("--univ", default=f"{C}/univ")                               # EQUITY_L.csv
ap.add_argument("--sme", default=f"{C}/sme_equity_l.csv")
ap.add_argument("--official", default=f"{C}/nse_mcap")                      # NSE MCAPddmmyyyy*.xlsx (lakhs) — the check
ap.add_argument("--out", default=os.path.join(ROOT, "docs", "hist_mcap.json"))
a = ap.parse_args()

SERIES_OK = ("EQ", "BE", "BZ", "SM", "ST", "SZ", "IV")
sh = json.load(open(os.path.join(HERE, "shares_history.json"), encoding="utf-8")); sh.pop("_meta", None)
ren = json.load(open(os.path.join(HERE, "_rename_map.json"), encoding="utf-8"))       # old -> new
old_of = collections.defaultdict(set)
for o, n in ren.items(): old_of[n].add(o)

cur_isin = {}
for path, sk, ik in ((os.path.join(a.univ, "EQUITY_L.csv"), "SYMBOL", "ISIN NUMBER"), (a.sme, "SYMBOL", "ISIN_NUMBER")):
    if not os.path.exists(path): continue
    for r in csv.DictReader(open(path, encoding="latin1")):
        r = {k.strip(): (v or "").strip() for k, v in r.items() if k}
        if r.get(sk) and r.get(ik): cur_isin.setdefault(r[sk], r[ik])
pfx = lambda isin: (isin or "")[:7]

def qe_trading_day(qe):
    """The quarter's last trading day = the latest bhavcopy on disk dated QE-6..QE (the file's own date decides, §89f)."""
    d = dt.date.fromisoformat(qe)
    for k in range(7):
        x = d - dt.timedelta(days=k)
        if os.path.exists(f"{a.bhav}/{x:%Y%m%d}.zip"): return x, "zip"
        if os.path.exists(f"{a.bhav_sec}/sec_bhavdata_full_{x:%d%m%Y}.csv"): return x, "sec"
    return None, None

def load_day(day, kind):
    """{symbol: (close, isin_or_None)} for equity series, checked against the date printed inside the file."""
    out = {}; want = {day.strftime("%d-%b-%Y").upper(), day.isoformat(), day.strftime("%d-%b-%Y")}
    if kind == "zip":
        z = zipfile.ZipFile(f"{a.bhav}/{day:%Y%m%d}.zip"); txt = z.read(z.namelist()[0]).decode("latin1")
    else:
        txt = open(f"{a.bhav_sec}/sec_bhavdata_full_{day:%d%m%Y}.csv", encoding="latin1").read()
    seen_dates = set()
    for r in csv.DictReader(io.StringIO(txt)):
        r = {(k or "").strip(): (v or "").strip() for k, v in r.items()}
        sym = r.get("SYMBOL") or r.get("TckrSymb"); ser = r.get("SERIES") or r.get("SctySrs")
        cl = r.get("CLOSE") or r.get("ClsPric") or r.get("CLOSE_PRICE")
        dd = r.get("TIMESTAMP") or r.get("TradDt") or r.get("DATE1"); seen_dates.add(dd)
        if ser not in SERIES_OK or not sym: continue
        try: cl = float(cl)
        except (TypeError, ValueError): continue
        if cl <= 0: continue
        prev = out.get(sym)
        if prev is None or ser == "EQ": out[sym] = (cl, r.get("ISIN") or None)
    if not (seen_dates & want):
        sys.exit(f"bhavcopy for {day} carries dates {sorted(seen_dates)[:3]} — not that session (§89f)")
    return out

qes = sorted({q for s in sh.values() for q in s})
days = {}
for q in qes:
    d, kind = qe_trading_day(q)
    if d: days[q] = (d, load_day(d, kind))
print("quarter-ends with a bhavcopy:", len(days), "of", len(qes), "| missing:", [q for q in qes if q not in days])

rows, skip = {}, collections.Counter()
for sym, qmap in sh.items():
    ref = pfx(cur_isin.get(sym)); cands = [sym] + sorted(old_of.get(sym, ()))
    got = []
    for q in sorted(qmap):
        shares = (qmap[q] or [None])[0]
        if not shares or shares <= 0: skip["no share count"] += 1; continue
        if q not in days: skip["no bhavcopy for that quarter-end"] += 1; continue
        d, bh = days[q]; hit = None
        for c in cands:
            if c in bh: hit = bh[c]; break
        if hit is None:
            src = str((qmap[q] or [None, None, ""])[2] or "")
            skip["BSE-only filer, no NSE row" if src.startswith("bse") else "no NSE row on the quarter-end"] += 1; continue
        cl, isin = hit
        if ref and isin and pfx(isin) != ref: skip["ISIN issuer differs (other company)"] += 1; continue
        got.append((int(d.strftime("%Y%m%d")), round(shares * cl / 1e7, 2), pfx(isin) if isin else None))
    if not ref:                                          # no current ISIN: only one issuer across its quarters
        issuers = {g[2] for g in got if g[2]}
        if len(issuers) > 1: skip["ticker spans two issuers, no current ISIN"] += len(got); continue
    if got: rows[sym] = [[g[0], g[1]] for g in got]

# Second reader: NSE's own all-company market-cap lists (in lakhs) for the dates on disk. Measured
# 2026-11-03 on 4 lists: 190 stocks differ from NSE by >2% somewhere. 51 differ on 2+ dates (BSE Ltd
# -88..-93%, HMT +157%, BHARTIARTL +7% every date) — their share count is off in every quarter, so the
# stock is dropped whole; same for a stock whose ONLY checked date differs. 139 differ on one date
# and agree on all their other checked dates (a share change or split around that quarter-end), so
# only that quarter is dropped. Every dropped cell is UNKNOWN until its own filing is read; the page
# falls back to today's share count there, marked "≈".
dropped, fails, checked = {}, collections.defaultdict(list), collections.Counter()
if os.path.isdir(a.official):
    import openpyxl
    for f in sorted(os.listdir(a.official)):
        if not (f.startswith("MCAP") and f.endswith(".xlsx")): continue
        ws = openpyxl.load_workbook(os.path.join(a.official, f), read_only=True).active
        hdr = next(ws.iter_rows(max_row=1, values_only=True))
        if "lakh" not in str(hdr[3]).lower(): print("skip (unit not lakhs):", f, hdr[3]); continue
        try: day = int(dt.datetime.strptime(f[4:12], "%d%m%Y").strftime("%Y%m%d"))
        except ValueError: continue
        for r in ws.iter_rows(min_row=2, values_only=True):
            if not r or not r[1] or not isinstance(r[3], (int, float)) or r[3] <= 0: continue
            sym = str(r[1]).strip()
            # "as on December 31, 2023" is that day's or the last session before it (29-Dec) — same quarter-end row
            hit = [(d, m) for d, m in rows.get(sym, []) if day - 7 < d <= day and (day // 100) == (d // 100)]
            if not hit: continue
            checked[sym] += 1
            if abs(hit[-1][1] / (r[3] / 100) - 1) > 0.02:
                fails[sym].append([hit[-1][0], hit[-1][1], round(r[3] / 100, 2)])
    whole = cells = 0
    for sym, fl in fails.items():
        if len(fl) >= 2 or checked[sym] == 1:
            rows.pop(sym, None); dropped[sym] = {"all": True, "differs": fl}; whole += 1
        else:
            bad = {f[0] for f in fl}
            rows[sym] = [c for c in rows[sym] if c[0] not in bad]; dropped[sym] = {"all": False, "differs": fl}; cells += 1
            if not rows[sym]: rows.pop(sym)
    print(f"official lists checked ({len(checked)} stocks): dropped {whole} whole, {cells} single quarters (>2% from NSE)")

n = sum(len(v) for v in rows.values())
meta = {"built": dt.datetime.now().strftime("%Y-%m-%d %H:%M IST"), "symbols": len(rows), "cells": n,
        "shape": "{SYM: [[yyyymmdd quarter's last trading day, mcap Rs Cr], ...]}",
        "formula": "shares_history.json shares x NSE bhavcopy raw close / 1e7", "skipped": dict(skip),
        "dropped_vs_nse_official": dropped, "runbook": "§233"}
# stocks NSE's lists checked and that still have rows: the page trusts these as they are; the rest get
# its sanity test against today's share count (a 0.4x-2.5x band) before use
chk = sorted(s for s in checked if s in rows)
json.dump({"_meta": meta, "chk": chk, "rows": rows}, open(a.out, "w", encoding="utf-8"), separators=(",", ":"))
print(json.dumps(meta, indent=1))
