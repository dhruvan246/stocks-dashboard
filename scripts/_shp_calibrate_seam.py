# -*- coding: utf-8 -*-
"""Calibrate OLD_OTHER_TO_DII: does the old format's 'Other institutions' row belong to
DII or FII? Parse a top-mcap sample of Jun-2022 (old-format) filings BOTH ways and compare
each against the stock's stored Sep-2022 (new-format) values — the right mapping minimizes
the median absolute quarter-over-quarter seam (genuine Q-o-Q change averages out).

Run AFTER the new-format backfill has filled 2022-09-30:
  python -X utf8 scripts/_shp_calibrate_seam.py
"""
import os, sys, json, gzip, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fundamentals as B
import fetch_shareholding as F

SAMPLE = 150
HERE = os.path.dirname(os.path.abspath(__file__))

hist = json.load(open(F.HIST, encoding="utf-8"))
slim = json.loads(gzip.decompress(open(F.SLIM, "rb").read()))
mcap = {}
for k, m in (slim.get("meta") or {}).items():
    mcap[str(m.get("symbol") or k.split(".")[0]).upper()] = m.get("mcap") or 0

jar = B.nse_jar()
recs = F.fetch_master(jar, "2022-06-30")
best = {}
for r in recs:
    sym = str(r.get("symbol") or "").strip().upper()
    xb = str(r.get("xbrl") or "")
    if not sym or not xb.startswith("http"): continue
    if sym not in hist or "2022-09-30" not in hist.get(sym, {}): continue
    best.setdefault(sym, xb)
cand = sorted(best, key=lambda s: -mcap.get(s, 0))[:SAMPLE]
print("sample: %d symbols (largest with a stored Sep-2022 cell)" % len(cand))

rows = []
for i, sym in enumerate(cand):
    try:
        txt = F.fetch_xbrl(best[sym], jar)
        F.OLD_OTHER_TO_DII = True
        a = F.parse_shp(txt, "2022-06-30")
        F.OLD_OTHER_TO_DII = False
        b = F.parse_shp(txt, "2022-06-30")
        if not a or not b: continue
        s = hist[sym]["2022-09-30"]  # [prom, fii, dii, mf, ins, sub]
        rows.append((sym,
                     abs(a["fii"] - s[1]) + abs(a["dii"] - s[2]),    # other -> DII seam
                     abs(b["fii"] - s[1]) + abs(b["dii"] - s[2]),    # other -> FII seam
                     a["fii"] - b["fii"]))                           # size of 'other'
    except Exception as e:
        print("  skip", sym, repr(e))
    if (i + 1) % 50 == 0: print("  ...", i + 1)

d2d = [r[1] for r in rows]; d2f = [r[2] for r in rows]
oth = [abs(r[3]) for r in rows]
print("\nn=%d | seam if OTHER->DII: median %.3f mean %.3f | if OTHER->FII: median %.3f mean %.3f"
      % (len(rows), statistics.median(d2d), statistics.fmean(d2d),
         statistics.median(d2f), statistics.fmean(d2f)))
print("'other institutions' size: median %.3f pp, p90 %.3f, max %.3f"
      % (statistics.median(oth), sorted(oth)[int(len(oth)*0.9)], max(oth)))
print("stocks where the choice matters most:")
for r in sorted(rows, key=lambda r: -abs(r[1] - r[2]))[:10]:
    print("  %-12s seamDII %.2f seamFII %.2f other %.2f" % r)
