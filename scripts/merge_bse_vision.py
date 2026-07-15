# -*- coding: utf-8 -*-
"""Merge vision-extracted BSE quarterly numbers into docs/bse_fundamentals.json and clear those scrips
from the OCR-fail ledger (so build_bse_results promotes them from 'PDF only' to real numbered rows).

Input: a JSON file (arg1) that is a list of
  {"ticker","scrip","ok":bool,"basis":"C|S","jun2026":{"rev","pat"},"jun2025":{"rev","pat"},"note"}
Values are ₹ crore. jun2025 is stored so YoY computes. ann date defaults per-quarter (filing month).

Run: python -X utf8 scripts/merge_bse_vision.py <results.json>
"""
import os, sys, json
HERE = os.path.dirname(os.path.abspath(__file__))
FUND = os.path.join(HERE, "..", "docs", "bse_fundamentals.json")
FAILS = os.path.join(HERE, "_bse_fund_fail.json")
DONE = os.path.join(HERE, "_bse_fund_done.json")
# nominal ann dates (filing months): current quarter shows this; year-ago ann unused for YoY
ANN = {"20260630": 20260715, "20260331": 20260615, "20250630": 0}

def put(px, scrip, qe, rec, basis):
    d = {"pat": rec.get("pat"), "ann": ANN.get(qe, 0), "basis": basis, "src": "vision"}
    if rec.get("rev") is not None: d["rev"] = round(float(rec["rev"]), 2)
    if d["pat"] is not None: d["pat"] = round(float(d["pat"]), 2)
    px.setdefault(scrip, {})[qe] = d

def main():
    items = json.load(open(sys.argv[1], encoding="utf-8"))
    data = json.load(open(FUND, encoding="utf-8"))
    px = data["px"]
    fails = json.load(open(FAILS)) if os.path.exists(FAILS) else {}
    done = set(json.load(open(DONE))) if os.path.exists(DONE) else set()
    n = 0
    for it in items:
        if not it.get("ok"): continue
        scrip = str(it["scrip"]); basis = it.get("basis", "S") or "S"
        j26 = it.get("jun2026") or {}
        if j26.get("pat") is None and j26.get("rev") is None: continue
        put(px, scrip, "20260630", j26, basis)
        j25 = it.get("jun2025") or {}
        if j25.get("pat") is not None or j25.get("rev") is not None:
            put(px, scrip, "20250630", j25, basis)
        fails.pop(scrip, None)                 # no longer a parse failure → drops from pdf_only
        done.add(scrip)
        n += 1
        print("  ✓ %s (%s) Jun26 rev=%s pat=%s | Jun25 rev=%s pat=%s"
              % (it["ticker"], scrip, j26.get("rev"), j26.get("pat"), j25.get("rev"), j25.get("pat")))
    json.dump(data, open(FUND, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    json.dump(fails, open(FAILS, "w"))
    json.dump(sorted(done), open(DONE, "w"))
    print("Merged %d companies into bse_fundamentals.json" % n)

if __name__ == "__main__":
    main()
