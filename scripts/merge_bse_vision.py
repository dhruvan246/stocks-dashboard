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
VFILLS = os.path.join(HERE, "..", "docs", "vision_fills.json")   # NSE overlay the page applies to empty cells
# nominal ann dates (filing months): current quarter shows this; year-ago ann unused for YoY
ANN = {"20260630": 20260715, "20260331": 20260615, "20250630": 0}

def feed_ann():
    """Real filing date (YYYYMMDD int) per ticker from the results feed — so the result-day price
    reaction computes off the actual announcement day, not a hardcoded date."""
    try:
        rows = json.load(open(os.path.join(HERE, "..", "docs", "results_feed.json"), encoding="utf-8"))["rows"]
    except Exception:
        return {}
    out = {}
    for r in rows:
        d = int(r[2][:10].replace("-", ""))
        if r[0] not in out or d > out[r[0]]: out[r[0]] = d
    return out

QMAP = [("jun2026", "20260630"), ("mar2026", "20260331"), ("jun2025", "20250630")]

def _num(v): return round(float(v), 2) if v is not None else None

def put(px, scrip, qe, rec, basis, ann=None):
    d = {"pat": _num(rec.get("pat")), "ann": ann or ANN.get(qe, 0), "basis": basis, "src": "vision"}
    if rec.get("rev") is not None: d["rev"] = _num(rec["rev"])
    if rec.get("op") is not None: d["op"] = _num(rec["op"])
    px.setdefault(scrip, {})[qe] = d

def main():
    items = json.load(open(sys.argv[1], encoding="utf-8"))
    data = json.load(open(FUND, encoding="utf-8"))
    px = data["px"]
    fails = json.load(open(FAILS)) if os.path.exists(FAILS) else {}
    done = set(json.load(open(DONE))) if os.path.exists(DONE) else set()
    vfills = json.load(open(VFILLS, encoding="utf-8")) if os.path.exists(VFILLS) else {}
    fann = feed_ann()
    nb = nn = 0
    for it in items:
        if not it.get("ok"): continue
        basis = it.get("basis", "S") or "S"
        sym = (it.get("ticker") or it.get("sym") or "").upper()
        j26 = it.get("jun2026") or {}
        if j26.get("pat") is None and j26.get("rev") is None: continue
        # NSE entries (exch=="NSE", or no BSE scrip) → overlay file the page applies only to EMPTY cells,
        # so real XBRL always supersedes this vision estimate once it lands. BSE-only → bse_fundamentals.
        if str(it.get("exch", "")).upper() == "NSE" or not str(it.get("scrip") or "").strip():
            e = vfills.setdefault(sym, {})
            for key, qe in QMAP:
                q = it.get(key) or {}
                if q.get("pat") is None and q.get("rev") is None and q.get("op") is None: continue
                e[qe] = {k: v for k, v in {"rev": _num(q.get("rev")), "op": _num(q.get("op")),
                          "pat": _num(q.get("pat")), "basis": basis, "src": "vision"}.items() if v is not None}
            nn += 1
            print("  ✓ NSE %-11s Jun26 rev=%s pat=%s op=%s (overlay)" % (sym, j26.get("rev"), j26.get("pat"), j26.get("op")))
        else:
            scrip = str(it["scrip"]); ann = fann.get(sym)   # real filing date → reaction computes
            for key, qe in QMAP:
                q = it.get(key) or {}
                if q.get("pat") is None and q.get("rev") is None and q.get("op") is None: continue
                put(px, scrip, qe, q, basis, ann=(ann if qe == "20260630" else None))
            fails.pop(scrip, None); done.add(scrip); nb += 1
            print("  ✓ BSE %-11s (%s) Jun26 rev=%s pat=%s op=%s" % (sym, scrip, j26.get("rev"), j26.get("pat"), j26.get("op")))
    json.dump(data, open(FUND, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    json.dump(fails, open(FAILS, "w"))
    json.dump(sorted(done), open(DONE, "w"))
    json.dump(vfills, open(VFILLS, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("Merged %d BSE-only into bse_fundamentals.json, %d NSE into vision_fills.json" % (nb, nn))

if __name__ == "__main__":
    main()
