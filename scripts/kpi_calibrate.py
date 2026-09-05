# -*- coding: utf-8 -*-
"""Measure the Insights ledgers against the calibration holdout (runbook §137).

scripts/kpi_calibration_screener.json holds the yearly tables the user read off screener.in's
login-gated "Insights" card (five screenshots, 2026-09-06). It is a SECOND READER, never a source:
this script only reports, per holdout metric, how our ledger's yearly view (an FY value, or the
FY-end quarter of a level metric — exactly what the card shows) compares:
    match      same number to the holdout's printed precision
    MISMATCH   both present, different  (a restatement, a different definition, or a wrong read)
    missing    holdout has a year we do not (unread filing, or the KPI lives in an annual report)
    extra      we have a year the holdout does not (not counted against anyone)
Usage: python3 scripts/kpi_calibrate.py [SYM ...]      (default: every symbol in the holdout)
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
HOLD = os.path.join(HERE, "kpi_calibration_screener.json")
LED = os.path.join(HERE, "kpi_insights")
STOP = {"of", "the", "and", "per", "in", "total", "number", "count", "ratio", "share", "rate"}


def toks(s):
    s = re.sub(r"\(.*?\)", " ", str(s).lower())
    return {t for t in re.findall(r"[a-z0-9]+", s) if t not in STOP and len(t) > 1}


def yearly_view(m, fy_end="03"):
    out = dict(m.get("y") or {})
    if m.get("kind") != "flow":
        for k, v in (m.get("q") or {}).items():
            if k[4:6] == fy_end and k not in out:
                out[k] = v
    return {k[:4]: v for k, v in out.items()}


def close(a, b, printed):
    dec = len(str(printed).split(".")[1]) if "." in str(printed) else 0
    return abs(float(a) - float(b)) <= 0.5 * 10 ** (-dec) + 1e-9


def main():
    hold = json.load(open(HOLD, encoding="utf-8"))
    syms = [s for s in sys.argv[1:]] or [k for k in hold if not k.startswith("_")]
    tot = {"match": 0, "MISMATCH": 0, "missing": 0}
    for sym in syms:
        p = os.path.join(LED, sym + ".json")
        if not os.path.exists(p):
            print("%s: no ledger yet" % sym); continue
        L = json.load(open(p, encoding="utf-8"))
        fy_end = "%02d" % int(L.get("fy_end_month") or 3)
        mets = L.get("metrics") or []
        print("== %s (%d ledger metrics, %d holdout metrics)" % (sym, len(mets), len(hold.get(sym, {}))))
        for key, years in hold.get(sym, {}).items():
            hname = key.split("|")[0]
            ht = toks(hname)
            best, score = None, 0.0
            hu = key.split("|")[1] if "|" in key else ""
            for m in mets:
                if ("%" in hu) != ("%" in (m.get("unit") or "")):
                    continue                        # a % share never matches a ₹/count row
                mt = toks(m["name"])
                j = len(ht & mt) / max(1, len(ht | mt))
                if j > score:
                    best, score = m, j
            if best is None or score < 0.34:
                print("   %-46s -> no ledger metric (best %.2f %s)" % (hname[:46], score, best["name"][:30] if best else "-"))
                tot["missing"] += len(years); continue
            view = yearly_view(best, fy_end)
            cells = []
            for y, hv in sorted(years.items()):
                ov = view.get(y)
                if ov is None:
                    cells.append("%s:missing" % y[2:]); tot["missing"] += 1
                elif close(ov, hv, hv):
                    cells.append("%s:ok" % y[2:]); tot["match"] += 1
                else:
                    cells.append("%s:MISMATCH(ours %s vs %s)" % (y[2:], ov, hv)); tot["MISMATCH"] += 1
            print("   %-46s -> %-38s %s" % (hname[:46], (best["name"][:36] + " [%s]" % best["unit"][:8]), " ".join(cells)))
    print("TOTAL", tot, "agreement on overlapping cells: %d/%d" % (tot["match"], tot["match"] + tot["MISMATCH"]))


if __name__ == "__main__":
    main()
