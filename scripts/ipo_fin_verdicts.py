# -*- coding: utf-8 -*-
"""Per-cell record for the Screener gate (runbook §232): re-opens each differing cell's own filing page, finds the printed
row carrying our figure, and writes a verdict for that cell. Writes scripts/ipo_fin_verdicts.json. Never changes data."""
import json, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fitz, ipo_statement_reader as R
CACHE = os.path.expanduser("~/stocks-cache/ipo_prospectus")
FIELD = {"Sales": "rev", "Revenue": "rev", "Net Profit": None, "Profit before tax": "pbt", "Depreciation": "dep", "Interest": "fc", "Other Income": "oi",
         "Total Assets": "assets", "Cash from Operating Activity": "cfo", "Cash from Investing Activity": "cfi", "Cash from Financing Activity": "cff"}
g = json.load(open("scripts/_ipo_fin_gate.json"))
docs, out = {}, []
for x in g["diff"]:
    d = json.load(open("docs/ipo_fin/%s.json" % x["sym"]))
    rec = d[x["basis"]][x["period"]]
    src, row = rec.get("_src"), None
    if src != "results" and rec.get("_p") and os.path.exists("%s/%s.pdf" % (CACHE, x["sym"])):
        doc = docs.setdefault(x["sym"], fitz.open("%s/%s.pdf" % (CACHE, x["sym"])))
        u = None
        for p in (rec["_p"], rec["_p"] - 1, rec["_p"] + 1, rec["_p"] + 2, rec["_p"] - 2):
            if not 0 < p <= len(doc): continue
            for r in R.table(doc[p - 1])[1]:
                for v in r["vals"].values():
                    if v and any(abs(v * k - x["ours"]) <= 0.006 for k in (1, .1, .01, .0001, 1e-7)) and (FIELD[x["field"]] is None or len(r["text"]) > 3):
                        row = {"page": p, "label": r["text"][:70], "value": v}; break
                if row: break
            if row: break
    oi, exc = rec.get("oi") or 0, rec.get("exc") or 0
    v, why = "UNKNOWN", "our figure read from the filing; Screener's source row not identified" if row or src == "results" else "our row not relocated"
    if x["field"] == "Other Income" and exc and min(abs(oi + exc - x["screener"]), abs(oi - exc - x["screener"])) <= 1.5:
        v, why = "RULE DIFFERENCE", "Screener moves the filing's exceptional item (%s) into Other Income; printed Other income is %s" % (exc, oi)
    if x["field"] in ("Sales", "Revenue") and rec.get("oi") and abs(rec["rev"] + rec["oi"] - x["screener"]) <= max(1.5, abs(x["screener"]) * 0.002):
        v, why = "RULE DIFFERENCE", "Screener counts the filing's Other income row (%s) inside Sales" % rec["oi"]
    out.append(dict(x, verdict=v, why=why, row=row, src=src or "prospectus"))
json.dump(out, open("scripts/ipo_fin_verdicts.json", "w", encoding="utf-8"), indent=0)
