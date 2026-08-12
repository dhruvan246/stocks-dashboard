# -*- coding: utf-8 -*-
"""Apply gated aggregator fills to the ledgers. FILL-ONLY, fast, and separate from the fetching.

Why it is its own script: several sessions push docs/sf_revop.json and scripts/revop_fundamentals.json
on the same day and both are single-line JSON, so a textual rebase conflict is certain. The write
therefore has to be ~1s -- fetch, gate and adjudicate happen in agg_sweep.py, this only replays the
proposal ledger (CLAUDE.md rule 4 / runbook §38).

Writes:
  docs/sf_revop.json            slot 0 = revS, slot 1 = revC
  scripts/revop_fundamentals.json   the same rows, the ledger the nightly rebuild reads
  scripts/agg_cell_fills.json   TRACKED provenance, one entry per cell: value, precision, the site,
                                the row LABEL it came from, how many of our stored quarters that
                                site's own series reproduced, and the worst anchor error.

An already-populated cell is NEVER overwritten (§6 fill-only) and a row that does not exist for
that quarter is skipped, not created -- a missing row means the quarter is not in the dataset's
frame and inventing one hides that.

  python3 -X utf8 scripts/agg_tools/apply_agg_fills.py --props <proposals.json>          # dry
  python3 -X utf8 scripts/agg_tools/apply_agg_fills.py --props <proposals.json> --apply
"""
import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
LEDGER = os.path.join(SCRIPTS, "agg_cell_fills.json")
# sf_revop cell layout: [revStd, revCon, opStd, opCon, patStd, patCon, fin, ebitStd, ebitCon]
SLOT = {"revS": 0, "revC": 1, "opS": 2, "opC": 3, "ebitS": 7, "ebitC": 8}
SITE_NAME = {"mc": "moneycontrol", "tl": "trendlyne", "tt": "tickertape",
             # screener.in comes through screener_opebit.py, which carries its own gate because
             # screener prints CRORE-ROUNDED integers where the other three print two decimals
             "sc": "screener.in"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--props", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--stamp", default=time.strftime("%Y-%m-%d"))
    a = ap.parse_args()

    props = json.load(open(a.props))["proposals"]
    journal, skipped, wrote = {}, [], 0

    for path in (os.path.join(ROOT, "docs", "sf_revop.json"),
                 os.path.join(SCRIPTS, "revop_fundamentals.json")):
        d = json.load(open(path))
        base = os.path.basename(path)
        n = 0
        for key in sorted(props):
            sym, qe, field = key.split("|")
            if field not in SLOT:
                skipped.append("%s: %s is not a sf_revop field" % (key, field))
                continue
            p = props[key]
            row = (d.get(sym) or {}).get(qe)
            if row is None:
                skipped.append("%s: no %s row in %s" % (key, qe, base))
                continue
            while len(row) < 9:
                row.append(None)
            i = SLOT[field]
            if row[i] is not None:
                skipped.append("%s: already = %s (%s)" % (key, row[i], base))
                continue
            row[i] = p["value"]
            d[sym][qe] = row
            n += 1
            ch = p["chosen"]
            # KEYED SYM|QE, not SYM|QE|FIELD: scripts/verify_fills_live.py (the blocking clobber
            # detector, runbook §41/§56b) rsplits a ledger key ONCE, so a three-part key makes it
            # read the FIELD as the quarter and silently check nothing -- a guard that looks wired
            # and guards air. Both fields of the same quarter merge into one entry.
            jkey = "%s|%s" % (sym, qe)
            journal.setdefault(jkey, {})
            journal[jkey].update({
                field: p["value"],
                "state": p["state"],
                "precision": ch["precision"],
                "src": "%s quarterly-results API (runbook §80)" % SITE_NAME.get(ch["site"],
                                                                               ch["site"]),
                "row_label": ch["row"],
                "evidence": ("gate A/A2 passed: that site's own %s series reproduces %d of our "
                             "stored quarters with zero disagreements, worst anchor error %.4f; "
                             "nearest anchor within 4 quarters" %
                             (field, ch["anchors"], ch["worst_anchor"])),
                "corroborated_by": [SITE_NAME.get(s, s) for s in p.get("corroborated_by", [])],
                "site_reach": p.get("sites", {}),
                "fy_check": p.get("fy_check"),
                "applied": "%s aggregator sweep" % a.stamp,
            })
        if a.apply:
            json.dump(d, open(path, "w"), separators=(",", ":"))
        print("%-32s %s %d cells" % (base, "filled" if a.apply else "would fill", n))
        wrote = max(wrote, n)

    for s in skipped[:40]:
        print("  skip: %s" % s)
    if len(skipped) > 40:
        print("  ... %d more skips" % (len(skipped) - 40))

    if a.apply and journal:
        led = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {}
        led.update(journal)
        json.dump(led, open(LEDGER, "w"), indent=1, sort_keys=True)
        print("journalled %d -> %s" % (len(journal), os.path.basename(LEDGER)))
    if not a.apply:
        print("DRY RUN -- nothing written.")
    return 0 if wrote or not props else 1


if __name__ == "__main__":
    sys.exit(main())
