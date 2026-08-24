#!/usr/bin/env python3
"""Apply scripts/revop_cell_fix.json — REVIEWED revenue value corrections — to the revop JSONs.

The revop analogue of apply_fund_cell_fix.py. Writes slot 0 (revStd) or slot 1 (revCon) of
docs/sf_revop.json AND the build ledger scripts/revop_fundamentals.json, so the next incremental
build_revop keeps the corrected value.

Guarded on `was`: idempotent, and refuses to overwrite a cell someone else has since moved (that
case is reported and left alone, never forced). Dry run by default.

Usage:  apply_revop_cell_fix.py            # report only
        apply_revop_cell_fix.py --apply    # write
"""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LEDGER = os.path.join(HERE, "revop_cell_fix.json")
TARGETS = [os.path.join(ROOT, "docs", "sf_revop.json"),
           os.path.join(HERE, "revop_fundamentals.json")]
SLOT = {"std": 0, "con": 1}
TOL = 0.01


def main():
    apply = "--apply" in sys.argv
    fixes = json.load(open(LEDGER))["fixes"]
    print("ledger: %d reviewed revenue corrections" % len(fixes))
    for path in TARGETS:
        if not os.path.exists(path):
            print("  (skip, absent) %s" % os.path.relpath(path, ROOT))
            continue
        d = json.load(open(path))
        rel = os.path.relpath(path, ROOT)
        towrite = already = absent = moved = 0
        for f in fixes:
            sym, qe, slot = f["sym"], str(f["qe"]), SLOT[f["basis"]]
            row = (d.get(sym) or {}).get(qe)
            if row is None or len(row) <= slot:
                absent += 1
                continue
            cur = row[slot]
            if cur is not None and abs(cur - f["fixed"]) <= TOL:
                already += 1
                continue
            if cur is None or abs(cur - f["was"]) > TOL:
                moved += 1
                print("  MOVED-ON %s %s %s: stored %s != was %s — left alone"
                      % (sym, qe, f["basis"], cur, f["was"]))
                continue
            print("  %s %s %s: %s -> %s" % (sym, qe, f["basis"], cur, f["fixed"]))
            if apply:
                row[slot] = f["fixed"]
                d[sym][qe] = row
            towrite += 1
        print("  [%s] to-write %d | already-correct %d | cell-absent %d | moved-on %d"
              % (rel, towrite, already, absent, moved))
        if apply and towrite:
            json.dump(d, open(path, "w"), separators=(",", ":"))
            print("  wrote %s" % rel)
    if not apply:
        print("\n(dry run — pass --apply to write)")


if __name__ == "__main__":
    main()
