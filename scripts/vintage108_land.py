# -*- coding: utf-8 -*-
"""Land §108 sweep proposals into the reviewed heal ledgers (append-only, deduped).

Adds `_vintage108_proposals.json` entries to `fund_cell_fix.json` (npStd) and
`revop_cell_fix.json` (rev/op/pat mirror). It does NOT touch the data files — the repo's own
`apply_fund_cell_fix.py` / `apply_revop_cell_fix.py` do that, guarded on `was`.

Refuses to add an entry whose (sym, qe, basis) is already in the ledger with a DIFFERENT `fixed`
value: that is two adjudications disagreeing, and it gets reported for a human, never overwritten.

RUN:  python3 scripts/vintage108_land.py            # report
      python3 scripts/vintage108_land.py --apply    # write the two ledgers
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROPS = os.path.join(HERE, "_vintage108_proposals.json")
FUND_LEDGER = os.path.join(HERE, "fund_cell_fix.json")
REVOP_LEDGER = os.path.join(HERE, "revop_cell_fix.json")


def merge(path, new, label, apply):
    d = json.load(open(path, encoding="utf-8"))
    have = {(f["sym"], str(f["qe"]), f["basis"]): f for f in d["fixes"]}
    added = dup = conflict = 0
    for f in new:
        k = (f["sym"], str(f["qe"]), f["basis"])
        old = have.get(k)
        if old is not None:
            if abs(float(old["fixed"]) - float(f["fixed"])) > 0.011:
                conflict += 1
                print("  CONFLICT %s %s %s: ledger says %s, sweep says %s — NOT overwritten"
                      % (k[0], k[1], k[2], old["fixed"], f["fixed"]))
            else:
                dup += 1
            continue
        d["fixes"].append({kk: vv for kk, vv in f.items() if not kk.startswith("_")})
        have[k] = f
        added += 1
    print("  [%s] add %d | already present %d | CONFLICT %d | ledger now %d"
          % (label, added, dup, conflict, len(d["fixes"])))
    if apply and added:
        json.dump(d, open(path, "w"), indent=1, ensure_ascii=False)
        print("  wrote %s" % os.path.basename(path))
    return added, conflict


def main():
    apply = "--apply" in sys.argv
    p = json.load(open(PROPS, encoding="utf-8"))
    print("proposals: %d npStd, %d revop" % (len(p["proposals"]), len(p["revop"])))
    a1, c1 = merge(FUND_LEDGER, p["proposals"], "fund_cell_fix", apply)
    a2, c2 = merge(REVOP_LEDGER, p["revop"], "revop_cell_fix", apply)
    if not apply:
        print("\n(dry run — pass --apply to write the ledgers)")
    if c1 or c2:
        print("\n%d CONFLICTS — resolve by hand before applying" % (c1 + c2))


if __name__ == "__main__":
    main()
