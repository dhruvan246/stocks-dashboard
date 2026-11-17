# -*- coding: utf-8 -*-
"""ONE active correction per cell in fund_cell_fix.json / revop_cell_fix.json — guard.

THE BUG THIS FIXES (found 2026-11-17, runbook §240). Two sessions corrected the same cell in
opposite directions and BOTH entries stayed in `fixes`: BDL Mar-25 std had 272.77 -> 217.70
(22-Oct, from BSE's detailed-result data) AND 217.70 -> 272.77 (17-Nov, from the quarter's own PDF).
apply_fund_cell_fix.py is guarded on `was`, so in one pass it flips the cell to 217.70 and back, and
verify_fills_live.py reports the older entry as REVERTED forever. Measured that day: 12 cells in
fund_cell_fix.json carried two active entries with different `fixed` values (10 of them A->B + B->A).

RULE. A new correction for a cell that already has one must, IN THE SAME COMMIT, move the older
entry from "fixes" into "retracted" with a "retracted_why" naming the filing that settles it
(read that cell's own filing first — DATA_RUNBOOK §0). Every reader of these ledgers reads only
"fixes", so a retracted entry is inert everywhere without touching any reader.

Known, still-unread conflicts live in scripts/cell_fix_conflict_allow.json so the guard can fail
on anything NEW without blocking work over an old open question. Remove a key from it once the
cell is settled.

Run:  python3 scripts/cell_fix_conflict_guard.py           # report; exit 1 on a NEW conflict
"""
import collections
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGERS = ("fund_cell_fix.json", "revop_cell_fix.json")
ALLOW = os.path.join(HERE, "cell_fix_conflict_allow.json")
SCANNED = tuple("scripts/" + n for n in LEDGERS) + ("scripts/cell_fix_conflict_allow.json",)


def conflicts(name):
    fixes = json.load(open(os.path.join(HERE, name), encoding="utf-8")).get("fixes") or []
    g = collections.defaultdict(list)
    for i, f in enumerate(fixes):
        g["%s|%s|%s" % (f.get("sym"), f.get("qe"), f.get("basis"))].append((i, f.get("was"), f.get("fixed")))
    return {k: v for k, v in g.items() if len({x[2] for x in v}) > 1}


def check():
    allow = json.load(open(ALLOW, encoding="utf-8")) if os.path.exists(ALLOW) else {}
    ok, lines = True, []
    for name in LEDGERS:
        if not os.path.exists(os.path.join(HERE, name)):
            continue
        c = conflicts(name)
        known = set(allow.get(name) or [])
        for k, v in sorted(c.items()):
            if k not in known:
                ok = False
                lines.append("NEW  %s %s  entries (index, was, fixed): %s" % (name, k, v))
        for k in sorted(known - set(c)):
            lines.append("info %s %s is settled — remove it from cell_fix_conflict_allow.json" % (name, k))
        lines.append("%s: %d cell(s) with conflicting active entries, %d of them known/allowed"
                     % (name, len(c), len(set(c) & known)))
    return ok, lines


def main():
    ok, lines = check()
    for l in lines:
        print(l)
    if not ok:
        print("\nA cell has two ACTIVE corrections with different values. Read that cell's own filing,"
              "\nthen move the losing entry from \"fixes\" to \"retracted\" with a \"retracted_why\".")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
