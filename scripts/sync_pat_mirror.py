#!/usr/bin/env python3
"""§239b. Keep the PROFIT MIRROR in sf_revop equal to the profit store — without losing the second reading.

sf_fundamentals is the authority for net profit. sf_revop slots 4/5 hold a mirror of it that several older
writers filled from their own read of the filing (build_revop, fill_revop_from_xbrl, the SME PDF reader...).
The two drifted: 580 cells were found different on 2026-11-17, and reading each cell's own filing showed the
MAIN store was the wrong side in 163 of the first 262 judged. So the mirror's own figure is a useful second
reading and must not be thrown away by a blind copy.

For every (stock, quarter, basis) where both stores hold a profit and they differ by more than Rs 0.011 cr:
  1. the mirror's own figure is written to scripts/pat_mirror_disagree.json  (key SYM|qe|s or c:
     {main, reader, first_seen}) — unless a figure is already recorded for that cell and the mirror now holds
     nothing new, or the mirror only holds the profit store's own pre-heal figure (the recorded "main", or the
     "was" of a fund_cell_fix.json entry for that cell) — that is our old copy, not a second reading;
  2. the mirror cell is set to the profit store's figure (docs/sf_revop.json and scripts/revop_fundamentals.json).
A recorded cell CLOSES by itself when the profit store comes to equal the recorded second reading (the main store
was healed through fund_cell_fix.json) or loses its figure. When the filing shows the main store was right, accept
the cell in scripts/xcheck_accept.json under "fund_pat_mirror" — it stays recorded, and stops being listed.
The nightly check fund_pat_mirror (scripts/xcheck/fund.py) lists every open cell on the Data Checks page.

Never fills an empty mirror cell and never touches the profit store. Dry run by default.

Usage:  sync_pat_mirror.py            # report only
        sync_pat_mirror.py --apply    # write
"""
import datetime, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
TARGETS = [os.path.join(ROOT, "docs", "sf_revop.json"), os.path.join(HERE, "revop_fundamentals.json")]
SIDE = os.path.join(HERE, "pat_mirror_disagree.json")
BASES = (("s", 1, 4), ("c", 3, 5))      # (basis, sf_fundamentals profit index, sf_revop mirror slot)
TOL = 0.011


def load_side():
    if not os.path.exists(SIDE): return {}
    return json.load(open(SIDE, encoding="utf-8")).get("cells", {})


def main():
    apply = "--apply" in sys.argv
    today = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")
    fund = json.load(open(FUND, encoding="utf-8"))
    main_ = {}                                          # (sym, qe str, basis) -> profit store figure
    for sym, rows in fund.items():
        if not isinstance(rows, list): continue
        for r in rows:
            for b, fi, _ in BASES:
                if len(r) > fi and r[fi] is not None: main_[(sym, str(r[0]), b)] = r[fi]
    side = load_side(); side0 = json.dumps(side, sort_keys=True)
    # A mirror cell that only holds the profit store's OWN earlier figure is not a second reading: after a heal through
    # fund_cell_fix.json the mirror still carries the pre-heal figure we copied into it (§239c, 2026-11-17 — 29 healed
    # cells were re-recorded with the old wrong figure as their "second reading" and would never have closed).
    healed = {}
    fp = os.path.join(HERE, "fund_cell_fix.json")
    if os.path.exists(fp):
        for x in json.load(open(fp, encoding="utf-8")).get("fixes", []):
            try: healed.setdefault((x["sym"], str(x["qe"]), {"std": "s", "con": "c"}[x["basis"]]), []).append(float(x["was"]))
            except (KeyError, TypeError, ValueError): pass
    new = 0
    for ti, path in enumerate(TARGETS):
        if not os.path.exists(path):
            print("  (skip, absent) %s" % os.path.relpath(path, ROOT)); continue
        d = json.load(open(path, encoding="utf-8")); moved = 0
        for sym, qs in d.items():
            if not isinstance(qs, dict): continue
            for qe, row in qs.items():
                if not isinstance(row, list): continue
                for b, _, slot in BASES:
                    f = main_.get((sym, qe, b))
                    if f is None or len(row) <= slot or row[slot] is None: continue
                    m = row[slot]
                    if abs(f - m) <= TOL: continue
                    if ti == 0:                         # the published mirror is the one whose reading is recorded
                        k = "%s|%s|%s" % (sym, qe, b); e = side.get(k)
                        own_old = (e is not None and abs(e["main"] - m) <= TOL) or \
                            any(abs(w - m) <= TOL for w in healed.get((sym, qe, b), ()))
                        if not own_old and (e is None or abs(e["reader"] - m) > TOL):
                            side[k] = {"main": f, "reader": m, "first_seen": today}; new += 1
                    row[slot] = f; moved += 1
        print("  [%s] mirror cells set to the profit store: %d" % (os.path.relpath(path, ROOT), moved))
        if apply and moved:
            json.dump(d, open(path, "w", encoding="utf-8"), separators=(",", ":"))
    closed = 0
    for k in list(side):
        sym, qe, b = k.split("|"); f = main_.get((sym, qe, b))
        if f is None or abs(f - side[k]["reader"]) <= TOL: del side[k]; closed += 1
        else: side[k]["main"] = f
    print("  second readings recorded: %d new, %d closed (profit store now equals them), %d open"
          % (new, closed, len(side)))
    if apply and json.dumps(side, sort_keys=True) != side0:
        out = {"_doc": "sync_pat_mirror.py — the mirror's own profit figure where it differed from sf_fundamentals "
                       "before the copy. Read the cell's filing, then heal sf_fundamentals via fund_cell_fix.json "
                       "(the entry closes itself) or accept it in xcheck_accept.json under fund_pat_mirror.",
               "cells": {k: side[k] for k in sorted(side)}}
        json.dump(out, open(SIDE, "w", encoding="utf-8"), indent=0, ensure_ascii=False)
    if not apply: print("  dry run — nothing written (use --apply)")


if __name__ == "__main__":
    main()
