# -*- coding: utf-8 -*-
"""Apply gated aggregator STANDALONE-PAT fills into docs/sf_fundamentals.json.

apply_agg_fills.py writes sf_revop (revS/revC slots 0/1) and refuses anything else; patS lives in
a different file with a different row shape, so it gets its own applier rather than a flag.

  row shape   [qEnd, npStd, annStd, npCon, annCon]   (build_fundamentals.py:14)
  we write    slot 1 only, and slot 2 when we create the row

WHY ROWS MAY BE CREATED HERE, when apply_agg_fills.py refuses to create them: in sf_revop a missing
row means the quarter is outside the dataset's frame. In sf_fundamentals the pre-2015 era IS the
frame -- 21,788 of its rows are pre-2015 and the twin scripts/fundamentals.json holds only 968 of
them, because that era was assembled by backfill campaigns, one appended row at a time
(scripts/apply_early_backfill.py is the same shape). A 2004 quarter with no row is a hole, not an
out-of-frame quarter.

⚠️ THE ANN-DATE SLOT IS A CONVENTION, NOT A MEASUREMENT. Moneycontrol's quarterly feed carries no
filing date. docs/backtest-engine.js:530 selects a quarter with `q[annIdx] <= dateInt`, so:
    ann = 0     -> "available since the epoch"  -> a look-ahead in every backtest
    ann = None  -> the cell is invisible to the engine, unlike its neighbours
    ann = qe+45d-> what this era's rows already say: MEASURED 2026-08-12, 20,818 of the 21,515
                   dated pre-2015 cells sit at exactly quarter-end + 45 days (runbook §52 records
                   the same default from the other side: "stored announce dates for old quarters
                   are not filing dates ... a quarter-end+45d default").
So qe+45d it is, and every ledger entry says `ann_basis: convention` in as many words. It is an
availability convention inherited from the surrounding rows, and no one should ever read it as the
date the company filed.

Safety, per §72 / the shape of scripts/_stdpat_apply.py:
  * FILL-ONLY -- a non-null npStd is never overwritten, only reported;
  * blast radius -- the patched file is diffed against the original and the run aborts unless the
    only differences are the intended cells;
  * idempotent -- a second run writes 0 (memory: feedback-a-heal-that-reapplies);
  * journal -- scripts/agg_pat_cell_fills.json, keyed SYM|QE (TWO parts: verify_fills_live.py
    rsplits a ledger key once, so a three-part key makes it read the field as the quarter and
    check nothing), registered in that script's LEDGERS at creation time.

  python3 -X utf8 scripts/agg_tools/apply_agg_pat_fills.py --props /tmp/final.json          # dry
  python3 -X utf8 scripts/agg_tools/apply_agg_pat_fills.py --props /tmp/final.json --apply
"""
import argparse
import copy
import datetime
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
LEDGER = os.path.join(SCRIPTS, "agg_pat_cell_fills.json")
SITE_NAME = {"mc": "moneycontrol", "tl": "trendlyne", "tt": "tickertape"}
SLOT = {"patS": 1, "patC": 3}
ANN_SLOT = {"patS": 2, "patC": 4}


def ann_default(qe):
    """quarter-end + 45 days, the convention this era's rows already carry (see module docstring)."""
    d = datetime.date(qe // 10000, (qe // 100) % 100, qe % 100) + datetime.timedelta(days=45)
    return d.year * 10000 + d.month * 100 + d.day


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--props", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--stamp", default=time.strftime("%Y-%m-%d"))
    a = ap.parse_args()

    props = json.load(open(a.props))["proposals"]
    orig = json.load(open(FUND))
    work = copy.deepcopy(orig)

    journal, skipped, created, filled = {}, [], 0, 0
    touched = set()                                  # (sym, qe) we intend to change

    for key in sorted(props):
        sym, qe, field = key.split("|")
        qe = int(qe)
        if field not in SLOT:
            skipped.append("%s: %s is not an sf_fundamentals field" % (key, field))
            continue
        p = props[key]
        arr = work.get(sym)
        if arr is None:
            skipped.append("%s: symbol absent from sf_fundamentals" % key)
            continue
        i, ai = SLOT[field], ANN_SLOT[field]
        row = next((r for r in arr if r and r[0] == qe), None)
        if row is None:
            row = [qe, None, None, None, None]
            arr.append(row)
            arr.sort(key=lambda r: r[0])
            created += 1
        while len(row) < 5:
            row.append(None)
        if row[i] is not None:
            skipped.append("%s: already = %s" % (key, row[i]))
            continue
        row[i] = p["value"]
        if row[ai] in (None, 0):
            row[ai] = ann_default(qe)
        filled += 1
        touched.add((sym, qe))

        ch = p["chosen"]
        jkey = "%s|%d" % (sym, qe)
        journal.setdefault(jkey, {})
        journal[jkey].update({
            field: p["value"],
            "state": p["state"],
            "precision": ch["precision"],
            "src": "%s quarterly-results API (runbook §81)" % SITE_NAME.get(ch["site"], ch["site"]),
            "row_label": ch["row"],
            "resolved_via": p.get("resolved_via", "symbol"),
            "ann_written": row[ai],
            "ann_basis": ("CONVENTION, not a filing date: quarter-end+45d, the default 20,818 of "
                          "the 21,515 dated pre-2015 cells already carry (runbook §52). "
                          "Moneycontrol's feed carries no filing date."),
            "evidence": ("gate A/A2 passed: that site's own %s series reproduces %d of our stored "
                         "quarters with zero disagreements in the local window, worst anchor error "
                         "%.4f; nearest anchor within 4 quarters" %
                         (field, ch["anchors"], ch["worst_anchor"])),
            "corroborated_by": [SITE_NAME.get(s, s) for s in p.get("corroborated_by", [])],
            "site_reach": p.get("sites", {}),
            "fy_check": p.get("fy_check"),
            "applied": "%s aggregator pre-2015 std-PAT sweep" % a.stamp,
        })

    # ---- BLAST RADIUS: nothing may differ except the cells we meant to touch
    diffs = []
    for sym in set(list(orig) + list(work)):
        o = {r[0]: r for r in orig.get(sym, [])}
        w = {r[0]: r for r in work.get(sym, [])}
        for qe in set(list(o) + list(w)):
            if o.get(qe) != w.get(qe) and (sym, qe) not in touched:
                diffs.append("%s %s: %s -> %s" % (sym, qe, o.get(qe), w.get(qe)))
    if diffs:
        print("ABORT -- %d unintended changes, e.g.:" % len(diffs))
        for d in diffs[:10]:
            print("   " + d)
        return 2

    print("%-32s %s %d cells (%d new rows created)"
          % ("docs/sf_fundamentals.json", "filled" if a.apply else "would fill", filled, created))
    for s in skipped[:30]:
        print("  skip: %s" % s)
    if len(skipped) > 30:
        print("  ... %d more skips" % (len(skipped) - 30))

    if a.apply and filled:
        tmp = FUND + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(work, fh, separators=(",", ":"))
        os.replace(tmp, FUND)
        led = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {}
        led.update(journal)
        with open(LEDGER, "w", encoding="utf-8") as fh:
            json.dump(led, fh, indent=1, sort_keys=True)
            fh.write("\n")
        print("journalled %d cells -> %s" % (len(journal), os.path.basename(LEDGER)))
    elif not a.apply:
        print("DRY RUN -- nothing written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
