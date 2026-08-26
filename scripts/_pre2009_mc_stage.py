# -*- coding: utf-8 -*-
"""PRE-2009 std-revenue campaign — stage Moneycontrol deep-feed cells with an ERA-SPECIFIC
convention gate.  (scripts/PLAN_PRE2009_STDREV.md; runbook §81 aggregator route.)

WHY THIS EXISTS RATHER THAN _mc_batch_fill.py
---------------------------------------------
_mc_batch_fill.py votes on the revenue convention across the symbol's WHOLE history. That is
right for 2009+, and wrong here, because MC's pre-2009 payload is a different document:

  * MEASURED 2026-08-26 over all 322 gap symbols: the pre-2009 rows carry only
    "Net Sales/Income from operations" (rev_ops).  "Total Income From Operations" (rev_total,
    the Clause-41 label) appears only from ~2008-06.  So a symbol whose 2009+ convention votes
    `rev_total` finds `None` on every 2002-07 quarter -> silent zero yield.
  * Worse, MC's pre-2009 "Net Sales" is GROSS OF EXCISE DUTY for many manufacturers while our
    stored revStd is net: CENTENKA ratio 0.82-0.86, LINDEINDIA 0.90, RELIANCE 0.91, NATIONALUM
    0.92-0.94.  Store-wide, MC rev_ops reproduces our stored pre-2009 revStd on only 63.6% of
    the 4,430 overlapping cells.  A whole-history vote would have called those symbols "rev_ops"
    off their 2009+ agreement and then written the gross figure into a net-convention series.
    (runbook: feedback-aggregator-two-revenue-definitions)
  * MC's pre-2009 payload for BANKS carries NO revenue row at all -- only PAT and Depreciation
    (measured on FEDERALBNK, KTKBANK).  So the "Interest Earned" convention the plan expected is
    simply absent here; banks must go to the NSE archive, not to MC.

THE GATE (all four must hold before a cell is staged)
  1. convention established WITHIN 2002-2008 against the symbol's OWN stored revStd:
     >= 3 quarters agreeing and ZERO disagreeing, at max(0.5cr, 1%).
  2. MC carries that same field on the target quarter.
  3. MC's pat_total matches the stored sf_fundamentals npStd (previewed here, RE-CHECKED by
     _apply_reads.py at write time -- that is the gate that actually binds).
  4. the cell is in the campaign's measured gap (npStd present, revStd absent).

Run:  python -X utf8 scripts/_pre2009_mc_stage.py --gaps <gaps.json> --emit <emit.json> [--min-agree 3]
      then:  python -X utf8 scripts/_mc_add.py < <emit.json>  &&  python -X utf8 scripts/_apply_reads.py
"""
import os, sys, json, collections

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "agg_tools"))
import agg_sources as A

LO, HI = 20020101, 20090101


def close(a, b, ta=0.5, tp=0.01):
    if a is None or b is None:
        return False
    return abs(a - b) <= max(ta, tp * max(abs(a), abs(b)))


def anchor_ok(a, b):
    """Same tolerance _apply_reads.py uses, so the preview matches the binding gate."""
    if a is None or b is None:
        return False
    return abs(a - b) <= max(2.0, 0.03 * max(abs(a), abs(b)))


def main():
    av = sys.argv
    gaps_path = av[av.index("--gaps") + 1]
    out_path = av[av.index("--emit") + 1]
    min_agree = int(av[av.index("--min-agree") + 1]) if "--min-agree" in av else 3
    only = set(av[av.index("--only") + 1].split(",")) if "--only" in av else None

    gaps = json.load(open(gaps_path))
    rev = json.load(open(os.path.join(ROOT, "docs", "sf_revop.json")))
    fund = json.load(open(os.path.join(ROOT, "docs", "sf_fundamentals.json")))

    emit, report = {}, {"per_sym": {}}
    tally = collections.Counter()
    for sym in sorted(gaps):
        if only and sym not in only:
            continue
        q, note = A.mc_quarters(sym, con=False)
        srev = rev.get(sym) or {}
        fmap = {r[0]: r for r in fund.get(sym, [])}
        finflag = 0
        for r in srev.values():
            if len(r) > 6 and r[6]:
                finflag = 1
                break
        agree = collections.Counter()
        disagree = collections.Counter()
        for qe_s, rr in srev.items():
            qe = int(qe_s)
            if not (LO <= qe < HI):
                continue
            st, mc = rr[0], q.get(qe)
            if st is None or not mc:
                continue
            for f in ("rev_ops", "rev_total"):
                v = mc.get(f)
                if v is None:
                    continue
                (agree if close(st, v) else disagree)[f] += 1
        conv = next((f for f in ("rev_ops", "rev_total")
                     if agree[f] >= min_agree and disagree[f] == 0), None)
        staged = 0
        for qe in gaps[sym]:
            mc = q.get(qe)
            if not mc:
                tally["no_mc_quarter"] += 1
                continue
            if conv is None or mc.get(conv) is None:
                tally["no_conv_or_field"] += 1
                continue
            row = fmap.get(qe)
            sp = row[1] if row else None
            if not anchor_ok(sp, mc.get("pat_total")):
                tally["anchor_preview_fail"] += 1
                continue
            emit.setdefault(sym, {})[str(qe)] = {
                "basis": "std", "rev": mc[conv], "pat_seen": mc["pat_total"], "fin": finflag,
                "src": ("moneycontrol std %s=%s pat=%s (deep feed, as-filed; era-gated conv=%s, "
                        "agree=%d/disagree=0 within 2002-2008) [pre2009 2026-08-26]"
                        % (conv, mc[conv], mc["pat_total"], conv, agree[conv]))}
            staged += 1
            tally["staged"] += 1
        if staged or conv:
            report["per_sym"][sym] = {"conv": conv, "agree": dict(agree), "disagree": dict(disagree),
                                      "staged": staged, "gap": len(gaps[sym]), "note": note}
    json.dump(emit, open(out_path, "w"), indent=1, sort_keys=True)
    json.dump(report, open(os.path.join(HERE, "_pre2009_mc_report.json"), "w"), indent=1, sort_keys=True)
    print("staged %d cells across %d symbols -> %s" % (tally["staged"], len(emit), out_path))
    print("blocked: %s" % dict(tally))


if __name__ == "__main__":
    main()
