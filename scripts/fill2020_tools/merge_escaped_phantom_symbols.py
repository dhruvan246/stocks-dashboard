# -*- coding: utf-8 -*-
"""HTML-ESCAPED PHANTOM SYMBOLS — recover the data we already own but never serve  (2026-08-11)

FOUND WHILE FIXING SOMETHING ELSE. The Moneycontrol resolver was writing off every symbol containing
an ampersand, and sizing that turned up 13 symbols in our own store that no exchange ever listed:

    M&AMP;M   M&AMP;MFIN   J&AMP;KBANK   IL&AMP;FSENGG   S&AMP;SPOWER   SURANAT&AMP;P
    GET&AMP;D   GVT&AMP;D   GMRP&AMP;UI   ARE&AMP;M   COX&AMP;KINGS   IL&AMP;FSTRANS   L&AMP;TFH

An `&` was HTML-escaped to `&amp;` somewhere in ingestion and then upper-cased with the ticker, so
`M&M` became `M&AMP;M` and got its own key. They are invisible to the site — absent from
quarterly_results.json and from the search index — yet they hold 271 revop quarters and 74
fundamentals rows between them. Data we already have, that nothing reads.

★ THE PHANTOM IS PROVED TO BE THE SAME COMPANY BY ITS OWN OVERLAP, not by the name resembling one.
Every slot of the revop row is compared, [revS, revC, opS, opC, patS, patC, fin, ebitS, ebitC], plus
both PAT slots of sf_fundamentals. Measured across all 13 phantoms: 798 overlapping values agree with
the clean symbol, 62 disagree, and 766 sit where the clean symbol's slot is EMPTY.

Per-phantom gate before any of its unique values are trusted — the same shape as every other route
here, because "the name looks right" is exactly the reasoning that produces a wrong-company merge:
    >= 3 agreeing overlaps  AND  disagreement rate < 15%
Measured verdicts, and note the gate costs us the two RICHEST phantoms rather than the poorest, which
is the point of having it:
    REFUSED  S&AMP;SPOWER   49 agree / 11 disagree (18.3%)  — 230 values left where they are
    REFUSED  IL&AMP;FSENGG  44 agree /  8 disagree (15.4%)  — 210 values left where they are
    REFUSED  COX&AMP;KINGS  13 agree /  7 disagree (35.0%)  —  57 values left where they are
    MERGED   the other ten, J&AMP;KBANK cleanest at 135 agree / 0 disagree
Result: 269 values merged, 497 refused.

★★ THE TARGET FOLLOWS A RENAME. GET&D and L&TFH have no revop rows of their own at all — they were
renamed to GVT&D and LTF. Unescaping alone would write into a dead key, so the target is
_rename_map.json's successor when one exists.

FILL-ONLY, ALWAYS. A disagreement is never resolved by overwriting: the 5 known ones (COX&KINGS
2019-06 430.16 vs 438.89, GET&D 2018-03 813.93 vs 760.11, M&M 2024-06 and 2024-09 consolidated) are
left exactly as they are and recorded. The phantom keys themselves are NOT deleted — that is a
separate decision with its own blast radius, and this script only moves values that no consumer can
currently see into keys they can.

Run: python -X utf8 scripts/fill2020_tools/merge_escaped_phantom_symbols.py [--apply]
"""
import html
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)

REVOP = os.path.join(ROOT, "docs", "sf_revop.json")
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
LEDGER = os.path.join(SCRIPTS, "revop_fundamentals.json")
RENAMES = os.path.join(SCRIPTS, "_rename_map.json")
FILLS = os.path.join(SCRIPTS, "phantom_symbol_merge.json")

MIN_AGREE = 3
MAX_DIS_RATE = 0.15
PAT_SLOT = {1: "std", 3: "con"}


def close(a, b):
    return abs(a - b) <= max(0.05, 0.002 * max(abs(a), abs(b)))


def main():
    apply_it = "--apply" in sys.argv
    revop = json.load(open(REVOP))
    fund = json.load(open(FUND))
    ledger = json.load(open(LEDGER))
    renames = json.load(open(RENAMES)) if os.path.exists(RENAMES) else {}
    fmap = {s: {r[0]: r for r in rows} for s, rows in fund.items()}
    out = {}

    phantoms = sorted({s for s in list(revop) + list(fund) if "&AMP;" in s.upper()})
    print("phantom symbols: %d" % len(phantoms))
    applied = refused = 0
    for ph in phantoms:
        clean = html.unescape(ph.replace("&AMP;", "&"))
        target = renames.get(clean) or clean
        agree = dis = 0
        cand = []
        # revenue/operating profit rows
        for q, r in (revop.get(ph) or {}).items():
            trow = (revop.get(target) or {}).get(q)
            for slot in range(len(r)):
                pv = r[slot]
                if pv is None:
                    continue
                tv = trow[slot] if trow and len(trow) > slot else None
                if tv is None:
                    cand.append(("revop", q, slot, pv))
                elif close(tv, pv):
                    agree += 1
                else:
                    dis += 1
        # fundamentals PAT slots
        for r in fund.get(ph, []):
            trow = (fmap.get(target) or {}).get(r[0])
            for slot in (1, 3):
                pv = r[slot] if len(r) > slot else None
                if pv is None:
                    continue
                tv = trow[slot] if trow and len(trow) > slot else None
                if tv is None:
                    cand.append(("fund", r[0], slot, pv))
                elif close(tv, pv):
                    agree += 1
                else:
                    dis += 1
        rate = dis / float(agree + dis) if (agree + dis) else 1.0
        ok = agree >= MIN_AGREE and rate < MAX_DIS_RATE
        print("  %-16s -> %-12s agree %3d  disagree %2d (%4.1f%%)  recoverable %3d   %s"
              % (ph, target, agree, dis, 100 * rate, len(cand), "MERGE" if ok else "REFUSED"))
        if not ok:
            refused += len(cand)
            out["%s|REFUSED" % ph] = {
                "target": target, "agree": agree, "disagree": dis,
                "why": ("phantom-to-target agreement too weak to trust its unique values: %d "
                        "agreements, %d disagreements (%.1f%%). Its %d otherwise-recoverable values "
                        "stay where they are." % (agree, dis, 100 * rate, len(cand)))}
            continue
        for kind, q, slot, pv in cand:
            # ★ AN EXACT 0 IS NOT A MEASUREMENT HERE. The revop row is
            # [revS, revC, opS, opC, patS, patC, fin, ebitS, ebitC] and slot 6 (finance cost) is
            # written 0 by the builder for rows where the figure was never present — propagating it
            # into an EMPTY target slot would assert "zero finance cost" where the truth is unknown
            # (memory: feedback-zero-is-a-no-base-sentinel). Skipped rather than merged.
            if pv == 0:
                out["%s|%s|%s|%d|SKIPPED-ZERO" % (target, kind, q, slot)] = {
                    "from_phantom": ph,
                    "why": "exact 0 may be the builder's not-present sentinel, not a measured zero"}
                continue
            if kind == "revop":
                row = (revop.get(target) or {}).get(q)
                if row is None or len(row) <= slot or row[slot] is not None:
                    continue
                if apply_it:
                    row[slot] = pv
                    lr = ledger.setdefault(target, {}).get(q)
                    if lr is None:
                        ledger[target][q] = list(row)
                    elif len(lr) > slot and lr[slot] is None:
                        lr[slot] = pv
            else:
                row = (fmap.get(target) or {}).get(q)
                if row is None or len(row) <= slot or row[slot] is not None:
                    continue
                if apply_it:
                    row[slot] = pv
            out["%s|%s|%s|%d" % (target, kind, q, slot)] = {
                "value": pv, "from_phantom": ph,
                "gate": ("%d values agree with %s on overlapping quarters, %d disagree (%.1f%%) — "
                         "same company, and this slot was empty" % (agree, target, dis, 100 * rate))}
            applied += 1

    print("\nrecoverable and merged: %d | refused on weak agreement: %d" % (applied, refused))
    if not apply_it:
        print("(dry run — re-run with --apply)")
        return
    json.dump(revop, open(REVOP, "w"), separators=(",", ":"))
    json.dump(fund, open(FUND, "w"), separators=(",", ":"))
    json.dump(ledger, open(LEDGER, "w"), separators=(",", ":"))
    json.dump(out, open(FILLS, "w"), indent=1, sort_keys=True)
    print("APPLIED %d values into keys the site can actually read" % applied)


if __name__ == "__main__":
    main()
