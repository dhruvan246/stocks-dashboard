# -*- coding: utf-8 -*-
"""FILL-2018 rev track: classify EVERY target revC cell against the NSE filing index (§54b).

The 2019 campaign learned that one undifferentiated "open" bucket hides four completely different
situations, each pointing at a different rung of the §57 ladder. This tool splits the target set by
what the EXCHANGE'S OWN RECORD says, before any document is fetched:

  con-row-exists   E1 ok, a CONSOLIDATED row exists for that exact quarter  -> a REAL fetch gap;
                   the filing demonstrably exists, so a refusal here is ours, not the company's.
  no-con-ever-yet  E1 ok + E2 (no con row) + E3 (the quarter precedes the company's FIRST
                   consolidated filing ever)                                -> §51a structure:
                   NOT-APPLICABLE, recorded with evidence, NO VALUE WRITTEN. Consolidated
                   quarterlies only became compulsory from FY2020, and stored con PAT is null for
                   these, so copying revS into revC would CREATE a figure no document asserts.
  con-gap-after    E1 ok + E2 + the quarter is AFTER the first con filing -> the company does
                   consolidate but the index has no con row that quarter: unknown, needs a document.
  index-silent     E1 fails: the index lists no standalone row either, so its silence about
                   consolidated is not evidence of anything (§63/§57 — never infer absence from
                   our own gaps, or from a hole in someone else's index).

Anchorability (§64) is recorded per cell too, because an unanchored cell is not reachable by any
PAT-anchored reader and mixing the two pools makes a reader look like it is failing when it is
being handed impossible work.

Run:  python -X utf8 scripts/fill2020_tools/classify_rev2018.py [--write-na]
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
SCRIPTS = os.path.join(ROOT, "scripts")
LIST_CACHE = os.path.join(SCRIPTS, "_nselist")
TARGETS = os.path.join(HERE, "_rev2020_targets.json")
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
REVOP = os.path.join(ROOT, "docs", "sf_revop.json")
OUT = os.path.join(HERE, "_class_rev2018.json")
NA_OUT = os.path.join(SCRIPTS, "no_con_quarterly_2018.json")

MON = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

NA_DOC = (
    "MEASURED evidence that a company filed NO CONSOLIDATED QUARTERLY RESULT for a given 2018 "
    "quarter, from NSE per-company filing index caches (scripts/_nselist, harvested 2026-08-11). "
    "Per cell the three conditions of runbook 54b hold: E1 the index lists a STANDALONE result for "
    "that exact quarter (so its silence on consolidated is meaningful, not a hole in the index); "
    "E2 it lists NO consolidated result for that quarter; E3 the quarter is EARLIER than the "
    "company's FIRST consolidated filing ever (leading-run rule). This is 51a structure: quarterly "
    "consolidated results only became compulsory from FY2020, and 2018 sits a full year deeper into "
    "that era than the 2019 set. IMPORTANT - these are recorded as NOT-APPLICABLE EVIDENCE ONLY. No "
    "value is written: stored con PAT is null for every one of them, so E4 fails and copying revS "
    "into revC would CREATE a consolidated figure no document asserts (the never-invent rule). "
    "E6 (NEW, 2026-08-11) is what makes this set smaller and safer than the 2019 one: E1+E2+E3 only "
    "establishes that THIS index has no row. Measured over the 725 2018 target cells, 290 sit in a "
    "direct contradiction - the index reports no consolidated filing before 2019 while our own "
    "store holds a materially DIFFERENT consolidated figure in that same era (AXISBANK Jun-2018 "
    "con PAT 721.86 vs std 701.09, index first-con Jun-2019). For those companies the index's "
    "pre-first-con silence is measured-incomplete and proves nothing, so they are NOT recorded "
    "here. Only cells whose company shows no such contradiction survive. "
    "Wiring this into the gap definition needs a first-con-quarter concept in files_con across "
    "build_fill_coverage.py, audit_coverage.py and build_targets.py TOGETHER (the three-copies "
    "rule) - deliberately NOT done in this session because it moves the live Fill Coverage page "
    "numbers while other sessions are writing."
)


def iso_qe(s):
    m = re.match(r"(\d{2})-([A-Za-z]{3})-(\d{4})", (s or "").strip())
    if not m or m.group(2).title() not in MON:
        return None
    return int(m.group(3)) * 10000 + MON[m.group(2).title()] * 100 + int(m.group(1))


def index_of(sym):
    p = os.path.join(LIST_CACHE, re.sub(r"[^A-Z0-9]", "_", sym.upper()) + ".json")
    if not os.path.exists(p):
        return None
    std, con = set(), set()
    for r in json.load(open(p)):
        qe = iso_qe(r.get("toDate"))
        if qe:
            (con if r.get("consolidated") == "Consolidated" else std).add(qe)
    return std, con


def _con_evidence_before(sym, firstcon, fund, revop):
    """Quarters STRICTLY BEFORE `firstcon` where our OWN store holds a consolidated figure that is
    materially DIFFERENT from the standalone one. Each is a counter-example to reading the index's
    pre-first-con silence as proof of non-filing: a distinct consolidated number had to be read
    from some document, in the very era the index reports as empty.

    ⚠ TWO BOUNDS THAT BOTH MATTER, and getting either wrong changes the verdict:

    * STRICTLY BEFORE, not at-or-before. A company whose index first-con is Jun-2019 (the quarter
      consolidated quarterlies became compulsory, §51a) and whose Jun-2019 filing shows con != std
      has only demonstrated that it HAS subsidiaries — not that it filed a consolidated quarterly
      earlier. That is the textbook §51a case and E3 stands. Including the first-con quarter itself
      wrongly retracted 27 such cells (BAJAJCON, BEL, BHEL, CANBK, CONCOR, DMART...) on a first cut.

    * MATERIALLY DIFFERENT, not merely present. Earlier passes in this repo manufactured con PAT by
      COPYING std (§54b's circularity warning), so a stored con equal to std is not independent
      evidence of anything. Only a figure that DIFFERS proves a real consolidated statement was
      read."""
    out = []
    lim = firstcon if firstcon is not None else 99999999
    for qe, (ps, pc) in sorted(fund.get(sym, {}).items()):
        if qe >= lim or ps is None or pc is None:
            continue
        if abs(pc - ps) > max(0.05, 0.01 * abs(ps)):
            out.append("patC@%d %.2f vs std %.2f" % (qe, pc, ps))
    for q, row in sorted((revop.get(sym) or {}).items()):
        if int(q) >= lim or row[0] is None or row[1] is None:
            continue
        if abs(row[1] - row[0]) > 0.01 * max(abs(row[0]), 1e-9):
            out.append("revC@%s %.2f vs std %.2f" % (q, row[1], row[0]))
    return out


def main():
    targets = json.load(open(TARGETS))
    fund = {s: {int(r[0]): (r[1], r[3] if len(r) > 3 else None) for r in rows}
            for s, rows in json.load(open(FUND)).items()}
    revop = json.load(open(REVOP))

    cells, kinds, na = {}, Counter(), {}
    anch = Counter()
    for sym, v in sorted(targets.items()):
        idx = index_of(sym)
        for qe in sorted(v.get("revC", [])):
            ps, pc = fund.get(sym, {}).get(qe, (None, None))
            row = (revop.get(sym) or {}).get(str(qe)) or [None] * 9
            rec = {"anchored": pc is not None, "stored_con_pat": pc, "stored_std_pat": ps,
                   "stored_rev_std": row[0]}
            if idx is None:
                rec["kind"] = "no-cached-index"
            else:
                std, con = idx
                firstcon = min(con) if con else None
                rec["first_con_filing_ever"] = firstcon
                rec["nse_index_std_quarters"] = len(std)
                rec["nse_index_con_quarters"] = len(con)
                if qe in con:
                    rec["kind"] = "con-row-exists"
                elif qe not in std:
                    rec["kind"] = "index-silent"
                elif firstcon is None or qe < firstcon:
                    # E6 - INDEX CREDIBILITY. E1+E2+E3 only means "no row in THIS index". If our own
                    # store holds a materially DIFFERENT consolidated figure anywhere in the same
                    # pre-first-con era, the index's silence is measured-incomplete for this company
                    # and proves nothing (§57, §63). Measured 2026-08-11: 98 of the 725 target cells
                    # sit in exactly that contradiction (AXISBANK Jun-2018 con PAT 721.86 vs std
                    # 701.09, index first-con Jun-2019).
                    con_era = _con_evidence_before(sym, firstcon, fund, revop)
                    if pc is not None or con_era:
                        rec["kind"] = "no-con-row-but-con-evidence"
                        rec["con_evidence"] = con_era[:3]
                    else:
                        rec["kind"] = "no-con-ever-yet"
                else:
                    rec["kind"] = "con-gap-after"
            cells["%s|%d" % (sym, qe)] = rec
            kinds[rec["kind"]] += 1
            anch[(rec["kind"], "anchored" if rec["anchored"] else "unanchored")] += 1
            if rec["kind"] == "no-con-ever-yet":
                na["%s|%d" % (sym, qe)] = {
                    "first_con_filing_ever": rec["first_con_filing_ever"],
                    "nse_index_con_quarters": rec["nse_index_con_quarters"],
                    "nse_index_std_quarters": rec["nse_index_std_quarters"],
                    "stored_con_pat": pc,
                    "verdict": "no consolidated quarterly result filed for this quarter "
                               "(E1+E2+E3+E6: no stored con PAT, and no contradicting consolidated "
                               "figure anywhere in this company's pre-first-con era)",
                    "written": None}

    json.dump({"cells": cells}, open(OUT, "w"), indent=1, sort_keys=True)
    print("classified %d revC cells" % len(cells))
    for k, n in kinds.most_common():
        a = anch[(k, "anchored")]
        print("  %-16s %4d   (anchored %d / unanchored %d)" % (k, n, a, n - a))

    # E4 audit on the na set: a stored con PAT would mean the identity route, not na.
    with_pat = [k for k, v in na.items() if v["stored_con_pat"] is not None]
    print("\nna set: %d cells; of those WITH a stored con PAT: %d" % (len(na), len(with_pat)))
    if "--write-na" in sys.argv:
        json.dump({"_doc": NA_DOC, "cells": sorted(na.items())},
                  open(NA_OUT, "w"), indent=1)
        print("wrote %s (%d cells)" % (os.path.basename(NA_OUT), len(na)))


if __name__ == "__main__":
    main()
