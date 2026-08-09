# -*- coding: utf-8 -*-
"""TRIPWIRE: consolidated slot holding a COPY of the standalone figure.

Why this exists, stated plainly. The user found TIMKEN 2025-09-30 storing con PAT 89.47 when the
real consolidated figure is 94 -- 89.47 is the STANDALONE value sitting in the con slot. Their
response was *"u must hv made such wrong entries for many stocks i feel"* and *"whenever i just find
a mistake of urs, u say ohh this is wrong"*. Both fair. The defect was found by a person spot-checking
one cell, not by any check of ours, and that is the failure this file fixes.

It is the SAME disease as the is_con_basis bug (§56) whose 1,656 fabricated cells were purged -- but
that purge only reached companies already listed in the hand-verified no_con_filing ledger. The
general case was never swept. On its first run this tripwire found 19 cells across 12 companies,
including three TIMKEN quarters where the user had spotted one.

THE TEST is decisive and needs no PDF:
    our stored patC == patS (exact copy)  AND  screener's con NP differs from its std NP
    -> our con slot is a copy of standalone, and the real consolidated figure exists.

A legitimate identity (no subsidiary, or an equity-method associate) shows con == std on SCREENER
TOO, so it is never flagged. The flag fires only when an independent source says the two differ.

LIMITS, stated so nobody mistakes a clean run for a clean dataset:
  * screener's quarterly table reaches ~13 quarters, so this covers roughly 2023+ only;
  * it catches EXACT copies -- a con value that is wrong but not equal to std slips through;
  * it only sees companies screener covers with both bases.
A zero here means "none of the copies this test can see", never "no copies".

  python -X utf8 scripts/detect_con_copy.py [--json OUT] [--limit N]
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import screener_fetch as SF                                       # noqa: E402

TOL_SAME = 0.0005          # ours con vs std: an exact copy
TOL_DIFF = 0.02            # screener con vs std: a MATERIAL difference


def main():
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 10 ** 9
    out_p = sys.argv[sys.argv.index("--json") + 1] if "--json" in sys.argv else None
    fund = json.load(open(os.path.join(ROOT, "docs", "sf_fundamentals.json")))
    try:
        ev = json.load(open(os.path.join(HERE, "con_filer_evidence.json")))
        syms = [s for s, v in ev.items() if v.get("files_con")]
    except Exception:
        syms = sorted(fund)

    bad, checked, n = [], 0, 0
    for sym in syms:
        if n >= limit:
            break
        rows = fund.get(sym) or []
        same = [r for r in rows if len(r) > 3 and r[1] is not None and r[3] is not None
                and abs(r[3] - r[1]) <= max(0.02, abs(r[1]) * TOL_SAME)]
        if not same:
            continue
        n += 1
        try:
            qc, qs = SF.quarters(sym, con=True), SF.quarters(sym, con=False)
        except Exception:
            continue
        if not qc or not qs:
            continue
        for r in same:
            dk = "%d-%02d-%02d" % (r[0] // 10000, (r[0] // 100) % 100, r[0] % 100)
            c = (qc.get(dk) or {}).get("Net Profit")
            s = (qs.get(dk) or {}).get("Net Profit")
            if c is None or s is None:
                continue
            checked += 1
            if abs(c - s) > max(1.0, abs(s) * TOL_DIFF):
                bad.append({"sym": sym, "qe": r[0], "our_patC": r[3], "our_patS": r[1],
                            "screener_con": c, "screener_std": s,
                            "why": "our con slot equals std, but screener reports con %s vs std %s"
                                   % (c, s)})

    print("companies scanned: %d | cells testable (ours con==std, screener has both): %d"
          % (n, checked))
    print("CON SLOT HOLDS STD: %d cells across %d companies\n"
          % (len(bad), len({b["sym"] for b in bad})))
    print("%-12s %-10s %11s %11s %11s" % ("sym", "quarter", "our patC", "scr con", "scr std"))
    for b in sorted(bad, key=lambda z: -abs(z["screener_con"] - z["screener_std"])):
        print("%-12s %-10d %11.2f %11.1f %11.1f"
              % (b["sym"], b["qe"], b["our_patC"], b["screener_con"], b["screener_std"]))
    print("\nLIMITS: screener's ~13-quarter window (roughly 2023+), EXACT copies only, and only "
          "companies screener covers on both bases. Zero here is not proof of a clean dataset.")
    if out_p:
        json.dump(bad, open(out_p, "w"), indent=1)
        print("-> %s" % out_p)


if __name__ == "__main__":
    main()
