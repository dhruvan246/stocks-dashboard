# -*- coding: utf-8 -*-
"""Detect (and optionally repair) backfilled cells that went missing from the SERVED payloads.

WHY THIS EXISTS. On 2026-08-06 a backfill pushed 193 consolidated-revenue cells; ten minutes later
a CI refresh restored its own pre-run snapshot over docs/sf_revop.json and silently reverted every
one of them. The values were never lost -- scripts/revop_fundamentals.json (the ledger) still held
them, because CI does not commit the scripts/ mirrors -- but the site served nulls, and the push
had been reported as successful because it WAS verified against origin at push time.

Two defences came out of that. The cause is fixed in scripts/ci_preserve_merge.py (CI now does a
three-way merge instead of a blind copy). This is the detector: it re-checks, against whatever the
payloads currently say, that every cell any fill ledger claims is still there. Run it after a push
and again once a refresh cycle has passed -- "verified at push time" is NOT verified (CLAUDE.md
rule 5, runbook §41).

Compares each ledger's claimed cells against docs/sf_revop.json and docs/sf_fundamentals.json:
  MISSING  = ledger has a value, the served payload has None      -> a clobber; --repair restores it
  DRIFT    = both present but different                           -> reported, NEVER auto-changed,
             because a later correction legitimately supersedes a backfill and only a human can say

Run:  python3 scripts/verify_fills_live.py [--repair] [--quiet]
Exit code 1 when anything is MISSING (so CI or a wrapper can fail loudly), else 0.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REVOP = os.path.join(ROOT, "docs", "sf_revop.json")
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")

# ledger -> (payload, value-key in the ledger entry, slot/field)
#   revop slots: 0 revS, 1 revC   |   fundamentals row idx: 1 stdPAT, 3 conPAT
LEDGERS = [
    ("nosub_rev_pre2020_fills.json", "revop", "revC", 1),
    ("con_rev_nse_reads.json",       "revop", "revC", 1),
    ("std_rev_detres_fills.json",    "revop", "revS", 0),
    ("std_rev_nse_reads.json",       "revop", "revS", 0),
    ("nosub_rev_fills.json",         "revop", "revC", 1),
    ("nosub_pat_fills.json",         "fund",  "con",  3),
    ("std_pat_detres_fills.json",    "fund",  "std",  1),
    ("con_pat_nse_reads.json",       "fund",  "con",  3),
    ("con_pat_fy_derived.json",      "fund",  "con",  3),
]
# NESTED correction ledgers: {SYM: {QE: {...}}} rather than the flat "SYM|QE" shape above.
# These carry DEFECT CORRECTIONS (a value we proved wrong and replaced), so a clobber here does not
# merely lose a backfill -- it silently restores a number a filing already refuted. The basis is
# read per entry where the ledger records one.
#   (file, payload, value-key, default slot, basis-key -> {basis: slot})
NESTED = [
    ("pat_defects.json", "fund",  "correct_pat",     1, None, None),
    ("pat_defects.json", "fund",  "correct_pat_con", 3, None, None),
    ("rev_defects.json", "revop", "correct_rev",     0, "basis", {"std": 0, "con": 1}),
]
TOL = 0.011


def main():
    repair = "--repair" in sys.argv
    quiet = "--quiet" in sys.argv
    revop = json.load(open(REVOP))
    fund = json.load(open(FUND))
    fmap = {s: {r[0]: r for r in rows} for s, rows in fund.items()}

    missing, drift, checked = [], [], 0
    for name, payload, key, slot in LEDGERS:
        p = os.path.join(HERE, name)
        if not os.path.exists(p):
            continue
        try:
            led = json.load(open(p))
        except Exception:
            continue
        for k, v in led.items():
            if not isinstance(v, dict) or v.get("skip") or key not in v or v[key] is None:
                continue
            if "|" not in k:
                continue
            sym, qe = k.rsplit("|", 1)
            want = v[key]
            checked += 1
            if payload == "revop":
                row = (revop.get(sym) or {}).get(qe)
                cur = row[slot] if row and len(row) > slot else None
            else:
                row = (fmap.get(sym) or {}).get(int(qe))
                cur = row[slot] if row and len(row) > slot else None
            if cur is None:
                missing.append((name, sym, qe, want, payload, slot))
            elif abs(cur - want) > TOL:
                drift.append((name, sym, qe, want, cur))

    for name, payload, key, dslot, bkey, bmap in NESTED:
        p2 = os.path.join(HERE, name)
        if not os.path.exists(p2):
            continue
        try:
            led = json.load(open(p2))
        except Exception:
            continue
        for sym, qd in led.items():
            if not isinstance(qd, dict):
                continue
            for qe, v in qd.items():
                if not isinstance(v, dict) or v.get("skip"):
                    continue
                want = v.get(key)
                if want is None:                 # a deliberate null verdict is not a claim
                    continue
                slot = dslot
                if bkey and bmap:
                    slot = bmap.get(str(v.get(bkey, "")).lower(), dslot)
                checked += 1
                if payload == "revop":
                    row = (revop.get(sym) or {}).get(str(qe))
                    cur = row[slot] if row and len(row) > slot else None
                else:
                    row = (fmap.get(sym) or {}).get(int(qe))
                    cur = row[slot] if row and len(row) > slot else None
                if cur is None:
                    missing.append((name, sym, str(qe), want, payload, slot))
                elif abs(cur - want) > TOL:
                    drift.append((name, sym, str(qe), want, cur))

    if not quiet:
        print("checked %d ledgered cells against the served payloads" % checked)
        print("  MISSING (clobbered): %d" % len(missing))
        print("  DRIFT   (superseded/corrected): %d" % len(drift))
        for m in missing[:15]:
            print("     MISSING %-30s %-12s %s  ledger=%s" % (m[0], m[1], m[2], m[3]))
        for d in drift[:10]:
            print("     DRIFT   %-30s %-12s %s  ledger=%s live=%s" % d)

    if missing and repair:
        for name, sym, qe, want, payload, slot in missing:
            if payload == "revop":
                row = (revop.get(sym) or {}).get(qe)
                if not row:
                    continue
                while len(row) <= slot:
                    row.append(None)
                row[slot] = want
                revop[sym][qe] = row
            else:
                row = (fmap.get(sym) or {}).get(int(qe))
                if not row:
                    continue
                while len(row) <= slot:
                    row.append(None)
                row[slot] = want
        json.dump(revop, open(REVOP, "w"), separators=(",", ":"))
        json.dump(fund, open(FUND, "w"), separators=(",", ":"))
        print("repaired %d cells into the served payloads "
              "(commit + push them, then re-run to confirm)" % len(missing))
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
