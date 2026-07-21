# -*- coding: utf-8 -*-
"""Post-backfill sanity sweep for sf_revop revenue fills. Nulls two error classes the anchor
can't catch on its own, logging every removal to _revgap_nulled.json:

  1. SCALE SPIKE  — a filled cell whose revenue is >4x the company's established (pre-existing,
     XBRL-reliable) max, when there is a solid reference (>=6 old cells, max>=20). Catches
     power-of-ten mis-reads (UNITDSPR 64223 vs ~7000).
  2. DUPLICATE    — the SAME revenue value (to the paisa) in two+ different quarters of one
     company. Real quarterly revenue essentially never repeats exactly; this is a comparative
     column mis-attributed to the wrong quarter (SIEMENS 3398.5 in both Jun-25 and Dec-25).
     Both offending cells are blanked (safer than keeping one wrong one).

Run:  python scripts/revop_sanity.py [--dry]   (compares docs/sf_revop.json vs git HEAD)
"""
import json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RV = os.path.join(ROOT, "docs", "sf_revop.json")
LED = os.path.join(HERE, "_revgap_nulled.json")


def revof(a):
    if not a:
        return None
    return a[0] if a[0] is not None else a[1]


def main():
    dry = "--dry" in sys.argv
    rv = json.load(open(RV))
    old = json.loads(subprocess.run(["git", "show", "HEAD:docs/sf_revop.json"],
                                    capture_output=True, text=True, cwd=ROOT).stdout or "{}")
    led = json.load(open(LED)) if os.path.exists(LED) else {}

    def wasold(s, qe):
        o = old.get(s, {}).get(str(qe))
        return bool(o) and (o[0] is not None or o[1] is not None)

    kill = []   # (sym, qe, val, reason)
    for s, d in rv.items():
        # established reference from pre-existing cells
        oldvals = [revof(old.get(s, {}).get(q)) for q in old.get(s, {})]
        oldvals = [v for v in oldvals if v is not None]
        ref_max = max(oldvals) if len(oldvals) >= 6 else None

        # value -> [quarters] for duplicate detection (only among NEW fills)
        seen = {}
        for qe, a in d.items():
            v = revof(a)
            if v is None:
                continue
            if not wasold(s, qe):
                # scale spike
                if ref_max is not None and ref_max >= 20 and v > 4 * ref_max:
                    kill.append((s, qe, v, "scale-spike>4x-established-max(%.0f)" % ref_max))
                    continue
                if v > 50:
                    seen.setdefault(round(v, 2), []).append(qe)
        for v, qs in seen.items():
            if len(qs) >= 2:                     # same paisa in >=2 quarters -> mis-attribution
                for qe in qs:
                    kill.append((s, qe, v, "duplicate-value-across-quarters(%s)" % ",".join(sorted(qs))))

    # de-dup kill list (a cell could hit both rules)
    seen_keys = set()
    kills = []
    for s, qe, v, why in kill:
        k = (s, qe)
        if k in seen_keys:
            continue
        seen_keys.add(k)
        kills.append((s, qe, v, why))

    print("sanity: %d cells to null (%d scale, %d duplicate)" % (
        len(kills),
        sum(1 for k in kills if k[3].startswith("scale")),
        sum(1 for k in kills if k[3].startswith("duplicate"))))
    for s, qe, v, why in sorted(kills):
        print("  %-12s %s  rev=%-10s  %s" % (s, qe, v, why))

    if not dry and kills:
        for s, qe, v, why in kills:
            a = rv[s][qe]
            a[0] = a[1] = a[2] = a[3] = a[7] = a[8] = None
            rv[s][qe] = a
            led["%s|%s" % (s, qe)] = {"nulled_rev": v, "reason": why}
        json.dump(rv, open(RV, "w"), separators=(",", ":"))
        json.dump(led, open(LED, "w"), indent=0)
        print("nulled %d cells; ledger now %d entries" % (len(kills), len(led)))


if __name__ == "__main__":
    main()
