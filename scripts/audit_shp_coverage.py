# -*- coding: utf-8 -*-
"""FII/DII (SHP) coverage vs POINT-IN-TIME Nifty-500 membership, 2002-12-31 -> latest closed quarter.

Survivorship-free: the denominator is who was IN the index on each quarter-end (nearest-prior
snapshot of indices_history "Nifty 500"), rename-normed, DUMMY* dropped — not today's members.
FII and DII coverage are always identical: parse_shp writes both slots or neither.

Cells listed in scripts/shp_no_filing.json (no filing was EVER made — merged/delisted mid-quarter)
leave the denominator entirely. Cells in scripts/_shp_bse_absent.json stay IN it: those are fetch
failures against one source, not proof the filing doesn't exist.

  python3 -X utf8 scripts/audit_shp_coverage.py                 # reads ORIGIN/MAIN (never the checkout)
  python3 -X utf8 scripts/audit_shp_coverage.py --local         # reads the working tree
  python3 -X utf8 scripts/audit_shp_coverage.py --csv out.csv   # per-quarter rows
  python3 -X utf8 scripts/audit_shp_coverage.py --missing 2019-09-30   # who is missing, from that QE on
"""
import os, sys, json, csv, argparse, subprocess, collections

HERE = os.path.dirname(os.path.abspath(__file__))
ERAS = [("Dec-2002..Jun-2010", "2002-12-31", "2010-06-30"),
        ("Sep-2010..Sep-2015", "2010-09-30", "2015-09-30"),
        ("Dec-2015..Mar-2016", "2015-12-31", "2016-03-31"),
        ("Jun-2016..Jun-2019", "2016-06-30", "2019-06-30"),
        ("Sep-2019..date",     "2019-09-30", "2099-12-31")]


def load(path, local):
    if local:
        return json.load(open(os.path.join(HERE, os.path.basename(path)), encoding="utf-8"))
    r = subprocess.run(["git", "show", "origin/main:" + path], capture_output=True, cwd=os.path.dirname(HERE))
    if r.returncode:
        sys.exit("git show failed for %s — fetch origin first" % path)
    return json.loads(r.stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", action="store_true")
    ap.add_argument("--csv", default="")
    ap.add_argument("--missing", default="", metavar="QE", help="list the missing cells from this quarter on")
    a = ap.parse_args()

    IH = load("scripts/indices_history.json", a.local)
    RMAP = load("scripts/_rename_map.json", a.local)
    SHP = load("scripts/shp_history.json", a.local)
    try:
        SKIP = load("scripts/shp_no_filing.json", a.local).get("cells", {})
    except SystemExit:
        SKIP = {}
    names = SHP.get("_names", {})

    def norm(s):
        s = str(s).strip().upper()
        seen = set()
        while s in RMAP and s not in seen and RMAP[s] != s:
            seen.add(s)
            s = RMAP[s]
        return s

    snaps = sorted((s["effectiveDate"], [norm(x) for x in s["symbols"]
                                         if not str(x).upper().startswith("DUMMY")])
                   for s in IH["Nifty 500"])

    def members(qe):
        best = []
        for ed, syms in snaps:
            if ed <= qe:
                best = syms
            else:
                break
        return best

    have = collections.defaultdict(dict)
    for k, v in SHP.items():
        if not k.startswith("_") and isinstance(v, dict):
            have[norm(k)].update(v)

    skip = {(norm(s), qe) for s, qs in SKIP.items() for qe in qs}
    last = max(q for qs in have.values() for q in qs)
    qes = ["%d%s" % (y, suf) for y in range(2002, int(last[:4]) + 1)
           for suf in ("-03-31", "-06-30", "-09-30", "-12-31")]
    qes = [q for q in qes if "2002-12-31" <= q <= last]

    rows, missing = [], []
    for qe in qes:
        mem = [s for s in members(qe) if (s, qe) not in skip]
        hit = [s for s in mem if qe in have.get(s, {})]
        rows.append((qe, len(mem), len(hit)))
        if a.missing and qe >= a.missing:
            missing += [(qe, s) for s in sorted(mem) if qe not in have.get(s, {})]

    print("point-in-time Nifty 500 x FII/DII, %s .. %s   (%d cells excluded as never-filed)"
          % (qes[0], qes[-1], len(skip)))
    print("\n%-22s %5s %12s %9s %7s" % ("era", "qtrs", "member-qtrs", "covered", "cov%"))
    for lbl, lo, hi in ERAS:
        sel = [r for r in rows if lo <= r[0] <= hi]
        if not sel:
            continue
        t, c = sum(r[1] for r in sel), sum(r[2] for r in sel)
        print("%-22s %5d %12d %9d %6.1f%%" % (lbl, len(sel), t, c, 100.0 * c / t if t else 0))
    t, c = sum(r[1] for r in rows), sum(r[2] for r in rows)
    print("%-22s %5d %12d %9d %6.1f%%" % ("WHOLE SAMPLE", len(rows), t, c, 100.0 * c / t))

    if missing:
        print("\nmissing cells from %s:" % a.missing)
        for qe, s in missing:
            print("  %s  %-12s %s" % (qe, s, (names.get(s) or "")[:44]))
        print("  by symbol:", ", ".join("%s(%d)" % kv for kv in
                                        collections.Counter(s for _, s in missing).most_common()))

    if a.csv:
        with open(a.csv, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["quarter_end", "n500_members_pit", "with_fii_dii", "coverage_pct"])
            for qe, n, h in rows:
                w.writerow([qe, n, h, round(100.0 * h / n, 1) if n else 0])
        print("\nwrote %s" % a.csv)


if __name__ == "__main__":
    main()
