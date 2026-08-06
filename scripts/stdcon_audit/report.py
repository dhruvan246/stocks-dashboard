# -*- coding: utf-8 -*-
"""Verdict table + defect-rate estimate WITH its uncertainty, from _audit.json + _sample.json."""
import json, math, os, sys, collections

HERE = os.path.dirname(os.path.abspath(__file__))
A = json.load(open(os.path.join(HERE, "_audit.json")))
S = json.load(open(os.path.join(HERE, "_sample.json")))
SCREEN = json.load(open(os.path.join(HERE, "_screen.json")))
MAN = json.load(open(os.path.join(HERE, "_manual.json")))     # hand-confirmed verdicts
for _k, _v in MAN.items():
    A.setdefault(_k, {}).update(_v)
    A[_k]["src_manual"] = True
POP = sum(len(v["cells"]) for v in SCREEN.values())

RESOLVED = {"OK", "DEFECT", "OTHER-DEFECT"}
# N/A-ZERO cells (stored std == stored con == 0.00) cannot exhibit this defect either way, so they
# are excluded from the denominator rather than counted as a clean pass.
EXCLUDED = {"N/A-ZERO"}


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def main():
    strat = {(c["era"], c["sz"]): 0 for c in S}
    for c in S:
        strat[(c["era"], c["sz"])] += 1
    rows = []
    for c in S:
        k = "%s|%d" % (c["sym"], c["qe"])
        v = A.get(k, {"verdict": "NOT-RUN"})
        rows.append((c, v))
    print("%-12s %-9s %-7s %-6s %-11s %-11s %-11s %-22s %s" %
          ("SYM", "QE", "ERA", "SIZE", "STORED", "STD_SRC", "CON_SRC", "VERDICT", "SOURCE"))
    for c, v in sorted(rows, key=lambda t: (t[1]["verdict"], t[0]["sym"])):
        src = ",".join(sorted({x["src"] for x in v.get("std", []) + v.get("con", [])})) or "-"
        if v.get("src_manual"):
            src = (src + ",VISION" if src != "-" else "vision/manual")
        print("%-12s %-9d %-7s %-6s %-11s %-11s %-11s %-22s %s" %
              (c["sym"], c["qe"], c["era"], c["sz"],
               v.get("stored_std"), v.get("std_val"), v.get("con_val"),
               v["verdict"] + ("*" if v.get("needs_eyes") else ""), src))
    cnt = collections.Counter(v["verdict"] for _, v in rows)
    n = len(rows) - sum(cnt[x] for x in EXCLUDED)
    res = sum(cnt[x] for x in RESOLVED)
    def_ = cnt["DEFECT"]
    other = cnt["OTHER-DEFECT"]
    print("\n%-26s %d" % ("sample cells", n))
    for k, c in cnt.most_common():
        print("  %-24s %d" % (k, c))
    lo, hi = wilson(def_, res)
    print("\nDEFECT RATE among RESOLVED cells: %d/%d = %.1f%%  (95%% Wilson CI %.1f%%-%.1f%%)"
          % (def_, res, 100.0 * def_ / res if res else 0, 100 * lo, 100 * hi))
    blo, bhi = wilson(def_, n)[0], wilson(def_ + (n - res), n)[1]
    print("Worst/best-case bracket over ALL sampled cells (unresolved counted as OK, then as "
          "defects): %.1f%% .. %.1f%%" % (100 * blo, 100 * bhi))
    print("Screen population: %d cells / %d companies" % (POP, len(SCREEN)))
    print("Implied defective cells in the screen (point est. on resolved): %.0f  (95%% CI %.0f-%.0f)"
          % (POP * def_ / res if res else 0, POP * lo, POP * hi))
    if other:
        print("NOTE: %d cell(s) verdict OTHER-DEFECT -- stored std is wrong but is NOT the "
              "consolidated figure; a different fault." % other)


main()
