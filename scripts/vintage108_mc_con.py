# -*- coding: utf-8 -*-
"""A SECOND READER FOR THE CONSOLIDATED BASIS — Moneycontrol, with the §85 fallback guard.

BSE's detailed-results JSON is STANDALONE-ONLY (§42), so every consolidated finding in this
campaign has exactly one reader: NSE's archive. One reader is a reading, not a fact (§58), and the
§109d rule — evidence AND no available reader may contradict — needs a second voice before a con
cell is healed. Moneycontrol's deep feed is that voice.

⚠️ THE §85 DEFECT MAKES MC'S CON TABLE UNUSABLE WITHOUT A GUARD: for companies with no
consolidated filing, MC's consolidated table silently FALLS BACK to the standalone numbers, and
every ratio/scale gate passes on it. So the con reading is only accepted when it DIFFERS from the
same feed's standalone reading for the same quarter. Where they are identical the cell is reported
`mc-con-is-std-fallback` — an absence of evidence, never evidence of agreement.

VERDICTS per cell:
  mc-backs-nse-as-filed   MC agrees with NSE's earliest-filed vintage, not with the store -> heal
  mc-backs-store          MC agrees with the STORE -> do NOT heal; NSE's page needs re-reading
  mc-backs-neither        three readings, three answers -> adjudicate by hand
  mc-con-is-std-fallback / no-mc-id / no-mc-quarter   the route is absent, which is not a verdict

OUT: scripts/_vintage108_mccon.json
RUN: python3 scripts/vintage108_mc_con.py [--limit N] [--only SYM,SYM]
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "agg_tools"))
import agg_sources  # noqa: E402

NSE_CON = os.path.join(HERE, "_vintage108_nse_con.json")
PROPS = os.path.join(HERE, "_vintage108_proposals.json")
OUT = os.path.join(HERE, "_vintage108_mccon.json")
ABS_TOL, REL_TOL = 2.0, 0.03


def agree(a, b):
    return a is not None and b is not None and abs(a - b) <= max(ABS_TOL, abs(b) * REL_TOL)


def main():
    args = sys.argv[1:]
    limit = int(args[args.index("--limit") + 1]) if "--limit" in args else 10 ** 9
    only = set(args[args.index("--only") + 1].split(",")) if "--only" in args else None

    nse = json.load(open(NSE_CON, encoding="utf-8"))
    bp = json.load(open(PROPS, encoding="utf-8"))["byproduct"]
    out = json.load(open(OUT, encoding="utf-8")) if os.path.exists(OUT) else {}
    for k in [k for k, v in out.items() if v.get("verdict", "").startswith("transport")]:
        del out[k]

    targets = []
    for cls, items in bp.items():
        for x in items:
            sym, qe, basis = x.split("|")
            if basis != "con" or (only and sym not in only):
                continue
            k = "%s|%s" % (sym, qe)
            if k not in out and k in nse:
                targets.append((sym, int(qe), k, cls))
    targets = targets[:limit]
    print("consolidated cells needing a second reader: %d" % len(targets))

    cache, n = {}, 0
    for sym, qe, k, cls in targets:
        if sym not in cache:
            try:
                con, cnote = agg_sources.mc_quarters(sym, True)
            except Exception as ex:
                con, cnote = {}, "exception %s" % type(ex).__name__
            try:
                std, snote = agg_sources.mc_quarters(sym, False)
            except Exception:
                std, snote = {}, "exception"
            cache[sym] = (con, std, cnote, snote)
        con, std, cnote, snote = cache[sym]
        v = nse[k]
        rec = {"sym": sym, "qe": qe, "class": cls, "stored": v["stored"],
               "nse_as_filed": v.get("as_filed"), "mc_note": cnote}
        mc = (con.get(qe) or {}).get("pat_total")
        mcs = (std.get(qe) or {}).get("pat_total")
        rec["mc_con"], rec["mc_std"] = mc, mcs
        if mc is None:
            rec["verdict"] = "no-mc-quarter" if con else "no-mc-id"
        elif mcs is not None and abs(mc - mcs) < 0.011:
            rec["verdict"] = "mc-con-is-std-fallback"          # §85 — absence, not agreement
        else:
            a, st = v.get("as_filed"), v["stored"]
            if agree(mc, a) and not agree(mc, st):
                rec["verdict"] = "mc-backs-nse-as-filed"
            elif agree(mc, st) and not agree(mc, a):
                rec["verdict"] = "mc-backs-store"
            elif agree(mc, a) and agree(mc, st):
                rec["verdict"] = "mc-cannot-separate"
            else:
                rec["verdict"] = "mc-backs-neither"
        out[k] = rec
        n += 1
        if n % 25 == 0:
            json.dump(out, open(OUT, "w"), indent=1)
            print("  .. %d/%d" % (n, len(targets)))
    json.dump(out, open(OUT, "w"), indent=1)
    from collections import Counter
    print("done: %d cells" % n)
    for kk, c in Counter(x.get("verdict") for x in out.values()).most_common():
        print("   %-26s %d" % (kk, c))


if __name__ == "__main__":
    main()
