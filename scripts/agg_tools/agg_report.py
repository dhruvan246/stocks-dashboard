# -*- coding: utf-8 -*-
"""Emit the honest per-cell report for an aggregator sweep.

Terminal states are runbook §61b's, and the wording matters: a cell nobody could reach is
`not-found-via:<sites>`, never "unfillable" (§0 / §57a). Anything the gate refused says WHICH gate
refused it, because "my reader found nothing" is a statement about the reader.

  python3 -X utf8 scripts/agg_tools/agg_report.py --props P.json --final F.json --md OUT.md
"""
import argparse
import collections
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))


def disp(key):
    """SYM|QE|field is unreadable inside a markdown table -- the pipes close the cell."""
    return key.replace("|", " ")


def gate_of(rep):
    """Which gate refused this cell, across the sites that HAD the quarter."""
    reasons = []
    for site, v in rep["sites"].items():
        for rej in v.get("rejected", []):
            m = re.search(r"(GATE-A\d?)", rej)
            if m:
                reasons.append("%s:%s" % (site, m.group(1)))
    return sorted(set(reasons))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--props", required=True)
    ap.add_argument("--final", required=True)
    ap.add_argument("--md", required=True)
    a = ap.parse_args()
    raw = json.load(open(a.props))
    fin = json.load(open(a.final))
    reps, kept, fys = raw["reports"], fin["proposals"], fin.get("fy_checks", {})

    L = []
    L.append("# Aggregator route — per-cell outcome (Moneycontrol / Trendlyne / Tickertape)\n")
    L.append("Generated %s. Sites tried per cell: %s. Terminal states are runbook §61b; a cell "
             "nobody reached is `not-found-via:<sites>`, never \"unfillable\" (§0/§57a).\n"
             % (raw["generated"], ", ".join(raw["sites"])))

    L.append("\n## FILLED (%d)\n" % len(kept))
    L.append("| cell | value | precision | site | row | local/total anchors | worst anchor | "
             "site FY identity (prev/target/next) | our FY identity |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for k in sorted(kept):
        p = kept[k]
        c = p["chosen"]
        f = fys.get(k, {})
        a5 = f.get("A5", {})
        L.append("| %s | %.2f | %s | %s | %s | %d/%d | %.2f | %s/%s/%s | %s |" % (
            disp(k), p["value"], c["precision"], c["site"], c["row"], c["local_anchors"],
            c["anchors"], c["worst_anchor"],
            a5.get("prev", {}).get("verdict", "-"), a5.get("target", {}).get("verdict", "-"),
            a5.get("next", {}).get("verdict", "-"),
            f.get("our_fy_identity", {}).get("verdict", "-")))

    vetoed = {k: v for k, v in fys.items() if v["state"] != "PASS"}
    L.append("\n## GATED OUT after passing the quarterly gate — restated financial year (%d)\n"
             % len(vetoed))
    L.append("These are NOT absences. The quarterly series matched ours on 27-40 anchors; the "
             "site's own four quarters then failed to sum to its own annual for the target FY or "
             "a neighbour, which is the §60d restatement signature. State: `NEEDS-CROSSCHECK` — "
             "reachable from a filing read, not from this route.\n")
    L.append("| cell | which FY is restated | site ΣQ | site annual | diff |")
    L.append("|---|---|---|---|---|")
    for k in sorted(vetoed):
        for tag, det in sorted(vetoed[k]["A5"].items()):
            if det.get("verdict") == "RESTATED":
                L.append("| %s | %s FY%d | %.2f | %.2f | %+.2f |" % (
                    disp(k), tag, det["fy"], det["site_sum4Q"], det["site_annual"], det["diff"]))

    others = collections.defaultdict(list)
    for k, r in sorted(reps.items()):
        if k in kept or k in vetoed:
            continue
        if r["state"] == "REJECT-EQUALS-OTHER-BASIS":
            others["REJECT-EQUALS-OTHER-BASIS (gate C — the copied-con fingerprint; belongs to the "
                   "§6A no-sub identity route, which writes it WITH evidence)"].append((k, r))
        elif gate_of(r):
            others["NEEDS-CROSSCHECK (a site HAD the quarter; its series does not reproduce ours — "
                   "%s)" % "/".join(sorted({g.split(":")[1] for g in gate_of(r)}))].append((k, r))
        else:
            others["not-found-via:%s (no site holds this quarter for this basis)"
                   % ",".join(raw["sites"])].append((k, r))
    for bucket in sorted(others):
        rows = others[bucket]
        L.append("\n## %s — %d cells\n" % (bucket, len(rows)))
        L.append("| cell | what each site said |")
        L.append("|---|---|")
        for k, r in rows:
            bits = []
            for s, v in sorted(r["sites"].items()):
                t = v.get("verdict") or "; ".join(v.get("rejected", [])) or v.get("note", "")
                bits.append("**%s** %s" % (s, t.replace("|", "/")[:150]))
            L.append("| %s | %s |" % (disp(k), "<br>".join(bits)))

    sus = collections.OrderedDict()
    for s in raw.get("suspects", []):
        key = (s["sym"], s["qe"], s["field"])
        sus.setdefault(key, []).append(s)
    L.append("\n## SUSPECT cells of OURS surfaced in passing (%d) — reported, NOT patched\n"
             % len(sus))
    L.append("§61a mode 6: a site reproduces our series everywhere except here. The indictment is "
             "against us. Correcting a stored value is the §2b procedure with its own evidence, "
             "never a side effect of a fill pass (§58d).\n")
    L.append("| cell | ours | site value(s) | series agreement |")
    L.append("|---|---|---|---|")
    for (sym, qe, field), rows in sorted(sus.items()):
        L.append("| %s %d %s | %s | %s | %s |" % (
            sym, qe, field, rows[0]["ours"],
            ", ".join("%s=%s" % (r["site"], r["site_val"]) for r in rows),
            ", ".join("%s %s" % (r["site"], r["series_agreement"]) for r in rows)))

    open(a.md, "w", encoding="utf-8").write("\n".join(L) + "\n")
    print("wrote %s  (filled %d, restated-veto %d, suspects %d)"
          % (a.md, len(kept), len(vetoed), len(sus)))


if __name__ == "__main__":
    main()
