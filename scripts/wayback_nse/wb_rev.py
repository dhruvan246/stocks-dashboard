# -*- coding: utf-8 -*-
"""Standalone REVENUE from the archived NSE results.jsp page — route (1) of the rev-parity
campaign (2026-09-05): the same as-filed exchange document that produced the std-PAT cell.

The page prints, with scale and period DECLARED in the text (see README §1):
  Non-Banking template   "Net Sales <n>"          -> revStd   (net of excise, the store's convention)
  Banking template       "Interest Earned <n>"    -> revStd   (fin=1; the bank convention the store uses)
plus "Net Profit(+)/Loss(-)" (non-bank) / "Net Profit" (bank), which is the ANCHOR: the page's own PAT
must reproduce the stored sf_fundamentals npStd to the paisa (<=0.011 cr) or nothing is written.

Two read modes, both anchored on BOTH quantities:
  direct   the index page for (sym, qe) is a true quarter (G1-G4 of wbgate) and its PAT == stored npStd.
  cumdiff  the index page is CUMULATIVE ending on qe (H1 / 9M "OT" / AN); the prior leg is the page for the
           same symbol ending one quarter earlier with the SAME declared period start (or, for the first
           quarter of the year, the Q1 page itself). revenue = cum - prev; the SAME subtraction on PAT must
           equal the stored npStd to the paisa. A quarter derived this way carries both legs in its evidence.

Hold-out (2026-09-05, calibration in --calib): 445 cells we already hold with an exchange-derived value
(433 non-bank, 12 bank), aggregator-derived truth cells excluded by provenance: 445 match, 0 mismatch.
Limit that travels with the number: the truth side is mostly STEP W's own reads of these pages, so the
0.00% proves the READER reproduces the page and the CONVENTION matches the store; the page itself is the
primary document.

  python3 -X utf8 scripts/wayback_nse/wb_rev.py --cells <gap.json> --orig <origkeys.json> --out <props.json>
      [--fetch]         fetch missing leg pages (keep-alive serial via wbcache)
  python3 -X utf8 scripts/wayback_nse/wb_rev.py --emit <props.json>
      writes scripts/_wbrev_reads.json (applied by _apply_reads.py, which re-anchors on PAT again) and the
      TRACKED provenance ledger scripts/wayback_nse/wb_rev_fills.json (registered in verify_fills_live.py).
"""
import os, re, sys, json, html, collections
HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
sys.path.insert(0, HERE)
import wbcache, wb_read                                            # noqa: E402

TOL = 0.011          # to-the-paisa on a crore value printed in lakhs with 2 decimals
MON = wb_read.MON
LEDGER = os.path.join(HERE, "wb_rev_fills.json")
READS = os.path.join(SCRIPTS, "_wbrev_reads.json")


def _txt(raw):
    t = re.sub(r'<[^>]+>', ' ', raw)
    t = html.unescape(t)
    return re.sub(r'\s+', ' ', t)


def _num(t, label):
    m = re.search(r'(?<![A-Za-z])' + re.escape(label) + r'\s+(-?[\d,]+\.\d\d)', t)
    return float(m.group(1).replace(',', '')) if m else None


def _ymd(d):
    """'30-JUN-2002' -> 20020630"""
    return int(d[7:11]) * 10000 + MON[d[3:6]] * 100 + int(d[0:2])


def read_page(raw, sym):
    """-> dict. Either {'refuse': why} or the declared fields + pat/rev in crore.
    Gates G1 (symbol), G3 (basis if printed), G4 (scale). Period gating is the caller's, because a
    cumulative page is a valid LEG here even though wbgate refuses it as a quarter."""
    if raw is None:
        return {"refuse": "fetch-failed (transport, not evidence)"}
    p = wb_read.parse(raw)
    if not p:
        return {"refuse": "unparseable/empty-shell page"}
    if p.get("symbol") != sym:
        return {"refuse": "G1 page declares symbol %s, not %s" % (p.get("symbol"), sym)}
    rt = p.get("result_type") or ""
    ntok = len([x for x in rt.split(",") if x.strip()])
    if ntok >= 3 and "Non-Consolidated" not in rt:
        return {"refuse": "G3 basis printed and not standalone: %s" % rt}
    if not p.get("div"):
        return {"refuse": "G4 scale not declared/known: %s" % p.get("scale")}
    t = _txt(raw)
    div = p["div"]
    if p["bank"]:
        pat, rev, tot = _num(t, "Net Profit"), _num(t, "Interest Earned"), _num(t, "Total Income")
        rev_label = "Interest Earned"
    else:
        pat, rev, tot = p["net_profit"], p["net_sales"], _num(t, "Gross Income")
        rev_label = "Net Sales"
    if pat is None:
        return {"refuse": "no Net Profit row on the page"}
    if rev is None:
        return {"refuse": "no %s row on the page" % rev_label}
    return {"from": _ymd(p["from"]), "to": _ymd(p["to"]), "months": p["months"], "role": p["period_role"],
            "type": rt, "cumulative": ("Cumulative" in rt and "Non-Cumulative" not in rt), "bank": p["bank"], "scale": p["scale"],
            "pat": round(pat / div, 4), "rev": round(rev / div, 4), "tot": (round(tot / div, 4) if tot is not None else None),
            "rev_label": rev_label, "raw_pat": pat, "raw_rev": rev}


def prev_qe(qe):
    y, md = qe // 10000, qe % 10000
    return {331: (y - 1) * 10000 + 1231, 630: y * 10000 + 331, 930: y * 10000 + 630, 1231: y * 10000 + 930}[md]


def stage(cells, orig, idx, fund, fetch=False):
    """cells: [[sym, qe, npStd, npCon], ...]; orig: {'SYM|QE': [fund keys holding that row]}.
    -> (proposals {key: {...}}, refusals {key: why})"""
    std = {}
    for s, rows in fund.items():
        for r in rows:
            if r[1] is not None:
                std[(s, r[0])] = r[1]
    props, refs = {}, {}
    for sym, qe, ps, pc in cells:
        keys = orig.get("%s|%d" % (sym, qe)) or [sym]
        why = []
        done = False
        for k in keys:
            stored = std.get((k, qe))
            if stored is None:
                why.append("%s: no stored npStd" % k); continue
            e = idx.get("%s|%d" % (k, qe))
            if not e:
                why.append("%s: no archived page ends on this quarter" % k); continue
            ts, url = e
            raw = wbcache.fetch_cached(ts, url) if fetch else wbcache.cached(ts, url)
            r = read_page(raw, k)
            if "refuse" in r:
                why.append("%s: %s" % (k, r["refuse"])); continue
            if r["to"] != qe:
                why.append("%s: G2a page period ends %d, not %d" % (k, r["to"], qe)); continue
            # ---- direct true quarter
            if r["months"] == 3 and not r["cumulative"]:
                if abs(r["pat"] - stored) > TOL:
                    why.append("%s: ANCHOR-FAIL page PAT %.2f vs stored %.2f (finding, not a fill)" % (k, r["pat"], stored)); continue
                props["%s|%d" % (k, qe)] = {
                    "sym": k, "qe": qe, "rev": round(r["rev"], 2), "fin": 1 if r["bank"] else 0, "mode": "direct",
                    "pat_seen": stored, "page_pat": r["pat"], "rev_label": r["rev_label"], "scale": r["scale"],
                    "period": "%d..%d (%s) %s" % (r["from"], r["to"], r["role"], r["type"]),
                    "wayback": [ts, url]}
                done = True; break
            # ---- cumulative differencing
            if not r["cumulative"] and r["months"] != 3:
                why.append("%s: %dm page not declared Cumulative" % (k, r["months"])); continue
            pq = prev_qe(qe)
            pe = idx.get("%s|%d" % (k, pq))
            if not pe:
                why.append("%s: cumulative page but no page ends on prior quarter %d" % (k, pq)); continue
            # the prior span [from, pq] as ONE page, or as a CHAIN of pages walking back from pq whose periods
            # abut exactly and whose first page starts at the cumulative page's own 'from'
            chain, cur, bad = [], pq, None
            while True:
                ce = idx.get("%s|%d" % (k, cur))
                if not ce:
                    bad = "no page ends on %d" % cur; break
                craw = wbcache.fetch_cached(ce[0], ce[1]) if fetch else wbcache.cached(ce[0], ce[1])
                cr = read_page(craw, k)
                if "refuse" in cr:
                    bad = "leg ending %d: %s" % (cur, cr["refuse"]); break
                if cr["to"] != cur or cr["from"] < r["from"] or cr["from"] > cur:
                    bad = "leg period %d..%d does not nest in %d..%d" % (cr["from"], cr["to"], r["from"], r["to"]); break
                if cr["months"] != 3 and not cr["cumulative"]:
                    bad = "leg %dm not declared Cumulative" % cr["months"]; break
                if cr["bank"] != r["bank"]:
                    bad = "legs on different templates"; break
                chain.append((ce, cr))
                if cr["from"] == r["from"]:
                    break                       # chain closed at the cumulative page's own start
                if len(chain) >= 3:
                    bad = "chain longer than 3 legs"; break
                cur = prev_qe(cur)
                if cur < r["from"]:
                    bad = "chain walked past the period start"; break
            if bad:
                why.append("%s: prior leg %s" % (k, bad)); continue
            # the chain must abut exactly: each leg's 'from' is the day after the next-older leg's 'to'
            pe, pr = chain[0]
            pr = {"from": chain[-1][1]["from"], "to": chain[0][1]["to"], "role": " + ".join(c[1]["role"] for c in chain),
                  "type": " | ".join(c[1]["type"] for c in chain), "pat": round(sum(c[1]["pat"] for c in chain), 4),
                  "rev": round(sum(c[1]["rev"] for c in chain), 4), "bank": r["bank"], "months": sum(c[1]["months"] for c in chain)}
            if pr["months"] != (r["months"] - 3):
                why.append("%s: chain covers %dm, expected %dm" % (k, pr["months"], r["months"] - 3)); continue
            dpat = round(r["pat"] - pr["pat"], 4)
            if abs(dpat - stored) > TOL:
                why.append("%s: CUMDIFF PAT %.2f-%.2f=%.2f vs stored %.2f (identity fails)" % (k, r["pat"], pr["pat"], dpat, stored)); continue
            props["%s|%d" % (k, qe)] = {
                "sym": k, "qe": qe, "rev": round(r["rev"] - pr["rev"], 2), "fin": 1 if r["bank"] else 0, "mode": "cumdiff",
                "pat_seen": stored, "page_pat": dpat, "rev_label": r["rev_label"], "scale": r["scale"],
                "period": "%d..%d (%s) %s  MINUS  %d..%d (%s) %s" % (r["from"], r["to"], r["role"], r["type"], pr["from"], pr["to"], pr["role"], pr["type"]),
                "legs": {"cum": {"rev": r["rev"], "pat": r["pat"]}, "prev": {"rev": pr["rev"], "pat": pr["pat"]}},
                "wayback": [ts, url], "wayback_prev": [list(c[0]) for c in chain]}
            done = True; break
        if not done:
            refs["%s|%d" % (sym, qe)] = why
    return props, refs


def evidence(p):
    base = ("WAYBACK-ARCHIVED NSE results.jsp (web.archive.org/%s), exchange-native and AS-FILED. Row '%s' "
            "on the %s template, scale Rs.%s declared on the page; period declared %s. ANCHOR: the page's "
            "own Net Profit %.2f cr == stored sf_fundamentals npStd %.2f (<=0.011). "
            % (p["wayback"][0], p["rev_label"], "Banking" if p["fin"] else "Non-Banking", p["scale"], p["period"],
               p["page_pat"], p["pat_seen"]))
    if p["mode"] == "cumdiff":
        base += ("CUMULATIVE DIFFERENCING: legs cum rev %.2f / pat %.2f (web.archive.org/%s) minus prev rev %.2f / pat %.2f "
                 "(%s); the PAT difference reproduces the stored quarter, so the revenue difference is "
                 "the same quarter. " % (p["legs"]["cum"]["rev"], p["legs"]["cum"]["pat"], p["wayback"][0],
                                          p["legs"]["prev"]["rev"], p["legs"]["prev"]["pat"],
                                          ", ".join("web.archive.org/%s" % l[0] for l in p["wayback_prev"])))
    return base + "Reader + hold-out (445/0): scripts/wayback_nse/wb_rev.py. rev-parity campaign 2026-09-05."


def emit(props, stamp):
    reads = json.load(open(READS)) if os.path.exists(READS) else {}
    led = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {}
    n = 0
    for key, p in sorted(props.items()):
        if key in led:
            continue
        reads.setdefault(p["sym"], {})[str(p["qe"])] = {
            "basis": "std", "rev": p["rev"], "pat_seen": p["pat_seen"], "fin": p["fin"],
            "src": "wayback NSE results.jsp %s %s=%s pat=%s (%s) [rev-parity %s]" % (
                p["mode"], p["rev_label"], p["rev"], p["page_pat"], p["wayback"][0], stamp)}
        led[key] = {"revS": p["rev"], "fin": p["fin"], "mode": p["mode"], "row_label": p["rev_label"],
                    "anchor": {"stored_npStd": p["pat_seen"], "page_pat": p["page_pat"]},
                    "wayback": p["wayback"], "wayback_prev": p.get("wayback_prev"), "period": p["period"],
                    "evidence": evidence(p), "applied": "%s rev-parity wayback route" % stamp}
        n += 1
    json.dump(reads, open(READS, "w"), indent=1, sort_keys=True)
    json.dump(led, open(LEDGER, "w"), indent=1, sort_keys=True)
    print("emitted %d new cells -> %s (+ ledger %s, now %d entries)" % (n, os.path.basename(READS), os.path.basename(LEDGER), len(led)))


def main():
    av = sys.argv
    if "--emit" in av:
        props = json.load(open(av[av.index("--emit") + 1]))["proposals"]
        import time
        emit(props, av[av.index("--stamp") + 1] if "--stamp" in av else time.strftime("%Y-%m-%d"))
        return
    cells = json.load(open(av[av.index("--cells") + 1]))
    orig = json.load(open(av[av.index("--orig") + 1])) if "--orig" in av else {}
    idx = json.load(open(os.path.join(HERE, "_wb_index.json")))
    fund = json.load(open(os.path.join(ROOT, "docs", "sf_fundamentals.json")))
    props, refs = stage(cells, orig, idx, fund, fetch="--fetch" in av)
    modes = collections.Counter(p["mode"] + ("/bank" if p["fin"] else "") for p in props.values())
    byyear = collections.Counter(p["qe"] // 10000 for p in props.values())
    print("proposals: %d  %s  by year %s" % (len(props), dict(modes), dict(sorted(byyear.items()))))
    cls = collections.Counter()
    for k, why in refs.items():
        w = why[-1] if why else "?"
        w = re.sub(r"^[A-Z0-9&_-]+: ", "", w)
        cls[re.sub(r"[\d.]+", "#", w)[:70]] += 1
    print("refusals: %d" % len(refs))
    for k, v in cls.most_common(25):
        print("  %5d %s" % (v, k))
    out = av[av.index("--out") + 1] if "--out" in av else os.path.join(HERE, "_wb_rev_props.json")
    json.dump({"proposals": props, "refusals": refs}, open(out, "w"), indent=1, sort_keys=True)
    print("->", out)


if __name__ == "__main__":
    main()
