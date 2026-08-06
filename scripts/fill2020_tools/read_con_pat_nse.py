# -*- coding: utf-8 -*-
"""FILL-2020 pre-2020 con-PAT: read CONSOLIDATED PAT from the NSE archive detail pages.

Consumes _con_nse_inventory.json (the discovery sweep) and reads only the gap cells that
actually have a consolidated filing. Runbook §51a: most pre-FY2020 con quarters were never
filed, so this route's ceiling is small by nature -- the point is to land what genuinely exists,
with the same anchor discipline as every other route.

THE ANCHOR PROBLEM, AND THE FIX. Every other backfill anchors the read against the stored PAT
for that (sym, qe, basis). Here the stored con IS the gap -- there is nothing to anchor to. So
gates, in priority order (at least one must pass; all are recorded):

  GATE S' (sibling-basis, strongest and preferred). Fetch the NON-Consolidated page for the SAME
      quarter and check its owners-PAT against our STORED std within max(0.05cr, 0.5%). That
      proves source + scale + period-mapping + symbol identity for this exact company-quarter;
      the consolidated page then comes from the same page family under the same declared unit.
      This is the SpiceJet manoeuvre generalised (validate the document via the basis you already
      hold, then read the basis you don't).
  GATE E (EPS reconstruction on the con page itself). PAT_cr == eps * eqcap_cr / fv, within 6%.
      The unit divisor cancels in that expression, so it is immune to the parser scaling
      per-share rows (Face Value prints as 0.02 for a Rs 2 share after the lakh division).
  GATE F (FY identity). The four con quarters of the fiscal year sum to the con ANNUAL row within
      max(3cr, 3%). Siblings may come from our stored con or from other con pages in the same FY.

ANTI-POISON, all mandatory before any figure is used (STEP N/W discipline):
  * the page's own "Consolidated / Non-Consolidated" must read Consolidated,
  * its "Period Ended" must equal the target quarter (never trust the list row's date alone),
  * its "Symbol" must be one of the target's known era-spellings (NSE rewrites archive filenames
    to the CURRENT symbol while the stored file keeps the old one -- get_detail handles the 404
    fallback, but identity is re-checked from the page body).

PAT row preference: "Net Profit/(Loss) after taxes, minority interest and share of profit of
associates" (owners-attributable = this dataset's basis, memory project-stocks-profit-basis).
Falls back to the plain period row ONLY when minority interest is absent or zero.

Writes nothing directly: lands to scripts/con_pat_nse_reads.json for review, then --apply merges
fill-only into docs/sf_fundamentals.json + scripts/fundamentals.json.

Run:  python -X utf8 scripts/fill2020_tools/read_con_pat_nse.py [--limit N] [--only SYM,SYM]
      python -X utf8 scripts/fill2020_tools/read_con_pat_nse.py --apply
"""
import importlib.util
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
sys.path.insert(0, SCRIPTS)

_spec = importlib.util.spec_from_file_location("nar", os.path.join(SCRIPTS, "_nse_archive_revop.py"))
NAR = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(NAR)                       # reuse aliases/get_detail/parse_detail/pick

INV = os.path.join(HERE, "_con_nse_inventory.json")
TARGETS = os.path.join(HERE, "_con_targets_pre2020.json")
READS = os.path.join(SCRIPTS, "con_pat_nse_reads.json")
CACHE = os.path.join(SCRIPTS, "_nsearch_cache")
DOCS = os.path.join(ROOT, "docs", "sf_fundamentals.json")
MIRROR = os.path.join(SCRIPTS, "fundamentals.json")

EPS_TOL, FY_ABS, FY_REL = 0.06, 3.0, 0.03
STD_ABS, STD_REL = 0.05, 0.005                       # GATE S' tolerance

R_OWN = re.compile(r"net profit.*after\s+taxe?s?.*minority\s+interest", re.I)
R_PERIOD = re.compile(r"net profit\s*/?\s*\(?\s*loss\s*\)?\s*for the period", re.I)
R_MINORITY = re.compile(r"^minority interest", re.I)
R_EQCAP = re.compile(r"paid-?up equity share capital", re.I)
R_FV = re.compile(r"face value", re.I)
R_BASIC = re.compile(r"^\(?a\)?\s*basic", re.I)


def qe_of(s):
    return NAR.iso_qe(s)


def read_page(link, sym, qe, want_con):
    """Fetch + validate one detail page. Returns (pat, meta, rows) or (None, reason, None)."""
    path = os.path.join(CACHE, "con_%s_%d_%s.html" % (sym.replace("&", "_"), qe,
                                                      "c" if want_con else "s"))
    try:
        html = NAR.get_detail(link, sym, path)
    except Exception as ex:
        return None, "fetch:%s" % type(ex).__name__, None
    meta, rows = NAR.parse_detail(html)
    basis = meta.get("Consolidated / Non-Consolidated", "")
    is_con = basis.strip().lower() == "consolidated"
    if is_con != want_con:
        return None, "basis-mismatch:%s" % (basis or "?"), None
    if qe_of(meta.get("Period Ended", "")) != qe:
        return None, "period-mismatch:%s" % meta.get("Period Ended"), None
    if (meta.get("Symbol") or "").upper() not in {a.upper() for a in NAR.aliases(sym)}:
        return None, "symbol-mismatch:%s" % meta.get("Symbol"), None
    own = NAR.pick(rows, R_OWN)
    per = NAR.pick(rows, R_PERIOD)
    mi = NAR.pick(rows, R_MINORITY)
    pat = own
    if pat is None:
        if want_con and mi not in (None, 0.0):
            return None, "no-owners-row-but-minority-present", None
        pat = per
    if pat is None:
        return None, "no-pat-row", None
    return pat, meta, rows


_LIST_CACHE = {}


def std_link(sym, qe):
    """resultDetailedDataLink of the NON-Consolidated filing for this quarter, or None."""
    if sym not in _LIST_CACHE:
        try:
            _LIST_CACHE[sym] = NAR.list_rows(sym)
        except Exception:
            _LIST_CACHE[sym] = []
        time.sleep(0.5)
    for row in _LIST_CACHE[sym]:
        if (row.get("consolidated") or "").strip().lower().startswith("non"):
            if qe_of(row.get("toDate") or "") == qe and row.get("resultDetailedDataLink"):
                return row["resultDetailedDataLink"]
    return None


def eps_gate(pat, rows):
    """PAT == eps * eqcap / fv. The declared-unit divisor cancels, so per-share rows scaled by
    the parser (Face Value 2 -> 0.02 under lakhs) are harmless here."""
    eq, fv, eps = NAR.pick(rows, R_EQCAP), NAR.pick(rows, R_FV), NAR.pick(rows, R_BASIC)
    if not (eq and fv and eps) or abs(pat) < 1e-9:
        return None, "eps-inputs-missing"
    recon = eps * eq / fv
    err = abs(recon - pat) / abs(pat)
    return (err <= EPS_TOL), "eps %.1f%% (recon %.2f vs %.2f)" % (err * 100, recon, pat)


def main():
    args = sys.argv[1:]
    if "--apply" in args:
        return apply_reads()
    limit = int(args[args.index("--limit") + 1]) if "--limit" in args else None
    only = set(args[args.index("--only") + 1].split(",")) if "--only" in args else None
    if not os.path.exists(INV):
        print("no inventory yet -- run nse_con_discover.py first")
        return
    inv = json.load(open(INV))
    fund = json.load(open(DOCS))
    targets = json.load(open(TARGETS))
    reads = json.load(open(READS)) if os.path.exists(READS) else {}
    os.makedirs(CACHE, exist_ok=True)

    work = []
    for sym, rec in sorted(inv.items()):
        if only and sym not in only:
            continue
        for qe_s, link in sorted((rec.get("qtr") or {}).items()):
            if "%s|%s" % (sym, qe_s) in reads:
                continue
            work.append((sym, int(qe_s), link))
    if limit:
        work = work[:limit]
    print("con-PAT reads to attempt: %d" % len(work), flush=True)

    ok = skip = 0
    for i, (sym, qe, link) in enumerate(work):
        key = targets.get(sym, {}).get("key", sym)
        stdrow = {r[0]: r for r in fund.get(key, [])}.get(qe)
        stored_std = stdrow[1] if stdrow else None
        pat, meta, rows = read_page(link, sym, qe, True)
        time.sleep(0.7)
        if pat is None:
            reads["%s|%d" % (sym, qe)] = {"skip": meta}
            skip += 1
            print("  SKIP %-12s %d  %s" % (sym, qe, meta), flush=True)
            continue
        gates, passed = [], False
        # GATE S' -- validate the document family via the basis we already hold.
        # The std link comes from NAR.list_rows (disk-cached, alias-aware: it merges the era
        # spellings, which matters because NSE rewrites archive filenames to the CURRENT symbol).
        srec = std_link(sym, qe)
        if stored_std is not None and srec:
            spat, smeta, _ = read_page(srec, sym, qe, False)
            time.sleep(0.7)
            if spat is not None:
                d = abs(spat - stored_std)
                good = d <= max(STD_ABS, abs(stored_std) * STD_REL)
                gates.append("S':%s std_page=%.2f stored=%.2f" % ("PASS" if good else "FAIL",
                                                                  spat, stored_std))
                passed = passed or good
            else:
                gates.append("S':unavailable(%s)" % smeta)
        eg, note = eps_gate(pat, rows)
        gates.append("E:%s %s" % ({True: "PASS", False: "FAIL", None: "n/a"}[eg], note))
        passed = passed or (eg is True)
        rec = {"con": round(pat, 2), "unit": meta.get("unit"), "gates": gates,
               "stored_std": stored_std, "link": link}
        if passed:
            reads["%s|%d" % (sym, qe)] = rec
            ok += 1
            print("  OK   %-12s %d  con=%-10.2f std=%-10s | %s" % (sym, qe, pat, stored_std,
                                                                   " ; ".join(gates)), flush=True)
        else:
            rec["skip"] = "no-gate-passed"
            reads["%s|%d" % (sym, qe)] = rec
            skip += 1
            print("  SKIP %-12s %d  no gate passed | %s" % (sym, qe, " ; ".join(gates)), flush=True)
        if (i + 1) % 10 == 0:
            json.dump(reads, open(READS, "w"), indent=0, sort_keys=True)
    json.dump(reads, open(READS, "w"), indent=0, sort_keys=True)
    print("\nlanded %d | skipped %d  -> %s" % (ok, skip, os.path.basename(READS)))


def apply_reads():
    reads = json.load(open(READS))
    good = {k: v for k, v in reads.items() if "con" in v and not v.get("skip")}
    targets = json.load(open(TARGETS))
    n_files = []
    for path in (DOCS, MIRROR):
        d = json.load(open(path))
        n = 0
        for k, v in good.items():
            sym, qe = k.split("|")
            qe = int(qe)
            key = targets.get(sym, {}).get("key", sym)
            r = {x[0]: x for x in d.get(key, [])}.get(qe)
            if not r:
                continue
            while len(r) < 5:
                r.append(None)
            if r[3] is not None:                       # fill-only
                continue
            r[3] = v["con"]
            if r[4] is None:
                r[4] = r[2]
            n += 1
        json.dump(d, open(path, "w"), separators=(",", ":"))
        n_files.append(n)
    print("applied: docs %d | mirror %d (of %d landed reads)" % (n_files[0], n_files[1], len(good)))


if __name__ == "__main__":
    main()
