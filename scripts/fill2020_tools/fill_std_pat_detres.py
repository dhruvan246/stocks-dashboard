# -*- coding: utf-8 -*-
"""FILL-2020: standalone PAT for 2015-2020 gap cells via the BSE detailed-results JSON (runbook §42).

WHY NOT THE PDF ROUTE. These cells resisted the announcement/PDF path for two reasons found
2026-08-06: (1) the stored announce dates for old quarters are unreliable -- several look like a
quarter-end+45d default rather than a real filing date (APLLTD Sep-2015 is stored 20151114, actually
filed 20151027), so a tight window finds nothing; (2) pre-2016 BSE attachments use an
underscore+timestamp name that AttachHis/AttachLive no longer serve (404), so even located filings
cannot be downloaded. The detailed-results JSON sidesteps both -- it is keyed by quarter, not by
announcement, and it is structured rather than scanned.

    https://api.bseindia.com/BseIndiaAPI/api/Corp_detailedResult_Transpose_ng/w?scrip_cd=<CODE>&qtr=<QID>
    QID = "NN.00", NN = 85 + (quarters since Mar-2015)     -- 85 = Mar-2015
    Values are AS-ORIGINALLY-FILED, in Rs MILLION -> /10 for crore. Standalone/primary basis only,
    which is exactly the basis these gaps need.

GATES (runbook §42 discipline -- a PAT fill needs a reconstruction, not just a printed number):
  1. Date Begin/End must span ~3 months and END on the target quarter-end. The same id space also
     holds annual and H1 rows; without this check a 12-month figure lands as a quarter.
  2. EPS reconstruction: EPS x (Equity Capital / Face Value) must reproduce Net Profit within
     EPS_TOL. This independently confirms the number is a quarterly owners-basis PAT at the
     declared scale. Cells that cannot be reconstructed are reported, never written.
  3. Fill-only: the target std cell must currently be None.

Sanity note, not a gate: for most of these companies the standalone result SHOULD differ from the
stored consolidated (they have subsidiaries -- that is why con was filed separately). An exact
con==std match is therefore worth eyeballing rather than trusting.

Run:  python -X utf8 scripts/fill2020_tools/fill_std_pat_detres.py [--apply] [--only SYM,SYM]
      (default DRY RUN.)
"""
import json
import os
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS = os.path.join(ROOT, "docs", "sf_fundamentals.json")
MIRROR = os.path.join(ROOT, "scripts", "fundamentals.json")
SCRIPS = os.path.join(ROOT, "scripts", "bse_scrips.json")
LEDGER = os.path.join(ROOT, "scripts", "std_pat_detres_fills.json")

API = ("https://api.bseindia.com/BseIndiaAPI/api/Corp_detailedResult_Transpose_ng/w"
       "?scrip_cd=%s&qtr=%s")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36")
EPS_TOL = 0.06                 # runbook §42: EPS-recon gate is +/-6%
MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
          "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


def qid(qe):
    """'NN.00' where NN counts quarters from Mar-2015 = 85."""
    y, m = qe // 10000, (qe // 100) % 100
    n = 85 + (y - 2015) * 4 + {3: 0, 6: 1, 9: 2, 12: 3}[m]
    return "%d.00" % n


def get(scrip, q):
    req = urllib.request.Request(API % (scrip, q),
                                 headers={"User-Agent": UA, "Referer": "https://www.bseindia.com/"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def parse_dt(s):
    try:
        d, mo, y = s.split("-")
        return (2000 + int(y)) * 10000 + MONTHS[mo] * 100 + int(d)
    except Exception:
        return None


def fields(js):
    out = {}
    for r in js.get("table1") or []:
        out.setdefault(r.get("fld_desc", "").strip(), r.get("Value"))
    return out


def fnum(f, *names):
    for n in names:
        v = f.get(n)
        if v not in (None, "", "-"):
            try:
                return float(v)
            except ValueError:
                pass
    return None


def ref_shares(scrip, qe, cache):
    """Share count (millions) from the NEAREST quarter that still prints Equity Capital + Face Value.

    The Ind-AS-era rows (roughly 2017+) drop both, and rename EPS to "Basic for discontinued &
    continuing operation", so the direct recon is impossible on those quarters alone. Share count is
    stable between corporate actions, so the closest quarter that does carry it is a sound reference
    -- and if a split/bonus HAS intervened the reconstruction simply fails the tolerance and the cell
    is skipped, which is the safe direction.
    """
    y, m = qe // 10000, (qe // 100) % 100
    base = (y - 2015) * 4 + {3: 0, 6: 1, 9: 2, 12: 3}[m]
    for off in (1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 7, -7, 8, -8):
        n = base + off
        if not 0 <= n <= 47:
            continue
        q = "%d.00" % (85 + n)
        if (scrip, q) not in cache:
            try:
                cache[(scrip, q)] = fields(get(scrip, q))
            except Exception:
                cache[(scrip, q)] = {}
            time.sleep(0.4)
        g = cache[(scrip, q)]
        eq, fv = fnum(g, "Equity Capital"), fnum(g, "Face Value (in Rs)")
        if eq and fv:
            return eq / fv, q
    return None, None


def check(scrip, qe, cache=None):
    """Return (value_cr, note) or (None, reason)."""
    cache = cache if cache is not None else {}
    js = get(scrip, qid(qe))
    f = fields(js)
    if not f:
        return None, "empty-response"
    b, e = parse_dt(f.get("Date Begin", "")), parse_dt(f.get("Date End", ""))
    if not b or not e:
        return None, "no-date-span"
    if e != qe:
        return None, "date-end=%s != target" % e
    span = (e // 10000 * 12 + (e // 100) % 100) - (b // 10000 * 12 + (b // 100) % 100)
    if span != 2:                                  # Jul->Sep = 2 month-steps = a 3-month quarter
        return None, "span=%d months (not a quarter)" % (span + 1)
    np = fnum(f, "Net Profit", "Net Profit (+)/ Loss (-) from Ordinary Activities after Ta")
    if np is None:
        return None, "no-net-profit-row"
    eq = fnum(f, "Equity Capital")
    fv = fnum(f, "Face Value (in Rs)")
    eps = fnum(f, "Basic & Diluted EPS after Extraordinary items",
               "Basic & Diluted EPS before Extraordinary items",
               "Basic EPS after Extraordinary items", "Basic EPS before Extraordinary items",
               "Basic for discontinued & continuing operation",
               "Diluted for discontinued & continuing operation",
               "Basic for continuing operation", "Basic EPS", "Basic & Diluted EPS")
    if eps is None:
        return None, "no-eps-row"
    if abs(np) < 1e-9:
        return None, "zero-net-profit"
    if eq and fv:
        shares, src = eq / fv, "own"               # both in millions -> share count in millions
    else:
        shares, refq = ref_shares(scrip, qe, cache)
        if not shares:
            return None, "no-share-count-anywhere"
        src = "ref:%s" % refq
    recon = eps * shares
    err = abs(recon - np) / abs(np)
    if err > EPS_TOL:
        return None, "eps-recon %.1f%% off (np=%.1f recon=%.1f, shares=%s)" % (
            err * 100, np, recon, src)
    return round(np / 10.0, 2), "eps-recon %.2f%% [%s]" % (err * 100, src)


def main():
    args = sys.argv[1:]
    apply_it = "--apply" in args
    only = set(args[args.index("--only") + 1].split(",")) if "--only" in args else None
    fund = json.load(open(DOCS))
    by_id = json.load(open(SCRIPS, encoding="utf-8"))["by_id"]
    targets = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "_std_targets.json")))
    ok, bad, journal, cache = [], [], {}, {}
    for sym, qes in sorted(targets.items()):
        if only and sym not in only:
            continue
        scrip = by_id.get(sym)
        if not scrip:
            bad.append((sym, "-", "no-bse-scrip"))
            continue
        rows = {r[0]: r for r in fund.get(sym, [])}
        for qe in qes:
            r = rows.get(qe)
            if not r:
                bad.append((sym, qe, "no-row"))
                continue
            if r[1] is not None:
                bad.append((sym, qe, "already-filled"))
                continue
            try:
                val, note = check(str(scrip), qe, cache)
            except Exception as ex:
                val, note = None, "fetch-error:%s" % type(ex).__name__
            time.sleep(0.5)
            con = r[3] if len(r) > 3 else None
            if val is None:
                bad.append((sym, qe, note))
                print("  SKIP %-11s %d  %s" % (sym, qe, note))
            else:
                same = (con is not None and abs(val - con) <= 0.01)
                ok.append((sym, qe, val, con, note, same))
                journal["%s|%d" % (sym, qe)] = {"std": val, "src": "bse-detres-§42",
                                                "qid": qid(qe), "gate": note,
                                                "stored_con": con,
                                                "applied": "2026-08-06 FILL-2020 std-2015-2020"}
                print("  OK   %-11s %d  std=%-10.2f (con=%-9s) %s%s"
                      % (sym, qe, val, con, note, "  <-- equals con, eyeball" if same else ""))
    print("\nPASS %d cells | SKIP %d" % (len(ok), len(bad)))
    if not apply_it:
        print("DRY RUN -- nothing written.")
        return
    for path in (DOCS, MIRROR):
        d = json.load(open(path))
        n = 0
        for sym, qe, val, con, note, same in ok:
            rows = {r[0]: r for r in d.get(sym, [])}
            r = rows.get(qe)
            if not r:
                continue
            while len(r) < 5:
                r.append(None)
            if r[1] is not None:
                continue
            r[1] = val
            n += 1
        json.dump(d, open(path, "w"), separators=(",", ":"))
        print("wrote %-28s %d cells" % (os.path.basename(path), n))
    led = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {}
    led.update(journal)
    json.dump(led, open(LEDGER, "w"), indent=1, sort_keys=True)
    print("journalled %d -> %s" % (len(journal), os.path.basename(LEDGER)))


if __name__ == "__main__":
    main()
