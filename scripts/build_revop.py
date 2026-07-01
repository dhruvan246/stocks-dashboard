# -*- coding: utf-8 -*-
"""OFFLINE re-parse of the NSE XBRL cache to add two point-in-time quarterly metrics the
net-profit builder never extracted: RevenueFromOperations and Operating Profit (EBITDA).

This is a RE-PARSE, not a re-fetch: every filing already lives in scripts/_xbrl_cache/
(downloaded by build_fundamentals.py). We walk those ~102k files, and from each financial
filing read — for the CURRENT 3-month quarter (context "OneD", and "FourD" for the other
basis in a combined filing) — the Schedule-III lines needed to reconstruct:

  Revenue          = RevenueFromOperations
  Operating Profit = ProfitBeforeExceptionalItemsAndTax + FinanceCosts
                     + DepreciationDepletionAndAmortisationExpense - OtherIncome
                   (= Revenue - operating expenses excl. interest & depreciation; i.e. EBITDA,
                      excluding other income — the basis Trendlyne/StockView call "Operating Profit")

Both XBRL taxonomies carry the same local names: in-bse-fin: (classic INDAS filings, ~2018-2024)
and in-capmkt: (Integrated-Filing, 2025+). The company SYMBOL and quarter-end live inside the file
(OneD context: NSESymbol identifier + endDate), so we can map a cache file -> (symbol, quarter)
with no network. PAT is also re-read so we can VALIDATE against fundamentals.json (npStd/npCon).

Banks/NBFCs (InterestEarned line, or BANKING/NBFC filing) are FLAGGED financial: their "revenue"
and "operating profit" aren't comparable to industrials, so the aggregator drops them from those
two medians (keeps them in PAT) — matching how Trendlyne reports the results-season figure.

Output: scripts/revop_fundamentals.json = { SYM: { "QE": [revStd, revCon, opStd, opCon,
        patStd, patCon, fin], ... } }  (values in Rs crore; null where the basis wasn't filed;
        fin = 1 if financial). LATEST filing wins per cell (revisions supersede provisional
        results; a descriptive median wants the finalised figure).

Run:  python -X utf8 build_revop.py [--limit N] [--fresh]
Resumable: checkpoints to scripts/_revop_progress.json every 10k files.
"""
import os, re, sys, json, glob, concurrent.futures

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "_xbrl_cache")
OUT = os.path.join(HERE, "revop_fundamentals.json")
DOCS_OUT = os.path.join(os.path.dirname(HERE), "docs", "sf_revop.json")  # web copy the daily cron maintains
PROG = os.path.join(HERE, "_revop_progress.json")
FUND = os.path.join(HERE, "fundamentals.json")

# Go back as far as the cache supports (chart starts ~2019 -> needs 2018 as the year-ago base).
MIN_QE = 20180101
MAX_QE = 20261231

CR = 1e7  # rupees -> crore

# --- compiled regexes -------------------------------------------------------------------
RE_SYM = re.compile(r'<xbrli:identifier scheme="http://www\.nseindia\.com/NSESymbol">([^<]+)</xbrli:identifier>')
RE_SYM2 = re.compile(r'<in-(?:bse-fin|capmkt):Symbol contextRef="OneD"[^>]*>([^<]+)<')
# OneD / FourD context period (entity then period; DOTALL for pretty-printed INDAS)
def _ctx_period_re(cid):
    return re.compile(r'<xbrli:context id="' + cid + r'">.*?<xbrli:startDate>(\d{4}-\d{2}-\d{2})</xbrli:startDate>'
                      r'<xbrli:endDate>(\d{4}-\d{2}-\d{2})</xbrli:endDate>', re.DOTALL)
RE_ONED = _ctx_period_re("OneD")
RE_FOURD = _ctx_period_re("FourD")
RE_CTX = {"OneD": RE_ONED, "FourD": RE_FOURD}
# OLDER INDAS filings (pre ~2021) don't carry the period inside the <xbrli:context> block the way
# the newer ones do; instead they tag DateOf{Start,End}OfReportingPeriod per context. Read those.
RE_DATE = {c: {b: re.compile(r'DateOf' + b + r'OfReportingPeriod contextRef="' + c + r'"[^>]*>(\d{4}-\d{2}-\d{2})')
               for b in ("Start", "End")} for c in ("OneD", "FourD")}
RE_NAT = {c: re.compile(r'NatureOfReportStandaloneConsolidated contextRef="' + c + r'">([^<]+)<') for c in ("OneD", "FourD")}
RE_TS = re.compile(r'(\d{12,14})')


def ctx_period(xml, cid):
    """(startDate, endDate) for context cid — from the <xbrli:context> block (newer files) or the
    DateOf{Start,End}OfReportingPeriod tags (older files). None if neither is present."""
    m = RE_CTX[cid].search(xml)
    if m:
        return m.group(1), m.group(2)
    ms = RE_DATE[cid]["Start"].search(xml)
    me = RE_DATE[cid]["End"].search(xml)
    if ms and me:
        return ms.group(1), me.group(1)
    return None

TAGS = ("RevenueFromOperations", "OtherIncome", "FinanceCosts",
        "DepreciationDepletionAndAmortisationExpense",
        "ProfitBeforeExceptionalItemsAndTax", "ProfitBeforeTax",
        "ProfitLossForPeriod", "ProfitOrLossAttributableToOwnersOfParent")
RE_TAG = {t: {c: re.compile(r'<in-(?:bse-fin|capmkt):' + t + r' contextRef="' + c + r'"[^>]*>([-0-9.eE+]+)<')
              for c in ("OneD", "FourD")} for t in TAGS}


def days_between(s, e):
    import datetime
    a = datetime.date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    b = datetime.date(int(e[:4]), int(e[5:7]), int(e[8:10]))
    return (b - a).days


def fnum(xml, tag, ctx):
    m = RE_TAG[tag][ctx].search(xml)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def metrics_for(xml, ctx):
    """Reconstruct (revenue_cr, ebitda_cr, ebit_cr, pat_total_cr, pat_owners_cr) for one context.
    Returns rupee->crore values; revenue/op are None when the inputs aren't industrial.
      op   = EBITDA  = PBET + FinanceCosts + Depreciation - OtherIncome  (= Trendlyne 'EBIDT')
      ebit = EBIT    = PBET + FinanceCosts - OtherIncome  (after depreciation; ~ Trendlyne 'Oper Profit')"""
    rev = fnum(xml, "RevenueFromOperations", ctx)
    oi = fnum(xml, "OtherIncome", ctx) or 0.0
    fc = fnum(xml, "FinanceCosts", ctx) or 0.0
    dep = fnum(xml, "DepreciationDepletionAndAmortisationExpense", ctx) or 0.0
    pbet = fnum(xml, "ProfitBeforeExceptionalItemsAndTax", ctx)
    if pbet is None:
        pbet = fnum(xml, "ProfitBeforeTax", ctx)
    pat = fnum(xml, "ProfitLossForPeriod", ctx)
    owners = fnum(xml, "ProfitOrLossAttributableToOwnersOfParent", ctx)
    op = (pbet + fc + dep - oi) if (pbet is not None) else None
    ebit = (pbet + fc - oi) if (pbet is not None) else None
    return (rev / CR if rev is not None else None,
            op / CR if op is not None else None,
            ebit / CR if ebit is not None else None,
            pat / CR if pat is not None else None,
            owners / CR if owners is not None else None)


def xbrl_revop(xml, basis_hint=None):
    """(rev_std, op_std, ebit_std, rev_con, op_con, ebit_con, fin) in Rs crore for the CURRENT quarter
    from ONE filing, mirroring build_fundamentals.xbrl_profit's OneD/FourD + NatureOfReport logic — so
    the daily incremental updater can read revenue + operating profit straight from the XBRL it already
    fetches for PAT, no disk cache needed.
      op   = EBITDA = PBET + FinanceCosts + Depreciation - OtherIncome  (= Trendlyne 'EBIDT')
      ebit = EBIT   = PBET + FinanceCosts - OtherIncome  (after depreciation; ~ Trendlyne 'Oper Profit')
    Banks/NBFCs (InterestEarned) -> rev/op/ebit None (not comparable), fin=1."""
    nat = {}
    for m in re.finditer(r'NatureOfReportStandaloneConsolidated contextRef="([^"]+)"[^>]*>([^<]+)<', xml):
        nat[m.group(1)] = m.group(2).strip().lower()
    hint = (basis_hint or "").lower()
    fin = 1 if "InterestEarned" in xml else 0
    rev_std = op_std = ebit_std = rev_con = op_con = ebit_con = None
    one_nat = nat.get("OneD", "") or hint
    rev, op, ebit, _, _ = metrics_for(xml, "OneD")
    if rev is not None or op is not None:
        if "consol" in one_nat:
            rev_con, op_con, ebit_con = rev, op, ebit
        else:
            rev_std, op_std, ebit_std = rev, op, ebit
    four_nat = nat.get("FourD", "")
    if four_nat and four_nat != one_nat:          # combined filing: FourD is the OTHER basis
        rev, op, ebit, _, _ = metrics_for(xml, "FourD")
        if "consol" in four_nat and rev_con is None:
            rev_con, op_con, ebit_con = rev, op, ebit
        elif "consol" not in four_nat and rev_std is None:
            rev_std, op_std, ebit_std = rev, op, ebit
    if fin:
        rev_std = op_std = ebit_std = rev_con = op_con = ebit_con = None
    r2 = lambda x: round(x, 2) if x is not None else None
    return r2(rev_std), r2(op_std), r2(ebit_std), r2(rev_con), r2(op_con), r2(ebit_con), fin


def ts_key(fname):
    m = RE_TS.search(fname)
    if not m:
        return fname
    d = m.group(1)
    if len(d) == 14:  # DDMMYYYYHHMMSS -> sortable YYYYMMDDHHMMSS
        return d[4:8] + d[2:4] + d[0:2] + d[8:]
    return d


def parse_file(path, fname):
    xml = open(path, encoding="utf-8", errors="replace").read()
    per = ctx_period(xml, "OneD")
    if not per:
        return None  # no current-quarter financial context (governance / balance-sheet-only)
    s, e = per
    if not (0 < days_between(s, e) <= 100):
        return None  # OneD must be the 3-month quarter, not an annual/YTD period
    qe = int(e.replace("-", ""))
    if qe < MIN_QE or qe > MAX_QE:
        return None
    sm = RE_SYM.search(xml) or RE_SYM2.search(xml)
    if not sm:
        return None
    sym = sm.group(1).strip().upper()
    fin = 1 if ("InterestEarned" in xml or fname.startswith("BANKING") or "NBFC" in fname) else 0

    one_nat = (RE_NAT["OneD"].search(xml) or [None, ""])
    one_nat = one_nat.group(1).strip().lower() if hasattr(one_nat, "group") else ""
    out = {"std": None, "con": None, "fin": fin, "qe": qe, "sym": sym, "ts": ts_key(fname)}
    rev, op, ebit, pat, owners = metrics_for(xml, "OneD")
    one = {"rev": rev, "op": op, "ebit": ebit, "pat": pat, "owners": owners}
    if "consol" in one_nat:
        out["con"] = one
    else:
        out["std"] = one
    # combined filing: FourD is the OTHER basis, current quarter (only if a real 3-month quarter)
    pf = ctx_period(xml, "FourD")
    if pf and 0 < days_between(pf[0], pf[1]) <= 100:
        four_nat = RE_NAT["FourD"].search(xml)
        four_nat = four_nat.group(1).strip().lower() if four_nat else ""
        if four_nat and four_nat != one_nat:
            rev, op, ebit, pat, owners = metrics_for(xml, "FourD")
            d = {"rev": rev, "op": op, "ebit": ebit, "pat": pat, "owners": owners}
            if "consol" in four_nat and out["con"] is None:
                out["con"] = d
            elif "consol" not in four_nat and out["std"] is None:
                out["std"] = d
    return out


def _worker(fname):
    try:
        return parse_file(os.path.join(CACHE, fname), fname)
    except Exception:
        return None


def main():
    args = sys.argv[1:]
    limit = None
    if "--limit" in args:
        limit = int(args[args.index("--limit") + 1])
    fresh = "--fresh" in args

    files = sorted(os.listdir(CACHE), key=ts_key)  # ascending -> last (latest) write wins
    # pre-filter: a filing can only carry a quarter >= MIN_QE (2018-01) if it was FILED in 2018+.
    # ts_key is YYYYMMDDHHMMSS (or the filename when unparseable, which sorts > "2018" -> kept).
    files = [f for f in files if ts_key(f)[:4] >= "2018"]
    if limit:
        # sample across the whole set, biased to recent (where revenue matters), for validation
        files = files[-limit:]
    total = len(files)

    data = {}   # sym -> { qe(str) -> [revStd,revCon,opStd,opCon,patStd,patCon,fin,ebitStd,ebitCon] }
                #   op* = EBITDA (Trendlyne EBIDT); ebit* = after-dep operating profit (~Oper Profit)
    fin_seen = {}  # (sym,qe) -> fin
    start_i = 0
    if not fresh and not limit and os.path.exists(PROG):
        try:
            p = json.load(open(PROG))
            data = json.load(open(OUT))
            start_i = p.get("done", 0)
            print("resuming from %d/%d" % (start_i, total))
        except Exception:
            data, start_i = {}, 0

    def cell(sym, qe):
        d = data.setdefault(sym, {})
        row = d.setdefault(str(qe), [None, None, None, None, None, None, 0, None, None])
        if len(row) < 9:                                # pad legacy 7-element rows (resume/merge path)
            row += [None] * (9 - len(row))
        return row

    def put(row, idx, val):
        if val is not None:                         # latest-filing-wins (files sorted asc -> last overwrites)
            row[idx] = round(val, 2)

    def accumulate(r):
        if not r:
            return
        row = cell(r["sym"], r["qe"])
        if r["fin"]:
            row[6] = 1
        if r["std"]:
            put(row, 0, r["std"]["rev"]); put(row, 2, r["std"]["op"]); put(row, 4, r["std"]["pat"])
            put(row, 7, r["std"]["ebit"])
        if r["con"]:
            put(row, 1, r["con"]["rev"]); put(row, 3, r["con"]["op"])
            put(row, 8, r["con"]["ebit"])
            # consolidated PAT = owners-attributable (matches the backtest basis), else total.
            # Guard a mis-tagged owners=0 (real profit sits only in ProfitLossForPeriod) — same
            # guard as apply_owners_full.py: owners ~0 while total is material -> use total.
            ow, tot = r["con"]["owners"], r["con"]["pat"]
            con_pat = tot if (ow is None or (abs(ow) < 0.005 and tot is not None and abs(tot) > 2)) else ow
            put(row, 5, con_pat)

    todo = files[start_i:]
    processed = 0
    if limit:                                   # validation runs: simple sequential
        for fname in todo:
            accumulate(_worker(fname)); processed += 1
    else:                                        # full walk: process pool (order preserved -> latest-wins holds)
        workers = min(8, (os.cpu_count() or 4))
        print("parsing %d files with %d workers" % (len(todo), workers), flush=True)
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as ex:
            for r in ex.map(_worker, todo, chunksize=64):
                accumulate(r); processed += 1
                if processed % 10000 == 0:
                    json.dump(data, open(OUT, "w"), separators=(",", ":"))
                    json.dump({"done": start_i + processed}, open(PROG, "w"))
                    print("  %d/%d files, %d symbols" % (start_i + processed, total, len(data)), flush=True)

    json.dump(data, open(OUT, "w"), separators=(",", ":"))
    if not limit:
        json.dump({"done": total}, open(PROG, "w"))
        json.dump(data, open(DOCS_OUT, "w"), separators=(",", ":"))   # web copy (daily cron maintains it)
    print("Wrote %s + %s: %d symbols, %d files processed" % (OUT, DOCS_OUT, len(data), processed))

    validate(data)


def validate(data):
    """Compare re-parsed PAT against fundamentals.json npStd/npCon to confirm the file->(symbol,qe)
    mapping is correct."""
    try:
        fund = json.load(open(FUND))
    except Exception:
        print("(no fundamentals.json to validate against)"); return
    ok = bad = miss = 0
    examples = []
    for sym, rows in fund.items():
        dd = data.get(sym)
        if not dd:
            continue
        for r in rows:
            qe, npStd, npCon = r[0], r[1], r[3]
            cell = dd.get(str(qe))
            if not cell:
                continue
            for stored, idx, lbl in ((npStd, 4, "std"), (npCon, 5, "con")):
                got = cell[idx]
                if stored is None or got is None:
                    continue
                base = max(1.0, abs(stored))
                if abs(got - stored) / base <= 0.02:
                    ok += 1
                else:
                    bad += 1
                    if len(examples) < 12:
                        examples.append("%s %d %s: parsed %.2f vs stored %.2f" % (sym, qe, lbl, got, stored))
    tot = ok + bad
    print("\nPAT validation vs fundamentals.json: %d/%d match within 2%% (%.1f%%), %d mismatches"
          % (ok, tot, 100.0 * ok / tot if tot else 0, bad))
    for ex in examples:
        print("   MISMATCH", ex)


if __name__ == "__main__":
    main()
