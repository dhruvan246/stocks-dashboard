# -*- coding: utf-8 -*-
"""DEEP offline re-parse of the NSE XBRL cache — the ~200 tags per filing the other
builders never read. Phase 2 of the stock-page upgrade (memory: project-stocks-stockpage-v2).

Same cache, same conventions as build_revop.py (READ THAT FIRST — file→(symbol,quarter)
mapping, OneD/FourD current-quarter contexts, NatureOfReport basis detection, rupees→crore,
scale_fix ledger, latest-filing-wins). This builder adds, per (symbol, quarter, basis):

  P&L detail   eps_b/eps_d (₹ per share), oi, fc, dep, tax, tax_c, tax_d, exc, pbt, pbet,
               emp, mat, oci
  Balance sheet (instant context OneI/FourI, present in half-yearly/annual filings)
               assets, eq (owners, falls back to Equity), borr (cur+noncur), cash, invnt,
               rec, pay, ppe, cwip, invst
  Cash flow    cfo, cfi, cff, capex, divp + cf_d (the tagged period's length in DAYS —
               filers tag CF against quarter, H1 or FY periods; consumers must read cf_d,
               never assume the quarter)
  Segments     seg: [[name, revenue, result], …] (result = SegmentProfitLossBeforeTaxAnd-
               FinanceCosts, falls back to SegmentProfitBeforeTax)
  Banking      gnpa_pct, nnpa_pct, cet1, car, roa (ratios as filed), dep_amt, adv, int_exp
  Meta         aud 'A'/'U', qual 1 when the auditor declaration mentions a qualification

CONTEXT RULES (the part that bit the first attempt):
  - Quarter facts live ONLY in OneD/FourD (validated 3-month duration). Other quarter-length
    contexts (TwoD/ThreeD…) are the COMPARATIVE quarters — reading them would file year-ago
    numbers under the current quarter. Never scan "any quarter-length context".
  - Balance sheet facts live in the instant contexts OneI/FourI; the instant date must equal
    the matching D-context's endDate.
  - Cash-flow facts may be tagged to OneD/FourD or to a longer YTD context ending at the
    same quarter end; the LONGEST such period wins and its day-count is stored as cf_d.
  - Segment facts live in OneSegment<n>D / FourSegment<n>D (same naming family as the NBFC
    OtherRevenueFromOperations components in build_revop).

Output: scripts/xbrl_extra.json = { SYM: { "QE": {"s": {...}, "c": {...}}, ... } }
        (₹ crore, EPS in ₹, ratios in %; latest filing wins per FIELD, so a corrigendum
        that re-tags only two lines can't null out the rest)

Run:  python -X utf8 scripts/build_xbrl_extra.py [--limit N] [--fresh]
Resumable: checkpoints to scripts/_xtra_progress.json every 10k files.
"""
import os, re, sys, json, datetime, concurrent.futures
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale_fix

HERE = os.path.dirname(os.path.abspath(__file__))
# XBRL_CACHE override: the nightly top-up routine runs from its OWN worktree (one writer per
# tree) but reads the single 80-GB cache that lives in the main checkout — see xtra_nightly.py.
CACHE = os.environ.get("XBRL_CACHE") or os.path.join(HERE, "_xbrl_cache")
OUT = os.path.join(HERE, "xbrl_extra.json")
PROG = os.path.join(HERE, "_xtra_progress.json")
SEEN = os.path.join(HERE, "_xtra_seen.json")
REVOP = os.path.join(HERE, "revop_fundamentals.json")

MIN_QE, MAX_QE = 20180101, 20261231
CR = 1e7
NS = r"in-(?:bse-fin|capmkt)"

RE_SYM = re.compile(r'<xbrli:identifier scheme="http://www\.nseindia\.com/NSESymbol">([^<]+)</xbrli:identifier>')
RE_SYM2 = re.compile(r'<' + NS + r':Symbol contextRef="OneD"[^>]*>([^<]+)<')
RE_TS = re.compile(r"(\d{12,14})")
RE_CTX_BLOCK = re.compile(r'<xbrli:context id="([^"]+)"[^>]*>(.*?)</xbrli:context>', re.DOTALL)
RE_INSTANT = re.compile(r"<xbrli:instant>(\d{4}-\d{2}-\d{2})<")
RE_STARTEND = re.compile(r"<xbrli:startDate>(\d{4}-\d{2}-\d{2})</xbrli:startDate>\s*<xbrli:endDate>(\d{4}-\d{2}-\d{2})<", re.DOTALL)
# older INDAS files carry the period as facts instead of inside the context block
RE_DATE = {c: {b: re.compile(r"DateOf" + b + r'OfReportingPeriod contextRef="' + c + r'"[^>]*>(\d{4}-\d{2}-\d{2})')
               for b in ("Start", "End")} for c in ("OneD", "FourD")}
RE_NAT = re.compile(r'NatureOfReportStandaloneConsolidated contextRef="([^"]+)"[^>]*>([^<]+)<')
RE_AUD = re.compile(r'WhetherResultsAreAuditedOrUnaudited contextRef="[^"]+"[^>]*>([^<]+)<')
RE_QUAL = re.compile(r"DeclarationOfUnmodifiedOpinionOrStatementOnImpactOfAuditQualification[^>]*>[^<]{0,300}?[Qq]ualif")
# Segment facts live in per-segment contexts named (One|Four)Reportable<seg><col>D (or
# ...Segment<n>D in some vintages). The <col> variants carry comparative periods too, so a
# segment context only counts when its period EQUALS the OneD current quarter.
RE_SEGCID = re.compile(r"^(One|Four)(?:Reportable|Segment)\d+D$")

# ---- tag tables: short key -> XBRL local names to try, in order ------------------------------
PNL = {  # quarter money, ₹ -> cr
    "oi": ["OtherIncome"], "fc": ["FinanceCosts"],
    "dep": ["DepreciationDepletionAndAmortisationExpense"],
    "tax": ["TaxExpense"], "tax_c": ["CurrentTax"], "tax_d": ["DeferredTax"],
    "exc": ["ExceptionalItemsBeforeTax"],
    "pbt": ["ProfitBeforeTax", "ProfitLossBeforeTax", "ProfitOrLossBeforeTax"],
    "pbet": ["ProfitBeforeExceptionalItemsAndTax"],
    "emp": ["EmployeeBenefitExpense"], "mat": ["CostOfMaterialsConsumed"],
    "oci": ["OtherComprehensiveIncomeNetOfTaxes"],
    "dep_amt": ["Deposits"], "adv": ["Advances"], "int_exp": ["InterestExpended"],
}
EPS = {  # quarter, ₹ per share — NOT crore-scaled
    "eps_b": ["BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
              "BasicEarningsPerShareAfterExtraordinaryItems",
              "BasicEarningsPerShareBeforeExtraordinaryItems"],
    "eps_d": ["DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
              "DilutedEarningsPerShareAfterExtraordinaryItems",
              "DilutedEarningsPerShareBeforeExtraordinaryItems"],
}
RATIO = {  # quarter, % / ratio as filed
    "gnpa_pct": ["PercentageOfGrossNpa"], "nnpa_pct": ["PercentageOfNpa"],
    "cet1": ["CET1Ratio"], "car": ["CapitalAdequacyRatio"], "roa": ["ReturnOnAssets"],
}
BS = {  # instant, ₹ -> cr; tuple entries are summed when at least one part is present
    "assets": ["Assets"], "eq": ["EquityAttributableToOwnersOfParent", "Equity"],
    "cash": ["CashAndCashEquivalents"], "invnt": ["Inventories"],
    "ppe": ["PropertyPlantAndEquipment"], "cwip": ["CapitalWorkInProgress"],
}
BS_SUM = {
    "borr": ["BorrowingsCurrent", "BorrowingsNoncurrent"],
    "rec": ["TradeReceivablesCurrent", "TradeReceivablesNoncurrent"],
    "pay": ["TradePayablesCurrent", "TradePayablesNoncurrent",
            "TradePayablesCurrentMicroAndSmallEnterprises", "TradePayablesCurrentOtherThanMicroAndSmallEnterprises"],
    "invst": ["CurrentInvestments", "NoncurrentInvestments"],
}
CF = {  # duration ending at the quarter end; ₹ -> cr
    "cfo": ["CashFlowsFromUsedInOperatingActivities"],
    "cfi": ["CashFlowsFromUsedInInvestingActivities"],
    "cff": ["CashFlowsFromUsedInFinancingActivities"],
    "divp": ["DividendsPaidClassifiedAsFinancingActivities"],
}
RE_CAPEX = re.compile(r"<" + NS + r':(PurchaseOfPropertyPlantAndEquipment\w*) contextRef="([^"]+)"[^>]*>([-0-9.eE+]+)<')

ALL_NAMES = sorted({n for d in (PNL, EPS, RATIO, BS, CF) for names in d.values() for n in names}
                   | {n for names in BS_SUM.values() for n in names})
RE_FACT = {n: re.compile(r"<" + NS + r":" + n + r' contextRef="([^"]+)"[^>]*>([-0-9.eE+]+)<') for n in ALL_NAMES}
RE_SEGDESC = re.compile(r'DescriptionOfReportableSegment contextRef="([^"]+)"[^>]*>([^<]+)<')
RE_SEGREV = re.compile(r"<" + NS + r':SegmentRevenue contextRef="([^"]+)"[^>]*>([-0-9.eE+]+)<')
RE_SEGRES = re.compile(r"<" + NS + r':(?:SegmentProfitLossBeforeTaxAndFinanceCosts|SegmentProfitBeforeTax) contextRef="([^"]+)"[^>]*>([-0-9.eE+]+)<')


def ts_key(fname):
    m = RE_TS.search(fname)
    if not m:
        return fname
    d = m.group(1)
    if len(d) == 14:  # DDMMYYYYHHMMSS -> YYYYMMDDHHMMSS
        return d[4:8] + d[2:4] + d[0:2] + d[8:]
    return d


def days_between(s, e):
    a = datetime.date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    b = datetime.date(int(e[:4]), int(e[5:7]), int(e[8:10]))
    return (b - a).days


def facts_by_ctx(xml, names):
    """{ctx: value} for the first tag name in `names` that has any facts."""
    for n in names:
        found = {}
        for ctx, raw in RE_FACT[n].findall(xml):
            try:
                found.setdefault(ctx, float(raw))
            except ValueError:
                pass
        if found:
            return found
    return {}


def parse_file(path, fname):
    xml = open(path, encoding="utf-8", errors="replace").read()
    sm = RE_SYM.search(xml) or RE_SYM2.search(xml)
    if not sm:
        return None
    sym = sm.group(1).strip().upper()

    # ---- contexts --------------------------------------------------------------------------
    ctx = {}  # cid -> ('I', date) | ('D', start, end)
    for m in RE_CTX_BLOCK.finditer(xml):
        cid, body = m.group(1), m.group(2)
        mi = RE_INSTANT.search(body)
        if mi:
            ctx[cid] = ("I", mi.group(1))
            continue
        ms = RE_STARTEND.search(body)
        if ms:
            ctx[cid] = ("D", ms.group(1), ms.group(2))
    for cid in ("OneD", "FourD"):          # older files: period tagged as facts, not in the block
        if cid not in ctx:
            s = RE_DATE[cid]["Start"].search(xml)
            e = RE_DATE[cid]["End"].search(xml)
            if s and e:
                ctx[cid] = ("D", s.group(1), e.group(1))

    # ---- current-quarter bases (OneD/FourD ONLY — everything else is a comparative) --------
    nat = {cid: v.strip().lower() for cid, v in RE_NAT.findall(xml)}
    bases = {}  # cid -> 's'|'c'
    one = ctx.get("OneD")
    if not (one and one[0] == "D" and 0 < days_between(one[1], one[2]) <= 100):
        return None
    qe = int(one[2].replace("-", ""))
    if not (MIN_QE <= qe <= MAX_QE):
        return None
    one_nat = nat.get("OneD", "")
    bases["OneD"] = "c" if "consol" in one_nat else "s"
    four = ctx.get("FourD")
    if four and four[0] == "D" and 0 < days_between(four[1], four[2]) <= 100 and four[2] == one[2]:
        four_nat = nat.get("FourD", "")
        if four_nat:
            bases["FourD"] = "c" if "consol" in four_nat else "s"
        else:
            bases["FourD"] = "s" if bases["OneD"] == "c" else "c"
    if len(bases) == 2 and bases["OneD"] == bases["FourD"]:
        del bases["FourD"]                 # same basis twice — trust OneD

    sc = scale_fix.factor(fname) or 1.0
    out = {"sym": sym, "qe": qe, "ts": ts_key(fname), "s": {}, "c": {}}

    def money(v):
        return round(v / sc / CR, 2)

    for cid, b in bases.items():
        row = out[b]
        for key, names in PNL.items():
            f = facts_by_ctx(xml, names)
            if cid in f:
                row[key] = money(f[cid])
        for key, names in EPS.items():
            f = facts_by_ctx(xml, names)
            if cid in f:
                row[key] = round(f[cid] / sc, 2)
        for key, names in RATIO.items():
            f = facts_by_ctx(xml, names)
            if cid in f:
                v = f[cid]
                if v == 0:
                    continue   # recent bank filings zero these tags and disclose only in the PDF —
                               # a literal 0.00% GNPA/CET1/ROA does not exist; 0 means "not disclosed"
                # Filers tag ratios either as percent (5.84) or as a fraction (0.0584).
                # GNPA/NNPA/CET1/CAR are structurally >1% when quoted as percent, so ≤1 means
                # fraction. Quarterly ROA in percent is itself <1, so its fraction cut is 0.03.
                if key == "roa":
                    if abs(v) < 0.03:
                        v *= 100
                elif abs(v) <= 1:
                    v *= 100
                row[key] = round(v, 2)

        # balance sheet: the matching instant context, dated exactly at this quarter end
        icid = cid[:-1] + "I"              # OneD -> OneI, FourD -> FourI
        inst = ctx.get(icid)
        if inst and inst[0] == "I" and inst[1] == one[2]:
            for key, names in BS.items():
                f = facts_by_ctx(xml, names)
                if icid in f:
                    row[key] = money(f[icid])
            for key, parts in BS_SUM.items():
                tot, seen = 0.0, False
                for n in parts:
                    f = facts_by_ctx(xml, [n])
                    if icid in f:
                        tot += f[icid]; seen = True
                if seen:
                    row[key] = money(tot)

    # ---- cash flow: any plain D context ending at the quarter end; longest period wins ------
    cf_ctx = {}  # cid -> days
    for cid, c in ctx.items():
        if c[0] == "D" and c[2] == one[2]:
            d = days_between(c[1], c[2])
            if d > 0:
                cf_ctx[cid] = d
    if cf_ctx:
        per_basis = {}  # basis -> (days, {key: val})
        capex_by_ctx = {}
        for tag, ccid, raw in RE_CAPEX.findall(xml):
            try:
                capex_by_ctx.setdefault(ccid, float(raw))
            except ValueError:
                pass
        for key, names in CF.items():
            for ccid, val in facts_by_ctx(xml, names).items():
                if ccid not in cf_ctx:
                    continue
                b = bases.get(ccid)
                if b is None:              # a YTD context: attributable only in single-basis filings
                    if len(bases) != 1:
                        continue
                    b = next(iter(bases.values()))
                d = cf_ctx[ccid]
                cur = per_basis.get(b)
                if cur is None or d > cur[0]:
                    per_basis[b] = (d, {})
                    cur = per_basis[b]
                if d == cur[0]:
                    cur[1][key] = money(val)
                    if ccid in capex_by_ctx:
                        cur[1]["capex"] = money(capex_by_ctx[ccid])
        for b, (d, vals) in per_basis.items():
            if vals:
                out[b].update(vals)
                out[b]["cf_d"] = d

    # ---- segments (only contexts whose period IS the current quarter — cols 2+ are
    #      comparatives with the same segment name and must not shadow the current one) -------
    segdesc = {cid: v.strip() for cid, v in RE_SEGDESC.findall(xml) if RE_SEGCID.match(cid)}
    if segdesc:
        segrev = {c: v for c, v in RE_SEGREV.findall(xml)}
        segres = {c: v for c, v in RE_SEGRES.findall(xml)}
        per_basis = {}
        for cid, name in sorted(segdesc.items()):
            c = ctx.get(cid)
            if not (c and c[0] == "D" and c[1] == one[1] and c[2] == one[2]):
                continue
            b = bases.get(("OneD" if cid.startswith("One") else "FourD"))
            if b is None:
                continue
            try:
                rv = money(float(segrev[cid])) if cid in segrev else None
                rs = money(float(segres[cid])) if cid in segres else None
            except ValueError:
                continue
            if rv is None and rs is None:
                continue
            # revenue and result arrive in PARALLEL context groups (Reportable1xD carries
            # SegmentRevenue, Reportable2xD the result), each re-declaring the segment name —
            # merge by name, never emit a second row for the same segment
            rows = per_basis.setdefault(b, [])
            for r in rows:
                if r[0] == name[:40]:
                    if r[1] is None: r[1] = rv
                    if r[2] is None: r[2] = rs
                    break
            else:
                rows.append([name[:40], rv, rs])
        for b, rows in per_basis.items():
            if rows:
                out[b]["seg"] = rows[:12]

    # ---- audited / qualification flags ------------------------------------------------------
    ma = RE_AUD.search(xml)
    if ma:
        aud = "U" if "un" in ma.group(1).strip().lower()[:2] else "A"
        for b in bases.values():
            out[b]["aud"] = aud
    if RE_QUAL.search(xml):
        for b in bases.values():
            out[b]["qual"] = 1

    if not out["s"] and not out["c"]:
        return None
    return out


def _worker(fname):
    try:
        return parse_file(os.path.join(CACHE, fname), fname)
    except Exception:
        return None


def main():
    args = sys.argv[1:]
    limit = int(args[args.index("--limit") + 1]) if "--limit" in args else None
    fresh = "--fresh" in args
    incremental = "--incremental" in args

    files = sorted(os.listdir(CACHE), key=ts_key)      # ascending -> latest filing wins
    files = [f for f in files if ts_key(f)[:4] >= "2018"]
    if limit:
        files = files[-limit:]

    # --incremental (nightly top-up): process exactly the cache files not in the seen-set.
    # A set difference, NOT a count-index resume — a late-arriving filing whose timestamp sorts
    # BEFORE already-processed files would be silently skipped by an index and lost forever.
    seen = None
    if incremental:
        try:
            seen = set(json.load(open(SEEN)))
            data = json.load(open(OUT))
        except Exception:
            sys.exit("ABORT: --incremental needs both %s and %s (seed them from a full run)"
                     % (os.path.basename(SEEN), os.path.basename(OUT)))
        files = [f for f in files if f not in seen]
        total = len(files)
        print("incremental: %d new cache files, %d symbols in ledger" % (total, len(data)))
        if not files:
            print("nothing new — ledger unchanged")
            return
        start_i = 0
    else:
        total = len(files)
        data, start_i = {}, 0
        if not fresh and not limit and os.path.exists(PROG):
            try:
                p = json.load(open(PROG))
                data = json.load(open(OUT))
                start_i = p.get("done", 0)
                print("resuming from %d/%d" % (start_i, total))
            except Exception:
                data, start_i = {}, 0

    def accumulate(r):
        if not r:
            return
        cell = data.setdefault(r["sym"], {}).setdefault(str(r["qe"]), {})
        for b in ("s", "c"):
            if r[b]:
                cell.setdefault(b, {}).update(r[b])   # per-field latest-wins (non-null only, by construction)

    todo = files[start_i:]
    processed = 0
    if limit or incremental:                            # small batches: sequential is fine
        for fname in todo:
            accumulate(_worker(fname)); processed += 1
    else:
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
    if incremental:
        seen.update(files)
        json.dump(sorted(seen), open(SEEN, "w"), separators=(",", ":"))
    elif not limit:
        json.dump({"done": total}, open(PROG, "w"))
        json.dump(sorted(files), open(SEEN, "w"), separators=(",", ":"))   # seed for --incremental
    n_q = sum(len(q) for q in data.values())
    print("Wrote %s: %d symbols, %d symbol-quarters, %d files processed" % (OUT, len(data), n_q, processed))
    validate(data)


def validate(data):
    """Identity check vs revop_fundamentals.json: for industrials, opStd == pbet+fc+dep−oi
    (that is literally how build_revop derives op, so agreement proves the file→(sym,qe,basis)
    mapping and unit handling; disagreement means context confusion)."""
    try:
        revop = json.load(open(REVOP))
    except Exception:
        print("(no revop_fundamentals.json to validate against)"); return
    ok = bad = 0
    examples = []
    for sym, qs in data.items():
        rv = revop.get(sym)
        if not rv:
            continue
        for qe, cell in qs.items():
            rrow = rv.get(qe)
            if not rrow or rrow[6] == 1:               # fin=1: op formula differs — skip
                continue
            for b, opidx in (("s", 2), ("c", 3)):
                d = cell.get(b)
                if not d:
                    continue
                op = rrow[opidx]
                if op is None or d.get("pbet") is None:
                    continue
                mine = d["pbet"] + (d.get("fc") or 0) + (d.get("dep") or 0) - (d.get("oi") or 0)
                if abs(mine - op) <= max(0.05, abs(op) * 0.01):
                    ok += 1
                else:
                    bad += 1
                    if len(examples) < 10:
                        examples.append("%s %s %s: recon %.2f vs revop %.2f" % (sym, qe, b, mine, op))
    tot = ok + bad
    print("op-identity validation vs revop: %d/%d within 1%% (%.1f%%)" % (ok, tot, 100.0 * ok / tot if tot else 0))
    for ex in examples:
        print("   MISMATCH", ex)


if __name__ == "__main__":
    main()
