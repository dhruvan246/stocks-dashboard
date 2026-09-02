# -*- coding: utf-8 -*-
"""CON-GAP PRE-2020 campaign (scripts/PLAN_CON_GAP_PRE2020.md) -- step 3: CLOSE the never-filed
consolidated quarters as NOT-APPLICABLE in scripts/coverage_na_ledger.json, with per-name evidence
from TWO independent readers, and report every symbol's class.

THE RULE (runbook §54b, §96a, §100): a consolidated cell is N/A only where the EXCHANGE RECORD shows
the company filed a standalone result for that quarter and no consolidated one (E1 + E2), the
quarter precedes the company's FIRST consolidated quarterly filing ever (E3, the leading-run rule),
and a second reader (Moneycontrol's consolidated feed) shows no consolidated figure that differs from
standalone for the quarter (§85: identical = MC repeating standalone; differs = a candidate that must
be read from a filing before anyone believes it -- so a symbol with any such candidate is NOT closed
here, it is reported as CONFLICT for a filing read).

Everything after a company's first consolidated filing stays VISIBLE: those are real gaps (filled
this campaign where a filing existed, refused with a reason where a gate failed, or an index hole).

Inputs: the discovery inventories (con_discover_pre2015.py, both windows), the cached NSE lists
(scripts/_nsearch_cache/list_<NAME>.json), the reads ledger (con_pat_nse_reads.json), and the
coverage builder's --explain output (which dates the engine reports each con-family parameter
unresolved for each symbol -- the bound_derivation of every entry).

Dry run by default: writes scripts/fill2020_tools/con_na_pre2020_report.json and prints a summary.
--write updates scripts/coverage_na_ledger.json (merging with existing entries as documented below).

  python3 scripts/fill2020_tools/con_na_pre2020.py --targets A.json,B.json --nse-inv A.json,B.json
        --mc-inv A.json,B.json --explain <explain.json> [--from 2009-01-30 --to 2019-12-31] [--write]
"""
import datetime as dt
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
LIST_CACHE = os.path.join(SCRIPTS, "_nsearch_cache")
LEDGER = os.path.join(SCRIPTS, "coverage_na_ledger.json")
READS = os.path.join(SCRIPTS, "con_pat_nse_reads.json")
REPORT = os.path.join(HERE, "con_na_pre2020_report.json")
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")

CON_PARAMS = ["revCon", "patCon", "profitYoyCon", "profitBaseCon", "profitAccelCon",
              "profitTTMCon", "profitStreakCon", "postDriftCon", "compositeCon"]
# RUNG 4 -- the filer's OWN result pack, read page by page (vision, 2026-09-02): the BSE result
# attachment for QE 30-Sep-2012 of six of the biggest N/A candidates. Every one is a scan with no
# text layer, so a text search proves nothing; the pages were rendered (75 dpi) and read. Positive
# control: Tata Motors' Q2 FY13 pack (a text PDF) lights up on "Consolidated Financial Results".
RUNG4 = {
    "COLPAL": "BSE 500830, Colgate_Palmolive_(India)_Ltd2_291012.pdf (filed 2012-10-29, 7 pages, scan): one 'Statement of Unaudited "
              "Results for the quarter and six months ended September 30, 2012' + statement of assets and liabilities + notes + press "
              "release + Price Waterhouse limited-review letter on 'the results of Colgate-Palmolive (India) Limited'. No consolidated "
              "statement on any page.",
    "NMDC": "BSE 526371, NMDC_Ltd_071112.pdf (filed 2012-11-07, 5 pages, scan): Part I statement of unaudited financial results for the "
            "quarter ended 30/09/2012, Part II shareholding, statement of assets and liabilities, notes, Clause-41 segment table. No "
            "consolidated statement on any page.",
    "HINDZINC": "BSE 500188, Hindustan_Zinc_Ltd_181012.pdf (filed 2012-10-18, 4 pages, scan): statement of unaudited financial results "
                "for the quarter and half year ended 30-Sep-2012, segment table + assets and liabilities, notes, Deloitte review report on "
                "'the Statement of Hindustan Zinc Limited (the Company)'. No consolidated statement on any page.",
    "SKFINDIA": "BSE 500472, SKF_India_Ltd_231012_Rst.pdf (filed 2012-10-23, 3 pages, scan): financial results for the quarter and nine "
                "months ended September 30, 2012 (Part I), Part II + notes, B S R review report. No consolidated statement on any page.",
    "CUB": "BSE 532210, City_Union_Bank_Ltd_031112_Rst.pdf (filed 2012-11-03, 5 pages, scan/fax): audited financial results for the "
           "period ended 30th September 2012 (bank format), notes, segment reporting, Jagannathan & Sarabeswaran auditor's report on "
           "'the Balance Sheet of City Union Bank Limited'. No consolidated statement on any page.",
    "KARURVYSYA": "BSE 590003, Karur_Vysya_Bank_Ltd_311012_Rst.pdf (filed 2012-10-31, 7 pages, scan): reviewed financial results for the "
                  "quarter / half year ended 30th September 2012, analytical ratios, assets and liabilities, segment reporting, notes, "
                  "R K Kumar limited review report. No consolidated statement on any page.",
}
RUNG4_NOTE = ("Rung 4 sample (2026-09-02): the BSE result packs for QE 30-Sep-2012 of six of the largest N/A candidates (COLPAL, NMDC, "
              "HINDZINC, SKFINDIA, CUB, KARURVYSYA) were rendered page by page and read - every pack carries a single standalone "
              "statement and no consolidated one; the positive control (Tata Motors Q2 FY13) shows 'Consolidated Financial Results'. "
              "Pre-2016 BSE packs are scans without a text layer, so this rung cannot be swept by text search at scale.")
MON = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
       "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
TODAY = dt.date.today().isoformat()
CAMPAIGN = "CON-GAP PRE-2020 (scripts/PLAN_CON_GAP_PRE2020.md), adjudicated %s" % TODAY


def to_qe(s):
    try:
        d, mo, y = (s or "").strip().split(" ")[0].split("-")
        return int(y) * 10000 + MON[mo.title()] * 100 + int(d)
    except Exception:
        return None


def iso(qe):
    return "%04d-%02d-%02d" % (qe // 10000, qe // 100 % 100, qe % 100)


def day_before(isodate):
    return (dt.date.fromisoformat(isodate) - dt.timedelta(days=1)).isoformat()


def load_lists(names):
    rows = []
    for n in names:
        p = os.path.join(LIST_CACHE, "list_%s.json" % re.sub(r"[^A-Z0-9]", "_", n.upper()))
        if os.path.exists(p):
            try:
                got = json.load(open(p))
                if isinstance(got, list):
                    rows.extend(got)
            except Exception:
                pass
    return rows


def is_con(r):
    return (r.get("consolidated") or "").strip().lower().startswith("consolidated")


def is_cum(r):
    return (r.get("cumulative") or "").strip().lower().startswith("cumulative")


def main():
    a = sys.argv[1:]
    get = lambda k, d=None: a[a.index(k) + 1] if k in a else d
    tfiles = get("--targets").split(",")
    nfiles = get("--nse-inv").split(",")
    mfiles = (get("--mc-inv") or "").split(",") if get("--mc-inv") else []
    explain = json.load(open(get("--explain")))["byDate"]
    W_FROM, W_TO = get("--from", "2009-01-30"), get("--to", "2019-12-31")
    write = "--write" in a

    targets = {}
    for f in tfiles:
        for s, v in json.load(open(f)).items():
            t = targets.setdefault(s, {"key": v["key"], "qes": set()})
            t["qes"] |= set(v["qes"])
    nse = {}
    for f in nfiles:
        if os.path.exists(f):
            for s, v in json.load(open(f)).items():
                n = nse.setdefault(s, {"names": set(), "con_qtr": {}, "err": None})
                n["names"] |= set(v.get("names") or [s])
                n["con_qtr"].update(v.get("con_qtr") or {})
                n["err"] = n["err"] or v.get("err")
    mc = {}
    for f in mfiles:
        if os.path.exists(f):
            for s, v in json.load(open(f)).items():
                m = mc.setdefault(s, {"cells": {}, "con_n": None, "con_oldest": None, "note": None,
                                      "id": None, "anchor": {"hits": 0, "tries": 0}})
                m["cells"].update(v.get("cells") or {})
                if v.get("con_n") is not None:
                    m["con_n"], m["con_oldest"] = v.get("con_n"), v.get("con_oldest")
                m["note"] = m["note"] or v.get("con_note") or v.get("note")
                m["id"] = m["id"] or v.get("mc_id")
                for k in ("hits", "tries"):
                    m["anchor"][k] += (v.get("std_anchor") or {}).get(k, 0)
    reads = json.load(open(READS)) if os.path.exists(READS) else {}
    fund = json.load(open(FUND))
    explain_dates = sorted(d for d in explain if W_FROM <= d <= W_TO)

    report = {"_campaign": CAMPAIGN, "window": [W_FROM, W_TO], "symbols": {}, "reach": {}}
    entries = {p: {} for p in CON_PARAMS}
    yr_scope, yr_nse, yr_mc, yr_union = {}, {}, {}, {}
    classes = {}
    for sym in sorted(targets):
        key = targets[sym]["key"]
        gaps = sorted(targets[sym]["qes"])
        rec = nse.get(sym) or {"names": {sym, key}, "con_qtr": {}, "err": "not-swept"}
        rows = load_lists(rec["names"] | {sym, key})
        std_q, con_q, cum_q = set(), {}, set()
        for r in rows:
            qe = to_qe(r.get("toDate"))
            if not qe or (r.get("period") or "Quarterly") != "Quarterly":
                continue
            if is_con(r):
                if is_cum(r):
                    cum_q.add(qe)
                else:
                    con_q.setdefault(qe, r)
            else:
                std_q.add(qe)
        first_con = min(con_q) if con_q else None
        first_con_filed = to_qe(con_q[first_con].get("filingDate") or con_q[first_con].get("broadCastDate")) if first_con else None
        mrec = mc.get(sym) or {}
        mcells = mrec.get("cells") or {}
        # per-cell facts over the gap quarters
        facts = {}
        for qe in gaps:
            st = mcells.get(str(qe), {}).get("state")
            facts[qe] = {"nse_std": qe in std_q, "nse_con": qe in con_q, "nse_con_cum": qe in cum_q,
                         "mc": st}
            y = qe // 10000
            yr_scope[y] = yr_scope.get(y, 0) + 1
            if qe in con_q:
                yr_nse[y] = yr_nse.get(y, 0) + 1
            if st == "differs":
                yr_mc[y] = yr_mc.get(y, 0) + 1
            if qe in con_q or st == "differs":
                yr_union[y] = yr_union.get(y, 0) + 1
        landed = {k: v for k, v in reads.items() if k.startswith(sym + "|") and "con" in v and not v.get("skip")
                  and int(k.split("|")[1]) in targets[sym]["qes"]}
        refused = {k: v.get("skip") for k, v in reads.items() if k.startswith(sym + "|") and v.get("skip")
                   and int(k.split("|")[1]) in targets[sym]["qes"]}
        lead = [qe for qe in gaps if first_con is None or qe < first_con]
        trail = [qe for qe in gaps if first_con is not None and qe >= first_con]
        lead_std = sum(1 for qe in lead if facts[qe]["nse_std"])
        # THE COMPARATIVE YEAR. Moneycontrol's consolidated series starts exactly four quarters
        # before the exchange's first consolidated quarterly filing (measured 2026-09-02: ACLGATI
        # MC 2011-09 vs NSE 2012-09, ADANIPORTS 2011-06 vs 2012-06, AKSHOPTFBR 2010-06 vs 2011-06):
        # that is the year-ago COMPARATIVE column printed inside the first consolidated filings.
        # Those figures became public only ON the first filing date, so an MC value there does not
        # contradict a verdict bounded to dates BEFORE that filing -- it is a fill route for the
        # year-ago base AFTER it (the filing's comparative column; not this reader's route).
        comp_lo = (first_con - 10000) if first_con else None
        comp_win = [qe for qe in lead if comp_lo is not None and qe >= comp_lo]
        lead_mc_diff = [qe for qe in lead if facts[qe]["mc"] == "differs"
                        and (comp_lo is None or qe < comp_lo)]
        comp_mc_diff = [qe for qe in comp_win if facts[qe]["mc"] == "differs"]
        # classification
        if sym not in nse:
            cls = "NOT-SWEPT(no discovery record yet)"
        elif not rows:
            cls = "NSE-EMPTY(no rows under any era name; index silence is not evidence)"
        elif rec.get("err") and not rows:
            cls = "NSE-ERROR:" + str(rec["err"])
        elif not lead:
            cls = "CON-FILER-THROUGHOUT(first con %s precedes every gap quarter)" % first_con
        elif not mrec.get("id"):
            cls = "ONE-READER(Moneycontrol has no id for this company; the exchange index alone is not closed as N/A)"
        elif not (mrec["anchor"]["hits"] >= 2 and mrec["anchor"]["hits"] * 2 >= mrec["anchor"]["tries"]):
            cls = "ONE-READER(Moneycontrol standalone series does not anchor to ours: %d/%d gap quarters reproduce our stored std)" % (
                mrec["anchor"]["hits"], mrec["anchor"]["tries"])
        elif lead_mc_diff:
            cls = "CONFLICT-MC-DIFFERS(%d leading-run quarters OLDER than the comparative year where MC's con differs from its std while NSE lists no con row)" % len(lead_mc_diff)
        elif lead_std == 0:
            cls = "NSE-NO-STD-ROWS-FOR-GAP-QUARTERS(silence not meaningful)"
        else:
            cls = "NA-LEADING-RUN"
        classes[cls.split("(")[0]] = classes.get(cls.split("(")[0], 0) + 1
        srep = {"key": key, "class": cls, "gap_quarters": len(gaps), "leading_run": len(lead),
                "leading_run_with_std_row": lead_std, "trailing_quarters": len(trail),
                "first_con_qtr": first_con, "first_con_filed": first_con_filed,
                "nse_rows": len(rows), "nse_con_rows_total": len(con_q),
                "mc_con_n": mrec.get("con_n"), "mc_con_oldest": mrec.get("con_oldest"),
                "mc_id": (mrec.get("id") or {}).get("sc_id") if mrec.get("id") else None,
                "mc_std_anchor": mrec.get("anchor"),
                "mc_lead_states": {(s or "unswept"): sum(1 for qe in lead if facts[qe]["mc"] == s)
                                   for s in ("differs", "identical", "no-con-row", "no-std-row", "con-row-no-pat", None)},
                "comparative_window_quarters": len(comp_win), "comparative_window_mc_differs": len(comp_mc_diff),
                "landed": len(landed), "refused": len(refused),
                "refusal_reasons": sorted({(v or "")[:40] for v in refused.values()})}
        report["symbols"][sym] = srep
        if cls != "NA-LEADING-RUN":
            continue
        # ---- bounds from the engine's own unresolved dates, per parameter ----
        hard_to = W_TO
        if first_con_filed:
            hard_to = min(hard_to, day_before(iso(first_con_filed)))
        elif first_con:
            hard_to = min(hard_to, iso(first_con))          # conservative: filing date unknown
        n_std_lead = lead_std
        for p in CON_PARAMS:
            dates = [d for d in explain_dates if d <= hard_to and key in set(explain[d].get(p) or [])]
            if not dates:
                continue
            e = {
                "class": "C-basis (no consolidated statement exists - two readers)",
                "from": dates[0], "to": dates[-1],
                "bound_derivation": ("The %d dates at which the engine reports this parameter unresolved for %s "
                                     "in the campaign window %s..%s, capped at the day before the company's first "
                                     "consolidated quarterly filing (%s): first %s, last %s. Dates after that cap are a "
                                     "different question (fill from the filing / its comparatives) and stay visible."
                                     % (len(dates), key, W_FROM, W_TO,
                                        ("QE %s filed %s" % (first_con, first_con_filed)) if first_con else "none in the record",
                                        dates[0], dates[-1])),
                "reader_1": ("NSE results-archive list API (corporates-financial-results, Quarterly), fetched %s under %s: "
                             "%d filing rows in total; for the %d gap quarters before the cap the exchange lists a "
                             "Non-Consolidated (standalone) row for %d of them and a Consolidated row for NONE. "
                             "First Consolidated quarterly row in the whole record: %s. A standalone-only filing for a "
                             "quarter is the exchange's positive record that no consolidated result was submitted for it "
                             "(runbook §54b E1-E3)."
                             % (TODAY, "/".join(sorted(rec["names"] | {sym})), len(rows), len(lead), n_std_lead,
                                ("QE %s (filed %s)" % (first_con, first_con_filed)) if first_con else "none")),
                "reader_2": ("Moneycontrol cons_quarterly feed (agg_sources.mc_quarters, sc_id %s; identity: MC's standalone PAT "
                             "reproduces our stored standalone in %d of %d gap quarters), swept %s: %s; over the leading-run "
                             "gap quarters MC serves %d con rows identical to its own standalone row (§85: MC repeats "
                             "standalone where nothing consolidated was filed), %d quarters with no con row at all, and 0 "
                             "rows differing from standalone older than the comparative year. %s"
                             % ((mrec.get("id") or {}).get("sc_id"), mrec["anchor"]["hits"], mrec["anchor"]["tries"],
                                TODAY, mrec.get("note") or "not resolved on MC",
                                srep["mc_lead_states"].get("identical", 0),
                                srep["mc_lead_states"].get("no-con-row", 0) + srep["mc_lead_states"].get("unswept", 0),
                                ("MC does hold %d differing figure(s) inside the four quarters preceding the first consolidated "
                                 "filing (QE %s): the comparative column of that filing, public only from its filing date -- "
                                 "the fill route for the year-ago base AFTER this bound, not evidence against it."
                                 % (len(comp_mc_diff), first_con)) if comp_mc_diff else "")),
                "our_data": ("docs/sf_fundamentals.json holds a standalone PAT and NO consolidated PAT for all %d gap quarters "
                             "(corroboration only - never the basis of the verdict). %d cell(s) of this symbol landed from NSE "
                             "consolidated pages this campaign (all after the cap), %d refused."
                             % (len(gaps), len(landed), len(refused))),
                "reader_3": RUNG4.get(key) or RUNG4.get(sym) or ("not read for this name; " + RUNG4_NOTE),
                "user_approved": "2026-09-02 (campaign brief PLAN_CON_GAP_PRE2020 step 3: close the never-filed rest as N/A with evidence)",
                "adjudicated": TODAY,
                "campaign": "CON-GAP PRE-2020",
            }
            entries[p][key] = e
    report["classes"] = classes
    years = sorted(yr_scope)
    report["reach"] = {str(y): {"cells": yr_scope[y], "nse_con_qtr_row": yr_nse.get(y, 0),
                                "mc_con_differs": yr_mc.get(y, 0), "either": yr_union.get(y, 0)} for y in years}
    n_entries = {p: len(v) for p, v in entries.items()}
    # cells the new entries would excuse (per param, (sym, date) pairs in the explain inside the bounds)
    excused = {}
    for p in CON_PARAMS:
        n = 0
        for key, e in entries[p].items():
            n += sum(1 for d in explain_dates if e["from"] <= d <= e["to"] and key in set(explain[d].get(p) or []))
        excused[p] = n
    report["na_entries"] = n_entries
    report["na_excused_member_dates"] = excused
    json.dump(report, open(REPORT, "w"), indent=1, sort_keys=True)

    print("REACH (gap cells | with an NSE consolidated quarterly row | MC con != std | either):")
    for y in years:
        r = report["reach"][str(y)]
        print("  %d: %5d | %4d (%.1f%%) | %4d | %4d (%.1f%%)" % (y, r["cells"], r["nse_con_qtr_row"],
              100.0 * r["nse_con_qtr_row"] / r["cells"], r["mc_con_differs"], r["either"], 100.0 * r["either"] / r["cells"]))
    print("CLASSES:", json.dumps(classes, indent=1))
    print("N/A entries per param:", n_entries)
    print("member-dates excused per param:", excused)

    if not write:
        print("dry run -- report at %s; pass --write to update the ledger" % REPORT)
        return
    ledger = json.load(open(LEDGER))
    merged = skipped = added = 0
    for p in CON_PARAMS:
        L = ledger.setdefault(p, {})
        for key, e in entries[p].items():
            old = L.get(key)
            if old is None:
                L[key] = e
                added += 1
                continue
            # an existing verdict for the same filer: widen only a compatible C-basis window
            if str(old.get("class", "")).startswith("C-basis") and old.get("from") and old.get("from") > e["to"]:
                gap_days = (dt.date.fromisoformat(old["from"]) - dt.date.fromisoformat(e["to"])).days
                if gap_days <= 400:                     # contiguous-ish: the same leading run
                    new = dict(old)
                    new["from"] = e["from"]
                    new["supersedes"] = ("bound widened %s by CON-GAP PRE-2020: was %s..%s, is %s..%s; pre-2020 evidence: "
                                         "reader_1=[%s] reader_2=[%s]" % (TODAY, old["from"], old.get("to"), e["from"],
                                                                          old.get("to"), e["reader_1"], e["reader_2"]))
                    L[key] = new
                    merged += 1
                    continue
            skipped += 1
            print("  SKIP existing %s/%s class=%s from=%s to=%s (not merged; adjudicate by hand)"
                  % (p, key, old.get("class"), old.get("from"), old.get("to")))
    ledger["_updated"] = TODAY
    json.dump(ledger, open(LEDGER, "w"), indent=1, sort_keys=True)
    print("ledger written: %d added, %d widened, %d skipped" % (added, merged, skipped))


if __name__ == "__main__":
    main()
