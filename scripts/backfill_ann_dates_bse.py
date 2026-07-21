# -*- coding: utf-8 -*-
"""Recover REAL declared dates for ann=0 (date-unknown sentinel) result rows from the BSE
announcement archive — metadata only, no PDFs, no vision, no API key.

The ann=0 rows (mostly BSE-PDF profit backfills that never captured the filing date) are
invisible to point-in-time screens/backtests: the quarter's number exists but the backtest
never learns WHEN the market saw it, so it skips the quarter. BSE's announcement archive
stamps every results filing with its exact date — this recovers genuine dates, never guesses:

  1. TARGETS  — every (sym, qe) in docs/sf_fundamentals.json with PAT present and ann==0.
  2. FETCH    — BSE announcements for the scrip in (qe+1 .. qe+150d) via fetch_insurers.datebound
                (result filings only, all categories, paginated).
  3. MATCH    — a filing whose NEWSSUB states the period (fetch_announcements.parse_qe) equal to
                the target qe wins (earliest such date = first declaration; audited refilings lose).
                Fallback: if NO filing states a period but the window holds exactly ONE distinct
                result-filing date inside (qe+5d .. qe+100d), that solo date is accepted ("solo").
                Anything else -> skip with reason (ambiguous / no-candidates / no-scrip).
  4. APPLY    — ledger scripts/ann_date_fills.json, then fill-only (current ann==0, PAT present)
                into BOTH docs/sf_fundamentals.json and scripts/fundamentals.json (master mirror —
                runbook: heal both or the nightly resurrects the 0). NEVER writes ann <= qe
                (impossible-pair rule) — window start makes that structurally true, checked anyway.

Resumable: fills ledger + scripts/_ann_date_skips.json are consulted on rerun. A burst of
consecutive empty windows = BSE rate-limit stub (162-byte-bse.json lesson) -> abort, nothing
recorded for the burst, rerun later.

Run:
  python -X utf8 scripts/backfill_ann_dates_bse.py --limit 30      # trial
  python -X utf8 scripts/backfill_ann_dates_bse.py                 # full sweep
  python -X utf8 scripts/backfill_ann_dates_bse.py --reapply       # ledger -> files, no fetching
  python -X utf8 scripts/backfill_ann_dates_bse.py --retry-skips   # re-attempt skipped keys
"""
import os, sys, json, time, argparse, datetime

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import fetch_insurers as FI                      # _opener/bse_get/datebound — proven BSE helpers
from fetch_announcements import parse_qe         # anchored period parser (runbook §15)

SF = os.path.join(ROOT, "docs", "sf_fundamentals.json")
MASTER = os.path.join(HERE, "fundamentals.json")
LEDGER = os.path.join(HERE, "ann_date_fills.json")
SKIPS = os.path.join(HERE, "_ann_date_skips.json")

def jload(p, default):
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:
        return default

def jsave(p, obj):
    tmp = p + ".tmp"
    json.dump(obj, open(tmp, "w", encoding="utf-8"), separators=(",", ":"))
    os.replace(tmp, p)

def qe_date(qe):
    return datetime.date(qe // 10000, (qe // 100) % 100, qe % 100)

def plus(qe, days):
    return int((qe_date(qe) + datetime.timedelta(days=days)).strftime("%Y%m%d"))

def scrip_map():
    m = dict((jload(os.path.join(HERE, "bse_scrips.json"), {}) or {}).get("by_id") or {})
    for r in (jload(os.path.join(ROOT, "docs", "bse_universe.json"), {}) or {}).get("rows") or []:
        m.setdefault(str(r[1]).upper(), r[0])
    return m

def targets(fund):
    out = []
    for sym, arr in fund.items():
        for q in arr:
            if not (isinstance(q, list) and len(q) >= 5 and isinstance(q[0], int)):
                continue
            if (q[1] is not None and q[2] == 0) or (q[3] is not None and q[4] == 0):
                out.append((sym, q[0]))
    return sorted(out)

def resolve(cands, qe):
    """cands = [(ann_int, attachment, newssub), ...] -> (ann, how) or (None, reason)."""
    cands = [c for c in cands if c[0] > qe]                      # impossible-pair rule, belt
    if not cands:
        return None, "no-candidates"
    exact = [c[0] for c in cands if parse_qe(c[2]) == qe]
    if exact:
        return min(exact), "exact"
    if any(parse_qe(c[2]) not in (0, qe) for c in cands) and not exact:
        # some filing in the window states a DIFFERENT period; unstated ones next to it are
        # as likely that other quarter's — refuse rather than guess
        stated = sorted({parse_qe(c[2]) for c in cands} - {0})
        return None, "other-period:%s" % ",".join(map(str, stated))
    band = sorted({c[0] for c in cands if plus(qe, 5) <= c[0] <= plus(qe, 100)})
    if len(band) == 1:
        return band[0], "solo"
    return None, ("ambiguous:%d-dates" % len(band)) if band else "outside-band"

def apply_ledger(ledger):
    """Fill-only into both fundamentals files. Returns (cells_docs, cells_master)."""
    counts = []
    for path in (SF, MASTER):
        data = jload(path, None)
        if data is None:
            print("WARN missing", path); counts.append(0); continue
        n = 0
        for key, rec in ledger.items():
            sym, qe = key.split("|"); qe = int(qe); ann = int(rec["ann"])
            if ann <= qe:                                        # never store an impossible pair
                continue
            for q in data.get(sym) or []:
                if not (isinstance(q, list) and len(q) >= 5 and q[0] == qe):
                    continue
                if q[1] is not None and q[2] == 0: q[2] = ann; n += 1
                if q[3] is not None and q[4] == 0: q[4] = ann; n += 1
        if n:
            jsave(path, data)
        counts.append(n)
        print("applied %d cells -> %s" % (n, os.path.normpath(path)))
    return counts

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max (sym,qe) rows to fetch this run")
    ap.add_argument("--only", default="", help="comma-separated symbols")
    ap.add_argument("--reapply", action="store_true", help="apply ledger to files, no fetching")
    ap.add_argument("--retry-skips", action="store_true")
    ap.add_argument("--max-minutes", type=float, default=0)
    args = ap.parse_args()

    ledger = jload(LEDGER, {})
    if args.reapply:
        apply_ledger(ledger); return

    skips = jload(SKIPS, {})
    fund = jload(SF, {})
    codes = scrip_map()
    todo = targets(fund)
    if args.only:
        only = {s.strip().upper() for s in args.only.split(",")}
        todo = [t for t in todo if t[0] in only]
    todo = [t for t in todo if "%s|%d" % t not in ledger
            and (args.retry_skips or "%s|%d" % t not in skips)]
    if args.limit:
        todo = todo[:args.limit]
    print("targets: %d rows across %d companies" % (len(todo), len({s for s, _ in todo})))

    o = FI.bse_session()
    t0 = time.time()
    done = filled = 0
    empty_streak = 0; streak_keys = []
    for sym, qe in todo:
        if args.max_minutes and (time.time() - t0) / 60 > args.max_minutes:
            print("time budget reached — stopping (resumable)"); break
        key = "%s|%d" % (sym, qe)
        code = codes.get(sym)
        if not code:
            skips[key] = "no-scrip"; done += 1; continue
        try:
            cands = FI.datebound(o, code, str(plus(qe, 1)), str(plus(qe, 150)))
        except Exception as ex:
            print("  %s fetch err: %s" % (key, str(ex)[:80])); cands = []
        if not cands:
            empty_streak += 1; streak_keys.append(key)
        else:
            empty_streak = 0; streak_keys = []
        if empty_streak >= 8:
            for k in streak_keys:                 # a burst of empties = rate-limit stub, not truth
                skips.pop(k, None)
            print("8 consecutive empty windows — BSE likely rate-limiting; aborting run (burst "
                  "not recorded), rerun later"); break
        ann, how = resolve(cands, qe)
        if ann:
            ledger[key] = {"ann": ann, "src": "bse:" + how}
            filled += 1
            print("  %-14s %d -> %d (%s)" % (sym, qe, ann, how))
        elif cands or how == "no-candidates":
            skips[key] = how
        done += 1
        if done % 25 == 0:
            jsave(LEDGER, ledger); jsave(SKIPS, skips)
            print("… %d/%d done, %d filled" % (done, len(todo), filled))
        time.sleep(0.6)

    jsave(LEDGER, ledger); jsave(SKIPS, skips)
    print("fetched: %d rows, %d dates recovered, %d skipped-this-run" % (done, filled, done - filled))
    if filled:
        apply_ledger(ledger)

if __name__ == "__main__":
    main()
