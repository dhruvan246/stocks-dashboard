#!/usr/bin/env python3
"""Runbook §230 (ann class "dates after the filing") — finish the §149 midnight rewrite for ann_date_fills entries
whose note carries the NSE archive time in the form "filingDate 14-Feb-2023 18:14".

WHY. rewrite_ledgers_midnight.py (2026-09-23) recognised a timestamp only in the arrow form
"2023-02-14T18:14 -> 20230215" and skipped `override` entries; its fallback un-gated only dates on the day after a
month-end. So 1,591 entries kept the retired 15:30 gate's date (ALLCARGO Dec-22: filed 14-Feb-2023 18:14, stored
15-Feb), and backfill_ann_dates_bse --reapply re-asserts that date every night.

RULE. An entry is rewritten only when its `ann` is EXACTLY the retired gate's form of its own recorded timestamp
(after 15:30 or a non-trading day -> next trading day, gate_calendar.json) — the same test as
rewrite_ledgers_midnight.legacy(). New ann = the calendar filing day; ann_1530 = old ann; rule = midnight-2026-09-23.
Anything else is reported and left alone. Scope: point-in-time Nifty 500 cells (member at qe, +120, +240, +360 or
+450 days, rename aliases included — the §225 window).

  python3 scripts/ungate_archive_notes.py            # dry run
  python3 scripts/ungate_archive_notes.py --apply    # then: backfill_ann_dates_bse.py --reapply
"""
import bisect, collections, datetime, json, os, re, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from rewrite_ledgers_midnight import legacy, RULE
NOTE = re.compile(r"filingDate (\d\d)-([A-Z][a-z]{2})-(\d{4}) (\d\d):(\d\d)")
MON = {m: i + 1 for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split())}
H = sorted(json.load(open(os.path.join(HERE, "indices_history.json")))["Nifty 500"], key=lambda s: s["effectiveDate"])
HD = [int(s["effectiveDate"].replace("-", "")) for s in H]; HS = [set(s["symbols"]) for s in H]
RM = json.load(open(os.path.join(HERE, "_rename_map.json")))
OLDS = collections.defaultdict(set)
for o, n in RM.items():
    if isinstance(n, str): OLDS[n].add(o)


def add(q, days):
    d = datetime.date(q // 10000, q // 100 % 100, q % 100) + datetime.timedelta(days=days)
    return d.year * 10000 + d.month * 100 + d.day


def in_n500(sym, qe):
    for d in (qe, add(qe, 120), add(qe, 240), add(qe, 360), add(qe, 450)):
        i = bisect.bisect_right(HD, d) - 1
        m = HS[i] if i >= 0 else set()
        if sym in m or any(o in m for o in OLDS.get(sym, ())): return True
    return False


def main(apply):
    p = os.path.join(HERE, "ann_date_fills.json")
    D = json.load(open(p, encoding="utf-8")); c = collections.Counter(); ex = collections.defaultdict(list)
    for k, e in D.items():
        if k.startswith("_") or not isinstance(e, dict) or e.get("rule") == RULE: continue
        m = NOTE.search(str(e.get("src") or ""))
        if not m: continue
        sym, qe = k.split("|")[:2]; qe = int(qe)
        if not in_n500(sym, qe): c["out_of_scope"] += 1; continue
        raw = int(m.group(3)) * 10000 + MON[m.group(2)] * 100 + int(m.group(1)); mins = int(m.group(4)) * 60 + int(m.group(5))
        ann = e.get("ann")
        if ann == raw: c["already_raw"] += 1; continue
        if raw <= qe: c["raw_not_after_qe"] += 1; ex["raw_not_after_qe"].append((k, ann, raw)); continue
        if legacy(raw, mins) != ann: c["anomaly_untouched"] += 1; ex["anomaly"].append((k, ann, m.group(0))); continue
        c["rewritten"] += 1; ex["rewritten"].append((k, ann, "->", raw))
        if apply:
            e["ann_1530"] = ann; e["ann"] = raw; e["rule"] = RULE
            e["evidence_1530"] = "§230: note's own NSE archive time %s -> calendar day (ungate_archive_notes.py)" % m.group(0)
    print(dict(c))
    for kind, L in ex.items():
        for x in L[:4]: print("   ", kind, x)
    if apply:
        json.dump(D, open(p, "w", encoding="utf-8"), separators=(",", ":"))
        print("WROTE", p)
    return ex


if __name__ == "__main__":
    main("--apply" in sys.argv)
