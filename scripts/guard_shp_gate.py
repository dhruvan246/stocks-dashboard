#!/usr/bin/env python3
"""Guard: the §12 15:30 visibility gate MUST be wired into BOTH SHP ingestion paths.

Why this exists (DATA_RUNBOOK §142a): commit 2ee1e235c (2026-09-05 13:33) wired `visible_iso()` into
refresh_quarters() and refresh_events(); commit 66a241e4f (13:41 the same day, a different workstream
carrying a stale copy of fetch_shareholding.py) silently deleted the function and both call sites. For
16 days every ingested filing stored NSE's raw submission day — a same-session look-ahead on 28 of the 34
filings ingested in that window. Nobody noticed because nothing asserted the gate's presence.

Run before the fetch steps in refresh-shareholding.yml. Exit 1 = do not fetch, do not commit."""
import os, re, sys
HERE = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(HERE, "fetch_shareholding.py"), encoding="utf-8").read()
errs = []
if "def visible_iso(rec):" not in src:
    errs.append("visible_iso() is missing from fetch_shareholding.py")
n_calls = src.count("sub = visible_iso(r)")
if n_calls != 2:
    errs.append("expected 2 ingestion call sites using visible_iso(r) (quarterly + events), found %d" % n_calls)
if re.search(r'sub = iso_date\(r\.get\("submissionDate"\)\)', src):
    errs.append("an ingestion path still assigns the RAW submissionDate")
if not errs:
    sys.path.insert(0, HERE)
    import fetch_shareholding as F
    cases = [("17-JUL-2026 12:24:26", "2026-07-17"),   # before close, trading day -> same day
             ("17-OCT-2025 16:46:27", "2025-10-20"),   # Friday after close -> Monday
             ("28-MAR-2026 20:37:02", "2026-03-30"),   # Saturday -> Monday
             ("01-JUL-2026 17:47:11", "2026-07-02"),   # weekday after close -> next day
             ("25-JAN-2026 10:00:00", "2026-01-27")]   # Sunday before Republic Day -> Tuesday
    for b, exp in cases:
        got = F.visible_iso({"broadcastDate": b, "submissionDate": b[:11]})
        if got != exp:
            errs.append("gate self-test: %s -> %s, expected %s" % (b, got, exp))
if errs:
    for e in errs: print("GATE GUARD FAIL:", e)
    sys.exit(1)
print("gate guard OK: visible_iso present, %d call sites, %d self-test cases pass" % (n_calls, len(cases)))
