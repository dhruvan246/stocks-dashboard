#!/usr/bin/env python3
"""FII definition guard (DATA_RUNBOOK §151) — runs in refresh-shareholding.yml before any fetch.

Rule (user, 2026-09-24): FII = the filing's foreign-institution lines WITHOUT the "Overseas
Depositories (holding DRs)" custodian line, in every format — the Screener/Trendlyne convention.
The 2022+ SEBI format prints that line inside Institutions (Foreign); taking the bucket total made
every DR issuer jump at Sep-2022 (DRREDDY 25.9 -> 36.3, UPL +6.3, TMPV +5.3, GRASIM +4.4, ...).

Fixtures are the real BSE XBRLs for Dr Reddy's (500124):
  2022-09-30 (new format): FPI I 25.70 + FPI II 0.56, Overseas Depositories 10.08, bucket total 36.34
  2022-06-30 (old format): FPI row 25.87, depositories line outside the FII rows
Exit 1 unless the parser returns the FPI-only figure for both.
"""
import gzip, os, sys
import xml.etree.ElementTree as ET
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.argv = [sys.argv[0]]
import fetch_shareholding as FS

def parse(name, qe):
    p = os.path.join(ROOT, "scripts", "tests", name)
    with gzip.open(p, "rb") as fh:
        return FS.parse_shp(ET.fromstring(fh.read()), qe)

fails = []
if FS.MEMBERS.get("OverseasDepositoriesMember") != "od":
    fails.append("MEMBERS no longer maps OverseasDepositoriesMember -> 'od' (§151)")
new = parse("shp_fixture_DRREDDY_2022-09-30.xml.gz", "2022-09-30") or {}
old = parse("shp_fixture_DRREDDY_2022-06-30.xml.gz", "2022-06-30") or {}
f_new, f_old = new.get("fii"), old.get("fii")
if f_new is None or not (26.15 <= f_new <= 26.40):
    fails.append("new-format fixture fii=%s, expected the FPI-only 26.26 (bucket total 36.34 = depositories included)" % f_new)
if not new.get("od") or not (9.9 <= new["od"] <= 10.3):
    fails.append("new-format fixture did not expose od≈10.08 (got %s)" % new.get("od"))
if f_old is None or not (25.75 <= f_old <= 26.00):
    fails.append("old-format fixture fii=%s, expected 25.87 (old format must be untouched)" % f_old)
if fails:
    for f in fails: print("GUARD FAIL:", f)
    sys.exit(1)
print("guard_shp_definition OK: new-format fii=%.4f (od %.4f removed), old-format fii=%.4f" % (f_new, new["od"], f_old))
