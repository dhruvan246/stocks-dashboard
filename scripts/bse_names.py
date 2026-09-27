#!/usr/bin/env python3
"""BSE company names without BSE's "-$" scrip-name marker (DATA_RUNBOOK §204).

BSE serves one company name in two variants and switches between them from one request to the next:
for ~310 active equity scrips one variant ends in "-$" ("UNO Minda Ltd-$"), the other does not
("UNO Minda Ltd"). Measured in the published dashboard feed (docs/dash_slim.bin, one build per day):
16-Jul-2026 312 marked names, 21-Jul 0, 29-Jul 311, 30-Jul 0, ... 25-Sep 0, 27-Sep 310 -- and inside
one day (19-Aug builds 10:09 / 11:36 / 11:57 / 16:21 IST: 310 / 0 / 310 / 0). Carried by
ListofScripData `Scrip_Name` and the announcement API's `SLONGNAME`; `Issuer_Name` never. The marker's
meaning is not established (the 310 span groups A/B/X/T/XT/Z, none SME); no BSE name carries "$"
anywhere else (27-Sep master: 5,043 Active/Equity rows, 310 end in "-$", 0 other "$").

Every place that turns a BSE-served name into a published one goes through clean_scrip_name(), so the
site's names no longer depend on which variant a run happened to receive. Stripping the marker gives
exactly the clean variant's name: 287/287 against the 09-Sep live build, 309/309 against 25-Sep.

  python3 scripts/bse_names.py --check FILE...
      exit 1 when any JSON string in FILE (gzip or plain) still ends in the marker. The builders clean
      with the same predicate, so this only fires when a writer bypasses clean_scrip_name().
"""
import gzip
import sys

MARKER = "-$"


def clean_scrip_name(name):
    """`name` stripped, without a trailing "-$" (the only form measured; nothing else is touched)."""
    s = str(name or "").strip()
    if s.endswith(MARKER):
        s = s[:-len(MARKER)].rstrip()
    return s


def marked_strings(raw):
    """JSON string values in `raw` (bytes) that end in the marker. A `"` right after `$` is always an
    unescaped string end, so counting `-$"` is exact; the value is recovered back to its opening quote."""
    needle = (MARKER + '"').encode()
    out, i = [], raw.find(needle)
    while i >= 0:
        j = i
        while j > 0 and not (raw[j - 1:j] == b'"' and raw[j - 2:j - 1] != b"\\"):
            j -= 1
        out.append(raw[j:i + len(MARKER)].decode("utf-8", "replace"))
        i = raw.find(needle, i + 1)
    return out


def check(paths):
    bad = 0
    for p in paths:
        raw = open(p, "rb").read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        hits = marked_strings(raw)
        if hits:
            bad += 1
            print("BSE NAME MARKER: %s carries %d name(s) ending in %r, e.g. %s — a writer bypassed "
                  "bse_names.clean_scrip_name() (DATA_RUNBOOK §204)" % (p, len(hits), MARKER, hits[:5]))
        else:
            print("bse_names: %s clean (0 names ending in %r)" % (p, MARKER))
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] != "--check":
        sys.exit(__doc__)
    sys.exit(check(sys.argv[2:]))
