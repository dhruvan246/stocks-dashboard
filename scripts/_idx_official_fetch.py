# -*- coding: utf-8 -*-
"""Fetch EVERY official archived NSE/niftyindices sub-index constituent CSV.

These are the ONLY ground-truth point-in-time memberships for the 8 broad tiers
(+ Nifty Bank since 2026-09-21, runbook section 141a: its 2017-2020 rosters were
scrapbook junk — PAYTM, KINDIA — because it had no pins and no pre-2021 events).
CDX-enumerate all 200-status captures for each tier's niftyindices slug AND its
CNX-era predecessor, fetch+parse each, plus grab the live current list.

Writes _idx_official_snaps.json = {tier: {YYYYMMDD: [symbols]}} (symbols direct,
authoritative, no name resolution needed).

Run:  python3 _idx_official_fetch.py                 # every tier, rewrites the file
      python3 _idx_official_fetch.py --only "Nifty Bank"   # one tier, MERGED into the existing file
"""
import csv, gzip, io, json, os, sys, time, urllib.request

from curl_cffi import requests as cr

UA = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "identity"}

# tier -> list of (host_path) to try; niftyindices current + CNX predecessor + nse archives
TIERS = {
    "Nifty 100": ["niftyindices.com/IndexConstituent/ind_nifty100list.csv"],
    "Nifty Midcap 100": ["niftyindices.com/IndexConstituent/ind_niftymidcap100list.csv",
                          "nseindia.com/content/indices/ind_cnxmidcaplist.csv"],
    "Nifty Midcap 50": ["niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv",
                        "nseindia.com/content/indices/ind_cnxmidcap50list.csv"],
    "Nifty Smallcap 100": ["niftyindices.com/IndexConstituent/ind_niftysmallcap100list.csv",
                          "nseindia.com/content/indices/ind_cnxsmallcaplist.csv"],
    "Nifty Midcap 150": ["niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv"],
    "Nifty Smallcap 250": ["niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"],
    "Nifty Smallcap 50": ["niftyindices.com/IndexConstituent/ind_niftysmallcap50list.csv"],
    "Nifty LargeMidcap 250": ["niftyindices.com/IndexConstituent/ind_niftylargemidcap250list.csv"],
    "Nifty MidSmallcap 400": ["niftyindices.com/IndexConstituent/ind_niftymidsmallcap400list.csv"],
    # sector index: 12 names 2006-2025, 14 from 2025-12-31 (ind_prs01122025). CNX-era captures
    # 2006-2015 carry the then-tickers (UTIBANK, ORIENTBANK, CORPBANK, SYNDIBANK ...) - the
    # membership builder's canon()/era_key handles them like every other tier.
    "Nifty Bank": ["niftyindices.com/IndexConstituent/ind_niftybanklist.csv",
                   "nseindia.com/content/indices/ind_cnxbanklist.csv",
                   "nseindia.com/content/indices/ind_niftybanklist.csv"],
    # 2026-09-21 (runbook section 141b): the flagship's own history began 2015-09-28; the S&P CNX
    # Nifty-era file (ind_niftylist.csv, 2006-2020) + the nifty50list captures pin it 2006-2026.
    "Nifty 50": ["niftyindices.com/IndexConstituent/ind_nifty50list.csv",
                 "nseindia.com/content/indices/ind_niftylist.csv",
                 "nseindia.com/content/indices/ind_nifty50list.csv",
                 "archives.nseindia.com/content/indices/ind_nifty50list.csv"],
}
# a capture must carry at least this many names to count as a full list (guards truncated pages)
MIN_NAMES = {"Nifty Bank": 10, "Nifty 50": 45}
DEFAULT_MIN = 30
# live current filenames (niftyindices serves these)
LIVE = {t: "https://www.niftyindices.com/IndexConstituent/ind_%slist.csv"
        % t.lower().replace("nifty ", "nifty").replace(" ", "")
        for t in TIERS}


def cdx(path):
    q = ("https://web.archive.org/cdx/search/cdx?url=%s&output=json"
         "&filter=statuscode:200&collapse=digest" % path)
    for _ in range(3):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(q, headers=UA), timeout=60))[1:]
        except Exception:
            time.sleep(4)
    return []


def parse_csv(txt):
    """Symbols from an NSE constituent CSV. The column is located from the header row when one
    exists ("Symbol"): the 2006-era CNX lists are (Company Name, Symbol, Series) with a title row
    above, the later ones (Company Name, Industry, Symbol, Series, ISIN). Falls back to column 2."""
    rows = list(csv.reader(io.StringIO(txt)))
    si = 2
    for row in rows:
        cells = [c.strip().lower() for c in row]
        if "symbol" in cells:
            si = cells.index("symbol"); break
    syms = []
    for row in rows:
        if len(row) <= si: continue
        v = row[si].strip()
        if not v or v.lower() in ("symbol", "series", "eq") or row[0].strip() in ("", "Company Name"):
            continue
        if row[0].strip().lower().startswith("constituents"):
            continue
        syms.append(v)
    return syms


def read_capture(ts, orig):
    raw = urllib.request.urlopen(
        urllib.request.Request("https://web.archive.org/web/%sid_/%s" % (ts, orig), headers=UA),
        timeout=45).read()
    if raw[:2] == b"\x1f\x8b":            # some captures are stored gzip-encoded (2026-08-27 bank list)
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", "replace")


OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_idx_official_snaps.json")
only = None
if len(sys.argv) >= 3 and sys.argv[1] == "--only":
    only = sys.argv[2:]
    bad = [t for t in only if t not in TIERS]
    if bad:
        raise SystemExit("unknown tier(s) %s - known: %s" % (bad, ", ".join(TIERS)))

try:
    _prev = json.load(open(OUT))
except Exception:
    _prev = {}
snaps = {}
for tier, paths in TIERS.items():
    if only and tier not in only:
        continue
    # --only starts from the tier's committed captures so a Wayback timeout on this run never
    # drops a list that was fetched fine before (measured 2026-09-21: 3 of 30 Nifty 50 captures
    # came back empty on one run and parsed on the next)
    snaps[tier] = dict(_prev.get(tier, {})) if only else {}
    mn = MIN_NAMES.get(tier, DEFAULT_MIN)
    for path in paths:
        for ts, orig in [(r[1], r[2]) for r in cdx(path)]:
            d = ts[:8]
            if d in snaps[tier]:
                continue
            try:
                raw = read_capture(ts, orig)
            except Exception:
                continue
            s = parse_csv(raw)
            if len(s) >= mn:
                snaps[tier][d] = s
    # live current
    try:
        raw = cr.get(LIVE[tier], impersonate="chrome", timeout=30, headers={"Accept-Encoding": "identity"}).text
        s = parse_csv(raw)
        if len(s) >= mn:
            snaps[tier]["LIVE"] = s
    except Exception:
        pass
    dates = sorted(snaps[tier])
    print("%-24s %2d snapshots: %s" % (tier, len(snaps[tier]),
                                       ", ".join("%s(%d)" % (d, len(snaps[tier][d])) for d in dates)), flush=True)

if only:                                   # merge the refreshed tier(s) into the committed file
    _prev.update(snaps)
    snaps = _prev
json.dump(snaps, open(OUT, "w"))
print("\nwrote %s (%d tiers)" % (OUT, len(snaps)))
