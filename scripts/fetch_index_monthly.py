#!/usr/bin/env python3
"""Daily top-up for docs/index_monthly.json (Monthly Returns page).

The feed holds month-end closes for ~32 NSE indices back to each index's base
date (one-time backfill from niftyindices.com's historical archive — that
endpoint is bot-walled for scripts, so the backfill was done via a browser
page-context XHR; see DATA_RUNBOOK §27). This script only maintains the
CURRENT month: it reads NSE's live index watch JSON (CDN-hosted, no bot wall)
and writes each index's latest close into its (year, month) cell, so the
in-progress month shows as a live MTD value and becomes the final month-end
close after the last trading day's run.

Safety properties:
  - The target (year, month) comes from the feed's own `timeVal` timestamp
    ("16-Jul-2026 15:30"), NEVER the wall clock — a morning catch-up run on the
    1st therefore still writes the PREVIOUS month's close into the previous
    month's cell instead of poisoning the new month.
  - If the live fetch fails entirely, the file is left untouched (exit 0) so
    history is never lost; guard_feed.py separately blocks shrunken commits.
  - Only the current cell is ever written — history cells never change here.
"""
import json
import os
import sys
import urllib.request
import datetime as dt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEED = os.path.join(ROOT, "docs", "index_monthly.json")
LIVE_URL = "https://liveindexsa.niftyindices.com/jsonfiles/LiveIndicesWatch.json"

# Feed keys use niftyindices HISTORICAL names; the live watch spells a few differently.
ALIAS = {
    "NIFTY SMALLCAP 100": "NIFTY SMLCAP 100",
    "NIFTY SMALLCAP 250": "NIFTY SMLCAP 250",
    "NIFTY MICROCAP 250": "NIFTY MICROCAP250",
    "NIFTY PRIVATE BANK": "NIFTY PVT BANK",
    "NIFTY INDIA DEFENCE": "NIFTY IND DEFENCE",
    "NIFTY INDIA CONSUMPTION": "NIFTY CONSUMPTION",
}
MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


def main():
    feed = json.load(open(FEED, encoding="utf-8"))

    try:
        req = urllib.request.Request(LIVE_URL, headers={"User-Agent": "Mozilla/5.0"})
        rows = json.load(urllib.request.urlopen(req, timeout=60))["data"]
    except Exception as e:  # fail-soft: keep yesterday's feed rather than break the page
        print("live fetch failed, feed left untouched:", repr(e))
        return 0

    live = {str(r.get("indexName", "")).strip(): r for r in rows}
    updated, asof = 0, None
    for idx in feed["indices"]:
        r = live.get(idx["key"]) or live.get(ALIAS.get(idx["key"], ""))
        if not r:
            print("WARN: no live row for", idx["key"])
            continue
        try:
            val = float(str(r["last"]).replace(",", ""))
        except (KeyError, ValueError):
            continue
        # "16-Jul-2026 15:30" -> the session this value belongs to
        tv = str(r.get("timeVal", "")).strip()
        try:
            day_s, mon_s, rest = tv.split("-")
            yr, mo, day = int(rest.split()[0]), MONTHS[mon_s], int(day_s)
        except Exception:
            print("WARN: bad timeVal for", idx["key"], repr(tv))
            continue
        if val <= 0 or yr < 2020:
            continue
        arr = idx["closes"].setdefault(str(yr), [None] * 12)
        arr[mo - 1] = round(val, 2)
        updated += 1
        dstr = "%04d-%02d-%02d" % (yr, mo, day)
        asof = dstr if asof is None or dstr > asof else asof

    if not updated:
        print("nothing updated (no joinable live rows)")
        return 0

    feed["asof"] = asof or feed.get("asof")
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    feed["updated"] = dt.datetime.now(ist).strftime("%Y-%m-%dT%H:%M:%S+05:30")
    json.dump(feed, open(FEED, "w", encoding="utf-8"), separators=(",", ":"))
    print("updated %d indices; asof %s" % (updated, feed["asof"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
