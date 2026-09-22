#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fill the dashboard price payload (scripts/stock_data.json, built by fetch_all.py from Yahoo)
for every `.NS` ticker Yahoo returned NOTHING for, from the NSE-bhavcopy store
(docs/sf_stock_data.bin release asset). DATA_RUNBOOK §145.

WHY: the dashboard universe now carries NSE's SME platform (SM/ST/SZ) and every main-board
series (EQ/BE/BZ). Yahoo has no chart for most SME tickers (SUNLITE.NS -> "Not Found",
measured 2026-09-22: 21 of 31 sampled) and types the rest MUTUALFUND, which fetch_all rejects
by design. Without this pass those rows would ship metadata-only ("—" prices) forever.

RULE: fill ONLY where Yahoo's series is empty — never mix two sources inside one ticker (the two
adjust splits on different bases). Provenance rides on meta[ticker]["src"] = "nse-bhavcopy".

SHAPE: exactly what fetch_all emits — [[unix_ts, close], ...], WEEKLY closes before 2020-01-01
(one bar per ISO week, stamped on that week's Monday 09:15 IST, the week's LAST close — Yahoo's
1wk convention) and DAILY closes from 2020-01-01 (stamped 09:15 IST = 03:45 UTC, Yahoo's
convention for .NS daily bars, so build_compressed's day offsets line up with Yahoo-sourced rows).

Non-fatal by design in the workflow: if the release asset cannot be fetched the Yahoo-only
payload still ships, and the step prints a ::warning:: so the gap is visible, not silent.

Env: SF_BIN=<path> reads a local bin instead of downloading the release asset (tests).
"""
import os, sys, json, gzip, time, datetime, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD = ROOT / "scripts" / "stock_data.json"
RELEASE_URL = "https://github.com/dhruvan246/stocks-dashboard/releases/download/data/sf_stock_data.bin"
DAILY_FROM = 20200101
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def load_bin():
    p = os.environ.get("SF_BIN")
    if p:
        print("sf-fill: base = local file %s" % p, flush=True)
        return json.loads(gzip.decompress(open(p, "rb").read()))
    last = None
    for attempt in range(3):
        try:
            raw = urllib.request.urlopen(urllib.request.Request(RELEASE_URL, headers={"User-Agent": "Mozilla/5.0"}), timeout=180).read()
            print("sf-fill: base = release asset (%.1f MB)" % (len(raw) / 1048576), flush=True)
            return json.loads(gzip.decompress(raw))
        except Exception as e:
            last = e; print("sf-fill: release fetch attempt %d failed (%s)" % (attempt + 1, e), flush=True); time.sleep(10)
    raise SystemExit("sf-fill: could not fetch the release asset after 3 tries (%s)" % last)


def ts_of(ymd, weekday_monday=False):
    d = datetime.date(ymd // 10000, ymd // 100 % 100, ymd % 100)
    if weekday_monday:
        d = d - datetime.timedelta(days=d.weekday())
    return int(datetime.datetime(d.year, d.month, d.day, 9, 15, tzinfo=IST).timestamp())


def series_from(e):
    """bin entry {d:[ymd], c:[adj close]} -> [[ts, close]] in fetch_all's weekly-then-daily shape."""
    weekly, daily = {}, []          # weekly: (iso year, iso week) -> bar; bars arrive in date order,
    for ymd, c in zip(e["d"], e["c"]):   # so the last assignment per week is that week's LAST close
        if c is None or c <= 0: continue
        if ymd >= DAILY_FROM:
            daily.append([ts_of(ymd), round(float(c), 2)])
        else:
            dt = datetime.date(ymd // 10000, ymd // 100 % 100, ymd % 100)
            weekly[dt.isocalendar()[:2]] = [ts_of(ymd, weekday_monday=True), round(float(c), 2)]
    out = list(weekly.values()) + daily
    out.sort(key=lambda b: b[0])
    return out


def main():
    payload = json.loads(PAYLOAD.read_text())
    meta, series = payload["meta"], payload["series"]
    todo = [t for t in meta if t.endswith(".NS") and not series.get(t)]
    print("sf-fill: %d .NS tickers with no Yahoo series (of %d)" % (len(todo), len(meta)), flush=True)
    if not todo:
        print("sf-fill: nothing to fill"); return
    D = load_bin()
    data = D.get("data") or {}
    filled, absent, short = 0, [], []
    for t in todo:
        sym = (meta[t].get("symbol") or t[:-3]).upper()
        e = data.get(sym)
        if not e or not e.get("d"):
            absent.append(sym); continue
        ser = series_from(e)
        if len(ser) < 2:
            short.append(sym); continue
        series[t] = ser
        meta[t]["src"] = "nse-bhavcopy"          # provenance: not a Yahoo series
        filled += 1
    payload["series"] = series
    PAYLOAD.write_text(json.dumps(payload, separators=(",", ":")))
    print("sf-fill: filled %d series from the bhavcopy store (bin end %s); %d symbols not in the store, %d too short"
          % (filled, D.get("end"), len(absent), len(short)), flush=True)
    if absent: print("  not in store (first 40): %s" % ", ".join(sorted(absent)[:40]))
    if short: print("  too short: %s" % ", ".join(sorted(short)[:40]))
    print("sf-fill: payload now %d tickers with prices of %d" % (sum(1 for t in meta if series.get(t)), len(meta)), flush=True)


if __name__ == "__main__":
    main()
