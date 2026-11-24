#!/usr/bin/env python3
"""Download NSE's daily all-indices file (open/high/low/close of EVERY index) into a raw local cache.

Source (measured 2026-11-24): https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv
  columns: Index Name, Index Date, Open/High/Low/Closing Index Value, Points Change, Change(%), Volume,
           Turnover (Rs. Cr.), P/E, P/B, Div Yield
  first file that exists: March 2012 (01-02-2012 and every earlier probe = 404); 31 indices then, 170 now.
  Index NAMES change over the years (S&P CNX Nifty -> CNX Nifty -> Nifty 50) — this script stores the rows
  exactly as published; joining the eras is a separate, evidence-checked step (never guessed here).

This is step 1 only: the RAW files, one per day, byte-for-byte, gzip'd, in a cache dir OUTSIDE the repo
(default ~/stocks-backups/index_ohlc_raw). Nothing under docs/ is written.

Resumable: a day already in the cache is skipped. A 404 is written to _missing.json with a count and is
RETRIED on every later run for weekdays — a transient 404 must never become a permanent "holiday"
(fetch_fo_spot_nse.py recorded 2026-08-24 and 2026-08-28 as false holidays that way, runbook §89).

Usage: python3 scripts/fetch_index_ohlc.py [START=2012-02-01] [END=today] [--cache DIR]
"""
import datetime as dt, gzip, json, os, random, sys, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import special_sessions   # weekend trading sessions (Diwali Muhurat …) are fetched like weekdays

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
URL = "https://nsearchives.nseindia.com/content/indices/ind_close_all_%s.csv"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    cache = os.path.expanduser("~/stocks-backups/index_ohlc_raw")
    if "--cache" in sys.argv:
        cache = sys.argv[sys.argv.index("--cache") + 1]; args = [a for a in args if a != cache]
    start = dt.date.fromisoformat(args[0]) if args else dt.date(2012, 2, 1)
    end = dt.date.fromisoformat(args[1]) if len(args) > 1 else dt.date.today()
    os.makedirs(cache, exist_ok=True)
    mp = os.path.join(cache, "_missing.json")
    missing = json.load(open(mp)) if os.path.exists(mp) else {}
    got = skipped = n404 = nerr = 0
    d = start
    while d <= end:
        iso = d.isoformat()
        if d.weekday() >= 5 and d not in special_sessions.DATES:
            d += dt.timedelta(days=1); continue
        fp = os.path.join(cache, iso + ".csv.gz")
        if os.path.exists(fp):
            skipped += 1; d += dt.timedelta(days=1); continue
        try:
            req = urllib.request.Request(URL % d.strftime("%d%m%Y"), headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=40) as r:
                raw = r.read()
            if not raw.lstrip().startswith(b"Index Name"):
                raise ValueError("200 but not the index CSV (%d bytes)" % len(raw))
            tmp = fp + ".tmp"
            with gzip.open(tmp, "wb") as fh:
                fh.write(raw)
            os.replace(tmp, fp)
            missing.pop(iso, None); got += 1
            time.sleep(0.25 + random.random() * 0.25)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                missing[iso] = missing.get(iso, 0) + 1; n404 += 1
            else:
                nerr += 1; print("%s: HTTP %s" % (iso, e.code), file=sys.stderr)
            time.sleep(0.4)
        except Exception as e:
            nerr += 1; print("%s: %s" % (iso, e), file=sys.stderr); time.sleep(2)
        if (got + n404) and (got + n404) % 200 == 0:
            json.dump(missing, open(mp, "w"), indent=0, sort_keys=True)
            print("...%s downloaded=%d 404=%d errors=%d" % (iso, got, n404, nerr), flush=True)
        d += dt.timedelta(days=1)
    json.dump(missing, open(mp, "w"), indent=0, sort_keys=True)
    have = sorted(f[:10] for f in os.listdir(cache) if f.endswith(".csv.gz"))
    print("DONE downloaded=%d already=%d 404=%d errors=%d | cache holds %d days %s -> %s | %s" % (
        got, skipped, n404, nerr, len(have), have[0] if have else "-", have[-1] if have else "-", cache))


if __name__ == "__main__":
    main()
