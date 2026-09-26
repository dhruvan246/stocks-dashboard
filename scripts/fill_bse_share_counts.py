#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Share counts for BSE-ONLY dashboard rows whose BSE scrip-master market cap is blank or zero.
DATA_RUNBOOK §145.

WHY: every BSE-only market cap on the dashboard is BSE's own `Mktcap` from ListofScripData
(fetch_all.py). For a scrip that has not TRADED in years BSE leaves it blank or 0.00 — measured
2026-09-23 on six scrips (TVOLCON, BENTCOM, ESQRMON, PUNCTRD, ZJEETMAC, PETPLST): BSE's per-stock
StockTrading API also says MktCapFull "-" / 0.00, the BSE bhavcopy has no row for them, and the
Yahoo series only repeats one frozen close (their last trade: 2001-2011). They are still LISTED and
still file a shareholding pattern with BSE every quarter, so the company's own filing carries a real
share count; market cap = that count x the last traded close (build_compressed.py), and the page
labels the row "not traded since <date>".

⚠️ Do NOT fill these from screener.in: for all six it shows Market Cap (Cr) == Current Price (Rs),
i.e. a placeholder of exactly 1 crore shares, and prices that are not the last trade (TVOLCON 10 vs
12, ESQRMON 10 vs 5.50, PUNCTRD 5.50 vs 1.00). The filed counts are 5-22 LAKH shares.

HOW: for each `.BO` row in scripts/stock_data.json with no mcap and a price series, resolve its BSE
scrip code (numeric ticker, else scrip_id -> SCRIP_CD from /tmp/bse.json), list its filings via
SHPQNewFormat, take the newest one's XBRL and read the total with fetch_shareholding.parse_shares
(the ONE share-count parser the NSE passes use). Results go to scripts/shares_bse_only.json, keyed by
the dashboard TICKER (never a bare symbol: a BSE scrip_id can equal an unrelated NSE symbol, §76).
A cached entry younger than MAX_AGE_DAYS is reused, so a normal run makes no BSE calls.

Run (refresh.yml does this after fetch_all.py; non-fatal):
  python3 scripts/fill_bse_share_counts.py            # uses /tmp/bse.json + scripts/stock_data.json
  python3 scripts/fill_bse_share_counts.py --tickers PETPLST.BO,TVOLCON.BO
"""
import os as _o, sys as _s; _s.path.insert(0, _o.path.dirname(_o.path.abspath(__file__))); import bse_headers as BH  # §181 BSE headers
import os, sys, json, time, datetime, subprocess
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
_argv = sys.argv; sys.argv = _argv[:1]
import fetch_shareholding as FS           # parse_shares — shared with the NSE passes
sys.argv = _argv

PAYLOAD = os.path.join(HERE, "stock_data.json")
BSE_JSON = "/tmp/bse.json"
LEDGER = os.path.join(HERE, "shares_bse_only.json")
MAX_AGE_DAYS = 100                           # one quarter's filing season, plus slack


def curl(url):
    r = subprocess.run(["curl", "-s", "--max-time", "40", "-A", BH.UA, *BH.CURL_ARGS, url],
                       capture_output=True, timeout=60)
    return r.stdout


def load_ledger():
    try:
        return json.load(open(LEDGER, encoding="utf-8"))
    except Exception:
        return {"_doc": "BSE-only share counts from each scrip's own newest BSE shareholding-pattern "
                        "XBRL, keyed by dashboard ticker. Written by scripts/fill_bse_share_counts.py; "
                        "read by build_compressed.py for .BO rows with no BSE market cap. DATA_RUNBOOK §145.",
                "fills": {}}


def main():
    only = None
    if "--tickers" in sys.argv:
        only = {t.strip() for t in sys.argv[sys.argv.index("--tickers") + 1].split(",") if t.strip()}
    led = load_ledger(); fills = led.setdefault("fills", {})
    try:
        payload = json.load(open(PAYLOAD, encoding="utf-8"))
    except Exception as e:
        sys.exit("fill_bse_share_counts: %s unreadable (%s)" % (PAYLOAD, e))
    meta, series = payload["meta"], payload["series"]
    sid2code = {}
    try:
        for b in json.load(open(BSE_JSON, encoding="utf-8")):
            sid, code = (b.get("scrip_id") or "").strip(), (b.get("SCRIP_CD") or "").strip()
            if sid and code: sid2code[sid] = code
    except Exception as e:
        print("fill_bse_share_counts: %s unreadable (%s) — numeric tickers only" % (BSE_JSON, e))
    today = datetime.date.today()
    todo = [t for t, m in meta.items() if t.endswith(".BO") and not m.get("mcap")
            and (series.get(t) or (m.get("lastTrade") or {}).get("p"))      # a series, or BSE's last trade (§145)
            and (only is None or t in only)]
    fresh = fetched = failed = 0
    for t in todo:
        cur = fills.get(t) or {}
        try:
            age = (today - datetime.date.fromisoformat(cur.get("checked", "1900-01-01"))).days
        except ValueError:
            age = 9999
        if cur.get("shares") and age <= MAX_AGE_DAYS:
            fresh += 1; continue
        base = t[:-3]
        code = base if base.isdigit() else sid2code.get(base)
        if not code:
            print("  %s: no BSE scrip code — skipped" % t); failed += 1; continue
        try:
            rows = (json.loads(curl("https://api.bseindia.com/BseIndiaAPI/api/SHPQNewFormat/w?scripcode=%s" % code))
                    .get("Table") or [])
            rows = [r for r in rows if r.get("XbrlFile")]
            if not rows:
                print("  %s (%s): no shareholding filing with an XBRL" % (t, code)); failed += 1; continue
            r = rows[0]                                     # BSE lists newest first
            url = "https://www.bseindia.com/XBRLFILES/SHPXBRLDataXML/" + r["XbrlFile"]
            n = FS.parse_shares(ET.fromstring(curl(url)))
        except Exception as e:
            print("  %s (%s): fetch/parse failed (%r)" % (t, code, e)); failed += 1; continue
        if not n:
            print("  %s (%s): filing carries no share count" % (t, code)); failed += 1; continue
        fills[t] = {"shares": n, "code": code, "qtr": r.get("qtr"),
                    "filed": (r.get("revised_date_time") or r.get("filing_date_time") or "")[:10],
                    "xbrl": url, "checked": today.isoformat()}
        fetched += 1
        print("  %s (%s): %s shares, %s filing" % (t, code, format(n, ","), r.get("qtr")))
        time.sleep(1.2)                                     # BSE rate-limits by IP (see refresh.yml)
    json.dump(led, open(LEDGER, "w", encoding="utf-8"), indent=1, sort_keys=True)
    print("fill_bse_share_counts: %d cap-less BSE rows — %d cached, %d fetched, %d without a count"
          % (len(todo), fresh, fetched, failed))


if __name__ == "__main__":
    main()
