# -*- coding: utf-8 -*-
"""
Fetches daily FII/DII activity (cash segment) and appends it to a committed
history file docs/fii_dii.json, so the FII/DII dashboard accumulates history.

Sources (both free, no key):
  - NiftyTrader  webapi/Resource/fii-dii-activity-data
      ~30 trading days of FII net, DII net and the Nifty 50 close + change%.
      This is the history backfill + ongoing trend, and self-extends each run.
  - NSE          api/fiidiiTradeReact
      Latest provisional day with the full BUY / SELL / NET breakdown for both
      FII/FPI and DII (richer than NiftyTrader's net-only). Needs a cookie warm-up.

Merge is by date (YYYY-MM-DD). NSE's buy/sell/net overrides for the latest day;
NiftyTrader supplies net + Nifty for the rest. Existing history is preserved, so
the series only grows. On total fetch failure the old file is left untouched.

Run:  python -X utf8 fetch_fii_dii.py
"""
import os, json, time, datetime, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, "docs", "fii_dii.json")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"


def _get(url, headers=None, jar=None, timeout=30, binary=False):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": UA})
    opener = urllib.request.build_opener()
    if jar is not None:
        opener.add_handler(urllib.request.HTTPCookieProcessor(jar))
    with opener.open(req, timeout=timeout) as r:
        data = r.read()
        return data if binary else data.decode("utf-8", "replace")


def _nse_jar():
    """Warm an NSE cookie jar (the homepage may 403 but still sets the cookie)."""
    import http.cookiejar
    jar = http.cookiejar.CookieJar()
    try:
        _get("https://www.nseindia.com/", headers={"User-Agent": UA, "Accept": "text/html"}, jar=jar, timeout=20)
    except Exception:
        pass
    return jar


def fetch_fo_for_date(dt, jar):
    """
    Fetch F&O participant data for one date (datetime.date):
      - participant-wise OI (net positions, contracts) for FII/DII/Pro/Client
      - FII derivative buy/sell VALUE (Rs cr) per instrument
    Returns a compact dict or None if that day's files aren't available.
    """
    import csv, io
    ddmmyyyy = dt.strftime("%d%m%Y")
    ddmonyyyy = dt.strftime("%d-%b-%Y")
    hdr = {"User-Agent": UA, "Referer": "https://www.nseindia.com/"}
    fo = {}
    # ---- participant-wise OI (net positions) ----
    try:
        raw = _get("https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_%s.csv" % ddmmyyyy,
                   headers=hdr, jar=jar, timeout=25)
        if "Participant" in raw:
            oi = {}
            for row in csv.reader(io.StringIO(raw)):
                if not row or row[0].strip() not in ("Client", "DII", "FII", "Pro"):
                    continue
                v = [int(float(x)) for x in (c.strip() or "0" for c in row[1:15])]
                # cols: 0 FutIdxL 1 FutIdxS 2 FutStkL 3 FutStkS 4 OptIdxCallL 5 OptIdxPutL
                #       6 OptIdxCallS 7 OptIdxPutS 8 OptStkCallL 9 OptStkPutL 10 OptStkCallS
                #       11 OptStkPutS 12 TotLong 13 TotShort
                oi[row[0].strip()] = {"futIdx": [v[0], v[1]], "futStk": [v[2], v[3]],
                                       "totL": v[12], "totS": v[13]}
            if oi:
                fo["oi"] = oi
    except Exception:
        pass
    # ---- FII derivative buy/sell value (Rs cr) ----
    try:
        import xlrd
        blob = _get("https://nsearchives.nseindia.com/content/fo/fii_stats_%s.xls" % ddmonyyyy,
                    headers=hdr, jar=jar, timeout=25, binary=True)
        wb = xlrd.open_workbook(file_contents=blob)
        sh = wb.sheet_by_index(0)
        want = {"INDEX FUTURES": "idxFut", "INDEX OPTIONS": "idxOpt",
                "STOCK FUTURES": "stkFut", "STOCK OPTIONS": "stkOpt"}
        bs = {}
        for r in range(sh.nrows):
            label = str(sh.cell_value(r, 0)).strip().upper()
            if label in want:
                buy = float(sh.cell_value(r, 2) or 0)   # BUY amount (Rs cr)
                sell = float(sh.cell_value(r, 4) or 0)  # SELL amount (Rs cr)
                bs[want[label]] = [round(buy, 2), round(sell, 2)]
        if bs:
            fo["bs"] = bs
    except Exception:
        pass
    return fo or None


def fetch_niftytrader():
    """Return {date: {fiiNet,diiNet,nifty,chg}} or {} on failure."""
    try:
        raw = _get("https://webapi.niftytrader.in/webapi/Resource/fii-dii-activity-data",
                   headers={"User-Agent": UA, "Referer": "https://www.niftytrader.in/"})
        rows = json.loads(raw)["resultData"]["fii_dii_data"]
        out = {}
        for r in rows:
            d = r["created_at"][:10]
            out[d] = {"fiiNet": r.get("fii_net_value"), "diiNet": r.get("dii_net_value"),
                      "nifty": r.get("last_trade_price"), "chg": r.get("change_per")}
        return out
    except Exception as e:
        print("  ! NiftyTrader fetch failed:", e)
        return {}


def fetch_nse():
    """Return {date: {fiiBuy,fiiSell,fiiNet,diiBuy,diiSell,diiNet}} for the latest day, or {}."""
    try:
        import http.cookiejar
        jar = http.cookiejar.CookieJar()
        h = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"}
        try:
            _get("https://www.nseindia.com/reports/fii-dii", headers=h, jar=jar, timeout=20)
        except Exception:
            pass  # the cookie still gets set even if the page 403s
        raw = _get("https://www.nseindia.com/api/fiidiiTradeReact",
                   headers={"User-Agent": UA, "Accept": "application/json",
                            "Referer": "https://www.nseindia.com/reports/fii-dii"}, jar=jar, timeout=25)
        arr = json.loads(raw)
        out = {}
        for r in arr:
            d = datetime.datetime.strptime(r["date"], "%d-%b-%Y").strftime("%Y-%m-%d")
            rec = out.setdefault(d, {})
            who = "fii" if r["category"].startswith("FII") else "dii"
            rec[who + "Buy"] = float(r["buyValue"])
            rec[who + "Sell"] = float(r["sellValue"])
            rec[who + "Net"] = float(r["netValue"])
        return out
    except Exception as e:
        print("  ! NSE fetch failed:", e)
        return {}


def main():
    # load existing history
    hist = {}
    try:
        old = json.load(open(OUT, encoding="utf-8"))
        for row in old.get("rows", []):
            hist[row["date"]] = row
    except Exception:
        pass
    n_before = len(hist)

    nt = fetch_niftytrader()
    nse = fetch_nse()
    if not nt and not nse:
        print("  ! both sources failed — keeping existing history untouched")
        return

    # merge NiftyTrader (net + nifty) first
    for d, v in nt.items():
        row = hist.setdefault(d, {"date": d})
        for k in ("fiiNet", "diiNet", "nifty", "chg"):
            if v.get(k) is not None:
                row[k] = v[k]
    # NSE overrides/extends the latest day with buy/sell/net
    for d, v in nse.items():
        row = hist.setdefault(d, {"date": d})
        row.update(v)

    # ---- F&O backfill: attach participant OI + FII deriv buy/sell to any date missing it ----
    # First run fills recent history (capped); later runs only the newest 1-2 days.
    missing = [d for d in sorted(hist) if "fo" not in hist[d]]
    if missing:
        jar = _nse_jar()
        done = 0
        for d in reversed(missing):          # newest first
            if done >= 30:
                break
            try:
                dt = datetime.datetime.strptime(d, "%Y-%m-%d").date()
                fo = fetch_fo_for_date(dt, jar)
                if fo:
                    hist[d]["fo"] = fo
                    done += 1
                time.sleep(0.4)
            except Exception:
                pass
        print("  F&O attached to %d/%d missing dates" % (done, len(missing)))

    rows = [hist[d] for d in sorted(hist)]
    json.dump({"updated": time.strftime("%Y-%m-%dT%H:%M:%S"), "rows": rows},
              open(OUT, "w", encoding="utf-8"), separators=(",", ":"))
    latest = rows[-1] if rows else {}
    print("  fii_dii.json: %d rows (was %d), latest %s — FII net %s, DII net %s" %
          (len(rows), n_before, latest.get("date"), latest.get("fiiNet"), latest.get("diiNet")))


if __name__ == "__main__":
    main()
