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


def _get(url, headers=None, jar=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": UA})
    opener = urllib.request.build_opener()
    if jar is not None:
        opener.add_handler(urllib.request.HTTPCookieProcessor(jar))
    with opener.open(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


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

    rows = [hist[d] for d in sorted(hist)]
    json.dump({"updated": time.strftime("%Y-%m-%dT%H:%M:%S"), "rows": rows},
              open(OUT, "w", encoding="utf-8"), separators=(",", ":"))
    latest = rows[-1] if rows else {}
    print("  fii_dii.json: %d rows (was %d), latest %s — FII net %s, DII net %s" %
          (len(rows), n_before, latest.get("date"), latest.get("fiiNet"), latest.get("diiNet")))


if __name__ == "__main__":
    main()
