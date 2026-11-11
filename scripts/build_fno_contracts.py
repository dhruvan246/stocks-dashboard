#!/usr/bin/env python3
"""docs/fno_contracts.json — every NSE stock future of the latest trading day: its contract name (= Zerodha's
tradingsymbol, e.g. CANBK26NOVFUT), expiry, lot size, settlement and close price. Read by the terminal's F&O tab
(user 2026-11-11) to size orders in whole lots and to roll a contract into the next month — lots can change between
months (BHEL Nov 2,625 → Jan 1,250, measured in the 2026-11-09 bhavcopy), so the next month's own lot is used.
Source: the NSE UDiFF F&O bhavcopy (same file scripts/build_fno_up3.py reads). Looks back up to 10 days for the
latest file; leaves the file unchanged when nothing newer than its own date is out."""
import csv, datetime as dt, io, json, os, sys, time, urllib.error, urllib.request, zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "fno_contracts.json")
FO_URL = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{}_F_0000.csv.zip"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")


def fetch(day):
    err = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(FO_URL.format(day.strftime("%Y%m%d")),
                                         headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"})
            z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(req, timeout=60).read()))
            return z.read(z.namelist()[0]).decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            err = e
        except Exception as e:
            err = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{day}: NSE F&O fetch failed: {err}")


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def main():
    old = json.load(open(OUT)) if os.path.exists(OUT) else {}
    today = dt.date.today()
    for back in range(11):
        day = today - dt.timedelta(back)
        if old.get("asof") and day.isoformat() <= old["asof"]:
            print(f"nothing newer than {old['asof']} -- file unchanged")
            return
        txt = fetch(day)
        if txt:
            break
    else:
        print("no F&O bhavcopy in the last 10 days -- file unchanged")
        return
    fut = {}
    for r in csv.DictReader(io.StringIO(txt)):
        if r.get("FinInstrmTp") != "STF":
            continue
        lot = num(r.get("NewBrdLotQty"))
        if not lot or lot <= 0:
            continue
        fut.setdefault(r["TckrSymb"].strip(), []).append(
            [r["FinInstrmNm"].strip(), r["XpryDt"].strip(), int(lot), num(r.get("SttlmPric")), num(r.get("ClsPric")),
             num(r.get("UndrlygPric"))])
    if len(fut) < 50:
        sys.exit(f"{day}: only {len(fut)} stock futures -- not a real F&O bhavcopy, file unchanged")
    for v in fut.values():
        v.sort(key=lambda c: c[1])
    doc = {"asof": day.isoformat(),
           "source": "NSE UDiFF F&O bhavcopy of that day, stock futures (STF) only",
           "cols": ["tradingsymbol", "expiry", "lot", "settle", "close", "underlying"],
           "f": dict(sorted(fut.items()))}
    with open(OUT + ".tmp", "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    os.replace(OUT + ".tmp", OUT)
    print(f"{day}: {len(fut)} underlyings, {sum(len(v) for v in fut.values())} contracts")


if __name__ == "__main__":
    main()
