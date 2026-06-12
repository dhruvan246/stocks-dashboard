# -*- coding: utf-8 -*-
"""
FULL survivorship-free price+turnover database from NSE daily bhavcopies.

  * Fetches DAILY (one bhavcopy = all stocks for that day) from START..today, so
    splits/bonuses adjust correctly via NSE's corporate-action-adjusted PREV_CLOSE
    (we chain daily returns close/prev_close into a clean adjusted index).
  * STORES weekly samples before DAILY_FROM (deep history, small) and daily after.
  * Includes every stock that ever traded — delisted ones too (kills survivorship bias).

Resumable: caches each day's parsed rows under scripts/_bhav_cache/ so re-runs skip
already-downloaded days. Output: docs/sf_stock_data.bin (gzip JSON the backtest fetches).

Run:  python -X utf8 build_sf_data.py [START=1996-01-01] [DAILY_FROM=2018-01-01]
"""
import os, sys, io, csv, json, gzip, time, zipfile, datetime, urllib.request, http.cookiejar

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CACHE = os.path.join(HERE, "_bhav_cache"); os.makedirs(CACHE, exist_ok=True)
OUT = os.path.join(ROOT, "docs", "sf_stock_data.bin")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
MON = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

START = datetime.date(1996, 1, 1)
DAILY_FROM = datetime.date(2018, 1, 1)
if len(sys.argv) > 1: START = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
if len(sys.argv) > 2: DAILY_FROM = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
END = datetime.date.today()


def jar():
    j = http.cookiejar.CookieJar()
    try:
        op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(j))
        op.open(urllib.request.Request("https://www.nseindia.com/", headers={"User-Agent": UA}), timeout=20).read()
    except Exception:
        pass
    return j


def get(url, j, timeout=30):
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(j))
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"})
    with op.open(req, timeout=timeout) as r:
        return r.read()


def parse_rows(text):
    rows = list(csv.reader(io.StringIO(text)))
    if not rows: return []
    hdr = [h.strip().upper() for h in rows[0]]
    def idx(*ns):
        for n in ns:
            if n in hdr: return hdr.index(n)
        return -1
    iS, iSer = idx("SYMBOL"), idx("SERIES")
    iC, iP, iT = idx("CLOSE_PRICE", "CLOSE"), idx("PREV_CLOSE", "PREVCLOSE"), idx("TURNOVER_LACS", "TOTTRDVAL")
    iH, iL = idx("HIGH_PRICE", "HIGH"), idx("LOW_PRICE", "LOW")
    if iS < 0 or iC < 0: return []
    out = []
    for r in rows[1:]:
        if len(r) <= max(iS, iC): continue
        if (r[iSer].strip() if iSer >= 0 else "EQ") not in ("EQ", "BE"): continue
        try:
            c = float(r[iC]); p = float(r[iP]) if iP >= 0 and r[iP].strip() else 0.0
            t = float(r[iT]) if iT >= 0 and r[iT].strip() else 0.0
            h = float(r[iH]) if iH >= 0 and r[iH].strip() else c
            l = float(r[iL]) if iL >= 0 and r[iL].strip() else c
        except ValueError:
            continue
        if c > 0: out.append([r[iS].strip(), c, p, t, h, l])
    return out


def fetch_day(d, j):
    cf = os.path.join(CACHE, d.strftime("%Y%m%d") + ".json")
    if os.path.exists(cf):
        try:
            rows = json.load(open(cf))
            # v1 cache rows lack high/low (4 cols) — refetch those days; holiday [] is reusable
            if not rows or len(rows[0]) >= 6:
                return rows
        except Exception: pass
    ddmmyyyy = d.strftime("%d%m%Y")
    new = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_%s.csv" % ddmmyyyy
    old = "https://nsearchives.nseindia.com/content/historical/EQUITIES/%d/%s/cm%02d%s%dbhav.csv.zip" % (
        d.year, MON[d.month-1], d.day, MON[d.month-1], d.year)
    for url in ([new, old] if d.year >= 2020 else [old, new]):
        try:
            blob = get(url, j)
            text = (zipfile.ZipFile(io.BytesIO(blob)).read(zipfile.ZipFile(io.BytesIO(blob)).namelist()[0]).decode("utf-8","replace")
                    if url.endswith(".zip") else blob.decode("utf-8","replace"))
            if "SYMBOL" in text[:200].upper():
                rows = parse_rows(text)
                json.dump(rows, open(cf, "w"))
                return rows
        except Exception:
            continue
    json.dump([], open(cf, "w"))   # cache the miss (holiday) so we don't refetch
    return []


def main():
    j = jar(); acc = {}; d = START; tried = got = 0
    while d <= END:
        if d.weekday() < 5:
            tried += 1
            rows = fetch_day(d, j)
            if rows:
                got += 1; ymd = int(d.strftime("%Y%m%d"))
                for row in rows:
                    sym, c, p, t = row[0], row[1], row[2], row[3]
                    h = row[4] if len(row) > 4 else c
                    l = row[5] if len(row) > 5 else c
                    acc.setdefault(sym, []).append((ymd, c, p, t, h, l))
            if tried % 250 == 0:
                print("  ...%s  days=%d/%d  symbols=%d" % (d, got, tried, len(acc)), flush=True)
                j = jar()
            time.sleep(0.30)
        d += datetime.timedelta(days=1)
    print("Fetched %d/%d trading days; %d symbols" % (got, tried, len(acc)), flush=True)

    cur = {}
    try:
        import re, base64
        h = open(os.path.join(ROOT, "docs", "nse-bse-dashboard.html"), encoding="utf-8").read()
        b64 = re.search(r'<script id="compressedData"[^>]*>([A-Za-z0-9+/=]+)</script>', h).group(1)
        D = json.loads(gzip.decompress(base64.b64decode(b64)))
        for m in D["meta"].values():
            cur[m["symbol"]] = {"name": m.get("name"), "industry": m.get("industry") or m.get("sector")}
    except Exception as e:
        print("  (current meta unavailable:", e, ")")

    df = int(DAILY_FROM.strftime("%Y%m%d"))
    data, meta, dead = {}, {}, 0
    for sym, obs in acc.items():
        obs.sort(); ds, cs, ts, hb, lb = [], [], [], [], []; adj = None; lastWeek = None
        for i, (ymd, c, p, t, h, l) in enumerate(obs):
            adj = c if adj is None else adj * ((c / p) if (p and p > 0) else (c / obs[i-1][1] if obs[i-1][1] else 1.0))
            if ymd >= df:
                keep = True                              # daily for recent
            else:
                wk = datetime.date(ymd//10000, ymd//100 % 100, ymd % 100).isocalendar()[:2]
                keep = (wk != lastWeek); lastWeek = wk   # weekly for old
            if keep:
                ds.append(ymd); cs.append(round(adj, 2)); ts.append(round(t, 1))
                # intraday high/low as per-mil offsets from close (split-adjustment cancels in the ratio)
                hb.append(max(0, round((h / c - 1) * 1000)) if h >= c else 0)
                lb.append(max(0, round((1 - l / c) * 1000)) if l <= c else 0)
        if len(ds) < 12: continue
        data[sym] = {"d": ds, "c": cs, "t": ts, "hb": hb, "lb": lb}
        alive = sym in cur
        dead += (not alive)
        meta[sym] = {"name": (cur.get(sym) or {}).get("name") or sym,
                     "ind": (cur.get(sym) or {}).get("industry") or "Unknown", "alive": alive}
    print("Stored %d symbols (%d delisted/absent today)" % (len(data), dead), flush=True)
    blob = gzip.compress(json.dumps({"start": START.isoformat(), "dailyFrom": DAILY_FROM.isoformat(),
                                     "end": END.isoformat(), "meta": meta, "data": data},
                                    separators=(",", ":")).encode(), 6)
    open(OUT, "wb").write(blob)
    print("Wrote %s (%.2f MB)" % (OUT, len(blob)/1048576), flush=True)


if __name__ == "__main__":
    main()
