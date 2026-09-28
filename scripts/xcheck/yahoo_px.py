"""Independent Yahoo reader for px_nse_yahoo (runbook §214a, user 2026-09-28 "build it").

Since §214a F1 the dashboard serves the NSE-bhavcopy store for every ever-Nifty-500 member, so the dashboard bin is no
longer a second reading of those members' prices — comparing it with the store compares the store with itself. This module
restores the second reader: it fetches Yahoo's .NS DAILY closes directly (chart API, 1996 → now, the split-adjusted
`close` fetch_all serves, never `adjclose`), independent of the refresh pipeline. Daily for the whole span, not fetch_all's
weekly-before-2020, so a disagreement's edges are one session apart and the side that jumped can be named (the weekly
comparison left 2,672 of 2,768 first-run findings side "?").

Request pattern mirrors fetch_all.fetch_chart (curl, the §181 standard headers), 8 workers, 3 tries per ticker (an empty
answer is retried — a transient must not read as "no series"). Cached for 12 h under common.CACHE."""
import concurrent.futures, datetime, gzip, json, os, subprocess, sys, time
from array import array
from . import common as C

START = 820454400                      # 1996-01-01 00:00 UTC, fetch_all's WEEKLY_START_TS on the CI runner
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
CACHE_FILE = "yahoo_daily.json.gz"
TTL = 12 * 3600


def _curl_args():
    try:
        sys.path.insert(0, C.HERE); import bse_headers as BH
        return list(BH.CURL_ARGS)
    except Exception:
        return []


def _one(ticker, end, args):
    """(ticker, [ymd], [close]) or (ticker, None, why)."""
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d"
           % (ticker, START, end))
    why = "empty"
    for attempt in range(3):
        try:
            out = subprocess.run(["curl", "-s", "--max-time", "25", "-A", UA, *args, url],
                                 capture_output=True, timeout=30).stdout
            res = (json.loads(out or b"{}").get("chart") or {}).get("result")
            if res:
                r = res[0]
                ts = r.get("timestamp") or []
                cl = ((r.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
                d, c = [], []
                for t, x in zip(ts, cl):
                    if x is None or x <= 0: continue
                    dd = datetime.datetime.fromtimestamp(t, IST).date()
                    y = dd.year * 10000 + dd.month * 100 + dd.day
                    if d and d[-1] == y: c[-1] = round(x, 4); continue      # a live bar re-stamped on the same day
                    d.append(y); c.append(round(x, 4))
                if d: return ticker, d, c
                why = "no bars"
            else:
                why = "no result"
        except Exception as e:
            why = type(e).__name__
        time.sleep(2 * (attempt + 1))
    return ticker, None, why


def fetch(tickers, workers=8):
    """{ticker: (array ymd, array close)} for `tickers`, plus {ticker: why} for the ones Yahoo gave nothing."""
    os.makedirs(C.CACHE, exist_ok=True)
    path = os.path.join(C.CACHE, CACHE_FILE)
    cache, fetched = {}, time.time()
    try:
        blob = json.loads(gzip.decompress(open(path, "rb").read()))
        if time.time() - blob.get("fetched", 0) < TTL:
            cache, fetched = blob.get("series") or {}, blob["fetched"]     # the OLDEST entry's age governs reuse
    except (OSError, ValueError, KeyError):
        pass
    todo = [t for t in tickers if t not in cache]
    failed = {}
    if todo:
        end, args = int(time.time()), _curl_args()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            for t, d, c in ex.map(lambda t: _one(t, end, args), todo):
                if d is None: failed[t] = c
                else: cache[t] = [d, c]
        tmp = path + ".part"
        open(tmp, "wb").write(gzip.compress(json.dumps({"fetched": fetched, "series": cache},
                                                        separators=(",", ":")).encode()))
        os.replace(tmp, path)
    out = {t: (array("i", cache[t][0]), array("d", cache[t][1])) for t in tickers if t in cache}
    return out, failed
