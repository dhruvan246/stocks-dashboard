"""Shared BSE fetch helpers for the ideas tool (scripts/ideas/*).

Self-contained on purpose: the daily routine runs on a fresh cloud VM and must not depend on
anything outside this folder except the Python standard library.
All calls go to public BSE endpoints that were verified to work on 2026-09-22:
  - scrip master   : api.bseindia.com/BseIndiaAPI/api/ListofScripData/w  (mcap in Rs crore)
  - daily bhavcopy : www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_YYYYMMDD_F_0000.CSV
  - announcements  : api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w (50 rows/page)
  - price history  : api.bseindia.com/BseIndiaAPI/api/StockPriceCSVDownload/w
  - corp actions   : api.bseindia.com/BseIndiaAPI/api/CorporateAction/w
  - attachment     : www.bseindia.com/xml-data/corpfiling/AttachLive/<ATTACHMENTNAME>
"""
import csv, io, json, os, time, datetime, urllib.request, urllib.parse

UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/128.0 Safari/537.36')
HDR = {'User-Agent': UA, 'Referer': 'https://www.bseindia.com/', 'Origin': 'https://www.bseindia.com',
       'Accept': 'application/json, text/plain, */*'}
CACHE = os.environ.get('IDEAS_CACHE', os.path.join(os.path.dirname(os.path.abspath(__file__)), '_cache'))
os.makedirs(CACHE, exist_ok=True)


def _get(url, timeout=60, retries=3, sleep=0.6):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HDR)
            with urllib.request.urlopen(req, timeout=timeout) as f:
                data = f.read()
            time.sleep(sleep)
            return data
        except Exception as e:  # noqa
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f'GET failed {url}: {last}')


def scrip_master():
    """All active BSE equity scrips with market cap (Rs crore). Cached for the day."""
    fn = os.path.join(CACHE, f'master_{datetime.date.today():%Y%m%d}.json')
    if os.path.exists(fn):
        return json.load(open(fn))
    data = _get('https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active')
    rows = json.loads(data)
    json.dump(rows, open(fn, 'w'))
    return rows


def bhavcopy(d):
    """BSE equity bhavcopy for date d (datetime.date) -> list of dict rows, or None if not published.
    Rows: scrip (str), isin, symbol, series, name, open, high, low, close, prev_close, volume, turnover, trades."""
    fn = os.path.join(CACHE, f'bhav_{d:%Y%m%d}.csv')
    if not os.path.exists(fn):
        if d.weekday() >= 5:
            return None
        url = f'https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{d:%Y%m%d}_F_0000.CSV'
        try:
            data = _get(url, sleep=0.3)
        except Exception:
            return None
        if len(data) < 100000 or not data.startswith(b'TradDt'):
            # holiday / not yet published: BSE returns a small HTML stub
            return None
        open(fn, 'wb').write(data)
    out = []
    for r in csv.DictReader(open(fn, encoding='utf-8', errors='ignore')):
        if r.get('FinInstrmTp') != 'STK':
            continue
        try:
            out.append(dict(scrip=r['FinInstrmId'].strip(), isin=r['ISIN'].strip(), symbol=r['TckrSymb'].strip(),
                            series=r['SctySrs'].strip(), name=r['FinInstrmNm'].strip(),
                            open=float(r['OpnPric'] or 0), high=float(r['HghPric'] or 0), low=float(r['LwPric'] or 0),
                            close=float(r['ClsPric'] or 0), prev_close=float(r['PrvsClsgPric'] or 0),
                            volume=float(r['TtlTradgVol'] or 0), turnover=float(r['TtlTrfVal'] or 0),
                            trades=int(float(r['TtlNbOfTxsExctd'] or 0))))
        except Exception:
            continue
    return out


def trading_days_back(n, end=None):
    """Return the last n dates for which a bhavcopy exists, newest first (walks back over holidays)."""
    d = end or datetime.date.today()
    out = []
    tries = 0
    while len(out) < n and tries < n * 3 + 15:
        if bhavcopy(d):
            out.append(d)
        d -= datetime.timedelta(days=1)
        tries += 1
    return out


def announcements(d_from, d_to=None, max_pages=40):
    """All BSE announcements between two dates (inclusive), as the API rows (paginated, 50/page)."""
    d_to = d_to or d_from
    fn = os.path.join(CACHE, f'ann_{d_from:%Y%m%d}_{d_to:%Y%m%d}.json')
    if os.path.exists(fn) and d_to < datetime.date.today():
        return json.load(open(fn))
    rows = []
    for p in range(1, max_pages + 1):
        url = ('https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d&strCat=-1&strPrevDate=%s'
               '&strScrip=&strSearch=P&strToDate=%s&strType=C&subcategory=-1' % (p, d_from.strftime('%Y%m%d'), d_to.strftime('%Y%m%d')))
        try:
            t = json.loads(_get(url, sleep=0.4)).get('Table') or []
        except Exception:
            break
        rows += t
        if len(t) < 50:
            break
    json.dump(rows, open(fn, 'w'))
    return rows


def attachment_url(row):
    a = (row.get('ATTACHMENTNAME') or '').strip()
    return f'https://www.bseindia.com/xml-data/corpfiling/AttachLive/{a}' if a else ''


def price_history(scrip, d_from=None, d_to=None):
    """Daily OHLC history for a scrip code from BSE (unadjusted). List of dict rows oldest first."""
    d_from = d_from or datetime.date(2021, 1, 1)
    d_to = d_to or datetime.date.today()
    url = ('https://api.bseindia.com/BseIndiaAPI/api/StockPriceCSVDownload/w?pageType=0&rbType=D&Scode=%s&FDates=%s&TDates=%s'
           % (scrip, d_from.strftime('%d/%m/%Y'), d_to.strftime('%d/%m/%Y')))
    data = _get(url).decode('utf-8', 'ignore')
    out = []
    for r in csv.DictReader(io.StringIO(data)):
        try:
            d = datetime.datetime.strptime(r['Date'].strip(), '%d-%B-%Y').date()
            out.append(dict(date=d, open=float(r['Open Price']), high=float(r['High Price']), low=float(r['Low Price']),
                            close=float(r['Close Price']), volume=float(r['No.of Shares'] or 0)))
        except Exception:
            continue
    out.sort(key=lambda r: r['date'])
    return out


def corporate_actions(scrip):
    """Bonus/split events -> list of (ex_date, factor, label). Factor >1 divides prices before ex_date."""
    import re
    try:
        ca = json.loads(_get(f'https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w?scripcode={scrip}'))
    except Exception:
        return []
    ev, seen = [], set()

    def pd(s):
        for f in ('%d %b %Y', '%d-%B-%Y', '%Y-%m-%d'):
            try:
                return datetime.datetime.strptime((s or '').strip(), f).date()
            except Exception:
                pass
        return None
    for e in ca.get('Table2') or []:
        c2 = (e.get('purpose_code') or '').strip(); p = e.get('purpose') or ''; ex = pd(e.get('Ex_date'))
        if not ex:
            continue
        f = None
        if c2 == 'BN' or 'bonus' in p.lower():
            mm = re.search(r'(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)', p)
            if mm:
                a, b = float(mm.group(1)), float(mm.group(2)); f = (a + b) / b
        elif c2 == 'SS' or 'split' in p.lower():
            mm = re.findall(r'Rs\.?\s*(\d+(?:\.\d+)?)', p)
            if len(mm) >= 2:
                f = float(mm[0]) / float(mm[1])
        if f and f > 1 and (ex, round(f, 4)) not in seen:
            seen.add((ex, round(f, 4))); ev.append((ex, f, p.strip()))
    for e in ca.get('Table1') or []:
        if 'bonus' not in (e.get('XTYPE') or '').lower():
            continue
        ex = pd(e.get('BCRD_FROM')); val = e.get('VALUE') or ''
        mm = re.search(r'(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)', val)
        if not ex or not mm:
            continue
        a, b = float(mm.group(1)), float(mm.group(2)); f = (a + b) / b
        if any(abs((ex - x[0]).days) <= 5 for x in ev):
            continue
        ev.append((ex, f, 'Bonus ' + val))
    return sorted(ev)


def adjusted_history(scrip, d_from=None):
    """Price history back-adjusted for bonus/split (recorded + unrecorded one-day gaps matching a standard factor)."""
    rows = price_history(scrip, d_from)
    if not rows:
        return rows, []
    events = corporate_actions(scrip)
    STD = [1.1, 1.2, 1.25, 1.333, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8, 10, 11, 20]
    for i in range(1, len(rows)):
        a, b = rows[i - 1]['close'], rows[i]['close']
        if a <= 0 or b <= 0:
            continue
        ratio = a / b
        if ratio < 1.3:
            continue
        if any(abs((rows[i]['date'] - ex).days) <= 5 for ex, _, _ in events):
            continue
        near = min(STD, key=lambda f: abs(f - ratio) / f)
        if abs(near - ratio) / near <= 0.06:
            events.append((rows[i]['date'], near, f'unrecorded gap x{ratio:.2f}'))
    events.sort()
    for ex, f, _ in events:
        for r in rows:
            if r['date'] < ex:
                for k in ('open', 'high', 'low', 'close'):
                    r[k] = r[k] / f
    return rows, events
