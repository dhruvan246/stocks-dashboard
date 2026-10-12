"""Breakout watch: stocks that rose a lot, fell a lot, and are back at (or just through) their old peak.

User, 2026-10-12, after Cupid (Rs 2.40 -> 27.39 in 2023-24, -58% to 11.52 by Apr-2025, through the old high on
18-Jul-2025, then 12.5x): "i want more such stocks which rose a lot then fell a lot and now are near breakout level".

Pattern, on bonus/split-adjusted closes of every NSE stock (the site's live survivorship-free store, sf-data):
  rise   a peak that was an all-time high to that date, at least RISE_X times the lowest close in the ~2 years
         (500 sessions) before it, peak on or after 2019-01-01
  fall   the lowest close after that peak is at least FALL below it
  near   no close above the peak since, and the latest close within NEAR of it          -> rows "near"
  broke  the first close above the peak after the fall came within the last RECENT sessions -> rows "broke"
  liquid median daily turnover over the last 60 sessions >= MIN_TURN_LAKH
Today's NSE bhavcopy close is appended when sf-data has not reached today yet (the evening job runs before the
sf-data refresh), unless that day's move is outside 0.6x-1.67x of the previous close (a split/bonus ex-date the
raw file is not adjusted for) - that stock is then judged on sf-data alone and marked.

Output docs/ideas/breakout_watch.json - derived, rebuilt every run (ideas-feeds.yml). Price patterns only: the
page says so; nothing here is a reason to buy.
"""
import datetime
import gzip
import json
import os
import statistics
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bse  # noqa: E402
import ist  # noqa: E402

OUT = os.environ.get('BREAKOUT_OUT') or os.path.join(HERE, '..', '..', 'docs', 'ideas', 'breakout_watch.json')
SF = 'https://dhruvan246.github.io/sf-data/'
RISE_X, FALL, NEAR, RECENT, MIN_TURN_LAKH, LOOKBACK = 3.0, 0.5, 0.15, 60, 50.0, 500


def _get(name):
    req = urllib.request.Request(SF + name, headers={'User-Agent': 'stocks-dashboard-breakout'})
    for i in range(3):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except Exception:
            if i == 2:
                raise
            time.sleep(5)


def load_sf():
    """The live sf-data store, merged the way scripts/fetch_live_sf.py does (deep parts first, recent appended)."""
    meta = json.loads(_get('sf_meta.json?t=%d' % time.time()))
    if not meta.get('deepFrom'):
        raise SystemExit('sf-data layout without deepFrom - update breakout.py like fetch_live_sf.py')
    names = ['sf_deep_%d.bin?v=%s' % (i + 1, meta['end']) for i in range(meta.get('deep', 0))]
    names += ['sf_recent_%d.bin?v=%s' % (i + 1, meta['end']) for i in range(meta.get('recent', 1))]
    data = {}
    for nm in names:
        part = json.loads(gzip.decompress(_get(nm)))['data']
        for sym, o in part.items():
            cur = data.get(sym)
            if cur is None:
                data[sym] = {k: o.get(k) for k in ('d', 'c', 't')}
                continue
            for k in ('d', 'c', 't'):
                if isinstance(cur.get(k), list) and isinstance(o.get(k), list):
                    cur[k] = cur[k] + o[k]
    return meta, data


def append_today(meta, data):
    """Append today's NSE close where sf-data stops before it. Returns (date appended or None, symbols skipped)."""
    end = datetime.date.fromisoformat(meta['end'])
    today = ist.today()
    if end >= today:
        return None, []
    rows = bse.nse_bhavcopy(today)
    if not rows:
        return None, []
    di = int(today.strftime('%Y%m%d'))
    skipped = []
    for r in rows:
        if r.get('series') not in ('EQ', 'BE', 'BZ', 'SM', 'ST') or not r.get('close'):
            continue
        o = data.get(str(r['symbol']).upper())
        if not o or not o['d'] or o['d'][-1] >= di:
            continue
        prev = o['c'][-1]
        if prev and not (0.6 <= r['close'] / prev <= 1.67):
            skipped.append(r['symbol'])
            continue
        o['d'].append(di)
        o['c'].append(float(r['close']))
        o['t'].append((r.get('turnover') or 0) / 1e5 if r.get('turnover') else (o['t'][-1] if o['t'] else 0))
    return today.isoformat(), skipped


def ymd(i):
    s = str(i)
    return f'{s[:4]}-{s[4:6]}-{s[6:]}'


def scan(data, last_day):
    near, broke = [], []
    for sym, o in data.items():
        d, c, t = o.get('d') or [], o.get('c') or [], o.get('t') or []
        if len(d) < 300 or d[-1] != last_day or len(c) != len(d):
            continue
        medt = statistics.median(t[-60:]) if len(t) >= 60 else 0
        if medt < MIN_TURN_LAKH:
            continue
        n = len(c)
        ath = 0
        best = None
        for i in range(n):
            if c[i] < c[ath]:
                continue
            ath = i
            if d[i] < 20190101:
                continue
            P = c[i]
            lo = min(c[max(0, i - LOOKBACK):i + 1])
            if lo <= 0 or P / lo < RISE_X:
                continue
            # fall: the deepest close after the peak and before any close above it
            j, T, Tj = i + 1, P, None
            while j < n and c[j] <= P:
                if c[j] < T:
                    T, Tj = c[j], j
                j += 1
            if Tj is None or T > (1 - FALL) * P:
                continue
            row = dict(sym=sym, low_before=round(lo, 2), rise_x=round(P / lo, 1), peak_date=ymd(d[i]), peak=round(P, 2),
                       trough_date=ymd(d[Tj]), trough=round(T, 2), fall_pct=round((T / P - 1) * 100, 1),
                       last=round(c[-1], 2), last_date=ymd(d[-1]), med_turn_cr=round(medt / 100, 2))
            if j >= n:                                    # never closed above the peak again
                if c[-1] >= (1 - NEAR) * P:
                    best = ('near', dict(row, below_peak_pct=round((c[-1] / P - 1) * 100, 1),
                                         from_low_pct=round((c[-1] / T - 1) * 100, 1)))
            elif n - 1 - j < RECENT:                      # broke out within the last RECENT sessions
                b = j
                best = ('broke', dict(row, breakout_date=ymd(d[b]), breakout_close=round(c[b], 2),
                                      days_since=n - 1 - b, ret_since_pct=round((c[-1] / c[b] - 1) * 100, 1),
                                      best_since_pct=round((max(c[b:]) / c[b] - 1) * 100, 1),
                                      low_to_breakout_days=(datetime.date.fromisoformat(ymd(d[b])) -
                                                            datetime.date.fromisoformat(ymd(d[Tj]))).days))
        if best:
            (near if best[0] == 'near' else broke).append(best[1])
    near.sort(key=lambda r: -r['below_peak_pct'])
    broke.sort(key=lambda r: r['days_since'])
    return near, broke


def main():
    meta, data = load_sf()
    appended, skipped = append_today(meta, data)
    last_day = int((appended or meta['end']).replace('-', ''))
    near, broke = scan(data, last_day)
    try:   # name and market cap (Rs crore) from the site's daily search index
        si = {r[0]: r for r in json.load(open(os.path.join(HERE, '..', '..', 'docs', 'search_index.json')))['s']}
    except Exception:
        si = {}
    for r in near + broke:
        e = si.get(r['sym'])
        r['name'] = e[1] if e else r['sym']
        r['mcap_cr'] = e[3] if e and isinstance(e[3], (int, float)) else None
    json.dump(dict(built=ist.now().strftime('%Y-%m-%d %H:%M IST'), prices_to=ymd(last_day), sf_end=meta['end'],
                   today_appended_from_bhavcopy=appended, skipped_on_big_move=skipped[:50],
                   rule=f'Bonus/split-adjusted NSE closes (sf-data). Rose >= {RISE_X:g}x to a peak (an all-time high on '
                        f'that day, since 2019) from the lowest close in the ~2 years before it, then fell >= '
                        f'{int(FALL * 100)}%. Near = no close above that peak since and the latest close within '
                        f'{int(NEAR * 100)}% of it. Broke out = first close above it within the last {RECENT} sessions. '
                        f'Median daily turnover >= Rs {MIN_TURN_LAKH / 100:g} cr. Price patterns only.',
                   near=near, broke=broke),
              open(OUT, 'w'), ensure_ascii=False, indent=1)
    print(f'breakout watch: prices to {ymd(last_day)} (sf-data {meta["end"]}, bhavcopy appended: {appended}); '
          f'{len(near)} near, {len(broke)} broke out in the last {RECENT} sessions; {len(skipped)} skipped on a big move')
    return 0


if __name__ == '__main__':
    sys.exit(main())
