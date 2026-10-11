"""Confirmed order-book jumps: share-price return since the day the market first saw the jump.

Input  docs/ideas/orderbook_jumps.json      LEDGER, hand/routine-written. One row per company whose order book was
                                            checked against its own filings: the latest and year-earlier figures, the
                                            true year-on-year change, and the TRIGGER — the first exchange filing whose
                                            stated order book was 50%+ above the figure stated about a year before.
                                            Only rows with qualifies=true and a trigger_filed date are tracked.
Output docs/ideas/orderbook_tracker.json    DERIVED, rebuilt daily (ideas-feeds.yml): per row the close on the
                                            trigger filing day (next trading day if it was a holiday), the latest
                                            close, the return, the best close since and when, days since.

Prices: BSE codes from BSE's per-scrip history back-adjusted for bonus/split (bse.adjusted_history), falling back
to BSE bhavcopies; NSE-only names from NSE bhavcopies (a bonus/split caught only by the one-day-gap rule). If no
price source answers, the previous run's numbers are carried forward and the row is marked stale — never dropped.
"""
import datetime
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse  # noqa: E402
import ist  # noqa: E402

DOCS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'docs', 'ideas')
LEDGER = os.environ.get('OB_JUMPS') or os.path.join(DOCS, 'orderbook_jumps.json')
OUT = os.environ.get('OB_TRACKER') or os.path.join(DOCS, 'orderbook_tracker.json')
D = datetime.date.fromisoformat


def history(row, d_from):
    """(rows oldest first, adjustment events, source label) — raises if no source has prices."""
    key = str(row['key'])
    if key.startswith('NSE:') or not key.isdigit():
        sym = (row.get('nse') or key.split(':', 1)[-1]).upper()
        h = bse.nse_bhav_history([sym], d_from).get(sym) or []
        if not h:
            raise RuntimeError(f'no NSE bhavcopy rows for {sym}')
        h, ev = bse.apply_adjustments(h, [])
        return h, ev, 'NSE bhavcopy (bonus/split caught by the one-day-gap rule only)'
    try:
        h, ev = bse.adjusted_history(key, d_from)
        if h:
            return h, ev, 'BSE, bonus/split adjusted'
    except Exception:
        pass
    h = bse.bhav_history([key], d_from).get(key) or []
    if not h:
        raise RuntimeError(f'no BSE prices for {key}')
    h, ev = bse.apply_adjustments(h, [])
    return h, ev, 'BSE bhavcopy (bonus/split caught by the one-day-gap rule only)'


def track(row):
    f = D(row['trigger_filed'])
    h, ev, src = history(row, f - datetime.timedelta(days=7))
    h = [x for x in h if x.get('close')]
    after = [x for x in h if str(x['date'])[:10] >= row['trigger_filed']]
    if not after:
        raise RuntimeError('no close on or after the trigger filing date yet')
    e, last = after[0], h[-1]
    pk = max(after, key=lambda x: x['close'])
    lo = min(after, key=lambda x: x['close'])
    return dict(entry_date=str(e['date'])[:10], entry_close=round(e['close'], 2),
                last_date=str(last['date'])[:10], last_close=round(last['close'], 2),
                ret_pct=round((last['close'] / e['close'] - 1) * 100, 1),
                peak_pct=round((pk['close'] / e['close'] - 1) * 100, 1), peak_date=str(pk['date'])[:10],
                low_pct=round((lo['close'] / e['close'] - 1) * 100, 1),
                days=(D(str(last['date'])[:10]) - D(str(e['date'])[:10])).days,
                adj_events=len(ev or []), price_source=src, stale=False)


def main():
    if not os.path.exists(LEDGER):
        print('no orderbook_jumps.json yet - nothing to track')
        return 0
    led = json.load(open(LEDGER))
    try:
        prev = {r['key']: r for r in json.load(open(OUT)).get('rows', [])}
    except Exception:
        prev = {}
    rows, failed = [], []
    for r in led.get('rows', []):
        if not (r.get('qualifies') and r.get('trigger_filed')):
            continue
        out = {k: r.get(k) for k in ('key', 'name', 'nse', 'bse', 'isin', 'year_ago_value_cr', 'year_ago_as_on',
                                     'year_ago_doc', 'year_ago_doc_title', 'latest_value_cr', 'latest_as_on',
                                     'latest_doc', 'latest_doc_title', 'true_yoy_pct', 'trigger_filed', 'trigger_doc',
                                     'trigger_doc_title', 'trigger_as_on', 'trigger_book_cr', 'trigger_prev_cr',
                                     'trigger_prev_as_on', 'trigger_yoy_pct', 'verdict', 'note', 'checked_on')}
        try:
            out.update(track(r))
        except Exception as ex:
            p = prev.get(r['key'])
            keep = {k: p[k] for k in ('entry_date', 'entry_close', 'last_date', 'last_close', 'ret_pct', 'peak_pct',
                                      'peak_date', 'low_pct', 'days', 'adj_events', 'price_source') if p and k in p}
            out.update(keep, stale=True, price_error=str(ex)[:160])
            failed.append(r['key'])
        rows.append(out)
        if str(r['key']).isdigit():
            time.sleep(1.0)  # BSE per-scrip API: stay slow
    rows.sort(key=lambda x: x.get('trigger_filed') or '', reverse=True)
    json.dump(dict(built=ist.now().strftime('%Y-%m-%d %H:%M IST'),
                   rule='Order book checked against the company\'s own filings; tracked from the first filing that '
                        'showed it 50%+ above the figure stated about a year earlier. Entry = close on that filing '
                        'day (next trading day if a holiday). Returns use bonus/split-adjusted closes.',
                   n=len(rows), stale=failed, rows=rows),
              open(OUT, 'w'), ensure_ascii=False, indent=1)
    print(f'{len(rows)} tracked, {len(failed)} carried forward stale: {failed}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
