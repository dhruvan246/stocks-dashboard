#!/usr/bin/env python
"""Apply the precise 15:30 IST availability gate to fundamentals ann-dates.

For every fundamentals filing whose annStd/annCon lands on a month-end trading
day (= a monthly-rebalance date), look up the BSE broadcast time. If EVERY result
broadcast for that scrip that day was after 15:30 (market close), the result was
not actually available at the 15:30 rebalance -> bump that ann-date to the next
trading day. The engine's existing `annDate <= rebalanceDate` check then treats it
correctly with NO engine change. See memory project-stocks-1530-gate.

Conservative by design: bump ONLY when we can CONFIRM after-close (a BSE time
exists and its min > 15:30). No BSE record, or any broadcast <= 15:30 -> leave as
is (preserves current behaviour, never wrongly excludes a legit pick).

Usage:  python scripts/gate_1530.py            # dry run (writes _gate_bumps.json)
        python scripts/gate_1530.py --apply     # also rewrite both fundamentals files
"""
import json, os, sys, bisect

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, '..', 'docs')
CUTOFF_MIN = 15 * 60 + 30  # 15:30 IST

def load(p): return json.load(open(p))

def news_min(ts):
    """Minutes-after-midnight from a BSE NEWS_DT like '2020-10-30T17:08:23.81'."""
    try:
        t = ts.split('T')[1]
        h, m = int(t[0:2]), int(t[3:5])
        return h * 60 + m
    except Exception:
        return None

def main():
    apply = '--apply' in sys.argv
    events = [tuple(x) for x in load(os.path.join(HERE, '_gate_events.json'))]
    times = load(os.path.join(HERE, '_filing_times.json'))
    by_id = load(os.path.join(HERE, 'bse_scrips.json'))['by_id']
    tdays = load(os.path.join(HERE, '_trading_days.json'))  # sorted ints

    def next_td(d):
        i = bisect.bisect_right(tdays, d)
        return tdays[i] if i < len(tdays) else None

    # Decide per (sym, date): bump?
    bump = {}          # (sym, date) -> newdate
    reasons = {'bumped': 0, 'before_close': 0, 'no_bse_record': 0, 'no_scrip': 0, 'no_next_td': 0}
    examples = []
    for sym, date in events:
        sc = by_id.get(sym)
        if sc is None:
            reasons['no_scrip'] += 1; continue
        day = times.get(str(date), {})
        tl = day.get(str(sc))
        if not tl:
            reasons['no_bse_record'] += 1; continue
        mins = [m for m in (news_min(t) for t in tl) if m is not None]
        if not mins:
            reasons['no_bse_record'] += 1; continue
        if min(mins) <= CUTOFF_MIN:
            reasons['before_close'] += 1; continue
        nd = next_td(date)
        if nd is None:
            reasons['no_next_td'] += 1; continue
        bump[(sym, date)] = nd
        reasons['bumped'] += 1
        if len(examples) < 25:
            examples.append((sym, date, min(mins), nd))

    print('=== decision summary ===')
    for k, v in reasons.items(): print(f'  {k:16s} {v}')
    print(f'  total events     {len(events)}')
    print('\n=== sample bumps (sym, monthEnd, filedMinAfterMidnight, -> nextTD) ===')
    for e in examples: print('  ', e[0], e[1], f'{e[2]//60:02d}:{e[2]%60:02d}', '->', e[3])

    # Build the concrete row-level bump log against current fundamentals.
    fpath = os.path.join(DOCS, 'sf_fundamentals.json')
    fund = load(fpath)
    log = []  # {sym, qe, field, old, new, filedMin}
    for sym, rows in fund.items():
        for r in rows:
            qe = r[0]
            # annStd = idx2, annCon = idx4
            for fld, idx in (('annStd', 2), ('annCon', 4)):
                if len(r) > idx and r[idx] is not None:
                    key = (sym, r[idx])
                    if key in bump:
                        log.append({'sym': sym, 'qe': qe, 'field': fld,
                                    'old': r[idx], 'new': bump[key]})
    json.dump(log, open(os.path.join(HERE, '_gate_bumps.json'), 'w'), separators=(',', ':'))
    print(f'\nrow-cells to bump: {len(log)} (written to scripts/_gate_bumps.json)')

    # JSL proof
    jsl = [b for b in log if b['sym'] == 'JSL' and b['old'] == 20201030]
    print('JSL Oct-2020 bump:', jsl if jsl else 'NOT FOUND')

    if not apply:
        print('\n(dry run — pass --apply to rewrite both fundamentals files)')
        return

    # Apply to BOTH files identically, in place, ann-date cells only.
    applied = 0
    for path in (os.path.join(DOCS, 'sf_fundamentals.json'),
                 os.path.join(HERE, 'fundamentals.json')):
        d = load(path)
        cnt = 0
        for sym, rows in d.items():
            for r in rows:
                for idx in (2, 4):
                    if len(r) > idx and r[idx] is not None and (sym, r[idx]) in bump:
                        r[idx] = bump[(sym, r[idx])]; cnt += 1
        json.dump(d, open(path, 'w'), separators=(',', ':'))
        print(f'applied {cnt} cell bumps -> {os.path.relpath(path, HERE)}')
        applied = cnt
    print('DONE.')

if __name__ == '__main__':
    main()
