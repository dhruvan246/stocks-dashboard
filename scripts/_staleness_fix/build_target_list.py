#!/usr/bin/env python3
"""Build the target list for the staleness campaign (DATA_RUNBOOK §102/§103):
cells whose ann-date is EXACTLY quarter-end + 45 days (the apply_agg_pat_fills.py
CONVENTION signature, confirmed 96.8% of ALL dated pre-2015 cells per that
script's own docstring — an organic filing-date distribution cannot produce
that concentration, so an exact-match is treated as "placeholder, not real").

Output: scripts/_staleness_fix/target_list.json
  { SYMBOL: [ [qe, basis('std'|'con'), current_ann], ... ] }
"""
import json, datetime

def qe45(qe):
    y, m, d = qe // 10000, (qe // 100) % 100, qe % 100
    return int((datetime.date(y, m, d) + datetime.timedelta(days=45)).strftime('%Y%m%d'))

def main():
    fund = json.load(open('docs/sf_fundamentals.json'))
    targets = {}
    n = 0
    for sym, arr in fund.items():
        rows = []
        for row in arr:
            qe = row[0]
            exp = qe45(qe)
            if row[2] and row[2] == exp:
                rows.append([qe, 'std', row[2]])
                n += 1
            if row[4] and row[4] == exp:
                rows.append([qe, 'con', row[4]])
                n += 1
        if rows:
            targets[sym] = rows
    json.dump(targets, open('scripts/_staleness_fix/target_list.json', 'w'))
    print(f'{n} target cells across {len(targets)} symbols -> scripts/_staleness_fix/target_list.json')

if __name__ == '__main__':
    main()
