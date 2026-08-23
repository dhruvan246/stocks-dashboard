#!/usr/bin/env python3
"""Phase 3b (durable route): write the recovered SHP filing dates into scripts/shp_history.json's
`sub` slot, so build_engine_feed() regenerates docs/shp_engine.json WITH them.

WHY HERE AND NOT shp_engine.json: that feed is regenerated ~2x/day, so a direct write to it
vanishes within hours while looking fixed — the journalled-is-not-live trap. shp_history.json is
the durable accumulator the feed is built from, AND fetch_shareholding.py's refine pass explicitly
preserves this slot ("the numbers are taken and slots 5+ are KEPT"), so a corrected sub survives
re-reads of the same filing from the other exchange.

Scope: ONLY cells whose stored sub is the quarter-end+21d convention AND that the ledger
(scripts/shp_sub_dates.json, built by apply_shp_dates.py from BSE SHPQNewFormat) has a real,
guard-passed filing date for. Idempotent: a cell already carrying the ledger date is skipped.
Never creates a cell, never touches the five holding values.

Usage:  patch_history.py            # dry run
        patch_history.py --apply    # write shp_history.json
"""
import json, os, sys, collections

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import shp_dates as SD

HIST = os.path.join(SCRIPTS, 'shp_history.json')
LEDGER = os.path.join(SCRIPTS, 'shp_sub_dates.json')


def iso(d):
    return f'{d // 10000:04d}-{(d // 100) % 100:02d}-{d % 100:02d}'


def main():
    apply = '--apply' in sys.argv
    hist = json.load(open(HIST))
    led = json.load(open(LEDGER))
    st = collections.Counter()
    samples = []

    for key, e in led.items():
        sym, qe = key.split('|')
        qe = int(qe)
        qs = hist.get(sym)
        if not isinstance(qs, dict):
            st['symbol_absent'] += 1
            continue
        qk = iso(qe)
        cell = qs.get(qk)
        if not isinstance(cell, list) or len(cell) < 6:
            st['cell_absent'] += 1
            continue
        cur = str(cell[5] or '').replace('-', '')
        want = iso(e['sub'])
        if cur == str(e['sub']):
            st['already'] += 1
            continue
        # only replace the convention value we measured — never a date someone else corrected
        if cur != str(e['was']):
            st['moved_on'] += 1
            continue
        if apply:
            cell[5] = want
        st['patched'] += 1
        if len(samples) < 8:
            samples.append((sym, qk, iso(e['was']), want, e['ts'][:19], e['gated_1530']))

    for k in sorted(st):
        print(f'  {k:14s} {st[k]}')
    print('  samples:')
    for s in samples:
        print('   ', s)
    if not apply:
        print('\n(dry run — pass --apply to write shp_history.json)')
        return
    tmp = HIST + '.tmp'
    json.dump(hist, open(tmp, 'w', encoding='utf-8'), separators=(',', ':'))
    os.replace(tmp, HIST)
    print(f'\nwrote {HIST}')


if __name__ == '__main__':
    main()
