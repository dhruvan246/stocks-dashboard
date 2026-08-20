#!/usr/bin/env python3
"""Apply the staleness campaign's fetched real dates to the fundamentals ann-date cells
(DATA_RUNBOOK §102/§103, PLAN_QUANTMAC_FIXES.md P2). Same dry-run-by-default convention as
scripts/gate_1530.py, which this deliberately mirrors — including running each matched cell
through the SAME 15:30 IST rule before writing, so the nightly gate finds nothing left to do.

For each cell in target_list.json with a match in fetch_results.json:
  1. Parse the real NEWS_DT -> a raw YYYYMMDD date + time-of-day.
  2. If broadcast time > 15:30 IST: bump to the next trading day (scripts/gate_calendar.json
     tdays) — same rule, same source of truth as gate_1530.py. Otherwise use the raw date.
  3. Skip (no-op) if the result equals what's already stored.
  4. Record direction (earlier/later) and day-delta vs the qe+45d placeholder being replaced —
     the campaign found this error runs BOTH ways (DATA_RUNBOOK §102, finding 3 note), so both
     get corrected the same way: write the truth.

Never invents a date: a cell with no fetch match is left untouched, exactly as stored.

Writes BOTH docs/sf_fundamentals.json and scripts/fundamentals.json (gate_1530.py precedent —
CI commits only the former; a local apply keeps the mirror in step) — dry run by default.
Also updates scripts/agg_pat_cell_fills.json entries whose ann_written equals the value being
replaced (2,046 of them per the plan's B4.1 measurement), so a future --repair-ann run doesn't
stamp the placeholder back (the retraction-needs-every-ledger class).

Usage:  python3 apply_redating.py              # dry run -> redate_ledger.json (audit, no writes)
        python3 apply_redating.py --apply      # also rewrite both fundamentals files + the ledger
"""
import json, os, sys, bisect, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
DOCS = os.path.join(SCRIPTS, '..', 'docs')
CUTOFF_MIN = 15 * 60 + 30  # 15:30 IST, same constant as gate_1530.py

def load(p):
    return json.load(open(p))

def news_min(ts):
    """Minutes-after-midnight from a BSE NEWS_DT like '2020-10-30T17:08:23.81'."""
    try:
        t = ts.split('T')[1]
        h, m = int(t[0:2]), int(t[3:5])
        return h * 60 + m
    except Exception:
        return None

def news_date(ts):
    try:
        return int(ts.split('T')[0].replace('-', ''))
    except Exception:
        return None

def month_idx(d):
    return (d // 10000) * 12 + (d // 100) % 100

def days_between(a, b):
    """Rough calendar-day delta between two YYYYMMDD ints, exact via datetime."""
    da = datetime.date(a // 10000, (a // 100) % 100, a % 100)
    db = datetime.date(b // 10000, (b // 100) % 100, b % 100)
    return (db - da).days

# A real filing can never predate its own quarter-end, and every verified case this campaign
# has actually confirmed (staleness agent + this session's own spot-checks) lags qe by 13-60
# days. 120 is a generous buffer past even the 60-day audited-annual deadline. Anything outside
# [0, 120] is almost certainly the OTHER kind of multi-date confusion `extract_all_qes` can't
# tell apart from a genuine combined disclosure: a comparative mention, not the filing's real
# subject — found live in the P2 smoke test (ALFA LAVAL's real 2004-09-30 result headline reads
# "...Rs 200 million for the quarter ended September 30, 2004 as compared to Rs 170.19 million
# for the quarter ended September 30, 2003" — the 2003 date is a YEAR-AGO COMPARISON figure, not
# a second real disclosure like ESSAROIL's genuine annual+quarterly combo). Reject, don't guess.
QE_LAG_MIN_DAYS = 0
QE_LAG_MAX_DAYS = 120

def build_decisions(target_list, fetch_results, tdays):
    """-> list of decision dicts, one per matched cell that resolves to a real date."""
    decisions = []
    reasons = {'matched': 0, 'no_match': 0, 'unparseable_news_dt': 0, 'no_next_td': 0, 'noop': 0,
               'rejected_implausible_lag': 0}

    def next_td(d):
        i = bisect.bisect_right(tdays, d)
        return tdays[i] if i < len(tdays) else None

    for sym, rows in target_list.items():
        entry = fetch_results.get(sym)
        if not entry:
            reasons['no_match'] += len(rows)
            continue
        matches = entry.get('matches', {})
        for qe, basis, old_ann in rows:
            key = f'{qe}|{basis}'
            m = matches.get(key)
            if not m:
                reasons['no_match'] += 1
                continue
            news_dt, newssub = m
            raw_date = news_date(news_dt)
            mins = news_min(news_dt)
            if raw_date is None:
                reasons['unparseable_news_dt'] += 1
                continue
            lag = days_between(qe, raw_date)
            if not (QE_LAG_MIN_DAYS <= lag <= QE_LAG_MAX_DAYS):
                reasons['rejected_implausible_lag'] += 1
                continue
            if mins is not None and mins > CUTOFF_MIN:
                new_ann = next_td(raw_date)
                gated = True
                if new_ann is None:
                    reasons['no_next_td'] += 1
                    continue
            else:
                new_ann = raw_date
                gated = False
            if new_ann == old_ann:
                reasons['noop'] += 1
                continue
            reasons['matched'] += 1
            delta_days = days_between(old_ann, new_ann)
            decisions.append({
                'sym': sym, 'qe': qe, 'basis': basis,
                'old_ann': old_ann, 'new_ann': new_ann,
                'direction': 'later' if delta_days > 0 else 'earlier',
                'delta_days': abs(delta_days),
                'gated_1530': gated,
                'news_dt': news_dt, 'newssub': newssub,
            })
    return decisions, reasons

def main():
    apply = '--apply' in sys.argv
    target_list = load(os.path.join(HERE, 'target_list.json'))
    fetch_results = load(os.path.join(HERE, 'fetch_results.json'))
    tdays = load(os.path.join(SCRIPTS, 'gate_calendar.json'))['tdays']

    decisions, reasons = build_decisions(target_list, fetch_results, tdays)

    print('=== decision summary ===')
    for k, v in reasons.items():
        print(f'  {k:22s} {v}')
    print(f'  cells to re-date       {len(decisions)}')
    by_dir = {'earlier': 0, 'later': 0}
    for d in decisions:
        by_dir[d['direction']] += 1
    print(f'  direction split        earlier={by_dir["earlier"]} later={by_dir["later"]}')
    gated_n = sum(1 for d in decisions if d['gated_1530'])
    print(f'  15:30-gated on write   {gated_n}')

    json.dump(decisions, open(os.path.join(HERE, 'redate_ledger.json'), 'w'), indent=1)
    print(f'\naudit ledger written -> redate_ledger.json ({len(decisions)} entries)')

    if not apply:
        print('\n(dry run — pass --apply to rewrite both fundamentals files + the agg ledger)')
        return

    by_sym_qe = {}
    for d in decisions:
        by_sym_qe.setdefault((d['sym'], d['qe']), {})[d['basis']] = d['new_ann']

    applied = 0
    for path in (os.path.join(DOCS, 'sf_fundamentals.json'),
                 os.path.join(SCRIPTS, 'fundamentals.json')):
        fund = load(path)
        cnt = 0
        for sym, rows in fund.items():
            for r in rows:
                qe = r[0]
                bmap = by_sym_qe.get((sym, qe))
                if not bmap:
                    continue
                if 'std' in bmap and len(r) > 2 and r[2] is not None:
                    r[2] = bmap['std']; cnt += 1
                if 'con' in bmap and len(r) > 4 and r[4] is not None:
                    r[4] = bmap['con']; cnt += 1
        json.dump(fund, open(path, 'w'), separators=(',', ':'))
        print(f'applied {cnt} cell writes -> {os.path.relpath(path, SCRIPTS)}')
        applied = cnt

    agg_path = os.path.join(SCRIPTS, 'agg_pat_cell_fills.json')
    agg = load(agg_path)
    agg_updated = 0
    for d in decisions:
        lkey = f"{d['sym']}|{d['qe']}"
        if lkey in agg and agg[lkey].get('ann_written') == d['old_ann']:
            agg[lkey]['ann_written'] = d['new_ann']
            agg[lkey]['ann_basis'] = f"bse-broadcast {d['news_dt']} (staleness campaign 2026-08-20, was CONVENTION quarter-end+45d)"
            agg_updated += 1
    if agg_updated:
        json.dump(agg, open(agg_path, 'w'), indent=1)
    print(f'synced {agg_updated} entries in agg_pat_cell_fills.json (of {len(decisions)} decisions)')
    print('DONE.')

if __name__ == '__main__':
    main()
