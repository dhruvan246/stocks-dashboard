"""Score every published idea against prices: return since the call close and the best close since.

Usage: python3 scripts/ideas/score.py
Reads docs/ideas/ideas.json, writes docs/ideas/track.json. Prices come from BSE per-scrip history,
back-adjusted for bonus/split (bse.adjusted_history). Call close = close of the call date (the routine
stores it when it publishes); if missing it is the first close on or after the call date.
"""
import json, os, sys, datetime, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse
import ist

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')


def score_idea(idea):
    call = datetime.date.fromisoformat(idea['call_date'])
    rows, events = bse.adjusted_history(idea['scrip'], d_from=call - datetime.timedelta(days=10))
    after = [r for r in rows if r['date'] >= call]
    if not after:
        return dict(idea_id=idea['id'], scrip=idea['scrip'], status='no prices yet')
    base = after[0]
    base_close = base['close']
    peak = max(after, key=lambda r: r['close'])
    last = after[-1]
    return dict(idea_id=idea['id'], scrip=idea['scrip'], ticker=idea.get('ticker'), name=idea.get('name'),
                call_date=base['date'].isoformat(), call_close=round(base_close, 2),
                last_date=last['date'].isoformat(), last_close=round(last['close'], 2),
                ret_pct=round((last['close'] / base_close - 1) * 100, 1),
                peak_date=peak['date'].isoformat(), peak_close=round(peak['close'], 2),
                peak_pct=round((peak['close'] / base_close - 1) * 100, 1),
                days=(last['date'] - base['date']).days,
                adj_events=[f'{ex.isoformat()} x{f:g} {lab}' for ex, f, lab in events if ex >= call], status='ok')


def main():
    fn = os.path.join(DOCS, 'ideas.json')
    if not os.path.exists(fn):
        print('no ideas.json yet'); return
    ideas = json.load(open(fn)).get('ideas', [])
    rows = []
    for it in ideas:
        try:
            rows.append(score_idea(it))
        except Exception as e:
            rows.append(dict(idea_id=it['id'], scrip=it['scrip'], status=f'error: {e}'))
    ok = [r for r in rows if r.get('status') == 'ok']
    summary = {}
    if ok:
        summary = dict(count=len(ok), median_ret=round(statistics.median([r['ret_pct'] for r in ok]), 1),
                       mean_ret=round(statistics.mean([r['ret_pct'] for r in ok]), 1),
                       up=sum(1 for r in ok if r['ret_pct'] > 0), median_peak=round(statistics.median([r['peak_pct'] for r in ok]), 1),
                       hit_20=sum(1 for r in ok if r['peak_pct'] >= 20), hit_50=sum(1 for r in ok if r['peak_pct'] >= 50))
    out = dict(updated=ist.stamp(), summary=summary, rows=rows)
    json.dump(out, open(os.path.join(DOCS, 'track.json'), 'w'), indent=1)
    print('scored', len(rows), 'ideas;', summary)


if __name__ == '__main__':
    main()
