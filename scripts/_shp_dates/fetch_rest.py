#!/usr/bin/env python3
"""Phase 4 fetch: the ~701 symbols the convention campaign never needed — every remaining symbol
holding post-2016 SHP cells — so the whole 63,567-cell population can be reconciled against BSE.
Same shape as fetch_full.py (raw cached, resumable, sharded).

Run: fetch_rest.py <shard_index> <n_shards>
"""
import json, os, sys, time, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(SCRIPTS, '_staleness_fix'))
import shp_dates as SD
import fetch_and_match as m

API = 'https://api.bseindia.com/BseIndiaAPI/api/SHPQNewFormat/w?scripcode='


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    out_p = os.path.join(HERE, f'rest_shard{shard}.json')
    raw_p = os.path.join(HERE, f'raw_rest{shard}.jsonl')
    log_p = os.path.join(HERE, f'rest_shard{shard}.log')

    def log(msg):
        with open(log_p, 'a') as f:
            f.write(f'{datetime.datetime.now().isoformat()} {msg}\n')

    shp = json.load(open(os.path.join(ROOT, 'docs', 'shp_engine.json')))
    by_id = json.load(open(os.path.join(SCRIPTS, 'bse_scrips.json')))['by_id']
    master = {}
    for r in json.load(open(os.path.join(SCRIPTS, '_bse_master_all.json'))):
        sid, cd = r.get('scrip_id'), r.get('SCRIP_CD')
        if sid and cd and sid not in master:
            try:
                master[sid] = int(cd)
            except (TypeError, ValueError):
                pass

    have = set()
    for i in (0, 1):
        p = os.path.join(HERE, f'full_shard{i}.json')
        if os.path.exists(p):
            have |= set(json.load(open(p)))

    need = []
    for sym, rows in shp.items():
        if sym in have:
            continue
        if any(isinstance(q, list) and len(q) > 3 and q[0] >= 20160101 and q[3]
               and not SD.is_convention(q[0], q[3]) for q in rows):
            need.append(sym)
    syms = sorted(need)[shard::nsh]
    results = json.load(open(out_p)) if os.path.exists(out_p) else {}
    log(f'START shard {shard}/{nsh}: {len(syms)} symbols, {len(results)} done')

    done = 0
    for sym in syms:
        if sym in results:
            continue
        sc = by_id.get(sym) or master.get(sym)
        entry = {'scripcode': sc, 'resolved': {}, 'error': None}
        if not sc:
            entry['error'] = 'no-scripcode'
        else:
            try:
                table = json.loads(m.get(API + str(sc), timeout=30))['Table']
                with open(raw_p, 'a') as f:
                    f.write(json.dumps({'sym': sym, 'scripcode': sc, 'table': table},
                                       separators=(',', ':')) + '\n')
                entry['resolved'] = {str(k): v for k, v in SD.resolve_rows(table).items()}
            except Exception as e:
                entry['error'] = f'{type(e).__name__}: {e}'
                log(f'  FAILED {sym}: {entry["error"]}')
        results[sym] = entry
        done += 1
        if done % 25 == 0:
            json.dump(results, open(out_p, 'w'))
            log(f'CHECKPOINT {len(results)}/{len(syms)}')
        time.sleep(0.3)
    json.dump(results, open(out_p, 'w'))
    log(f'COMPLETE done={len(results)}')


if __name__ == '__main__':
    main()
