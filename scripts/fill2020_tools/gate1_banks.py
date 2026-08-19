#!/usr/bin/env python3
"""Gate-1 v2 harvest for the 28-name bank/NBFC circular-risk bucket.

For each symbol, fetch NSE's FULL per-quarter results list ONCE (both std and con
rows for every toDate it has), and build perq[qe] = {con, std, filed} — a positive
per-quarter reading, same shape as con_floor_v2.jsonl from the earlier campaign.
No repo writes; this is evidence only.
"""
import sys, json, time, re
SP = '/private/tmp/claude-501/-Users-dhruvan-stocks-dashboard/23cc50b5-3124-4158-827b-739753da254c/scratchpad/'
sys.path.insert(0, SP)
import batch_contriad as B
import urllib.parse

SYMS = ['J&KBANK','CANBK','CENTRALBK','PFC','BANKBARODA','INDIANB','LICHSGFIN','MAHABANK','NIACL','PNB',
        'UNIONBANK','BANKINDIA','FEDERALBNK','IDBI','IFCI','MUTHOOTFIN','RECLTD','AXISBANK','ICICIGI',
        'IDFCFIRSTB','RBLBANK','SUNDARMFIN','HDFCBANK','HDFCLIFE','CIEINDIA','GODREJAGRO','NFL','REPCOHOME']

OUT = SP + 'gate1_banks.jsonl'
done = set()
try:
    for line in open(OUT):
        done.add(json.loads(line)['sym'])
except FileNotFoundError:
    pass

fh = open(OUT, 'a')
for i, sym in enumerate(SYMS):
    if sym in done:
        continue
    rec = {'sym': sym}
    for attempt in (1, 2, 3):
        try:
            u = ('https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol=%s&period=Quarterly'
                 % urllib.parse.quote(sym, safe=''))
            lst = json.loads(B.get(u))
            rows = lst if isinstance(lst, list) else lst.get('data', [])
            perq = {}
            for r in rows:
                qe = B.iso(r.get('toDate'))
                if qe == '?':
                    continue
                is_con = 'Non' not in str(r.get('consolidated'))
                slot = perq.setdefault(qe, {'con': 0, 'std': 0, 'filed': None})
                if is_con:
                    slot['con'] = 1
                else:
                    slot['std'] = 1
                fd = r.get('filingDate')
                if fd and (not slot['filed'] or fd < slot['filed']):
                    slot['filed'] = fd
            rec['rows'] = len(rows)
            rec['quarters_covered'] = len(perq)
            rec['perq'] = perq
            break
        except Exception as e:
            rec['error'] = '%s: %s' % (type(e).__name__, str(e)[:150])
            if attempt < 3:
                time.sleep(5)
    fh.write(json.dumps(rec) + '\n'); fh.flush()
    ncon = sum(1 for v in rec.get('perq', {}).values() if v['con'])
    print('%2d/%2d %-12s rows=%-4s qtrs=%-4s con_qtrs=%-3s %s' % (
        i + 1, len(SYMS), sym, rec.get('rows', '-'), rec.get('quarters_covered', '-'), ncon,
        ('ERR:' + rec['error']) if 'error' in rec and 'perq' not in rec else ''), flush=True)
    time.sleep(1.3)
print('DONE ->', OUT)
