"""HOLD-OUT calibration of the FULL Wayback-NSE gate (G1-G5) against cells we ALREADY hold.

Measures the only metric that licenses a write: of the cells the gate WOULD WRITE, how many
disagree with what we already store? Run this BEFORE landing anything from this source, and
re-run it whenever a gate clause is relaxed (runbook §112f).

⚠️ Its own limit, which must travel with the number: cells we already hold skew to WELL-COVERED
companies, while the cells actually wanted have no stored value to check against. The measured
mismatch rate is a LOWER bound on the rate that matters.

⚠️ Wayback throttles to ~9 s/request under sustained fetching. Slowness is infrastructure state,
never evidence about the data.

  python3 -X utf8 scripts/wayback_nse/calibrate_gate.py <N cells> [seed]
"""
import sys, os, json, collections, random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from wb_read import fetch, parse, face_of
F=json.load(open(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),'docs','sf_fundamentals.json')))
idx=json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'_wb_index.json')))
std={}; con={}
for s,rows in F.items():
    for r in rows:
        if len(r)>1 and r[1] is not None: std[(s,r[0])]=r[1]
        if len(r)>3 and r[3] is not None: con[(s,r[0])]=r[3]
cands=[k for k in idx if tuple([k.split('|')[0],int(k.split('|')[1])]) in std]
random.seed(int(sys.argv[2]) if len(sys.argv)>2 else 5); random.shuffle(cands)
N=int(sys.argv[1])
res=collections.Counter(); mism=[]; tok=collections.Counter()
seen=0
for k in cands:
    if res['WRITE']+res['REFUSE'] >= N: break
    s,q=k.split('|')[0],int(k.split('|')[1])
    ts,u=idx[k]; raw=fetch(ts,u)
    if raw is None: res['fetch-fail']+=1; continue
    p=parse(raw)
    if not p: res['REFUSE']+=1; res['r:unreadable']+=1; continue
    rt=p.get('result_type') or ''
    ntok=len([x for x in rt.split(',') if x.strip()])
    why=None
    if p.get('symbol')!=s: why='G1'
    elif p['months']!=3 or 'Non-Cumulative' not in rt: why='G2'
    elif ntok>=3 and 'Non-Consolidated' not in rt: why='G3'
    elif p.get('div') is None: why='G4'
    elif p.get('pat_cr') is None: why='G4b'
    else:
        fv=face_of(raw); np_,pu,eps=p['net_profit'],p['paidup'],p['eps']
        if not (fv and pu and eps is not None and np_ is not None and pu>0 and eps!=0): why='G5-untestable'
        else:
            imp=np_*fv/pu
            if abs(imp-eps)>max(0.05,0.03*max(abs(eps),abs(imp))): why='G5-fails'
    if why: res['REFUSE']+=1; res['r:'+why]+=1; continue
    res['WRITE']+=1; tok[ntok]+=1
    v=p['pat_cr']; sv=std[(s,q)]
    if abs(v-sv)<=max(0.05,0.01*abs(sv)): res['MATCH']+=1; tok[('m',ntok)]+=1
    else:
        res['MISMATCH']+=1; mism.append((s,q,ntok,v,sv,con.get((s,q))))
        print(f'  MISMATCH {s:12s} {q} tok={ntok} arch={v} stored={sv} con={con.get((s,q))}')
    if (res['WRITE']+res['REFUSE'])%20==0:
        print(f"  ... {res['WRITE']} write / {res['REFUSE']} refuse | match {res['MATCH']} mismatch {res['MISMATCH']}"); sys.stdout.flush()
print('\n=== FULL-GATE HOLD-OUT ===')
print('would WRITE:',res['WRITE'],' of which MATCH',res['MATCH'],'MISMATCH',res['MISMATCH'],
      f"= {100*res['MISMATCH']/max(1,res['WRITE']):.1f}% mismatch")
print('refused:',res['REFUSE'],{k[2:]:v for k,v in res.items() if k.startswith('r:')})
print('by token count among WRITEs:',{k:v for k,v in tok.items()})
print('fetch failures:',res['fetch-fail'])
for m in mism: print('  ',m)
