# -*- coding: utf-8 -*-
"""Regenerate _wf_gaps.json (current gaps across the 2024 union, 2020Q4..Mar26) and _wf_bins.json
(balanced agent partition: big insurers isolated, then non-insurers packed by std-weight).
Run between chunks so already-filled cells drop out. Usage: python -X utf8 _wf_regap.py
"""
import json
data=json.load(open("../docs/sf_fundamentals.json"))
union=set(json.load(open("_full_union_2024.json")))
INSURERS={"LICI","SBILIFE","HDFCLIFE","ICICIPRULI","ICICIGI","GICRE","NIACL","STARHEALTH","GODIGIT","NIVABUPA","MFSL"}
QES=[y*10000+md for y in range(2020,2027) for md in (331,630,930,1231) if 20200331<=y*10000+md<=20260331]
def rowof(rec,q): return next((x for x in rec if x[0]==q),None)
gaps={}
for sym in union:
    rec=data.get(sym)
    if not rec: continue
    qmin=min(x[0] for x in rec); qmax=max(x[0] for x in rec)
    span=[q for q in QES if qmin<=q<=max(qmax,20260331)]
    g=[]; series={str(x[0]):[x[1],x[3] if len(x)>3 else None] for x in rec if 20180101<=x[0]<=20260331}
    for q in span:
        r=rowof(rec,q); miss=[]
        if r is None: miss=["std","con"]
        else:
            if r[1] is None: miss.append("std")
            if (r[3] if len(r)>3 else None) is None: miss.append("con")
        if miss: g.append({"qe":q,"miss":miss})
    if g: gaps[sym]={"gaps":g,"series":series}
json.dump(gaps,open("_wf_gaps.json","w"))
cells=sum(len(gg["miss"]) for v in gaps.values() for gg in v["gaps"])
con=sum(gg["miss"].count("con") for v in gaps.values() for gg in v["gaps"])
std=cells-con
print("gaps: %d companies, %d cells (con %d std %d)"%(len(gaps),cells,con,std))

# partition
def cs(sym):
    c=s=0
    for gg in gaps[sym]["gaps"]:
        for b in gg["miss"]:
            if b=="con": c+=1
            else: s+=1
    return c,s
def work(sym): c,s=cs(sym); return s*1.0+c*0.3
bins=[]
for s in ["HDFCLIFE","ICICIPRULI","GICRE","NIACL"]:
    if s in gaps: bins.append([s])
tiny=[s for s in ("LICI","MFSL","SBILIFE","ICICIGI","STARHEALTH","GODIGIT","NIVABUPA") if s in gaps]
if tiny: bins.append(tiny)
done=set(sum(bins,[]))
rest=sorted([s for s in gaps if s not in done], key=lambda s:-work(s))
TARGET=8.0; cur=[]; curw=0
for s in rest:
    w=work(s)
    if cur and (curw+w>TARGET or len(cur)>=4):
        bins.append(cur); cur=[]; curw=0
    cur.append(s); curw+=w
if cur: bins.append(cur)
json.dump(bins,open("_wf_bins.json","w"))
print("bins(agents):",len(bins),"| insurer bins:",[i for i,b in enumerate(bins) if any(x in INSURERS for x in b)])
for i,b in enumerate(bins):
    print("  bin%2d (%.1f): %s"%(i,sum(work(x) for x in b),b))
