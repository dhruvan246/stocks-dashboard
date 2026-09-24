#!/usr/bin/env python3
"""Row-level DII heal for the 2015-2022 old-format XBRL era (DATA_RUNBOOK §158): DII = Institutions(Domestic) B1 in
every format. Stages (all local; the ledger is the only repo output):
  classify  -> <work>/proposals.json   one proposal per stored N500 cell that moves >= 0.05 pp (dii or fii)
  one SYM,.. -> verbose trace of one or more symbols
  verify    -> apply the proposals to a COPY of shp_history.json, re-measure the Sep-2022 / 2016 seams
  revfix    -> <work>/revfix.json      the same rules applied to re-filing rows (scripts/shp_revisions.json)
  write     -> merge proposals into scripts/shp_cell_fix.json (+ scripts/_shp_dii_rowfix_audit.json), revfix into
               scripts/shp_revisions.json
Inputs (local, not tracked): BSE SHPQNewFormat lists per symbol (<lists>/<SYM>.json, the 2026-09-22 cache — api.bseindia.com
is 403 since 23-Sep), raw XBRLs in the cache dirs (www.bseindia.com/XBRLFILES/SHPXBRLDataXML/<file> with a bseindia Referer),
<work>/n500_syms.json (the N500 roster present in the store). Environment: DII_ROWFIX_WORK, DII_ROWFIX_LISTS,
DII_ROWFIX_CACHES (os.pathsep-separated). See the runbook section for the rules; every decision is written to the audit file.
"""
import json, os, re, sys, time, copy, collections, difflib
import xml.etree.ElementTree as ET
SCRIPTS=os.path.dirname(os.path.abspath(__file__))
REPO=os.path.dirname(SCRIPTS)
HERE=os.environ.get("DII_ROWFIX_WORK") or os.path.join(SCRIPTS,"_shp_dii_rowfix_work")
LISTS=os.environ.get("DII_ROWFIX_LISTS") or os.path.join(HERE,"bse_all")
CACHES=(os.environ.get("DII_ROWFIX_CACHES") or os.path.join(HERE,"xbrl_bse")).split(os.pathsep)
sys.path.insert(0, SCRIPTS); sys.argv=sys.argv[:1]+sys.argv[1:]
import fetch_shareholding as F

def breakdown(txt):
    """{member: pct} for every single-explicit-member context (no typed members), scaled to percent."""
    root=ET.fromstring(txt) if isinstance(txt,(str,bytes)) else txt
    strip=lambda t:t.split("}",1)[-1]; ctx={}
    for c in root.iter():
        if strip(c.tag)!="context": continue
        mems=[]; typed=False
        for m in c.iter():
            st=strip(m.tag)
            if st=="explicitMember": mems.append((m.text or "").split(":")[-1].strip())
            elif st=="typedMember": typed=True
        ctx[c.get("id")]=mems if not typed else None
    out={}
    for f in root.iter():
        if strip(f.tag)!="ShareholdingAsAPercentageOfTotalNumberOfShares": continue
        mems=ctx.get(f.get("contextRef"))
        if not mems: continue
        try: v=float(str(f.text).strip())
        except Exception: continue
        out["/".join(mems)]=v
    tot=out.get("ShareholdingPatternMember")
    if tot is not None and tot<=1.5: out={k:v*100 for k,v in out.items()}
    return out
MON={"March":3,"June":6,"September":9,"December":12}
LAB_FII=re.compile(r"\bfiis?\b|\bfpis?\b|\bqfi\b|foreig\w* portfolio|foreig\w* instit|foreign bank|foreign venture|qualified foreign|sovereign", re.I)
LAB_PUB=re.compile(r"overseas corporate|\bocb\b|foreig\w* compan|foreig\w* (corporate )?bod|corporate bodies|foreig\w* national|foreig\w* individual|non.?resident|\bnri\b", re.I)
FORLAB=re.compile(r'foreig|muscat|s\.?a\.?o\.?g\b|overseas|\bfpi\b|\bfii\b|\bocb\b|non.?resident|\bnri\b|mauritius|singapore|\bpte\b|\bb\.?v\.?\b|\bllc\b|\bl\.?p\.?\b|\binc\b|\bplc\b|\bltd\.? *\((uk|usa|us)\)|university|college|\bsa\b|\bag\b|\bgmbh\b|\bnv\b|luxembourg|cayman|netherlands|\busa\b|\buk\b|japan|korea|hong ?kong|cyprus|delaware|\bsarl\b|\bs\.?a\.?r\.?l\b|holdings? (ii|iii|iv|v)\b|\bpty\b|\bcapital partners\b|\bglobal\b|international', re.I)
DOMSTRONG=re.compile(r"insur|assurance|provident|pension|nps trust|national pension|mutual fund|\blic\b|\blici\b|qualified inst|q[au]+lified|instit\w* buyers?|\bqib", re.I)
DOMLAB=re.compile(r"insur|assurance|provident|pension|nps trust|national pension|mutual fund|\blic\b|\blici\b|qualified inst|q[au]+lified|instit\w* buyers?|\bqib|\bnbfc|non.?banking|financial institution|\bbank|alternat(e|ive) investment|venture capital|asset reconstruct|general insurance corp", re.I)
INSURER=re.compile(r'insur|assurance|\blic\b|\blici\b|life ins', re.I)
def qe_of(qtr):
    q=(qtr or "").strip().split()
    if len(q)!=2 or q[0] not in MON: return None
    m=MON[q[0]]; return "%s-%02d-%02d"%(q[1],m,31 if m in (3,12) else 30)
def find_file(f):
    for c in CACHES:
        p=os.path.join(c,f)
        if os.path.exists(p): return p
    return None
def rows_of(txt):
    """Typed rows in filer order: [(axis, seq, pct, kind, cat, name)]. Values scaled to percent."""
    root=ET.fromstring(txt); strip=lambda t:t.split("}",1)[-1]
    ctx={}
    for c in root.iter():
        if strip(c.tag)!="context": continue
        for m in c.iter():
            if strip(m.tag)=="typedMember":
                dim=(m.get("dimension") or "").split(":")[-1].replace('DetailsOfSharesHeldBy','').replace('DetailsSharesHeldBy','').replace('Axis','')
                val="".join((x.text or "") for x in m.iter() if x is not m).strip(); ctx[c.get("id")]=(dim,val)
    R={}; frac=True
    for f in root.iter():
        t=strip(f.tag); k=ctx.get(f.get("contextRef"))
        if t=="ShareholdingAsAPercentageOfTotalNumberOfShares" and not k:
            try:
                if float(f.text)>1.5: frac=False
            except: pass
        if not k: continue
        r=R.setdefault(k,{})
        if t=="ShareholdingAsAPercentageOfTotalNumberOfShares":
            try: r['p']=float(f.text)
            except: pass
        elif t.startswith("CategoryOf"): r['cat']=(f.text or '').strip()
        elif t.startswith("NameOf"): r['name']=(f.text or '').strip()
        elif t.startswith("WhetherACategory"): r['kind']=(f.text or '').strip()
    out=[]
    for (ax,val),v in R.items():
        if 'p' not in v: continue
        seq=int(re.sub(r'\D','',val) or 0)
        out.append((ax,seq,v['p']*(100 if frac else 1),(v.get('kind') or ''),(v.get('cat') or ''),(v.get('name') or '')))
    return sorted(out)
def groups(rows, axis):
    """Category rows on one axis with their attached >=1% holders. A holder row carries the filer's own
    category text; it is attached to the category row with the SAME text that has room for it (the nearest
    preceding one first), then to any same-text row, then to the preceding row if it fits, else it forms
    an orphan group (pct 0) so it is never counted twice. Filers list holders before OR after the row."""
    cats=[]; holders=[]
    for ax,seq,p,kind,cat,name in rows:
        if ax!=axis: continue
        is_cat = kind.lower().startswith('categ') or (not kind and not name)
        if is_cat: cats.append({"seq":seq,"pct":p,"label":(cat+" "+name).strip(),"cat":cat,"name":name,"holders":[]})
        else: holders.append((seq,p,name or cat,cat))
    def room(g,p): return sum(h[0] for h in g["holders"])+p<=g["pct"]+0.02
    for seq,p,name,cat in sorted(holders,key=lambda x:-x[1]):
        same=[g for g in cats if norm(g["cat"])==norm(cat)]
        prev=[g for g in same if g["seq"]<seq]; nxt=[g for g in same if g["seq"]>seq]
        target=None
        for g in sorted(prev,key=lambda g:-g["seq"])+sorted(nxt,key=lambda g:g["seq"]):
            if room(g,p): target=g; break
        if target is None:
            # no same-text row has room: the holder still belongs to SOME row on this axis (the filer changed
            # its label between the category row and the holder row) — nearest preceding row with room, else next
            before=[g for g in cats if g["seq"]<seq and room(g,p)]; after=[g for g in cats if g["seq"]>seq and room(g,p)]
            if before: target=before[-1]
            elif after: target=after[0]
        if target is None:
            target={"seq":seq,"pct":0.0,"label":cat,"cat":cat,"name":"","holders":[],"orphan":True}; cats.append(target)
        target["holders"].append((p,name))
    return sorted([g for g in cats if g["pct"]>0.0049 or g["holders"]],key=lambda g:g["seq"])
def norm(n): return re.sub(r'[^A-Z0-9]','',(n or '').upper())
def load_verdicts():
    a=json.load(open(os.path.join(REPO,"scripts","_shp_other_inst_audit.json")))
    return {norm(k):v for k,v in (a.get("name_verdicts") or {}).items()}
def holder_class(name, verdicts, newmap):
    """-> (cls 'foreign'|'domestic'|None, dest 'fii'|'public'|None, evidence). Priority: the filer's own
    new-format placement > curated verdict > strong domestic markers (insurer/NPS/LIC/MF/QIB) > foreign
    markers (jurisdiction, plc/llc/pte, 'global', university...) > weak domestic markers (bank, FI, AIF)."""
    n=norm(name); newmap=newmap or {}
    hit=None
    if n in newmap: hit=(newmap[n],"new-format:"+newmap[n][1])
    else:
        best=None
        for k,(cls,src) in newmap.items():
            if len(n)>=10 and len(k)>=10 and abs(len(n)-len(k))<=8:
                r=difflib.SequenceMatcher(None,n,k).ratio()
                if r>=0.85 and (best is None or r>best[0]): best=(r,(cls,src))
        if best: hit=(best[1],"new-format~:"+best[1][1])
    if hit:
        (cls,src),how=hit
        if cls=="fii": return "foreign","fii",how
        if cls=="public": return "foreign","public",how
        return "domestic",None,how
    if n in verdicts: return verdicts[n], ("fii" if verdicts[n]=="foreign" else None), "curated"
    if DOMSTRONG.search(name) and not FORLAB.search(name.replace("International","").replace("INTERNATIONAL","")): return "domestic", None, "regex"
    if DOMSTRONG.search(name) and re.search(r"pension fund global|government of|monetary authority|\bsingapore\b|\bnorges\b|abu dhabi|\bqatar\b|\bkuwait\b", name, re.I): return "foreign","fii","regex"
    if DOMSTRONG.search(name): return "domestic", None, "regex"
    if FORLAB.search(name): return "foreign", "fii", "regex"
    if DOMLAB.search(name): return "domestic", None, "regex"
    return None, None, ""
NEWFOR={"InstitutionsForeignPortfolioInvestorCatergoryOneMember","InstitutionsForeignPortfolioInvestorCatergoryTwoMember","ForeignDirectInvestmentMember","ForeignVentureCapitalInvestorsMember","SovereignWealthFundsMember","OtherInstitutionsForeignMember","ForeignPortfolioInvestorMember"}
def newmap_for(sym, bse_rows):
    """Holder -> ('fii'|'public'|'domestic', axis) from the filer's FIRST new-format filing (Sep-2022+)."""
    cands=sorted([(qe_of(r.get("qtr")),(r.get("XbrlFile") or "").strip(),r.get("filing_date_time") or "") for r in bse_rows if qe_of(r.get("qtr")) and qe_of(r.get("qtr"))>="2022-09-30" and r.get("XbrlFile")])
    for qe,f,fd in cands[:3]:
        p=find_file(f)
        if not p:
            try:
                from curl_cffi import requests as cr
                x=cr.get("https://www.bseindia.com/XBRLFILES/SHPXBRLDataXML/"+f,headers={"Referer":"https://www.bseindia.com/"},impersonate="chrome",timeout=60)
                if x.status_code==200 and len(x.content)>2000: open(os.path.join(CACHES[-1],f),"wb").write(x.content); p=os.path.join(CACHES[-1],f); time.sleep(0.5)
            except Exception as e: print("  newmap fetch err",sym,f,e,file=sys.stderr)
        if not p: continue
        txt=open(p,'rb').read(); bd=breakdown(txt)
        if "InstitutionsDomesticMember" not in bd and "InstitutionsForeignMember" not in bd: continue
        M={}
        for ax,seq,pct,kind,cat,name in rows_of(txt):
            if kind.lower().startswith('categ') or not name: continue
            if ax.startswith("ForeignPortfolioInvestor") or ax in ("ForeignDirectInvestment","ForeignVentureCapitalInvestors","SovereignWealthFunds","OtherInstitutionsForeign","InstitutionsForeign"): M[norm(name)]=("fii",ax)
            elif ax in ("ForeignCompanies","ForeignNationals","OtherForeignShareholders","BodiesCorporate","OtherNonInstitutions","NonResidentIndians"): M[norm(name)]=("public",ax)
            elif ax in ("MutualFundsOrUti","MutualFundsOrUTI","AlternativeInvestmentFunds","InsuranceCompanies","Banks","ProvidentFundsOrPensionFunds","NBFCsRegisteredWithRbi","OtherFinancialInstitutions","OtherInstitutionsDomestic","SovereignWealthFundsDomestic","VentureCapitalFunds","AssetReconstructionCompanies"): M[norm(name)]=("domestic",ax)
        return M, f
    return {}, None

import difflib
class SymCtx:
    """Per-symbol state: the filer's first new-format holder map, holder memory, label memory."""
    def __init__(self, sym, bse_rows, verdicts):
        self.sym=sym; self.bse_rows=bse_rows; self.verdicts=verdicts
        self.newmap=None; self.newfile=None; self.memory={}; self.label_memory={}
    def hclass(self, hn):
        if self.newmap is None: self.newmap,self.newfile=newmap_for(self.sym,self.bse_rows)
        c,dest,src=holder_class(hn,self.verdicts,self.newmap)
        n=norm(hn)
        if src.startswith("new-format") or src=="curated": self.memory[n]=(c,dest,src)
        elif n in self.memory: c,dest,src=self.memory[n]; src="memory"
        else:
            for k,v in self.memory.items():
                if len(n)>=8 and difflib.SequenceMatcher(None,n,k).ratio()>=0.85: c,dest,src=v; src="memory~"; break
        return c,dest,src

def eval_filing(ctx, qe, txt, bd, res, cur, final=True, unres_log=None):
    """Apply R1/R2/R3 to one old-format filing whose parse `res` describes the stored row `cur`.
    -> dict(t_fii,t_dii,add_ins,ev,split,mv_fii,mv_pub,keep,unres,add_dii,overflow) or None (split unknown)."""
    oth_inst=bd.get("OtherInstitutionsMember") or 0.0
    dom_only=res["dii"]-oth_inst
    # how much of the Any-Other block the STORE already holds in fii (0 = raw parse; oth_inst = a whole-block
    # heal such as §22i / SW-2; anything between = a labelled-part heal such as the FII session's §156).
    fii_shift=(cur[1] or 0)-res["fii"]
    if fii_shift<-0.03 or fii_shift>oth_inst+0.03: return None
    fii_shift=min(max(fii_shift,0.0),oth_inst)
    split="a" if fii_shift<=0.03 else ("b" if abs(fii_shift-oth_inst)<=0.03 else "p")
    rows=rows_of(txt); gi=groups(rows,"OtherInstitutions"); gn=groups(rows,"OtherNonInstitutions")
    ev=[]; mv_fii=mv_pub=keep=unres=0.0; overflow=False
    if oth_inst>=0.005:
        if not gi: unres+=oth_inst; ev.append(("R1-unresolved","no typed rows",round(oth_inst,4)))
        for g in gi:
            lab=g["label"]; hs=[(hp,hn)+ctx.hclass(hn) for hp,hn in g["holders"]]
            lab_kind=("domestic" if (DOMLAB.search(lab) and not LAB_FII.search(lab)) else "public" if LAB_PUB.search(lab) else "fii" if (LAB_FII.search(lab) or FORLAB.search(lab)) else None)
            lab_src="keyword"
            if lab_kind is None and not g["holders"]:
                ltxt=re.sub(r"^(other|others|any other)\s*","",lab,flags=re.I).strip()
                c,dest,src=ctx.hclass(ltxt) if ltxt else (None,None,"")
                if c=="foreign": lab_kind=dest or "fii"; lab_src="label-as-holder:"+src
                elif c=="domestic": lab_kind="domestic"; lab_src="label-as-holder:"+src
                elif norm(lab) in ctx.label_memory: lab_kind=ctx.label_memory[norm(lab)]; lab_src="label-memory"
            hs2=[]
            for hp,hn,c,dest,src in hs:
                inst_tag=bool(re.search(r"\((fpi|fii)\)|\bfpi\b|\bfii\b|foreign portfolio|foreign institutional|\bfvci\b|foreign venture|foreign bank|sovereign", hn, re.I))
                if c=="foreign" and not src.startswith("new-format") and src not in ("memory","memory~") and lab_kind in ("public","fii"):
                    # the label decides an unnamed-type holder; an institution-type holder (FPI/FII tag in its own name, or a
                    # curated FPI fund) is FII whatever the row was called — the 2022 form would list it in B2
                    if src=="curated" or inst_tag: dest="fii"
                    else: dest=lab_kind
                hs2.append((hp,hn,c,dest,src))
            hs=hs2
            fh=[h for h in hs if h[2]=="foreign"]; dh=[h for h in hs if h[2]=="domestic"]
            named_f_pub=sum(h[0] for h in fh if h[3]=="public"); named_f_fii=sum(h[0] for h in fh if h[3]=="fii"); named_d=sum(h[0] for h in dh)
            contained=(sum(h[0] for h in hs)<=g["pct"]+0.02)
            rest=max(0.0,g["pct"]-(sum(h[0] for h in hs) if contained else 0.0))
            desc="; ".join("%s %.2f %s->%s(%s)"%(h[1],h[0],h[2] or '?',h[3] or '-',h[4] or '-') for h in hs)
            if lab_kind=="domestic":
                mv_pub+=named_f_pub; mv_fii+=named_f_fii; keep+=rest+named_d
                ev.append(("R1-domestic-label" if not fh else "R1-foreign-holder-under-domestic-label",lab,round(g["pct"],4),"public=%.2f fii=%.2f"%(named_f_pub,named_f_fii),desc,lab_src))
            elif lab_kind=="fii":
                mv_pub+=named_f_pub; mv_fii+=named_f_fii+rest; keep+=named_d
                ev.append(("R1-fii-label",lab,round(g["pct"],4),"public=%.2f fii=%.2f"%(named_f_pub,named_f_fii+rest),desc,lab_src))
            elif lab_kind=="public":
                mv_pub+=named_f_pub+rest; mv_fii+=named_f_fii; keep+=named_d
                ev.append(("R1-public-label",lab,round(g["pct"],4),"public=%.2f fii=%.2f"%(named_f_pub+rest,named_f_fii),desc,lab_src))
            else:
                if fh and not dh:
                    rest_dest="public" if all(h[3]=="public" for h in fh) else "fii"
                    ctx.label_memory.setdefault(norm(lab),rest_dest)
                    mv_pub+=named_f_pub+(rest if rest_dest=="public" else 0.0); mv_fii+=named_f_fii+(rest if rest_dest=="fii" else 0.0)
                    ev.append(("R1-foreign-holders",lab,round(g["pct"],4),"public=%.2f fii=%.2f"%(named_f_pub+(rest if rest_dest=="public" else 0.0),named_f_fii+(rest if rest_dest=="fii" else 0.0)),desc))
                elif dh and not fh:
                    keep+=(g["pct"] if contained else named_d+g["pct"]); ev.append(("R1-domestic-holders",lab,round(g["pct"],4),desc)); ctx.label_memory.setdefault(norm(lab),"domestic")
                elif fh and dh:
                    mv_pub+=named_f_pub; mv_fii+=named_f_fii; keep+=named_d+rest
                    ev.append(("R1-mixed",lab,round(g["pct"],4),"public=%.2f fii=%.2f"%(named_f_pub,named_f_fii),desc))
                else:
                    unres+=g["pct"]; ev.append(("R1-unresolved",lab,round(g["pct"],4),desc,lab_src))
                    if g["pct"]>=0.5 and final and unres_log is not None: unres_log.append((ctx.sym,qe,lab,round(g["pct"],2),[h[1] for h in hs]))
        rem=oth_inst-(mv_fii+mv_pub+keep+unres)
        if rem>0.02: unres+=rem; ev.append(("R1-uncovered-remainder",round(rem,4)))
        tot=mv_fii+mv_pub+keep+unres
        if tot>oth_inst+0.05:
            overflow=True; ev.append(("R1-overflow",round(tot,2),round(oth_inst,2))); mv_fii=mv_pub=keep=0.0; unres=oth_inst
    add_dii=add_ins=0.0
    for g in gn:
        lab=g["label"]
        if (LAB_PUB.search(lab) or LAB_FII.search(lab) or FORLAB.search(lab)) and not DOMLAB.search(lab): continue
        hs=[(hp,hn)+ctx.hclass(hn) for hp,hn in g["holders"]] if g["holders"] else []
        dh=[h for h in hs if h[2]=="domestic" and (DOMLAB.search(h[1]) or h[4].startswith("new-format"))]
        fh=[h for h in hs if h[2]=="foreign"]
        contained=(sum(h[0] for h in hs)<=g["pct"]+0.02)
        if DOMLAB.search(lab) and not re.search(r'trust', lab, re.I):
            take=g["pct"]-(sum(h[0] for h in fh) if contained else 0.0)
            if not contained: take+=sum(h[0] for h in dh)
            if take>0.005:
                add_dii+=take; add_ins+=sum(h[0] for h in dh if INSURER.search(h[1]))
                ev.append(("R2-label",lab,round(take,4),"; ".join("%s %.2f"%(h[1],h[0]) for h in hs)))
        elif dh:
            take=sum(h[0] for h in dh); add_dii+=take; add_ins+=sum(h[0] for h in dh if INSURER.search(h[1]))
            ev.append(("R2-named",lab,round(take,4),"; ".join("%s %.2f(%s)"%(h[1],h[0],h[4]) for h in dh)))
    nbfc=bd.get("NBFCsRegisteredWithRbiMember") or 0.0
    if nbfc>=0.005: add_dii+=nbfc; ev.append(("R3-nbfc","NBFCsRegisteredWithRbi",round(nbfc,4)))
    # the unresolved part stays where the store has it: in fii up to the shift my resolved holders do not explain
    u_fii=min(unres,max(0.0,fii_shift-mv_fii)); u_dii=unres-u_fii
    t_dii=dom_only+keep+u_dii+add_dii
    t_fii=res["fii"]+mv_fii+u_fii
    return dict(t_fii=round(max(0.0,t_fii),4),t_dii=round(max(0.0,t_dii),4),add_ins=round(add_ins,4),ev=ev,split=split,
                mv_fii=round(mv_fii,4),mv_pub=round(mv_pub,4),keep=round(keep,4),unres=round(unres,4),add_dii=round(add_dii,4),overflow=overflow)

def quarter_files(bse_rows, lo="2015-06-30", hi="2022-06-30"):
    byq={}
    for r in bse_rows or []:
        qe=qe_of(r.get("qtr")); f=(r.get("XbrlFile") or "").strip()
        if not qe or not f or not (lo<=qe<=hi): continue
        byq.setdefault(qe,[]).append(((r.get("filing_date_time") or ""),f))
    return {q:sorted(v) for q,v in byq.items()}

def match_filing(fl, qe, cur, stats=None):
    """The filing among `fl` whose parse describes the row `cur` (prom within 0.06, fii+dii within 0.06)."""
    for fd,f in fl:
        p=find_file(f)
        if not p: continue
        txt=open(p,'rb').read()
        try: bd=breakdown(txt); res=F.parse_shp(txt,qe)
        except Exception:
            if stats is not None: stats["parse_err"]+=1
            continue
        if not res:
            if stats is not None: stats["parse_none"]+=1
            continue
        if "InstitutionsMember" not in bd or "InstitutionsDomesticMember" in bd:
            if stats is not None: stats["not_old_fmt"]+=1
            continue
        if abs((res["prom"] or 0)-(cur[0] or 0))<=0.06 and abs((res["fii"]+res["dii"])-((cur[1] or 0)+(cur[2] or 0)))<=0.06:
            return f,txt,bd,res
        if stats is not None: stats["value_mismatch_try"]+=1
    return None

def classify(limit=0, start=0, only=None, verbose=False):
    syms=json.load(open(os.path.join(HERE,"n500_syms.json")))
    if only: syms=[x for x in syms if x in only]
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json")))
    led=json.load(open(os.path.join(REPO,"scripts","shp_cell_fix.json"))).get("fix",{})
    verdicts=load_verdicts()
    P={}; stats=collections.Counter(); t0=time.time(); unres_log=[]
    for si,sym in enumerate(syms):
        if si<start: continue
        lp=os.path.join(LISTS,sym+".json")
        if not os.path.exists(lp): stats["no_bse_list"]+=1; continue
        d=json.load(open(lp)); bse_rows=d.get("Table") if isinstance(d,dict) else d
        byq=quarter_files(bse_rows); ctx=SymCtx(sym,bse_rows,verdicts)
        order=[(q,f,False) for q,f in sorted(byq.items(),reverse=True)]+[(q,f,True) for q,f in sorted(byq.items())]
        for qe,fl,final in order:
            if final: stats["rows"]+=1
            cur=(hist.get(sym) or {}).get(qe)
            if not cur:
                if final: stats["no_store_row"]+=1
                continue
            chosen=match_filing(fl,qe,cur,stats if final else None)
            if not chosen:
                if final:
                    if not any(find_file(f) for fd,f in fl): stats["not_cached"]+=1
                    else: stats["no_matching_filing"]+=1; verbose and print("  %s NO MATCHING FILING stored=%s files=%s"%(qe,cur[:3],[x[1] for x in fl]))
                continue
            f,txt,bd,res=chosen
            r=eval_filing(ctx,qe,txt,bd,res,cur,final,unres_log)
            if not final: continue
            stats["matched"]+=1
            if r is None: stats["split_unknown"]+=1; continue
            if r["overflow"]: stats["r1_overflow"]+=1
            stats["split_"+r["split"]]+=1
            dd=r["t_dii"]-(cur[2] or 0); df=r["t_fii"]-(cur[1] or 0)
            if verbose: print("  %s %s split=%s res(fii %.2f dii %.2f) -> t_fii %.2f t_dii %.2f | %s"%(qe,f,r["split"],res["fii"],res["dii"],r["t_fii"],r["t_dii"],"; ".join(str(e)[:160] for e in r["ev"])))
            if abs(dd)<0.05 and abs(df)<0.05: stats["unchanged"]+=1; continue
            new=list(cur); new[1]=r["t_fii"]; new[2]=r["t_dii"]
            if cur[4] is not None and r["add_ins"]>0: new[4]=round((cur[4] or 0)+r["add_ins"],4)
            prior=(led.get(sym) or {}).get(qe)
            P["%s|%s"%(sym,qe)]={"file":f,"was":cur,"cell":new,"split":r["split"],"d_dii":round(dd,4),"d_fii":round(df,4),"to_public":r["mv_pub"],"to_fii":r["mv_fii"],"keep_dom":r["keep"],"unresolved":r["unres"],"add_dii":r["add_dii"],"add_ins":r["add_ins"],"ev":r["ev"],"prior_entry":bool(prior),"prior_applied":bool(prior and F._cell_eq(cur,prior.get("cell"))),"newmap_file":ctx.newfile}
            stats["proposed"]+=1
        if si%50==0: print("  %d/%d %s %s %.0fs"%(si,len(syms),sym,dict(stats),time.time()-t0),file=sys.stderr)
        if limit and si>=limit: break
    outp="proposals_one.json" if only else "proposals.json"
    json.dump(P,open(os.path.join(HERE,outp),"w"),indent=0)
    if not only: json.dump(unres_log,open(os.path.join(HERE,"unresolved.json"),"w"),indent=0)
    print("classify done",dict(stats),"unresolved>=0.5:",len(unres_log))
    return P

def revfix():
    """Re-filing rows (scripts/shp_revisions.json) of proposal cells: heal them the same way, from THEIR filing."""
    P=json.load(open(os.path.join(HERE,"proposals.json")))
    revs=json.load(open(os.path.join(REPO,"scripts","shp_revisions.json")))
    verdicts=load_verdicts(); out={}; stats=collections.Counter()
    for k,v in sorted(P.items()):
        sym,qe=k.split("|"); rc=(revs.get(sym) or {}).get(qe)
        if not rc: continue
        stats["rev_rows"]+=1
        # (i) same raw fii/dii as the original -> same healed values
        if abs(float(rc[1])-float(v["was"][1]))<=0.0100001 and abs(float(rc[2])-float(v["was"][2]))<=0.0100001:
            new=list(rc); new[1]=v["cell"][1]; new[2]=v["cell"][2]
            if rc[4] is not None and v["cell"][4] is not None and v["was"][4] is not None: new[4]=round(float(rc[4])+(float(v["cell"][4])-float(v["was"][4])),4)
            out[k]={"was":rc,"cell":new,"how":"same raw fii/dii as the original -> original's healed values"}; stats["same_raw"]+=1; continue
        # (ii) different numbers -> evaluate the re-filing's own document
        lp=os.path.join(LISTS,sym+".json"); d=json.load(open(lp)); bse_rows=d.get("Table") if isinstance(d,dict) else d
        fl=quarter_files(bse_rows).get(qe,[]); ctx=SymCtx(sym,bse_rows,verdicts)
        chosen=match_filing([x for x in fl if x[1]!=v["file"]],qe,rc) or match_filing(fl,qe,rc)
        if not chosen: out[k]={"was":rc,"how":"NO MATCHING DOCUMENT for the re-filing row (left as is)"}; stats["no_doc"]+=1; continue
        f,txt,bd,res=chosen; r=eval_filing(ctx,qe,txt,bd,res,rc)
        if r is None: out[k]={"was":rc,"how":"split unknown (left as is)"}; stats["split_unknown"]+=1; continue
        if abs(r["t_dii"]-float(rc[2]))<0.05 and abs(r["t_fii"]-float(rc[1]))<0.05: stats["rev_unchanged"]+=1; continue
        new=list(rc); new[1]=r["t_fii"]; new[2]=r["t_dii"]
        if rc[4] is not None and r["add_ins"]>0: new[4]=round(float(rc[4])+r["add_ins"],4)
        out[k]={"was":rc,"cell":new,"how":"re-filing document %s re-read: %s"%(f,"; ".join(" ".join(str(x) for x in e) for e in r["ev"])[:600])}; stats["re_read"]+=1
    json.dump(out,open(os.path.join(HERE,"revfix.json"),"w"),indent=0)
    print("revfix",dict(stats))
    return out

def seam_stats(hist, syms, qa="2022-06-30", qb="2022-09-30"):
    import statistics as st
    ds=[]
    for s in syms:
        a=(hist.get(s) or {}).get(qa); b=(hist.get(s) or {}).get(qb)
        if a and b and a[2] is not None and b[2] is not None: ds.append((b[2]-a[2],s,a[2],b[2]))
    v=sorted(x[0] for x in ds)
    q=lambda p: v[int(p*(len(v)-1))]
    return dict(n=len(v),median=round(st.median(v),2),p5=round(q(.05),2),p95=round(q(.95),2),ge3=sum(1 for x in v if abs(x)>=3),ge3pct=round(100*sum(1 for x in v if abs(x)>=3)/len(v),1)), sorted(ds,key=lambda x:-abs(x[0]))[:20]

def verify():
    P=json.load(open(os.path.join(HERE,"proposals.json")))
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json")))
    syms=json.load(open(os.path.join(HERE,"n500_syms.json")))
    before,_=seam_stats(hist,syms)
    H=copy.deepcopy(hist); n=bad=0
    for k,v in P.items():
        sym,qe=k.split("|"); cur=H[sym][qe]
        if not F._cell_eq(cur,v["was"]): bad+=1; continue
        H[sym][qe]=list(v["cell"]); n+=1
        c=v["cell"]
        if c[1]+c[2]>100-c[0]+2 or c[2]<0 or c[1]<0 or (c[4] is not None and c[4]>c[2]+0.05) or (c[3] or 0)>c[2]+0.05: print("SANITY",k,c)
    after,top=seam_stats(H,syms)
    print("applied %d (was-mismatch %d)"%(n,bad)); print("Sep-2022 seam before",before); print("Sep-2022 seam after ",after)
    for d,s,a,b in top: print("   %-12s %7.2f -> %7.2f (%+.2f)"%(s,a,b,d))
    for qa,qb in (("2016-03-31","2016-06-30"),("2016-06-30","2016-09-30"),("2021-12-31","2022-03-31")):
        b0,_=seam_stats(hist,syms,qa,qb); a0,t0=seam_stats(H,syms,qa,qb)
        print("%s->%s before %s after %s"%(qa,qb,b0,a0)); print("   top after:",[(s,round(d,2)) for d,s,a,b in t0[:6]])
    # fii seam too
    def fseam(h):
        ds=[]
        for s in syms:
            a=(h.get(s) or {}).get("2022-06-30"); b=(h.get(s) or {}).get("2022-09-30")
            if a and b: ds.append(b[1]-a[1])
        return sum(1 for x in ds if abs(x)>=3), len(ds)
    print("fii |seam|>=3pp before %s after %s"%(fseam(hist),fseam(H)))
    json.dump(H,open(os.path.join(HERE,"shp_history_healed.json"),"w"),separators=(",",":"))

def write(stamp=None):
    stamp=stamp or time.strftime("%Y-%m-%d")
    P=json.load(open(os.path.join(HERE,"proposals.json")))
    path=os.path.join(REPO,"scripts","shp_cell_fix.json"); raw=open(path,encoding="utf-8").read(); led=json.loads(raw); fix=led.setdefault("fix",{})
    ascii_only = ("\\u00" in raw)     # the file's own style: json.dump(ensure_ascii=True) escapes every non-ASCII char
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json")))
    n_new=n_sup=n_skip=0; audit={"_doc":[
        "§158 row-level DII heal (%s), old-format XBRL era Jun-2015..Jun-2022, Nifty 500. DII = Institutions(Domestic) in every format."%stamp,
        "R1 Institutions->Any Other rows: foreign label/holders leave dii (destination = the filer's own placement of that holder in its first new-format filing: FDI/FPI -> fii, non-inst Foreign Companies -> public; OCB/Foreign Corporate Bodies/Foreign Nationals labels -> public; FII/FPI labels -> fii). Domestic labels/holders (QIB, LIC, insurers, NPS, PF, banks, AIF) stay. Unresolved rows keep the stored split.",
        "R2 Non-institutions->Any Other rows labelled as domestic institutions (Qualified Institutional Buyer, insurance, provident/pension, NBFC, FI, bank, AIF) join dii in full; generic labels contribute only their NAMED domestic-institution holders (>=1% rows). Named insurers also raise the ins slot.",
        "R3 the old-format NBFC row joins dii (the 2022 form lists NBFCs inside Institutions(Domestic)).",
        "Materiality: a cell is entered only when dii or fii moves >= 0.05pp. Evidence per cell: file, stored split, every rule hit with labels and holder names."],"cells":{}}
    for k,v in sorted(P.items()):
        sym,qe=k.split("|"); cur=(hist.get(sym) or {}).get(qe)
        if cur is None or not F._cell_eq(cur,v["was"]): n_skip+=1; continue
        why=("§158 row-level DII heal (%s, DII = Institutions(Domestic) in every format): dii %.2f -> %.2f, fii %.2f -> %.2f. "%(stamp,cur[2],v["cell"][2],cur[1],v["cell"][1])
             +"; ".join(" ".join(str(x) for x in e) for e in v["ev"])[:900]+". Evidence: _shp_dii_rowfix_audit.json")
        ent={"cell":list(v["cell"]),"was":list(cur),"src":"bsexbrl:%s"%v["file"],"why":why}
        prior=(fix.get(sym) or {}).get(qe)
        if prior:
            if not F._cell_eq(cur,prior.get("cell")): n_skip+=1; continue
            ent["superseded"]=prior; n_sup+=1
        else: n_new+=1
        fix.setdefault(sym,{})[qe]=ent
        audit["cells"][k]={x:v[x] for x in ("file","split","d_dii","d_fii","to_public","to_fii","keep_dom","unresolved","add_dii","add_ins","ev","newmap_file")}
    # re-filing rows: same rules, from their own document (revfix stage); the sidecar is edited in place
    rp=os.path.join(HERE,"revfix.json"); n_rev=0
    if os.path.exists(rp):
        R=json.load(open(rp)); rpath=os.path.join(REPO,"scripts","shp_revisions.json"); revs=json.load(open(rpath,encoding="utf-8"))
        audit["revisions"]={}
        for k,v in sorted(R.items()):
            if not v.get("cell"): audit["revisions"][k]={"how":v["how"],"row":v["was"]}; continue
            sym,qe=k.split("|"); rc=(revs.get(sym) or {}).get(qe)
            if not rc or [round(float(x),4) for x in rc[:5] if x is not None]!=[round(float(x),4) for x in v["was"][:5] if x is not None]: audit["revisions"][k]={"how":"sidecar row moved since revfix — left as is","row":rc}; continue
            new=list(rc); new[1]=v["cell"][1]; new[2]=v["cell"][2]; new[4]=v["cell"][4]
            if len(new)>7 and isinstance(new[7],str) and "§158" not in new[7]: new[7]=new[7]+" §158 heal:"+("inherited" if v["how"].startswith("same raw") else "re-read")
            revs[sym][qe]=new; n_rev+=1; audit["revisions"][k]={"how":v["how"],"was":rc,"cell":new}
        json.dump(revs,open(rpath,"w",encoding="utf-8"),separators=(",",":"),sort_keys=True)
    json.dump(led,open(path,"w",encoding="utf-8"),indent=1,ensure_ascii=ascii_only)
    json.dump(audit,open(os.path.join(REPO,"scripts","_shp_dii_rowfix_audit.json"),"w",encoding="utf-8"),indent=0,ensure_ascii=False)
    print("write: %d new, %d superseding earlier entries, %d skipped (store moved), %d re-filing rows healed"%(n_new,n_sup,n_skip,n_rev))

if __name__=="__main__":
    st=sys.argv[1]
    if st=="classify": classify(int(sys.argv[2]) if len(sys.argv)>2 else 0)
    elif st=="one": classify(only=sys.argv[2].split(","), verbose=True)
    elif st=="revfix": revfix()
    elif st=="verify": verify()
    elif st=="write": write()
