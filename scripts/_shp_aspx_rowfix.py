#!/usr/bin/env python3
"""Page-era row-level heal, Jun-2006..Mar-2016 (DATA_RUNBOOK §160): DII = Institutions(Domestic), FII = Institutions(Foreign) in
every format, read from BSE's ShareholdingPattern.aspx tables (+ the linked shpperent.aspx >1% table for the Dec-2015/Mar-2016 seam
renderings). Companion of scripts/_shp_dii_rowfix.py (§158), which it imports for the holder-placement machinery.
Local inputs (never in the repo): <work>/aspx_pages/<code>_<qtrid>.html.gz, <work>/shpperent/<code>_<qtrid>.html.gz,
<work>/aspx_codes.json (symbol -> BSE scrip code), <work>/n500_syms.json. Environment: DII_ROWFIX_WORK (work dir, shared with §158),
DII_ROWFIX_LISTS, DII_ROWFIX_CACHES (see _shp_dii_rowfix.py). Run from <work>: classify (label rules), seam (88/89 reconstruction),
verify, write.
"""
import json, os, re, sys, gzip, html, time, collections, urllib.parse, statistics as st
SCRIPTS=os.path.dirname(os.path.abspath(__file__)); REPO=os.path.dirname(SCRIPTS)
HERE=os.environ.get("DII_ROWFIX_WORK") or os.path.join(SCRIPTS,"_shp_dii_rowfix_work")
os.chdir(HERE); sys.path.insert(0,SCRIPTS); sys.path.insert(0,HERE)
import _shp_dii_rowfix as D

# ---- page table parser ----
def rows_of(h):
    out=[]
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', h, re.S):
        tds=[html.unescape(re.sub(r'\s+',' ',re.sub('<[^>]+>','',t))).strip() for t in re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.S)]
        tds=[t for t in tds if t!='']
        if len(tds)>=1: out.append(tds)
    return out
def num(x):
    try: return float(x.replace(',',''))
    except: return None
def parse(h):
    """-> dict(prom, inst, noninst, c) of (label, %(A+B+C)) rows. Blocks switch on the section headings ('(A) ...', '(1) Institutions',
    '(2) Non-Institutions', '(C) ...') so filers with no promoter block (ITC, ICICIBANK, KARURVYSYA) parse too; the 'Total shareholding
    of Promoter' / 'Sub Total' rows switch as a fallback (columns: holders, shares, demat, %(A+B), %(A+B+C), pledged...)."""
    rs=rows_of(h); blocks={'prom':[],'inst':[],'noninst':[],'c':[]}; cur='prom'
    for t in rs:
        lab=t[0]; L=re.sub(r'\s+',' ',lab.lower()).strip()
        if re.match(r'\(a\)\s*(share|promoter)', L): cur='prom'; continue
        if re.match(r'\(b\)\s*public', L): continue
        bare=len(t)==1 or not any(num(v) is not None for v in t[1:])      # a bare heading row carries no numbers (the promoter-foreign block has an 'Institutions' ROW)
        if re.match(r'\(1\)\s*institution', L) or (bare and L=='institutions'): cur='inst'; continue
        if re.match(r'\(2\)\s*non', L) or (bare and L in ('non-institutions','non institutions')): cur='noninst'; continue
        if re.match(r'\(c\)\s*shares held by custodian', L) or L.startswith('(c) shares held by custodians'): cur='c'; continue
        vals=t[1:]; nums=[num(v) for v in vals]
        if len(nums)>=5 and all(n is not None for n in nums[:5]): pct_abc=nums[4]
        else: continue
        if L.startswith('total shareholding of promoter'): blocks['prom'].append((lab,pct_abc)); cur='inst'; continue
        if L.startswith('sub total'):
            if cur=='inst': cur='noninst'
            elif cur=='noninst': cur='c'
            continue
        if L.startswith('total public') or L.startswith('total (a)'): continue
        blocks[cur].append((lab,pct_abc))
    return blocks

# ---- linked shpperent.aspx table (>1% holders; category lumps on the 88/89 pages) ----
H={"Referer":"https://www.bseindia.com/corporates/ShareholdingPattern.aspx","Accept":"text/html,application/xhtml+xml"}
def fetch(code, qtrid, comp="X", qname="X"):
    p="shpperent/%d_%d.html.gz"%(code,qtrid)
    if os.path.exists(p): return gzip.open(p,'rt',encoding='utf-8').read()
    from curl_cffi import requests as cr
    u="https://www.bseindia.com/corporates/shpperent.aspx?scripcd=%d&qtrid=%d&CompName=%s&QtrName=%s"%(code,qtrid,urllib.parse.quote(comp),urllib.parse.quote(qname))
    for a in range(3):
        try:
            r=cr.get(u,headers=H,impersonate="chrome",timeout=60)
            if r.status_code==200 and len(r.text)>2000:
                with gzip.open(p,'wt',encoding='utf-8') as fh: fh.write(r.text)
                time.sleep(0.8); return r.text
        except Exception as e: print("  err",code,qtrid,e,file=sys.stderr)
        time.sleep(3+3*a)
    return None
def rows(h):
    out=[]
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', h, re.S):
        tds=[html.unescape(re.sub(r'\s+',' ',re.sub('<[^>]+>','',t))).strip() for t in re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.S)]
        tds=[t for t in tds if t]
        if len(tds)>=4 and tds[0].isdigit():
            try: sh=int(tds[2].replace(',','')); pct=float(tds[3])
            except Exception: continue
            out.append((tds[1], sh, pct if pct>0 else None))
    return out

# ---- Dec-2015 / Mar-2016 seam reconstruction ----
FIIL=re.compile(r"foreign instit|foreign port|\bfpi|\bfii\b|qualified foreign|\bqfi", re.I)
CATLIKE=re.compile(r"corporate|clearing|trust|foreign|partnership|margin|association|custodian|domestic|nationals?$|resident|hindu|director|employee|body|bodies|iepf|escrow|unclaimed|others?$|individual|member|institution|investor|fund$", re.I)
def total_shares(main_html):
    for t in rows_of(main_html):
        if t[0].lower().startswith('total (a)+(b)+(c)') and len(t)>=4:
            try: return int(t[2].replace(',',''))
            except Exception: pass
    for t in rows_of(main_html):
        if t[0].lower().startswith('total (a)+(b)') and len(t)>=4:
            try: return int(t[2].replace(',',''))
            except Exception: pass
HOLDERISH=re.compile(r" - |a/c|\b(fund|limited|ltd|llc|plc|inc|corporation|company|authority|pte|sa|bv|nv|holdings?|trust)\b", re.I)
def nkey(n):
    """Holder-name key across the filer's own spellings: 'Indium V (Mauritius) Holdings Limited' == '... Ltd'."""
    n=re.sub(r"\b(limited)\b","LTD",n,flags=re.I); n=re.sub(r"\b(private)\b","PVT",n,flags=re.I); n=re.sub(r"\b(company)\b","CO",n,flags=re.I); n=re.sub(r"\bthe\b","",n,flags=re.I)
    return D.norm(n)
def in_known(key, known):
    """The filer's own spellings drift between tables ('Indian V (Mauritius) Holding Ltd' vs 'INDIUM V (MAURITIUS) HOLDINGS LIMITED', names cut at 40 chars)."""
    if not known or not key: return False
    if key in known: return True
    import difflib
    for k in known:
        if len(key)>=12 and len(k)>=12 and (key.startswith(k) or k.startswith(key)): return True
        if len(key)>=10 and difflib.SequenceMatcher(None,key,k).ratio()>=0.85: return True
    return False
def sibling_foreign_names(code):
    """Holders the filer itself prefixed FII/FPI/QFI on either seam table (88 or 89): foreign on the sibling page too (POONAWALLA 'QFI - LEAPFROG' on 88, bare name on 89)."""
    out=set()
    for q in (88,89):
        h=fetch(code,q)
        if not h: continue
        for n,sh,p in rows(h):
            m=re.match(r"^\s*(fii|fpi|qfi)s?\s*[-:]\s*(\S.*)$",n,re.I)
            if m: out.add(nkey(m.group(2)))
    return out
def reconstruct(sym, code, q, ctx, verdicts, known_foreign=None):
    """fii on a seam page = the main page's standard foreign rows + EITHER the table's FII/FPI CATEGORY lump(s) (named FPI holders
    sit inside them: FEDERALBNK Mar-16 lump 26.9 + the same names again = 58 -> held before this rule) OR, when the table carries no
    category lump, the FII/FPI-prefixed holder rows + named holders the filer's own 2022 row / SW-2 curated verdict / name markers
    place in fii (de-duplicated by normalised name). Company/individual-type category rows never join."""
    main=gzip.open('aspx_pages/%d_%d.html.gz'%(code,q),'rt',encoding='utf-8').read(); tot=total_shares(main)
    if not tot: return None,"no total shares"
    b=parse(main); si,subi=classify_rows(b['inst'],'inst'); sn,subn=classify_rows(b['noninst'],'noninst')
    base_fii=si.get('fii',0)+si.get('fpi',0)+si.get('qfi',0)+si.get('fvci',0)
    lumps=si.get('other',0)+sn.get('other',0)
    h=fetch(code,q)
    if not h: return None,"shpperent unavailable"
    rs=[(n,s/tot*100) for n,s,p in rows(h)]
    cover=sum(v for n,v in rs)
    def is_fii_label(n): return bool(FIIL.search(n)) and 'DR' not in n.upper().replace('-',' ').split()
    PREFIXED=re.compile(r"^\s*(fii|fpi|qfi)s?\s*[-:]\s*\S", re.I)          # 'FII- Samena Special Situations Mauritius' is a holder with a category prefix, not a lump
    cat=[(n,v) for n,v in rs if v>=0.005 and is_fii_label(n) and not HOLDERISH.search(n) and not PREFIXED.match(n)]
    add=0.0; ev=[]; seen=set()
    if cat:
        for n,v in cat: add+=v; ev.append(("lump-fii",n,round(v,4)))
    else:
        for n,v in rs:
            if v<0.005: continue
            key=nkey(re.sub(r"^\s*(fii|fpi|qfi)\s*[-:]\s*","",n,flags=re.I))
            if key in seen: continue
            if is_fii_label(n):
                if re.match(r"^\s*fiis?\s*[-:]",n,re.I) and base_fii>0.005: ev.append(("prefixed-fii-inside-fii-row",n,round(v,4))); seen.add(key); continue   # 'FII - X' rows are the main page's FII row itself (POONAWALLA 'FII - BAY POND' 1.62+3.29 inside 14.49)
                seen.add(key); add+=v; ev.append(("prefixed-fii",n,round(v,4))); continue
            if re.match(r"^(foreign bod|foreign compan|overseas corporate|\bocb|non.?resident|\bnri|foreign national|foreign individual)", n.strip(), re.I): continue   # company/individual-type label -> public
            if CATLIKE.search(n) and not re.search(r"\b(fund|limited|ltd|llc|plc|inc|corporation|company|authority|pte|sa|bv|nv)\b", n, re.I): continue   # a category lump, not a holder
            c,dest,src=ctx.hclass(n,v)                                # a named holder: placement by the filer's 2022 row / curated / markers
            if (c is None or dest is None) and in_known(key,known_foreign): c,dest,src="foreign","fii","prefixed FII/FPI/QFI on the sibling seam page"
            if c=="foreign" and dest=="fii": seen.add(key); add+=v; ev.append(("named-fii",n,round(v,4),src))
    whole_block=False
    block=si.get('other',0)
    if not cat and block>=1.0 and add>=0.9*block:     # §158 R1 whole-block rule: every named holder of the institutional Any-Other block is foreign -> the unnamed rest follows (POONAWALLA Dec-15: 49.62 block, 49.19 named)
        ev.append(("whole-block-fii","institutional Any-Other block %.2f, named foreign %.2f"%(block,add),round(block-add,4))); add=block; whole_block=True
    return dict(t_fii=round(base_fii+add,4),base_fii=base_fii,lumps=lumps,cover=cover,ev=ev,whole_block=whole_block,rows=[(n,round(v,2)) for n,v in rs if v>=0.3]),None
def plausible(t, prev_fii, next_fii, stored):
    """The reconstruction must sit within the neighbours' range ±3 pp AND be closer to the next (healed XBRL) quarter than the stored value."""
    lo=min(prev_fii,next_fii)-3.0; hi=max(prev_fii,next_fii)+3.0
    return lo<=t<=hi and abs(t-next_fii)<abs(stored-next_fii)

# ---- label rules, classify, seam pass, verify, write ----
FII_LAB=re.compile(r"foreign port\s?[fo]?olio|\bfpi|foreign institutional|\bfii|qualified foreign|\bqfi|foreign venture|\bfvci|foreign bank|foreign mutual|foreign financial|sovereign", re.I)
DR_LAB=re.compile(r"\bd\.?\s?r\.?\b|\bgdr|\badr|depositor", re.I)          # depository-receipt lines never join fii (§151 user rule) -> unresolved, keep stored placement
MLT_LAB=re.compile(r"multilateral|bilateral|international finance|world bank|\bifc\b|asian development", re.I)   # IFC/ADB-type rows: the SW-2 curated verdict places IFC/CDC/ADB in fii in the XBRL era (§156/§159) -> fii here too
def label_class(lab):
    if DR_LAB.search(lab): return None
    if MLT_LAB.search(lab): return "fii"
    f=bool(FII_LAB.search(lab)); pb=bool(PUB_LAB.search(lab)); d=bool(DII_LAB.search(lab))
    if f and not pb: return "fii"          # 'Foreign Banks & Foreign Companies' (fii+pub) is a mixed lump -> None
    if pb and not f: return "pub"
    if d and not pb and not f: return "dii"
    return None
DII_LAB=re.compile(r"qualified institutional|\bqib|insurance|assurance|provident|pension|nps|alternat(e|ive) investment|venture capital fund|\bvcf|nbfc|non.?banking|mutual fund|financial institution|\bbanks?\b|\blic\b", re.I)
PUB_LAB=re.compile(r"overseas corporate|\bocb|foreign compan|foreign (corporate )?bod|foreign national|non.?resident|\bnri|clearing|trust|huf|director|employee|bodies corporate|individual|iepf|escrow|unclaimed|custodian|depositor|market maker|hindu", re.I)
STD_INST={"mutual funds / uti":"mf","financial institutions / banks":"bank","insurance companies":"ins","foreign institutional investors":"fii","central government / state government(s)":"gov","venture capital funds":"vcf","foreign venture capital investors":"fvci","qualified foreign investor":"qfi","foreign portfolio investors":"fpi","any others (specify)":"other","any other (specify)":"other"}
def qtrid(q):
    y=int(q[:4]); m=int(q[5:7]); return (y-2001)*4+{3:29,6:30,9:31,12:32}[m]
def classify_rows(rows, block):
    """rows of one block -> (std dict, subs list) where subs = rows under an 'Any Others' header with their label class."""
    std={}; subs=[]; in_other=False; other_val=0.0
    for lab,p in rows:
        L=re.sub(r"\s+"," ",lab.strip().lower())
        key=STD_INST.get(L)
        if key=="other": in_other=True; other_val=p; std["other"]=std.get("other",0.0)+p; continue
        if key and not in_other: std[key]=std.get(key,0.0)+p; continue
        if block=="noninst" and not in_other and (L.startswith("bodies corporate") or L.startswith("individual")): std.setdefault("std_noninst",0.0); std["std_noninst"]+=p; continue
        if key and in_other: in_other=False; std[key]=std.get(key,0.0)+p; continue
        # a sub-row (after Any Others) OR a non-standard standalone row (e.g. 'Foreign Portfolio Investors' listed as its own non-inst row)
        subs.append((lab,p,label_class(lab),in_other))
    return std,subs
HANDOFF={  # symbol -> which row / holder the XBRL era shows to be B2-placed (FII session's measured hand-off, §159). The AMOUNT is always the
           # page's own row for that quarter, or the linked shpperent table's named holding for that quarter — never a number carried across quarters.
    "ITC":dict(rx=re.compile(r"foreign (bodies )?corporate|foreign compan|overseas corporate|foreign bod", re.I), names=re.compile(r"tobac+o manufactur|myddleton|rothmans", re.I),
               note="BAT entities (Tobacco Manufacturers, Myddleton, Rothmans), FDI in the 2022 form"),
    "ZENSARTECH":dict(rx=re.compile(r"overseas corporate|foreign (bodies )?corporate|foreign compan", re.I), names=re.compile(r"marina holdco", re.I), note="Marina Holdco (FPI) Ltd, curated FPI"),
    "KOTAKBANK":dict(rx=re.compile(r"foreign bank", re.I), row_ok=False, names=re.compile(r"sumitomo mitsui", re.I), note="Sumitomo Mitsui Banking Corp, OtherInstitutionsForeign in the 2022 form (named holding only, never the 'Foreign Banks' row)"),
}
TOL=0.10     # a stored cell must equal one reading convention of the page within this (BSE's two renderings round differently: ITC Dec-15 dii 35.21 vs 35.29)
_KNOWN={}
def evaluate(h, cur, sym=None, qi=None, code=None, ctx=None):
    b=parse(h)
    if not b["inst"] and not b["noninst"]: return None,"no table"
    prom=b["prom"][-1][1] if b["prom"] else 0.0
    si,subi=classify_rows(b["inst"],"inst"); sn,subn=classify_rows(b["noninst"],"noninst")
    HO=HANDOFF.get(sym); hand=set()
    if HO:
        for lab,p,cls,io in subi+subn:
            if HO["rx"].search(lab.strip()) and p>=0.005: hand.add(lab)
    subi=[(lab,p,("hand" if lab in hand else cls),io) for lab,p,cls,io in subi]; subn=[(lab,p,("hand" if lab in hand else cls),io) for lab,p,cls,io in subn]   # a hand-off row is decided by the hand-off rule only
    base_dii=si.get("mf",0)+si.get("bank",0)+si.get("ins",0); base_fii=si.get("fii",0)
    extra_fii_std=si.get("fpi",0)+si.get("qfi",0)+si.get("fvci",0)
    fii_subs_inst=sum(p for lab,p,cls,io in subi if cls=="fii"); other_inst=si.get("other",0.0)
    dii_subs_inst=sum(p for lab,p,cls,io in subi if cls=="dii")
    std_for_noninst=[(lab,p) for lab,p in b["noninst"] if STD_INST.get(re.sub(r"\s+"," ",lab.strip().lower())) in ("fii","fpi","qfi","fvci")]   # a standard foreign label filed INSIDE non-institutions (JUBLPHARMA Sep-15 'Foreign Portfolio Investors' 8.41)
    std_dom_noninst=[(lab,p) for lab,p in b["noninst"] if STD_INST.get(re.sub(r"\s+"," ",lab.strip().lower())) in ("vcf","mf","bank","ins")]
    fii_rows_noninst=sum(p for lab,p,cls,io in subn if cls=="fii")+sum(p for lab,p in std_for_noninst); dii_rows_noninst=sum(p for lab,p,cls,io in subn if cls=="dii")+sum(p for lab,p in std_dom_noninst)
    ev=[]; prom_fix=None
    if abs(prom-(cur[0] or 0))>0.06:
        if (cur[0] or 0)==0 and prom>0: prom_fix=prom; ev.append(("prom-from-page","stored promoter 0.00, page %.2f"%prom,round(prom,4)))
        else: return None,"prom mismatch page %.2f store %.2f"%(prom,cur[0] or 0)
    # which reading produced the stored cell? try the known conventions, closest wins
    fii_c=[("fii-row",base_fii),("fii+std",base_fii+extra_fii_std),("fii+std+subs",base_fii+extra_fii_std+fii_subs_inst),
           ("fii+std+other",base_fii+extra_fii_std+other_inst),("fii+std+subs+other",base_fii+extra_fii_std+fii_subs_inst+other_inst),
           ("fii+std+noninst",base_fii+extra_fii_std+fii_rows_noninst),("fii+std+subs+noninst",base_fii+extra_fii_std+fii_subs_inst+fii_rows_noninst)]
    dii_c=[("mf+bank+ins",base_dii),("mf+bank+ins+vcf",base_dii+si.get("vcf",0)),("mf+bank+ins+subs",base_dii+dii_subs_inst),("mf+bank+ins+vcf+subs",base_dii+si.get("vcf",0)+dii_subs_inst),("mf+bank+ins+noninst",base_dii+dii_rows_noninst)]
    fm=min(fii_c,key=lambda x:abs(x[1]-(cur[1] or 0))); dm=min(dii_c,key=lambda x:abs(x[1]-(cur[2] or 0)))
    if abs(fm[1]-(cur[1] or 0))>TOL: return None,"fii mismatch page %.2f(+%.2f) store %.2f"%(base_fii,extra_fii_std,cur[1] or 0)
    if abs(dm[1]-(cur[2] or 0))>TOL: return None,"dii mismatch page %.2f store %.2f"%(base_dii,cur[2] or 0)
    # holder rule (§158 R1 at page level): an unresolved or company-labelled sub-row >= 1pp whose value is EXACTLY the sum of named >1%
    # holders that the filer's own 2022 row / SW-2 curated verdict / the filer's own FII-FPI-QFI prefix on its seam tables place in fii
    # (never a bare name marker) -> fii. POONAWALLA Sep-15 'Others' 30.89 = Indium 8.60 + LeapFrog 7.82 + Zend 14.47 (QFI-prefixed on the Dec-15 table).
    hold_ev=[]
    if ctx is not None and code and qi:
        cands=[i for i,(lab,p,cls,io) in enumerate(subi) if cls in (None,"pub") and p>=1.0]
        candn=[i for i,(lab,p,cls,io) in enumerate(subn) if cls is None and p>=1.0]
        if cands or candn:
            tot=total_shares(h); hp=fetch(code,qi)
            if code not in _KNOWN: _KNOWN[code]=sibling_foreign_names(code)
            known=_KNOWN[code]
            if tot and hp:
                hold=[]
                for n,sh,_ in rows(hp):
                    v=sh/tot*100
                    if v<0.05: continue
                    m=re.match(r"^\s*(fii|fpi|qfi)s?\s*[-:]\s*(\S.*)$",n,re.I)
                    if m and m.group(1).lower().startswith("fii"): continue          # inside the page's FII row
                    key=nkey(m.group(2) if m else n)
                    if HO and HO["names"].search(n): continue        # the hand-off moves these itself
                    c,dest,src=ctx.hclass(n,v)
                    if c=="domestic": continue
                    strong=(c=="foreign" and dest=="fii" and (src.startswith("new-format") or src in ("curated","memory","memory~"))) or bool(m) or in_known(key,known) or bool(re.search(r"\((fpi|fii)\)",n,re.I))
                    if strong and not (c=="foreign" and dest=="public" and src.startswith("new-format")): hold.append((key,n,round(v,4),src if (c=="foreign" and dest=="fii") else "filer FII/FPI/QFI prefix"))
                hold=hold[:14]; used=set()
                def explain(p):
                    idx=[i for i in range(len(hold)) if i not in used]
                    for mask in range(1,1<<len(idx)):
                        ssum=sum(hold[idx[k]][2] for k in range(len(idx)) if mask>>k&1)
                        if abs(ssum-p)<=max(0.06,0.006*p): return [idx[k] for k in range(len(idx)) if mask>>k&1]
                    return None
                for lst,cand in ((subi,cands),(subn,candn)):
                    for i in sorted(cand,key=lambda i:-lst[i][1]):
                        lab,p,cls,io=lst[i]; got=explain(p)
                        if got:
                            used.update(got); lst[i]=(lab,p,"fii",io); hold_ev.append(("row-by-holders",lab,round(p,4),"; ".join("%s %.2f (%s)"%(hold[g][1][:40],hold[g][2],hold[g][3]) for g in got)))
    fii_subs_inst=sum(p for lab,p,cls,io in subi if cls=="fii"); dii_subs_inst=sum(p for lab,p,cls,io in subi if cls=="dii")     # re-summed after the holder rule (the reading convention above was identified on the label classes alone)
    fii_rows_noninst=sum(p for lab,p,cls,io in subn if cls=="fii")+sum(p for lab,p in std_for_noninst); dii_rows_noninst=sum(p for lab,p,cls,io in subn if cls=="dii")+sum(p for lab,p in std_dom_noninst)
    ev.extend(hold_ev)
    # target = B2-equivalent / B1-equivalent from the page's labelled rows
    io_sum=sum(p for lab,p,cls,io in subi if io); other_resid=max(0.0,other_inst-io_sum)
    unres_io=sum(p for lab,p,cls,io in subi if io and cls is None)
    t_fii=base_fii+extra_fii_std+fii_subs_inst+fii_rows_noninst+((unres_io+other_resid) if "other" in fm[0] else 0.0)   # unresolved / unlabelled parts of a stored-in-fii Any-Other block keep the stored placement
    t_dii=base_dii+si.get("vcf",0)+dii_subs_inst+dii_rows_noninst
    add_ins=sum(p for lab,p,cls,io in subi+subn if cls=="dii" and re.search(r"insur|assurance|\blic\b",lab,re.I))
    if extra_fii_std>0.005 and "std" not in fm[0]: ev.append(("std-fii-rows","FPI/QFI/FVCI rows in institutions",round(extra_fii_std,4)))
    if si.get("vcf",0)>0.005 and "vcf" not in dm[0]: ev.append(("std-vcf","Venture Capital Funds row",round(si["vcf"],4)))
    for lab,p,cls,io in subi:
        if p<0.005: continue
        if cls=="fii" and "subs" not in fm[0] and not ("other" in fm[0] and io): ev.append(("inst-sub-fii",lab,round(p,4)))
        elif cls=="dii" and "subs" not in dm[0]: ev.append(("inst-sub-dii",lab,round(p,4))+(("leaves fii",) if ("other" in fm[0] and io) else ()))
        elif cls=="pub" and "other" in fm[0] and io: ev.append(("inst-sub-pub-leaves-fii",lab,round(p,4)))
        elif cls is None: ev.append(("inst-sub-unresolved",lab,round(p,4),"stays in fii" if ("other" in fm[0] and io) else "stays as stored"))
    for lab,p,cls,io in subn:
        if p<0.005: continue
        if cls=="fii" and "noninst" not in fm[0]: ev.append(("noninst-fii",lab,round(p,4)))
        elif cls=="dii" and "noninst" not in dm[0]: ev.append(("noninst-dii",lab,round(p,4)))
    for lab,p in std_for_noninst:
        if p>=0.005 and "noninst" not in fm[0]: ev.append(("noninst-fii",lab,round(p,4)))
    for lab,p in std_dom_noninst:
        if p>=0.005 and "noninst" not in dm[0]: ev.append(("noninst-dii",lab,round(p,4)))
    # hand-off symbols: the row the XBRL era shows to be B2-placed moves IN FULL at the page's own value; when the page labels it
    # differently (ITC Jun/Sep-2015 'Others', the 88/89 seam renderings) the linked shpperent table's NAMED holding for that quarter is used
    used_shp=False
    if HO:
        moved=0.0
        if HO.get("row_ok",True):
            for lab,p,cls,io in subi+subn:
                if cls=="hand" and p>=0.005: moved+=p; ev.append(("handoff-fii",lab,round(p,4),HO["note"]))
        if moved<0.005 and qi and code:
            tot=total_shares(h); hp=fetch(code,qi)
            if tot and hp:
                for n,sh,_ in rows(hp):
                    if HO["names"].search(n):
                        v=sh/tot*100
                        if v>=0.005: moved+=v; used_shp=True; ev.append(("handoff-fii",n,round(v,4),HO["note"]+" (shpperent)"))
        t_fii+=moved
    return dict(t_fii=round(t_fii,4),t_dii=round(t_dii,4),add_ins=round(add_ins,4),ev=ev,base=(prom,base_fii,base_dii),conv=(fm[0],dm[0]),prom_fix=prom_fix,shpperent=used_shp),None
def classify():
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json")))
    codes=json.load(open("aspx_codes.json")); syms=json.load(open("n500_syms.json"))
    led=json.load(open(os.path.join(REPO,"scripts","shp_cell_fix.json"))).get("fix",{})
    verdicts=D.load_verdicts()
    P={}; stats=collections.Counter(); reasons=collections.Counter(); unres=collections.Counter()
    for s in syms:
        c=codes.get(s)
        if not c: continue
        ctx=None; lp=os.path.join(D.LISTS,s+".json")
        if os.path.exists(lp):
            dd=json.load(open(lp)); bse_rows=dd.get("Table") if isinstance(dd,dict) else dd; ctx=D.SymCtx(s,bse_rows,verdicts)
        for q in sorted(hist.get(s,{})):
            if not ("2006-06-30"<=q<="2016-03-31"): continue
            f="aspx_pages/%d_%d.html.gz"%(c,qtrid(q))
            if not os.path.exists(f): stats["no_page"]+=1; continue
            stats["pages"]+=1; cur=hist[s][q]
            try: h=gzip.open(f,"rt",encoding="utf-8").read(); r,why=evaluate(h,cur,s,qtrid(q),c,ctx)
            except Exception as e: stats["parse_err"]+=1; print("  err",s,q,repr(e)[:120],file=sys.stderr); continue
            if r is None: stats["no_match"]+=1; reasons[why.split(" page")[0]]+=1; continue
            for e in r["ev"]:
                if e[0]=="inst-sub-unresolved": unres[e[1][:40]]+=1
            stats["conv:"+r["conv"][0]]+=1
            if (abs(r["t_fii"]-(cur[1] or 0))<0.05 and abs(r["t_dii"]-(cur[2] or 0))<0.05 and not r["prom_fix"]) or not (r["ev"] or r["prom_fix"]): stats["unchanged"]+=1; continue
            new=list(cur)      # a slot that does not move >= 0.05 keeps the STORED value: shp_refine_4dp re-derives dii/ins at 4 dp after apply_cell_fix and would otherwise diverge from the ledger (89 cells aligned by hand on 2026-09-24)
            if abs(r["t_fii"]-(cur[1] or 0))>=0.05: new[1]=r["t_fii"]
            if abs(r["t_dii"]-(cur[2] or 0))>=0.05: new[2]=r["t_dii"]
            if r["prom_fix"]: new[0]=round(r["prom_fix"],4)
            if cur[4] is not None and r["add_ins"]>0: new[4]=round((cur[4] or 0)+r["add_ins"],4)
            prior=(led.get(s) or {}).get(q)
            P["%s|%s"%(s,q)]={"file":os.path.basename(f),"was":cur,"cell":new,"d_dii":round(r["t_dii"]-(cur[2] or 0),4),"d_fii":round(r["t_fii"]-(cur[1] or 0),4),"add_ins":r["add_ins"],"ev":r["ev"],"prior_entry":bool(prior),"shpperent":r.get("shpperent",False)}
            stats["proposed"]+=1
    json.dump(P,open("proposals_aspx.json","w"),indent=0)
    print("aspx classify",dict(stats)); print(" no-match reasons:",reasons.most_common(6)); print(" unresolved inst sub-row labels:",unres.most_common(10))
    return P
def seam_pass(names=None, dip=5.0):
    """Phase 2b: qtrid 88/89 FII reconstruction for names whose fii dips >= `dip` pp at Dec-15/Mar-16 vs both neighbours, the Sep-15
    neighbour taken AFTER the label-rule proposals (ZENSARTECH: Sep-15 13.93 -> 36.99 makes the stored 14.11 a dip). A cell whose
    label-rule proposal already sits inside the neighbours' range is left to that proposal (ITC/ZENSARTECH hand-off by name)."""
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json"))); codes=json.load(open("aspx_codes.json")); syms=json.load(open("n500_syms.json"))
    P=json.load(open("proposals_aspx.json")) if os.path.exists("proposals_aspx.json") else {}
    verdicts=D.load_verdicts(); out={}; stats=collections.Counter()
    for s in syms:
        if names and s not in names: continue
        pa=P.get(s+"|2015-09-30"); a=pa["cell"] if pa else (hist.get(s) or {}).get("2015-09-30"); c=(hist.get(s) or {}).get("2016-06-30")
        if not a or not c or a[1] is None or c[1] is None: continue
        ctx=None
        for q,qe in ((88,"2015-12-31"),(89,"2016-03-31")):
            b=(hist.get(s) or {}).get(qe)
            if not b or b[1] is None: continue
            pb=P.get("%s|%s"%(s,qe))
            if pb and plausible(pb["cell"][1],a[1],c[1],b[1]): stats["label_rule_already_plausible"]+=1; continue
            if not (a[1]-b[1]>=dip and c[1]-b[1]>=dip): continue
            stats["dip_cells"]+=1
            if not os.path.exists("aspx_pages/%d_%d.html.gz"%(codes[s],q)): stats["no_page"]+=1; continue
            if ctx is None:
                lp=os.path.join(D.LISTS,s+".json"); dd=json.load(open(lp)); bse_rows=dd.get("Table") if isinstance(dd,dict) else dd; ctx=D.SymCtx(s,bse_rows,verdicts)
            r,why=reconstruct(s,codes[s],q,ctx,verdicts,known_foreign=sibling_foreign_names(codes[s]))
            if r is None: stats["no_recon"]+=1; out["%s|%s"%(s,qe)]={"held":why}; continue
            ok=plausible(r["t_fii"],a[1],c[1],b[1]) or (r.get("whole_block") and abs(r["t_fii"]-c[1])<abs(b[1]-c[1]))   # the whole-block rule is the page's own total: the neighbour band is waived, closer-to-Jun-16 kept
            if not ok: stats["held"]+=1; out["%s|%s"%(s,qe)]={"held":"outside neighbours' range or not closer to Jun-16","t_fii":r["t_fii"],"sep15":a[1],"jun16":c[1],"ev":r["ev"]}; continue
            new=list(b); new[1]=r["t_fii"]
            out["%s|%s"%(s,qe)]={"file":"%d_%d.html.gz"%(codes[s],q),"was":b,"cell":new,"d_fii":round(r["t_fii"]-b[1],4),"d_dii":0.0,"add_ins":0.0,"ev":[("seam-fii-reconstruction","base %.2f"%r["base_fii"],round(r["t_fii"]-r["base_fii"],4))]+r["ev"],"shpperent":True,"prior_entry":False}
            stats["proposed"]+=1
    json.dump(out,open("proposals_seam.json","w"),indent=0); print("seam pass",dict(stats)); return out
def write_aspx(stamp=None):
    """Merge page-era proposals (proposals_aspx.json + proposals_seam.json) into scripts/shp_cell_fix.json with the §160 marker."""
    stamp=stamp or time.strftime("%Y-%m-%d")
    P=json.load(open("proposals_aspx.json")); S={k:v for k,v in json.load(open("proposals_seam.json")).items() if v.get("cell")} if os.path.exists("proposals_seam.json") else {}
    for k,v in S.items(): P[k]=v      # a seam reconstruction supersedes the label-rule proposal for the same cell
    path=os.path.join(REPO,"scripts","shp_cell_fix.json"); raw=open(path,encoding="utf-8").read(); led=json.loads(raw); fix=led.setdefault("fix",{}); ascii_only=("\\u00" in raw)
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json"))); codes=json.load(open("aspx_codes.json"))
    audit={"_doc":["§160 page-era row-level heal (%s): BSE ShareholdingPattern.aspx (Clause-35 / 88-89 layouts) + shpperent.aspx; rules in DATA_RUNBOOK §160."%stamp],"cells":{}}
    n_new=n_sup=n_skip=0
    for k,v in sorted(P.items()):
        sym,qe=k.split("|"); cur=(hist.get(sym) or {}).get(qe)
        if cur is None or not D.F._cell_eq(cur,v["was"]): n_skip+=1; continue
        src="bseaspx:%s"%v["file"].replace(".html.gz","")+(" shpperent" if v.get("shpperent") else "")
        why=("§160 page-era row-level heal (%s, DII = Institutions(Domestic), FII = Institutions(Foreign) in every format): fii %.2f -> %.2f, dii %.2f -> %.2f%s. "%(stamp,cur[1],v["cell"][1],cur[2],v["cell"][2],(", prom %.2f -> %.2f"%(cur[0],v["cell"][0]) if abs(v["cell"][0]-cur[0])>0.005 else ""))
             +"; ".join(" ".join(str(x) for x in e) for e in v["ev"])[:900]+". Evidence: _shp_aspx_rowfix_audit.json")
        ent={"cell":list(v["cell"]),"was":list(cur),"src":src,"why":why}
        prior=(fix.get(sym) or {}).get(qe)
        if prior:
            if not D.F._cell_eq(cur,prior.get("cell")) and not (prior.get("was") and D.F._cell_eq(cur,prior.get("was"))): n_skip+=1; continue
            ent["superseded"]=prior; n_sup+=1
        else: n_new+=1
        fix.setdefault(sym,{})[qe]=ent; audit["cells"][k]={x:v.get(x) for x in ("file","d_dii","d_fii","add_ins","ev","shpperent")}
    json.dump(led,open(path,"w",encoding="utf-8"),indent=1,ensure_ascii=ascii_only)
    json.dump(audit,open(os.path.join(REPO,"scripts","_shp_aspx_rowfix_audit.json"),"w",encoding="utf-8"),indent=0,ensure_ascii=False)
    print("write_aspx: %d new, %d superseding, %d skipped"%(n_new,n_sup,n_skip))

def verify():
    """Seam / QoQ statistics before vs after the proposals (label rule + seam) applied on the store; writes shp_history_aspx_healed.json."""
    hist=json.load(open(os.path.join(REPO,"scripts","shp_history.json"))); syms=json.load(open("n500_syms.json"))
    P=json.load(open("proposals_aspx.json")); S={k:v for k,v in json.load(open("proposals_seam.json")).items() if v.get("cell")} if os.path.exists("proposals_seam.json") else {}
    P.update(S); heal={s:{q:list(v) for q,v in hist.get(s,{}).items()} for s in syms}
    for k,v in P.items():
        s,q=k.split("|"); heal[s][q]=v["cell"]
    def seam(store,a,b,idx,th):
        n=0; movers=[]; tot=0
        for s in syms:
            x=(store.get(s) or {}).get(a); y=(store.get(s) or {}).get(b)
            if not x or not y or x[idx] is None or y[idx] is None: continue
            tot+=1; d=y[idx]-x[idx]
            if abs(d)>=th: n+=1; movers.append((s,round(d,2)))
        return n,tot,sorted(movers,key=lambda m:-abs(m[1]))
    def qoq(store,idx,th,lo="2006-06-30",hi="2016-06-30"):
        n=0; tot=0; ex=[]
        for s in syms:
            qs=sorted(q for q in (store.get(s) or {}) if lo<=q<=hi)
            for a,b in zip(qs,qs[1:]):
                x=store[s][a]; y=store[s][b]
                if x[idx] is None or y[idx] is None: continue
                tot+=1
                if abs(y[idx]-x[idx])>=th: n+=1; ex.append((s,b,round(y[idx]-x[idx],2)))
        return n,tot,sorted(ex,key=lambda m:-abs(m[2]))
    res={}
    for name,idx in (("fii",1),("dii",2)):
        for a,b in (("2016-03-31","2016-06-30"),("2015-06-30","2015-09-30"),("2015-09-30","2015-12-31"),("2015-12-31","2016-03-31")):
            n0,t0,m0=seam(hist,a,b,idx,3.0); n1,t1,m1=seam(heal,a,b,idx,3.0); res["%s seam %s->%s"%(name,a[:7],b[:7])]=(n0,n1,t1)
            print("%s seam %s->%s  >=3pp: %d/%d -> %d/%d   top after: %s"%(name,a[:7],b[:7],n0,t0,n1,t1,m1[:6]))
        n0,t0,e0=qoq(hist,idx,5.0); n1,t1,e1=qoq(heal,idx,5.0); res["%s qoq5"%name]=(n0,n1,t1)
        print("%s QoQ >=5pp Jun-06..Jun-16: %d/%d -> %d/%d   top after: %s"%(name,n0,t0,n1,t1,e1[:8]))
    json.dump(heal,open("shp_history_aspx_healed.json","w")); return res

if __name__=="__main__":
    st_=sys.argv[1] if len(sys.argv)>1 else "classify"
    if st_=="classify": classify()
    elif st_=="seam": seam_pass(dip=float(sys.argv[2]) if len(sys.argv)>2 else 5.0)
    elif st_=="verify": verify()
    elif st_=="write": write_aspx()
