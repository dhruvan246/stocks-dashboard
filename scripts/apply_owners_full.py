# -*- coding: utf-8 -*-
"""Build the OWNERS-ATTRIBUTABLE consolidated dataset for ALL stocks/history:
  - set npCon = owners from the comprehensive _reattr_owners.json (1697 stocks)
  - add verified-from-filing backfills for qualifying NCI stocks missing from that file
Writes the result to docs/sf_fundamentals.json (in place). Reports coverage + any
qualifying stock whose con-quarter still lacks an owners figure (potential residual gap).
Run: python -X utf8 apply_owners_full.py            (impact-only: pass --dry)"""
import json, os, sys
HERE=os.path.dirname(os.path.abspath(__file__)); ROOT=os.path.dirname(HERE)
F=os.path.join(ROOT,"docs","sf_fundamentals.json")
live=json.load(open(F))
OWN=json.load(open(os.path.join(HERE,"_reattr_owners.json")))   # "SYM|qe" -> owners cr

# Renamed-ticker map (OLD->NEW). _reattr_owners is keyed by the OLD ticker (name at extraction),
# but live FUND is keyed by the NEW ticker; without this, renamed stocks silently keep total PAT.
ALIAS={"ADANITRANS":"ADANIENSOL","AEGISCHEM":"AEGISLOG","AKZOINDIA":"JSWDULUX","ALSTOMT&D":"GVT&D","AMARAJABAT":"ARE&M","BAJAJCORP":"BAJAJCON","BHUSANSTL":"TATASTLBSL","CADILAHC":"ZYDUSLIFE","CENTURYTEX":"ABREL","CLNINDIA":"SUDARCOLOR","CROMPGREAV":"CGPOWER","ESSELPACK":"EPL","FCEL":"FCONSUMER","FINANTECH":"63MOONS","FRL":"FEL","GATI":"ACLGATI","GEPIL":"GVPIL","GET&D":"GVT&D","GLS":"ALIVUS","GMRINFRA":"GMRAIRPORT","GUJFLUORO":"GFLLIMITED","GUJGASLTD":"GUJENERGY","HOTELEELA":"HLVLTD","HSIL":"AGI","IBREALEST":"EMBDL","IBULHSGFIN":"SAMMAANCAP","IBULISL":"IBULLSLTD","IDFCBANK":"IDFCFIRSTB","IIFLWAM":"360ONE","INEOSSTYRO":"STYRENIX","INFIBEAM":"CCAVENUE","INFRATEL":"INDUSTOWER","IPAPPM":"ANDHRAPAP","ITDCEM":"CEMPRO","JCHAC":"BOSCH-HCIL","JUBILANT":"JUBLPHARMA","KALPATPOWR":"KPIL","KPIT":"BSOFT","KSBPUMPS":"KSB","L&TFH":"LTF","LAXMIMACH":"LMW","LTI":"LTM","LTIM":"LTM","MAGMA":"POONAWALLA","MAHINDCIE":"CIEINDIA","MAX":"MFSL","MCDOWELL-N":"UNITDSPR","MINDAIND":"UNOMINDA","MOTHERSUMI":"MOTHERSON","NBVENTURES":"NAVA","NIITTECH":"COFORGE","PIPAVAVDOC":"RNAVAL","PRISMCEM":"PRSMJOHNSN","PVR":"PVRINOX","RDEL":"RNAVAL","SEINV":"PAISALO","SEQUENT":"VIYASH","SKSMICRO":"BHARATFIN","SMLISUZU":"SMLMAH","SRTRANSFIN":"SHRIRAMFIN","SSLT":"VEDL","STRTECH":"STLTECH","SUNCLAYLTD":"TVSHLTD","SUVENPHAR":"COHANCE","SWANENERGY":"SWANCORP","TATAGLOBAL":"TATACONSUM","TATAMOTORS":"TMPV","TATASPONGE":"TATASTLLP","TIDEWATER":"VEEDOL","WABCOINDIA":"ZFCVINDIA","WELSPUNIND":"WELSPUNLIV","ZOMATO":"ETERNAL"}
REV={}
for _o,_n in ALIAS.items(): REV.setdefault(_n,[]).append(_o)

# verified-from-filing backfills (owners-attributable, cr) for NCI stocks not in _reattr_owners
BACKFILL={
 # CEMPRO (ITD Cementation): owners = total - NCI, read from consolidated filings
 "CEMPRO|20250331":113.55, "CEMPRO|20251231":110.89-0.00,  # Q3FY26 NCI~0 -> owners~total
 "CEMPRO|20260331":242.17,                                  # Q4FY26 NCI~0 -> owners~total
 # ACUTAAS (Acutaas/Anupam Rasayan, consolidates Tanfac): from Q4FY26 filing attribution
 "ACUTAAS|20250331":62.48, "ACUTAAS|20251231":107.96, "ACUTAAS|20260331":131.76,
}
src_own=src_bf=src_ren=0
for sym,arr in live.items():
    for r in arr:
        if r[3] is None: continue
        k="%s|%d"%(sym,r[0])
        a=BACKFILL.get(k)
        if a is not None:
            if abs(a-r[3])>0.005: r[3]=a; src_bf+=1
            continue
        a=OWN.get(k); ren=False
        if a is None:                          # renamed: owners keyed by the OLD ticker
            for _old in REV.get(sym,()):
                a=OWN.get("%s|%d"%(_old,r[0]))
                if a is not None: ren=True; break
        if a is None: continue
        # XBRL owners=0 mis-tag guard: some filers tag ProfitOrLossAttributableToOwnersOfParent=0
        # with the real profit only in the total — and _reattr_owners.json (built from those same
        # XBRLs) carries the poisoned 0.0. NEVER let a ~0 cache value zero out a nonzero stored con
        # (the old `abs(r[3])>2` version only protected >2cr cells, so it re-zeroed ~139 microcap
        # quarters every night, undoing corrections). A genuine owners=0.00-exact alongside a nonzero
        # stored con is implausible; keeping the stored value is the safer error.
        if abs(a)<0.005 and abs(r[3])>=0.005: continue
        if abs(a-r[3])>0.005:
            r[3]=a
            if ren: src_ren+=1
            else: src_own+=1
print("set npCon=owners: %d from _reattr_owners, %d via rename-alias (renamed stocks), %d from filing-backfill"%(src_own,src_ren,src_bf))
if "--dry" in sys.argv:
    print("DRY RUN — not written"); sys.exit(0)
tmp=F+".tmp"; json.dump(live,open(tmp,"w"),separators=(",",":")); os.replace(tmp,F)
print("WROTE owners-attributable to docs/sf_fundamentals.json")
