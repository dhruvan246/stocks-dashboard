#!/usr/bin/env python3
"""Normalize the F&O history's ticker names to CURRENT names (old -> current), matching how the
price + fundamentals data is keyed (continuous history lives under the current name, e.g. TMPV) and
how the index universes already work. The earlier point-in-time rebuild used old names (TATAMOTORS,
GMRINFRA, PVR…); on the live data those series are split/truncated under the old key, so the stocks
dropped out of F&O backtests. This remaps them to the current name so they resolve correctly.
Survivorship-free INCLUSION is unchanged (delisted names with no successor, e.g. IDFC, stay as-is).
Writes scripts/fno_history.json + patches docs/stock_data.bin.
"""
import json, gzip
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
HIST = ROOT / "scripts" / "fno_history.json"
BIN = ROOT / "docs" / "stock_data.bin"
ALIAS = {"ADANITRANS":"ADANIENSOL","AEGISCHEM":"AEGISLOG","AKZOINDIA":"JSWDULUX","ALSTOMT&D":"GVT&D","AMARAJABAT":"ARE&M","BAJAJCORP":"BAJAJCON","BHUSANSTL":"TATASTLBSL","CADILAHC":"ZYDUSLIFE","CENTURYTEX":"ABREL","CLNINDIA":"SUDARCOLOR","CROMPGREAV":"CGPOWER","ESSELPACK":"EPL","FCEL":"FCONSUMER","FINANTECH":"63MOONS","FRL":"FEL","GATI":"ACLGATI","GEPIL":"GVPIL","GET&D":"GVT&D","GLS":"ALIVUS","GMRINFRA":"GMRAIRPORT","GUJFLUORO":"GFLLIMITED","HOTELEELA":"HLVLTD","HSIL":"AGI","IBREALEST":"EMBDL","IBULHSGFIN":"SAMMAANCAP","IBULISL":"IBULLSLTD","IDFCBANK":"IDFCFIRSTB","IIFLWAM":"360ONE","INEOSSTYRO":"STYRENIX","INFIBEAM":"CCAVENUE","INFRATEL":"INDUSTOWER","IPAPPM":"ANDHRAPAP","ITDCEM":"CEMPRO","JCHAC":"BOSCH-HCIL","JUBILANT":"JUBLPHARMA","KALPATPOWR":"KPIL","KPIT":"BSOFT","KSBPUMPS":"KSB","L&TFH":"LTF","LAXMIMACH":"LMW","LTI":"LTM","LTIM":"LTM","MAGMA":"POONAWALLA","MAHINDCIE":"CIEINDIA","MAX":"MFSL","MCDOWELL-N":"UNITDSPR","MINDAIND":"UNOMINDA","MOTHERSUMI":"MOTHERSON","NBVENTURES":"NAVA","NIITTECH":"COFORGE","PIPAVAVDOC":"RNAVAL","PRISMCEM":"PRSMJOHNSN","PVR":"PVRINOX","RDEL":"RNAVAL","SEINV":"PAISALO","SEQUENT":"VIYASH","SKSMICRO":"BHARATFIN","SMLISUZU":"SMLMAH","SRTRANSFIN":"SHRIRAMFIN","SSLT":"VEDL","STRTECH":"STLTECH","SUNCLAYLTD":"TVSHLTD","SUVENPHAR":"COHANCE","SWANENERGY":"SWANCORP","TATAGLOBAL":"TATACONSUM","TATAMOTORS":"TMPV","TATASPONGE":"TATASTLLP","TIDEWATER":"VEEDOL","WABCOINDIA":"ZFCVINDIA","WELSPUNIND":"WELSPUNLIV","ZOMATO":"ETERNAL"}

H = json.loads(HIST.read_text())
renamed = set()
for snap in H:
    for s in snap['symbols']:
        if s in ALIAS: renamed.add(s)
    snap['symbols'] = sorted(set(ALIAS.get(s, s) for s in snap['symbols']))
# dedupe consecutive identical membership
ded = []
for snap in H:
    if ded and set(snap['symbols']) == set(ded[-1]['symbols']): continue
    ded.append(snap)
HIST.write_text(json.dumps(ded, separators=(',', ':')))
D = json.loads(gzip.decompress(BIN.read_bytes()))
D['fnoHistory'] = ded
BIN.write_bytes(gzip.compress(json.dumps(D, separators=(',', ':')).encode(), 6))

def snap_at(date):
    cur = None
    for s in ded:
        if s['effectiveDate'] <= date: cur = s
    return set(cur['symbols']) if cur else set()
print(f"renamed old tickers found & normalized: {len(renamed)} -> {sorted(renamed)}")
print(f"snapshots: {len(H)} -> {len(ded)} (after dedupe)")
j = snap_at('2024-01-31')
print(f"Jan-2024 universe: {len(j)} stocks | TMPV={'TMPV' in j} TATAMOTORS={'TATAMOTORS' in j} IDFC={'IDFC' in j} GVT&D={'GVT&D' in j}")
print(f"latest snap {ded[-1]['effectiveDate']}: TMPV={'TMPV' in set(ded[-1]['symbols'])} GVT&D={'GVT&D' in set(ded[-1]['symbols'])} ({len(ded[-1]['symbols'])} stocks)")
