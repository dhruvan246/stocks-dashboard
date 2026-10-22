#!/usr/bin/env python3
"""Runbook §230 — local XBRL screen of the quarterly profit store. Needs the 5.8 GB scripts/_xbrl_cache, so it runs
on the Mac, not in CI (the CI-side store-intrinsic screen is xcheck `fund_cell_shape`).

  python3 scripts/screen_profit_xbrl.py index    # one pass over scripts/_xbrl_cache -> OUT/xbrl_index.json (~10 min)
  python3 scripts/screen_profit_xbrl.py screen   # docs/sf_fundamentals.json vs the index -> OUT/screen_out.json + counts

OUT = ~/stocks-cache/profit-xbrl-screen. Scope: PIT Nifty 500 (a member at qe, +120, +240, +360 or +450 days, rename
aliases included). Classes: tci_as_owners, total_not_owners, zero, onedp_proven (1 dp stored where the filing has 2 dp),
con_eq_std_*, nomatch_scale/sign/ytd/other, later_filing, ann_after_xbrl, ann_is_deadline, dup_consecutive_*.
A flagged cell is a CANDIDATE only: read that cell's own filing before changing anything (DATA_RUNBOOK §0).
Optional: CNEXT=<json of [sym, qe, ...]> marks cells another campaign owns (cnext=True in the output)."""
import sys, os, re, json, html, collections, bisect, datetime
from multiprocessing import Pool
W = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(W, 'scripts'))
import build_revop as BR, xbrl_symbol, scale_fix
OUT = os.path.expanduser('~/stocks-cache/profit-xbrl-screen')
CACHE = os.path.join(W, 'scripts', '_xbrl_cache')
TAGS = {'pat': ['ProfitLossForPeriod', 'ProfitLossForThePeriod', 'ProfitLossAfterTaxAndExtraordinaryItems', 'ProfitLossAfterTax'],
        'own': ['ProfitOrLossAttributableToOwnersOfParent'], 'nci': ['ProfitOrLossAttributableToNonControllingInterests'],
        'bank': ['ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOfAssociates'],
        'tci': ['ComprehensiveIncomeForThePeriod'], 'tcio': ['ComprehensiveIncomeForThePeriodAttributableToOwnersOfParent'],
        'tcin': ['ComprehensiveIncomeForThePeriodAttributableToNonControllingInterests'],
        'pbt': ['ProfitBeforeTax', 'ProfitLossBeforeTax', 'ProfitOrLossBeforeTax'], 'tax': ['TaxExpense'],
        'rev': ['RevenueFromOperations', 'InterestEarned'], 'eps': ['BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations', 'BasicEarningsLossPerShare'],
        'assoc': ['ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod']}
RX = {k: [(t, {c: re.compile(r'<[a-z\-]+:' + t + r' contextRef="' + c + r'"([^>]*)>\s*([-0-9.eE+]+)\s*<') for c in ('OneD', 'FourD')}) for t in v] for k, v in TAGS.items()}
def get(xml, k, c):
    for t, d in RX[k]:
        m = d[c].search(xml)
        if m:
            dec = re.search(r'decimals="([^"]+)"', m.group(1))
            try: return float(m.group(2)), m.group(2), (dec.group(1) if dec else None)
            except ValueError: return None
    return None
def one(fname):
    try:
        xml = open(os.path.join(CACHE, fname), encoding='utf-8', errors='replace').read()
        sm = BR.RE_SYM.search(xml) or BR.RE_SYM2.search(xml) or re.search(r'<in-(?:bse-fin|capmkt):Symbol contextRef="[^"]+"[^>]*>([^<]+)<', xml)
        if not sm: return None
        sym = xbrl_symbol.resolve(html.unescape(sm.group(1)).strip().upper(), xml)
        if not sym: return None
        out = []
        for c in ('OneD', 'FourD'):
            p = BR.ctx_period(xml, c)
            if not p: continue
            n = BR.RE_NAT[c].search(xml)
            rec = {'f': fname, 'sym': sym.upper(), 'ctx': c, 's': p[0], 'e': p[1], 'days': BR.days_between(*p), 'nat': n.group(1).strip().lower() if n else '', 'ts': BR.ts_key(fname), 'sc': scale_fix.factor(fname)}
            for k in TAGS:
                v = get(xml, k, c)
                if v: rec[k] = v[0]; 
                if v and k in ('pat', 'own'): rec[k + '_raw'] = v[1]; rec[k + '_dec'] = v[2]
            out.append(rec)
        return out
    except Exception as ex:
        return [{'f': fname, 'err': str(ex)[:100]}]
def index():
    files = sorted(os.listdir(CACHE)); res = []
    with Pool(8) as p:
        for i, r in enumerate(p.imap_unordered(one, files, chunksize=200)):
            if r: res.extend(r)
            if i % 10000 == 0: print(i, flush=True)
    os.makedirs(OUT, exist_ok=True); json.dump(res, open(os.path.join(OUT, 'xbrl_index.json'), 'w'))
    print('done', len(res))

def screen():
    F = json.load(open(W + '/docs/sf_fundamentals.json'))
    IDX = json.load(open(os.path.join(OUT, 'xbrl_index.json')))
    CN = {(s, q) for s, q, *_ in json.load(open(os.environ['CNEXT']))} if os.environ.get('CNEXT') else set()
    H = sorted(json.load(open(W + '/scripts/indices_history.json'))['Nifty 500'], key=lambda s: s['effectiveDate'])
    HD = [int(s['effectiveDate'].replace('-', '')) for s in H]; HS = [set(s['symbols']) for s in H]
    RM = json.load(open(W + '/scripts/_rename_map.json'))
    src = open(W + '/scripts/apply_owners_full.py').read(); ALIAS = eval(re.search(r'ALIAS=(\{.*?\})\n', src).group(1))
    def key_of(sym):
        seen = set(); s = sym
        while s not in F and s not in seen:
            seen.add(s); s = RM.get(s) if isinstance(RM.get(s), str) else ALIAS.get(s)
            if s is None: return None
        return s if s in F else None
    OLDS = collections.defaultdict(set)
    for o, n in list(RM.items()) + list(ALIAS.items()):
        if isinstance(n, str): OLDS[n].add(o)
    def members(ymd):
        i = bisect.bisect_right(HD, int(ymd)) - 1
        return HS[i] if i >= 0 else set()
    def ymd_add(q, days):
        d = datetime.date(q // 10000, q // 100 % 100, q % 100) + datetime.timedelta(days=days); return int(d.strftime('%Y%m%d'))
    def in_n500(key, qe):
        for d in (qe, ymd_add(qe, 120), ymd_add(qe, 240), ymd_add(qe, 360), ymd_add(qe, 450)):
            m = members(d)
            if key in m or any(o in m for o in OLDS.get(key, ())): return True
        return False
    # filings by (key, qe, basis)
    FIL = collections.defaultdict(list); YTD = collections.defaultdict(list)
    for r in IDX:
        if 'err' in r or not r.get('e'): continue
        k = key_of(r['sym'])
        if not k: continue
        qe = int(r['e'].replace('-', '')); b = 'con' if 'consol' in r['nat'] else 'std'
        sc = r.get('sc') or 1
        v = {t: (r[t] / 1e7 / sc if t in r else None) for t in ('pat', 'own', 'nci', 'tci', 'tcio', 'tcin', 'bank', 'pbt', 'tax', 'rev')}
        v.update(f=r['f'], ts=r['ts'], days=r['days'], pat_raw=r.get('pat_raw'), own_raw=r.get('own_raw'))
        (FIL if 0 < r['days'] <= 100 else YTD)[(k, qe, b)].append(v)
    for L in FIL.values(): L.sort(key=lambda x: x['ts'])
    def eq(a, b, t=0.0051): return a is not None and b is not None and abs(a - b) <= t
    def onedp(v): return abs(v * 10 - round(v * 10)) < 1e-6
    res = collections.defaultdict(list)
    def deadline(qe):
        return ymd_add(qe, 60 if qe % 10000 == 331 else 45)
    for key, rows in F.items():
        prev = None
        for r in rows:
            qe = r[0]
            if not in_n500(key, qe): continue
            for b, si, ai in (('std', 1, 2), ('con', 3, 4)):
                v = r[si]; ann = r[ai]
                if v is None: continue
                cell = dict(sym=key, qe=qe, b=b, v=v, ann=ann, cnext=(key, qe) in CN)
                fl = FIL.get((key, qe, b), [])
                cands = []
                for f in fl:
                    for t in ('own', 'pat', 'bank'):
                        if f.get(t) is not None: cands.append(f[t])
                    if f.get('pat') is not None and f.get('nci') is not None: cands.append(f['pat'] - f['nci'])
                if v == 0: res['zero'].append(dict(cell, fl=[(f['f'], f.get('pat'), f.get('own')) for f in fl]))
                if onedp(v) and v != 0:
                    prov = [c for c in cands if abs(c - v) < 0.1 and not eq(c, v, 0.0049) and not onedp(round(c, 2))]
                    res['onedp_proven' if prov else 'onedp_noxbrl' if not fl else 'onedp_xbrl_same'].append(dict(cell, x=prov[:3]))
                if b == 'con' and fl:
                    for f in fl[:1]:
                        tcio, tci, pat, own, nci = f['tcio'], f['tci'], f['pat'], f['own'], f['nci']
                        if tci is not None and pat is not None and not eq(tci, pat) and (eq(v, tcio) or (eq(v, own) and eq(own, tci) and not nci)):
                            res['tci_as_owners'].append(dict(cell, x=dict(pat=pat, own=own, nci=nci, tci=tci, tcio=tcio, f=f['f'])))
                        if eq(v, pat) and nci is not None and abs(nci) >= 0.0051 and not eq(own, v):
                            res['total_not_owners'].append(dict(cell, x=dict(pat=pat, own=own, nci=nci, f=f['f'])))
                if b == 'con' and r[1] is not None and eq(v, r[1], 0.0001):
                    has_con = bool(fl); has_std = bool(FIL.get((key, qe, 'std')))
                    res['con_eq_std_nocon' if (has_std and not has_con) else 'con_eq_std_con' if has_con else 'con_eq_std_nox'].append(cell)
                if fl and not any(eq(c, v, 0.006) for c in cands):
                    first = fl[0]; c0 = [first.get(t) for t in ('own', 'pat', 'bank')]
                    sub = 'other'
                    for c in [x for x in cands if x]:
                        for k in (10, 100, 1000, 0.1, 0.01, 0.001):
                            if eq(v, c * k, abs(c * k) * 0.001 + 0.006): sub = 'scale'
                        if eq(v, -c): sub = 'sign'
                    for y in YTD.get((key, qe, b), []):
                        for t in ('own', 'pat', 'bank'):
                            if eq(v, y.get(t), 0.006): sub = 'ytd'
                    res['nomatch_' + sub].append(dict(cell, x=[(f['f'], f.get('pat'), f.get('own'), f.get('nci')) for f in fl[:3]]))
                elif len(fl) > 1:
                    f0 = fl[0]; c0 = [f0.get(t) for t in ('own', 'pat', 'bank')] + ([f0['pat'] - f0['nci']] if f0.get('pat') is not None and f0.get('nci') is not None else [])
                    if any(x is not None for x in c0) and not any(eq(c, v, 0.006) for c in c0 if c is not None):
                        res['later_filing'].append(dict(cell, x=[(f['f'], f.get('pat'), f.get('own')) for f in fl[:3]]))
                if fl and ann:
                    d0 = int(fl[0]['ts'][:8])
                    if ann > d0: res['ann_after_xbrl'].append(dict(cell, x=d0, lag=(datetime.date(ann//10000, ann//100%100, ann%100) - datetime.date(d0//10000, d0//100%100, d0%100)).days))
                if ann and ann == deadline(qe): res['ann_is_deadline'].append(cell)
            if prev and r[3] is not None and prev[3] is not None and r[3] == prev[3] and abs(r[3]) >= 1: res['dup_consecutive_con'].append(dict(sym=key, qe=qe, v=r[3], cnext=(key, qe) in CN))
            if prev and r[1] is not None and prev[1] is not None and r[1] == prev[1] and abs(r[1]) >= 1: res['dup_consecutive_std'].append(dict(sym=key, qe=qe, v=r[1], cnext=(key, qe) in CN))
            prev = r
    json.dump(res, open(os.path.join(OUT, 'screen_out.json'), 'w'), indent=0, default=str)
    for k, L in sorted(res.items()):
        by = collections.Counter('2022+' if c['qe'] >= 20220101 else '2020-21' if c['qe'] >= 20200101 else 'pre2020' for c in L)
        print('%-22s %6d  %s  cnext=%d' % (k, len(L), dict(by), sum(1 for c in L if c.get('cnext'))))


if __name__ == '__main__':
    {'index': index, 'screen': screen}[sys.argv[1] if len(sys.argv) > 1 else 'screen']()
