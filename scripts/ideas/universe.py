"""Build docs/ideas/universe.json: every active BSE equity with market cap between 200 and 2,000 crore.

Usage: python3 scripts/ideas/universe.py [--min 200] [--max 2000]
Market cap comes from BSE's scrip master (Rs crore, updated daily by BSE). Suspended groups (Z, ZP) are
dropped. NSE symbols are joined by ISIN from the two NSE lists when present next to this script
(nse_equity_l.csv, nse_sme.csv) so the routine can quote both tickers.
"""
import json, os, sys, csv, datetime, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse
import ist

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')


def nse_by_isin():
    out = {}
    for fn, sym, isin, seg in (('nse_equity_l.csv', 'SYMBOL', ' ISIN NUMBER', 'NSE'),
                               ('nse_sme.csv', 'SYMBOL', 'ISIN_NUMBER', 'NSE-SME')):
        p = os.path.join(HERE, fn)
        if not os.path.exists(p):
            continue
        for r in csv.DictReader(open(p)):
            k = (r.get(isin) or r.get(isin.strip()) or '').strip()
            if k:
                out[k] = (r[sym].strip(), seg)
    return out


def build(lo, hi):
    master = bse.scrip_master()
    nse = nse_by_isin()
    uni = []
    for x in master:
        try:
            mcap = float(x.get('Mktcap') or 0)
        except Exception:
            continue
        grp = (x.get('GROUP') or '').strip()
        if not (lo <= mcap <= hi) or grp in ('Z', 'ZP'):
            continue
        isin = (x.get('ISIN_NUMBER') or '').strip()
        n = nse.get(isin, ('', ''))
        uni.append(dict(scrip=str(x['SCRIP_CD']).strip(), id=(x.get('scrip_id') or '').strip(), name=(x.get('Scrip_Name') or '').strip(),
                        issuer=(x.get('Issuer_Name') or '').strip(), isin=isin, group=grp, mcap=round(mcap, 1),
                        face_value=x.get('FACE_VALUE'), nse=n[0], nse_seg=n[1],
                        sme=grp in ('M', 'MT', 'MS')))
    uni.sort(key=lambda r: -r['mcap'])
    return uni


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--min', type=float, default=200)
    ap.add_argument('--max', type=float, default=2000)
    a = ap.parse_args()
    uni = build(a.min, a.max)
    os.makedirs(DOCS, exist_ok=True)
    out = dict(asof=ist.today().isoformat(), mcap_min=a.min, mcap_max=a.max, count=len(uni), rows=uni)
    json.dump(out, open(os.path.join(DOCS, 'universe.json'), 'w'), separators=(',', ':'))
    import collections
    print('universe', len(uni), 'names |', collections.Counter(r['group'] for r in uni).most_common(), '| with NSE symbol:',
          sum(1 for r in uni if r['nse']), '| SME:', sum(1 for r in uni if r['sme']))
