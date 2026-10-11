"""Order wins that ADD UP: the running total of order wins since a company's last stated order book.

Why (user, 2026-10-11): a company with a Rs 10,000 cr book can win 2,000 + 3,000 + 2,000 + ... and double its book
in six months without any single order looking big, and without the quarterly order-book comparison (orderbook.py)
seeing it until the next results. Welspun Corp Aug-2024 -> Mar-2025: book Rs 8,508 cr, no order above 0.2x of it,
book Rs 20,000 cr seven months later.

Ledger  docs/ideas/order_wins.json         {built, companies: {ISIN: {name, nse, bse, wins: [{filed, value_cr, display,
                                            currency, fx, src, doc, subject}]}}} - one row per order-win letter with
                                            the value read from the letter (scripts/enrich_order_values.extract). A
                                            letter with no readable value is kept with value_cr null (the sum is then a
                                            floor). The same letter on both exchanges is kept once.
Signals docs/ideas/orderwins_signals.json   per company with a stated order book (docs/ideas/order_book.json): the
                                            base = the latest stated book; wins filed after the base's filing date and
                                            within 183 days are added up; flagged at >= 0.5x the base. Also the largest
                                            single order / base, and the first date the running sum crossed 0.5x.

Daily (ideas-feeds.yml): `--days-back 4` reads the whole-market BSE day lists and NSE announcements (main board +
SME) for the last few days. Backfill: `--days-back 365 --nse-only --known-books` (NSE lists only, letters only of
companies that have a stated order book) - keeps BSE out of it (BSE blocks heavy use, measured 2026-10-11).

USD values are converted at that day's USD/INR from docs/macro.json (Yahoo INR=X), stated in each row; other
currencies are kept unconverted (value_cr null).
"""
import argparse
import datetime
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append(os.path.join(HERE, '..'))
import bse  # noqa: E402
import ist  # noqa: E402
import updates  # noqa: E402
import orderbook as ob  # noqa: E402
from enrich_order_values import extract  # noqa: E402

DOCS = os.path.join(HERE, '..', '..', 'docs')
LEDGER = os.environ.get('OW_LEDGER') or os.path.join(DOCS, 'ideas', 'order_wins.json')
SIGNALS = os.environ.get('OW_SIGNALS') or os.path.join(DOCS, 'ideas', 'orderwins_signals.json')
BOOKS = os.path.join(DOCS, 'ideas', 'order_book.json')
THRESH, WINDOW = 0.5, 183

WIN_RX = re.compile(r'bagging|receiving of orders|receipt of (?:an? |new )?(?:work )?orders?|award of (?:an? )?(?:order|contract|work)|'
                    r'orders?/contracts?|order\(s\)|(?:bags?|bagged|secures?|secured|wins?|won|receives?|received)\b.{0,40}\b(?:orders?|contracts?|LoA|LoI)\b|'
                    r'letter of (?:award|acceptance|intent)|\bLoA\b|\bLoI\b|work order|purchase order', re.I)
NOT_WIN_RX = re.compile(r'cancel|terminat|withdraw|clarification|amendment|revis|correction|trading window|newspaper|'
                        r'tax|gst|customs|penalt|demand order|court order|tribunal|nclt|arbitra|sebi order', re.I)
D = datetime.date.fromisoformat


def usdinr():
    try:
        return json.load(open(os.path.join(DOCS, 'macro.json')))['series']['usdinr']['d']
    except Exception:
        return {}


def fx_on(series, day):
    for back in range(0, 8):
        k = (day - datetime.timedelta(days=back)).isoformat()
        if k in series:
            return series[k], k
    return None, None


def value_of(txt, day, fxs):
    """(value_cr, display, currency, fx_note) from the letter text, or (None, display/None, None, None)."""
    # "Rs. 1, 030 Crores" (KEC 08-Oct-2026): a space after the thousands comma splits the number -> close it
    txt = re.sub(r'(\d)\s*,\s+(\d{2,3})(?!\d)', r'\1,\2', txt or '')
    got = extract(txt)
    if not got:
        return None, None, None, None
    disp, cr = got
    if disp.lstrip('~ ').startswith('US$'):
        m = re.search(r'US\$\s*([\d,]*\.?\d+)\s*(bn|mn)?', disp)
        if not m:
            return None, disp, 'USD', None
        v = float(m.group(1).replace(',', '')) * {'bn': 1e9, 'mn': 1e6}.get(m.group(2) or '', 1)
        rate, rd = fx_on(fxs, day)
        if not rate:
            return None, disp, 'USD', 'no USD/INR rate for the date'
        return round(v * rate / 1e7, 2), disp, 'USD', f'USD/INR {rate} ({rd}, Yahoo INR=X)'
    if disp.lstrip('~ ').startswith('₹'):
        return cr, disp, 'INR', None
    return None, disp, disp.lstrip('~ ').split(' ')[0], None


def isin_maps():
    """BSE code -> ISIN and NSE symbol -> ISIN, from the newest bhavcopies in the last 10 days."""
    b, n = {}, {}
    d = ist.today()
    for _ in range(10):
        rows = bse.bhavcopy(d)
        if rows:
            b = {str(r['scrip']): r.get('isin') for r in rows if r.get('isin')}
            break
        d -= datetime.timedelta(days=1)
    d = ist.today()
    for _ in range(10):
        rows = bse.nse_bhavcopy(d)
        if rows:
            n = {str(r['symbol']).upper(): r.get('isin') for r in rows if r.get('isin')}
            break
        d -= datetime.timedelta(days=1)
    return b, n


def read_days(led, days_back, nse_only=False, only_isins=None):
    fxs = usdinr()
    bmap, nmap = isin_maps()
    today = ist.today()
    probs, read, added = [], 0, 0
    for back in range(days_back, -1, -1):
        day = today - datetime.timedelta(days=back)
        items = []
        if not nse_only:
            try:
                for r in bse.announcements(day):
                    subj = f"{r.get('NEWSSUB') or ''} {r.get('HEADLINE') or ''} {r.get('CATEGORYNAME') or ''} {r.get('SUBCATNAME') or ''}"
                    code = str(r.get('SCRIP_CD') or '')
                    items.append(('BSE', code, '', bmap.get(code), r.get('SLONGNAME') or '', day.isoformat(), subj, bse.attachment_url(r)))
                if getattr(bse, 'last_announcements_capped', False) or getattr(bse, 'last_announcements_partial', False):
                    probs.append(f'BSE {day} list incomplete')
            except Exception as e:
                probs.append(f'BSE {day} failed ({str(e)[:60]})')
        for ix in ('equities', 'sme'):
            for r in updates._rows(f'https://www.nseindia.com/api/corporate-announcements?index={ix}&from_date={day:%d-%m-%Y}&to_date={day:%d-%m-%Y}', probs, f'NSE {day} {ix}'):
                subj = f"{r.get('desc') or ''} {r.get('attchmntText') or ''}"
                sym = str(r.get('symbol') or '').upper()
                items.append(('NSE', '', sym, r.get('sm_isin') or nmap.get(sym), r.get('sm_name') or sym, day.isoformat(), subj, r.get('attchmntFile') or ''))
        for src, code, sym, isin, name, date, subj, pdf in items:
            if not isin or not pdf or not WIN_RX.search(subj) or NOT_WIN_RX.search(subj[:200]):
                continue
            if only_isins is not None and isin not in only_isins:
                continue
            c = led['companies'].setdefault(isin, dict(name=name, nse=None, bse=None, wins=[]))
            c['name'] = c.get('name') or name
            if sym:
                c['nse'] = sym
            if code:
                c['bse'] = code
            if any(w['doc'] == pdf for w in c['wins']):
                continue
            txt = ob.pdf_text(pdf)
            read += 1
            v, disp, cur, fxn = value_of(txt, D(date), fxs)
            # the same order filed twice - both exchanges, a press release beside the letter, or a re-filing the next
            # day: same value within 3 days -> keep one (two unread letters the same day on two exchanges, likewise)
            if any(abs((D(w['filed']) - D(date)).days) <= 3 and ((v is not None and w.get('value_cr') == v) or
                   (v is None and w.get('value_cr') is None and w['filed'] == date and w['src'] != src)) for w in c['wins']):
                continue
            c['wins'].append(dict(filed=date, value_cr=v, display=disp, currency=cur, fx=fxn, src=src, doc=pdf, subject=subj[:200]))
            added += 1
    led['built'] = ist.stamp()
    led['last_read'] = dict(at=ist.stamp(), days_back=days_back, nse_only=nse_only, letters_read=read, wins_added=added, problems=probs[:20])
    print(f'order wins: {days_back + 1} days, {read} letters read, {added} wins added' + (f'; problems: {"; ".join(probs)[:300]}' if probs else ''))


def book_isins():
    """ISIN -> (order_book.json key, company entry) for every company with a stated order book."""
    books = json.load(open(BOOKS))['companies']
    bmap, nmap = isin_maps()
    out = {}
    for k, c in books.items():
        if not any(r.get('value_cr') for r in c.get('readings', [])):
            continue
        isin = nmap.get(k[4:].upper()) if k.startswith('NSE:') else bmap.get(k) or (nmap.get((c.get('nse') or '').upper()))
        if isin:
            out[isin] = (k, c)
    return out


def build_signals(led):
    rows = []
    for isin, (k, c) in book_isins().items():
        R = [r for r in c.get('readings', []) if r.get('value_cr')]
        last_d = max(D(r['as_on_eff']) for r in R)
        base = ob.pick_book([r for r in R if r['as_on_eff'] == last_d.isoformat()], last_d)
        if not base or not base['value_cr']:
            continue
        w = (led['companies'].get(isin) or {}).get('wins', [])
        start = base['filed']
        win = sorted([x for x in w if x['filed'] > start and (D(x['filed']) - D(start)).days <= WINDOW], key=lambda x: x['filed'])
        if not win:
            continue
        run, cross = 0.0, None
        for x in win:
            run += x['value_cr'] or 0
            if cross is None and run >= THRESH * base['value_cr']:
                cross = x['filed']
        vals = [x['value_cr'] for x in win if x['value_cr']]
        rows.append(dict(isin=isin, key=k, name=c.get('name') or (led['companies'].get(isin) or {}).get('name'), nse=c.get('nse') or (led['companies'].get(isin) or {}).get('nse'),
                         base_cr=base['value_cr'], base_as_on=base['as_on_eff'], base_filed=base['filed'], base_doc=base.get('doc'),
                         n_wins=len(win), n_no_value=sum(1 for x in win if not x['value_cr']), sum_cr=round(run, 2),
                         ratio=round(run / base['value_cr'], 3), largest_ratio=round(max(vals) / base['value_cr'], 3) if vals else None,
                         cross_date=cross, flagged=cross is not None, wins=win))
    rows.sort(key=lambda r: -r['ratio'])
    out = dict(built=ist.stamp(), rule=f'Order wins filed after the last stated order book (and within {WINDOW} days of it), added up; '
               f'flagged at >= {THRESH}x that book. Values read from each order letter; a letter with no readable value adds 0 '
               '(the sum is a floor). Script-read: confirm against the letters before acting.',
               n_flagged=sum(1 for r in rows if r['flagged']), rows=rows)
    json.dump(out, open(SIGNALS, 'w'), ensure_ascii=False, indent=1)
    print(f'signals: {len(rows)} companies with wins since their last stated book, {out["n_flagged"]} at >= {THRESH}x')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days-back', type=int, default=4)
    ap.add_argument('--nse-only', action='store_true', help='NSE lists only (backfill: keeps BSE out of it)')
    ap.add_argument('--known-books', action='store_true', help='read letters only of companies with a stated order book')
    ap.add_argument('--signals-only', action='store_true')
    a = ap.parse_args()
    try:
        led = json.load(open(LEDGER))
    except Exception:
        led = dict(companies={})
    if not a.signals_only:
        read_days(led, a.days_back, a.nse_only, set(book_isins()) if a.known_books else None)
        json.dump(led, open(LEDGER, 'w'), ensure_ascii=False, indent=1)
    build_signals(led)
    return 0


if __name__ == '__main__':
    sys.exit(main())
