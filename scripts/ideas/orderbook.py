"""Order-book ledger: read the order book each company STATES in its own filings, and flag big year-on-year jumps.

Usage:
  python3 scripts/ideas/orderbook.py --scrips 543410,544750 [--days 480]     # read those companies' filings
  python3 scripts/ideas/orderbook.py --universe [--days 480] [--limit N]     # every company in docs/ideas/universe.json
  python3 scripts/ideas/orderbook.py --signals                               # rebuild docs/ideas/orderbook_signals.json only

Order books are not in any structured feed: companies state them in investor presentations, call transcripts, results
press releases, business updates, annual reports and some order-win letters. For each company this reads its BSE
filing list (90-day windows, every page), keeps the documents that can carry an order book, extracts the PDF text and
pulls every sentence that states an order book / order backlog / unexecuted orders with a rupee amount. Each reading is
stored with the filing URL, the filing date, the "as on" date when the sentence gives one, and the sentence itself, so
every number can be checked by eye. Nothing is inferred: no amount in the sentence, no reading.

Ledger: docs/ideas/order_book.json  {built, companies: {scrip: {name, nse, read_through, readings: [...]}}}
Signals: docs/ideas/orderbook_signals.json — per company the latest order book against the one ~a year earlier
(the reading nearest 365 days before, 270-460 days back) and against trailing-year revenue where known.
"""
import json, os, sys, re, datetime, argparse, time, urllib.request
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append(os.path.dirname(HERE))
import bse, ist, dossier, updates

DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
LEDGER = os.environ.get('OB_LEDGER') or os.path.join(DOCS, 'order_book.json')
SIGNALS = os.path.join(DOCS, 'orderbook_signals.json')
CACHE = os.path.join(bse.CACHE, 'ob_text')
os.makedirs(CACHE, exist_ok=True)

# Lean mode (backfill): only the documents that state the QUARTERLY order book - decks, transcripts, results press
# releases, business updates, annual reports. Individual order letters (80-100 a year for serial filers) are skipped
# unless their subject says "order book". Measured 2026-10-11: the full read ran ~100 s per company.
LEAN_RX = re.compile(r'presentation|transcript|earnings call|conference call|con\. ?call|concall|press release|media release|'
                     r'business update|business highlights|order ?book|annual report|investor (?:release|update)|financial result|outcome of board', re.I)
# Leaner (whole-market pass): decks, transcripts and press releases only - where a company that does not file order
# letters states its order book. Results PDFs and annual reports are left to the order-filer pass.
LEANER_RX = re.compile(r'presentation|transcript|earnings call|conference call|con\. ?call|concall|press release|media release|'
                       r'business update|business highlights|order ?book|investor (?:release|update)', re.I)
# Filings that can carry a stated order book. Housekeeping never does.
DOC_RX = re.compile(r'presentation|transcript|earnings call|conference call|con\. ?call|concall|press release|media release|'
                    r'business update|business highlights|order ?book|investor|annual report|outcome of board|financial result|'
                    r'award|order|contract|bagging|letter of (?:award|intent)|\bloa\b|\bloi\b', re.I)
SKIP_RX = re.compile(r'trading window|newspaper|reg(?:ulation)?\.? ?74|certificate under|compliance certificate|'
                     r'loss of share|duplicate share|investor complaint|scrutini[sz]er|voting result|book closure|record date', re.I)

OB_RX = re.compile(r'order[\s-]*book|order[\s-]*backlog|unexecuted\s+orders?|orders?\s+(?:in\s+hand|on\s+hand)|'
                   r'outstanding\s+orders?|pending\s+orders?|executable\s+orders?|un-?executed\s+order|order\s+position', re.I)
# The amount must carry a rupee marker or an Indian unit; a bare number is never read as rupees.
AMT_RX = re.compile(r'(?:(?:₹|Rs\.?|INR|Rupees)\s*)?([\d]{1,3}(?:,\d{2,3})+(?:\.\d+)?|\d+(?:\.\d+)?)\s*'
                    r'(lakh\s*crores?|lac\s*crores?|crores?|crs?\.?|cr\b|lakhs?|lacs?|mn\b|million|bn\b|billion)', re.I)
USD_RX = re.compile(r'(US\$|USD|\$|EUR|€)\s*$')
NOT_BOOK = re.compile(r'order\s+inflow|orders?\s+(?:received|won|secured|bagged)|inflow|new\s+orders?|L-?1\b|bid\s+pipeline|'
                      r'pipeline|tender|bids?\s+(?:submitted|under)|enquir', re.I)
ASON_RX = re.compile(r'as\s+(?:on|of|at)\s+(\d{1,2}(?:st|nd|rd|th)?[\s\-./]+[A-Za-z]{3,9}[\s\-.,/]+\d{2,4}|\d{1,2}[\-./]\d{1,2}[\-./]\d{2,4}|'
                     r'[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|[A-Za-z]{3,9}[\s\-\']+\d{2,4}|(?:Q[1-4]|H[12])\s*FY\s*\'?\d{2,4})', re.I)
MONTHS = {m: i for i, m in enumerate(['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], 1)}


def to_cr(num, unit):
    v = float(num.replace(',', ''))
    u = unit.lower().replace(' ', '')
    if u.startswith(('lakhcr', 'laccr')): return v * 1e5
    if u.startswith(('crore', 'cr')): return v
    if u.startswith(('lakh', 'lac')): return v / 100
    if u in ('mn', 'million'): return v / 10
    if u in ('bn', 'billion'): return v * 100
    return None


def parse_ason(txt):
    t = txt.strip().lower().replace(',', ' ')
    m = re.match(r'(q[1-4]|h[12])\s*fy\s*\'?(\d{2,4})', t)
    if m:
        fy = int(m.group(2)); fy = fy + 2000 if fy < 100 else fy
        end = {'q1': (fy - 1, 6, 30), 'q2': (fy - 1, 9, 30), 'q3': (fy - 1, 12, 31), 'q4': (fy, 3, 31), 'h1': (fy - 1, 9, 30), 'h2': (fy, 3, 31)}[m.group(1)]
        return datetime.date(*end)
    m = re.match(r'(\d{1,2})[\-./](\d{1,2})[\-./](\d{2,4})', t)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3)); y = y + 2000 if y < 100 else y
        try: return datetime.date(y, mo, d)
        except ValueError: return None
    m = re.match(r'(\d{1,2})(?:st|nd|rd|th)?[\s\-./]+([a-z]{3,9})[\s\-./]+(\d{2,4})', t)
    if m and m.group(2)[:3] in MONTHS:
        y = int(m.group(3)); y = y + 2000 if y < 100 else y
        try: return datetime.date(y, MONTHS[m.group(2)[:3]], int(m.group(1)))
        except ValueError: return None
    m = re.match(r'([a-z]{3,9})\s+(\d{1,2})\s+(\d{4})', t)
    if m and m.group(1)[:3] in MONTHS:
        try: return datetime.date(int(m.group(3)), MONTHS[m.group(1)[:3]], int(m.group(2)))
        except ValueError: return None
    m = re.match(r'([a-z]{3,9})[\s\-\']+(\d{2,4})$', t)
    if m and m.group(1)[:3] in MONTHS:
        y = int(m.group(2)); y = y + 2000 if y < 100 else y
        mo = MONTHS[m.group(1)[:3]]
        nxt = datetime.date(y + (mo == 12), mo % 12 + 1, 1)
        return nxt - datetime.timedelta(days=1)
    return None


# Words that, sitting between "order book" and an amount, mean the amount is something else (income, a loss, a single
# contract) - measured on the 2026-10-11 pilot: SEPC's tax-loss carry-forward, Marine's PAT, SEPC's total income.
OTHER_RX = re.compile(r'income|revenue|turnover|sales|ebitda|profit|\bpat\b|loss|margin|capex|borrowing|debt|cash|'
                      r'contract\s+(?:worth|of|value)|order\s+(?:worth|of\s+(?:rs|₹|inr))|won|received|award|toll\s+operations\s+contract|'
                      r'target|aim|aspir|expect|guid|by\s+fy|crore\s+in\s+order|segment', re.I)
LINK_RX = re.compile(r'^[\s:\-–—(]*(?:of|at|is|was|stood|stands|standing|stand|amount(?:ing|ed|s)?\s+to|aggregat\w*\s+to|'
                     r'valued\s+at|position|crossed|crosses|reached|totall?ing|worth|approximately|approx\.?|around|about|'
                     r'nearly|over|more\s+than|in\s+excess\s+of|close\s+to|of\s+around|of\s+approx\.?|of\s+about|'
                     r'as\s+(?:on|of|at)\s+[^.;]{0,40}?|in\s+hand|on\s+hand|currently|now|has|have|been|grew|grown|rose|'
                     r'increased|to|the|company|\'s|our|total|consolidated|healthy|robust|strong|record|position|number|figure|value|book|backlog|\s)*'
                     r'(?:₹|rs\.?|inr|rupees|`)?\s*$', re.I)
# Verification of the 29 biggest screen jumps (2026-10-11) found these misreads; each is now rejected in a
# ~70-char window around the figure: another metric (pipeline, intake, received, revenue, billed), a segment / entity /
# region, an analyst's question or a hypothetical, a conditional contract.
NEAR_BAD_RX = re.compile(r'pipeline|intake|inflow|received|topline|revenue|turnover|billed|billing|bid(?:s|ding)?\b|'
                         r'segment|division|standalone|subsidiar|textile|o\s*&\s*m\b|operations\s+and\s+maintenance|'
                         r'\bsay\b|e\.g\.|for\s+example|if\s+(?:that|it)\s+gets|right\?|isn\'t\s+it|which\s+i\s+think|'
                         r'question|could\s+you|can\s+you|on\s+hold', re.I)
FX_RX = re.compile(r'US\$|USD|\$|EUR|€|AED|GBP|£|SAR|dollar', re.I)
ASDATE_RX = re.compile(r'as\s+(?:on|of)\s+(?:date|today|the\s+date)|current\s+order\s*book|as\s+on\s+the\s+date\s+of', re.I)


# Deck charts flatten to "688 2,073 Q1FY26 Q1FY27 Order Backlog (Rs. Cr.)" - two values, two period labels, the label.
# (Marine Electricals' Q1 FY27 release: its year-ago 688 existed only in this chart.)
CHART_RX = re.compile(r'([\d,]+(?:\.\d+)?)\s+([\d,]+(?:\.\d+)?)\s+((?:Q[1-4]|H[12])\s*FY\s*\'?\d{2,4})\s+((?:Q[1-4]|H[12])\s*FY\s*\'?\d{2,4})\s+'
                      r'(?:Total\s+|Consolidated\s+)?Order\s*(?:Book|Backlog)\s*\(\s*(?:Rs\.?|₹|INR)\s*(Cr|Crore|Mn|Million|Lakh)s?\.?\s*\)', re.I)


def chart_readings(flat):
    out = []
    for m in CHART_RX.finditer(flat):
        for num, lab in ((m.group(1), m.group(3)), (m.group(2), m.group(4))):
            d = parse_ason(lab)
            v = to_cr(num, m.group(5))
            if d and v and v >= 1:
                out.append(dict(value_cr=round(v, 2), as_on=d.isoformat(), quote=m.group(0)[:360]))
    return out


def readings_from_text(text):
    """Every (amount_cr, as_on, sentence) where a sentence states an order book with an amount.

    An amount counts only when it FOLLOWS the phrase and only linking words sit between them ("order book of /
    stood at / position as on 30 June 2026 is ₹X cr"), or when it sits immediately before the phrase
    ("₹789 Cr Order Book"). Anything else in between - income, PAT, a loss, a contract won, a target - means the
    amount is not the order book and is dropped."""
    out = []
    flat = re.sub(r'\s+', ' ', text)
    for m in OB_RX.finditer(flat):
        after = flat[m.end():m.end() + 140]
        cand = None
        for a in AMT_RX.finditer(after):
            gap = after[:a.start()]
            if USD_RX.search(flat[max(0, m.end() + a.start() - 6):m.end() + a.start()]):
                break
            if OTHER_RX.search(gap) or NOT_BOOK.search(gap):
                break
            if LINK_RX.match(gap):
                cand = a
            break                                  # only the first amount after the phrase is ever considered
        if cand is None:
            before = flat[max(0, m.start() - 26):m.start()]
            b = list(AMT_RX.finditer(before))
            if b and re.fullmatch(r'\s*(?:total|consolidated|healthy|robust|strong|record|unexecuted)?\s*', before[b[-1].end():], re.I) \
                    and not USD_RX.search(before[max(0, b[-1].start() - 6):b[-1].start()]):
                cand = b[-1]
        if cand is None:
            continue
        v = to_cr(cand.group(1), cand.group(2))
        if not v or v < 1 or v > 5e6:
            continue
        near = flat[max(0, m.start() - 70):m.end() + 140]
        if NEAR_BAD_RX.search(near):
            continue
        # "15 million" with no rupee marker next to it and a foreign currency nearby is not rupees
        if cand.group(2).lower() in ('mn', 'million', 'bn', 'billion') and FX_RX.search(near) and not re.search(r'₹|rs\.?|inr|rupee', near, re.I):
            continue
        win = flat[max(0, m.start() - 160):m.end() + 220]
        ason = None
        if ASDATE_RX.search(win):
            ason = 'FILING_DATE'
        else:
            am = ASON_RX.search(flat[max(0, m.start() - 60):m.end() + 200])
            if am:
                d = parse_ason(am.group(1))
                ason = d.isoformat() if d else None
        out.append(dict(value_cr=round(v, 2), as_on=ason, quote=win.strip()[:360]))
    return out + chart_readings(flat)


def pdf_text(url):
    key = re.sub(r'\W+', '_', url)[-150:]
    fn = os.path.join(CACHE, key + '.txt')
    if os.path.exists(fn):
        return open(fn, encoding='utf-8', errors='ignore').read()
    txt = ''
    for u in (url, url.replace('/AttachLive/', '/AttachHis/')):
        try:
            if 'bseindia.com' in u:
                raw = bse.get_attachment(u)
            else:   # nsearchives.nseindia.com serves filings to a plain browser UA, no cookie needed
                raw = urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': updates.UA}), timeout=90).read()
        except Exception:
            raw = None
        if raw and raw[:4] == b'%PDF':
            try:
                import fitz
                doc = fitz.open(stream=raw, filetype='pdf')
                txt = '\n'.join(p.get_text() for p in doc[:80])
            except Exception:
                txt = ''
            break
    open(fn, 'w', encoding='utf-8').write(txt)
    return txt


def quarter_end_before(d):
    q = [datetime.date(d.year, 3, 31), datetime.date(d.year, 6, 30), datetime.date(d.year, 9, 30), datetime.date(d.year, 12, 31),
         datetime.date(d.year - 1, 12, 31)]
    return max(x for x in q if x < d)


LEAN = False


def read_company(scrip, days, read_through=None, nse=None, sme=False):
    """scrip = BSE code (reads BSE), or '' with nse= set (NSE-only names read NSE's announcement feed)."""
    if scrip:
        anns = dossier.announcements_for(scrip, days)
        status = dossier.last_ann_status
    else:
        probs = []
        today = ist.today()
        got = updates.nse_items(nse, sme, today - datetime.timedelta(days=days), today, probs)
        if not got and not sme:      # NSE Emerge names file under the SME index
            got = updates.nse_items(nse, True, today - datetime.timedelta(days=days), today, probs)
        got += [x for x in updates.nse_other_items(nse, sme, today - datetime.timedelta(days=days), today, probs)
                if x['category'] == 'Annual report']
        anns = [dict(date=x['date'], subject=x['subject'], headline=x['headline'], category=x['category'], sub='', pdf=x['pdf'])
                for x in got]
        status = 'complete' if not probs else 'SUSPECT - ' + '; '.join(probs)
    out, docs = [], 0
    for a in anns:
        if read_through and a['date'] <= read_through:
            continue
        subj = f"{a.get('subject') or ''} {a.get('headline') or ''} {a.get('category') or ''} {a.get('sub') or ''}"
        if SKIP_RX.search(subj) or not ((LEANER_RX if LEAN == 2 else LEAN_RX) if LEAN else DOC_RX).search(subj) or not a.get('pdf'):
            continue
        txt = pdf_text(a['pdf'])
        docs += 1
        if not txt or not OB_RX.search(txt):
            continue
        fdate = datetime.date.fromisoformat(a['date'])
        seen = set()
        for r in readings_from_text(txt):
            k = (r['value_cr'], r['as_on'])
            if k in seen:
                continue
            seen.add(k)
            if r['as_on'] == 'FILING_DATE':
                r = dict(r, as_on=a['date']); basis = 'as on filing date'
            else:
                basis = 'stated' if r['as_on'] else 'quarter before filing'
            # an "as on" date after the filing date is a target, not a book
            if r['as_on'] and r['as_on'] > a['date']:
                continue
            out.append(dict(r, filed=a['date'], as_on_basis=basis,
                            as_on_eff=r['as_on'] or quarter_end_before(fdate).isoformat(),
                            doc=a['pdf'], doc_title=(a.get('subject') or '')[:160]))
        time.sleep(0.2)
    return out, docs, status


def pick_book(readings, target):
    """The company's order book nearest a date: among readings whose effective date is closest to target, the value
    stated most often (presentation + transcript usually repeat it), ties -> the larger (consolidated)."""
    if not readings:
        return None
    best_gap = min(abs((datetime.date.fromisoformat(r['as_on_eff']) - target).days) for r in readings)
    near0 = [r for r in readings if abs((datetime.date.fromisoformat(r['as_on_eff']) - target).days) == best_gap]
    # one document often states the total and its segments (SEPC: 10,670 total = 5,270 domestic + 5,400 international):
    # keep each document's largest figure for that date, then the value the most documents agree on
    per_doc = {}
    for r in near0:
        if r['doc'] not in per_doc or r['value_cr'] > per_doc[r['doc']]['value_cr']:
            per_doc[r['doc']] = r
    near = list(per_doc.values())
    cnt = {}
    for r in near:
        cnt[r['value_cr']] = cnt.get(r['value_cr'], 0) + 1
    v = sorted(cnt.items(), key=lambda kv: (-kv[1], -kv[0]))[0][0]
    r = next(r for r in near if r['value_cr'] == v)
    return dict(r, n_same=cnt[v], n_near=len(near))


def build_signals(ledger):
    rev = {}
    try:
        qr = json.load(open(os.path.join(DOCS, '..', 'quarterly_results.json')))
        rev = qr if isinstance(qr, dict) else {}
    except Exception:
        pass
    sig = []
    for scrip, c in ledger['companies'].items():
        R = [r for r in c.get('readings', []) if r.get('value_cr')]
        if not R:
            continue
        latest_d = max(datetime.date.fromisoformat(r['as_on_eff']) for r in R)
        now = pick_book(R, latest_d)
        older = [r for r in R if 300 <= (latest_d - datetime.date.fromisoformat(r['as_on_eff'])).days <= 430]
        prev = pick_book(older, latest_d - datetime.timedelta(days=365)) if older else None
        row = dict(scrip=scrip, name=c.get('name'), nse=c.get('nse'), latest=now, year_ago=prev)
        if prev and prev['value_cr']:
            row['yoy_pct'] = round((now['value_cr'] / prev['value_cr'] - 1) * 100, 1)
            # a jump this large is more often a misread (a segment, a multi-year figure) than a fact: read the quotes
            row['check'] = bool(row['yoy_pct'] > 400 or row['yoy_pct'] < -60)
        sig.append(row)
    try:
        ver = {v['key']: v for v in json.load(open(os.path.join(DOCS, 'orderbook_verified.json')))['rows']}
    except Exception:
        ver = {}
    for row in sig:
        v = ver.get(row['scrip'])
        if v:
            row['verified'] = {k: v.get(k) for k in ('latest_verdict', 'latest_correct_value_cr', 'latest_as_on', 'year_ago_verdict',
                                                     'year_ago_correct_value_cr', 'year_ago_as_on', 'true_yoy_pct', 'book_to_ttm_sales', 'notes', 'checked')}
    sig.sort(key=lambda r: -(r.get('yoy_pct') or -1e9))
    out = dict(built=ist.stamp(), rule='latest stated order book vs the one stated ~a year earlier (10-14 months before); '
               'readings come from the companies\' own filings, each with its sentence and document', rows=sig)
    json.dump(out, open(SIGNALS, 'w'), indent=1, ensure_ascii=False)
    return out


def refresh_by_day(ledger, days_back):
    """Daily: read each recent day's WHOLE-MARKET filing lists (BSE's full day list, NSE announcements on both boards),
    keep the documents that can state an order book, and read only those - a few hundred PDFs, not 5,000 companies'
    filing lists (which would also get us rate-limited by BSE: measured 2026-10-11, 12 parallel per-company readers
    drew HTTP 403s within minutes). Every company is covered, including ones not yet in the ledger."""
    by_scrip = {k: k for k in ledger['companies'] if not k.startswith('NSE:')}
    by_nse = {}
    for k, c in ledger['companies'].items():
        if c.get('nse'):
            by_nse[c['nse'].upper()] = k
    today = ist.today()
    added, docs_read, probs = 0, 0, []
    for back in range(days_back, -1, -1):
        day = today - datetime.timedelta(days=back)
        items = []
        try:
            for r in bse.announcements(day):
                subj = f"{r.get('NEWSSUB') or ''} {r.get('HEADLINE') or ''} {r.get('CATEGORYNAME') or ''} {r.get('SUBCATNAME') or ''}"
                items.append(('bse', str(r.get('SCRIP_CD') or ''), '', (r.get('NEWS_DT') or '')[:10] or day.isoformat(), subj, bse.attachment_url(r)))
            if getattr(bse, 'last_announcements_capped', False) or getattr(bse, 'last_announcements_partial', False):
                probs.append(f'BSE {day} list incomplete')
        except Exception as e:
            probs.append(f'BSE {day} failed ({str(e)[:60]})')
        for ix in ('equities', 'sme'):
            for r in updates._rows(f'https://www.nseindia.com/api/corporate-announcements?index={ix}&from_date={day:%d-%m-%Y}&to_date={day:%d-%m-%Y}', probs, f'NSE {day} {ix}'):
                subj = f"{r.get('desc') or ''} {r.get('attchmntText') or ''}"
                items.append(('nse', '', str(r.get('symbol') or '').upper(), day.isoformat(), subj, r.get('attchmntFile') or ''))
        seen_docs = set()
        for src, scrip, sym, date, subj, pdf in items:
            if not pdf or pdf in seen_docs or SKIP_RX.search(subj) or not DOC_RX.search(subj):
                continue
            seen_docs.add(pdf)
            key = (scrip if src == 'bse' else by_nse.get(sym) or ('NSE:' + sym))
            if not key:
                continue
            txt = pdf_text(pdf)
            docs_read += 1
            if not txt or not OB_RX.search(txt):
                continue
            c = ledger['companies'].setdefault(key, dict(readings=[], nse=sym or None))
            have = {(r['doc'], r['value_cr'], r['as_on']) for r in c['readings']}
            fdate = datetime.date.fromisoformat(date)
            for r in readings_from_text(txt):
                if r['as_on'] == 'FILING_DATE':
                    r = dict(r, as_on=date); basis = 'as on filing date'
                else:
                    basis = 'stated' if r['as_on'] else 'quarter before filing'
                if r['as_on'] and r['as_on'] > date:
                    continue
                if (pdf, r['value_cr'], r['as_on']) in have:
                    continue
                c['readings'].append(dict(r, filed=date, as_on_basis=basis, as_on_eff=r['as_on'] or quarter_end_before(fdate).isoformat(),
                                          doc=pdf, doc_title=subj[:160]))
                have.add((pdf, r['value_cr'], r['as_on'])); added += 1
            c['read_through'] = today.isoformat()
    ledger['built'] = ist.stamp()
    ledger['last_refresh'] = dict(at=ist.stamp(), days_back=days_back, docs_read=docs_read, readings_added=added, problems=probs[:20])
    json.dump(ledger, open(LEDGER, 'w'), indent=1, ensure_ascii=False)
    print(f'refresh: {days_back + 1} days of whole-market lists, {docs_read} documents read, {added} new readings'
          + (f'; problems: {"; ".join(probs)[:300]}' if probs else ''))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scrips')
    ap.add_argument('--universe', action='store_true')
    ap.add_argument('--days', type=int, default=480)
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--signals', action='store_true')
    ap.add_argument('--nse', help='comma-separated NSE symbols of NSE-only companies (no BSE code)')
    ap.add_argument('--lean', action='store_true', help='skip individual order letters (backfill speed)')
    ap.add_argument('--leaner', action='store_true', help='decks, transcripts and press releases only (whole-market pass)')
    ap.add_argument('--merge', help='comma-separated ledger files to merge into the main ledger, then rebuild signals')
    ap.add_argument('--reextract', action='store_true', help='re-read every document already in the ledger from the text cache with '
                    'the current rules (after a rule fix), without re-listing filings')
    ap.add_argument('--days-back', type=int, default=4, help='refresh: how many calendar days of whole-market filing lists to read')
    ap.add_argument('--refresh', action='store_true', help='daily: every company already in the ledger, only filings since its read_through, '
                    'plus any company that filed an order win on NSE in the last 30 days')
    a = ap.parse_args()
    global LEAN
    LEAN = 2 if a.leaner else (1 if a.lean else 0)
    ledger = json.load(open(LEDGER)) if os.path.exists(LEDGER) else dict(companies={})
    if a.merge:
        for f in a.merge.split(','):
            part = json.load(open(f))
            for k, c in part.get('companies', {}).items():
                ledger['companies'][k] = c
        ledger['built'] = ist.stamp()
        json.dump(ledger, open(LEDGER, 'w'), indent=1, ensure_ascii=False)
        a.signals = True
    if a.reextract:
        for k, c in ledger['companies'].items():
            docs = {}
            for r in c.get('readings', []):
                docs.setdefault(r['doc'], (r['filed'], r.get('doc_title', '')))
            new = []
            for doc, (filed, title) in docs.items():
                txt = pdf_text(doc)
                fdate = datetime.date.fromisoformat(filed)
                seen = set()
                for r in readings_from_text(txt):
                    if r['as_on'] == 'FILING_DATE':
                        r = dict(r, as_on=filed); basis = 'as on filing date'
                    else:
                        basis = 'stated' if r['as_on'] else 'quarter before filing'
                    if r['as_on'] and r['as_on'] > filed:
                        continue
                    kk = (r['value_cr'], r['as_on'])
                    if kk in seen:
                        continue
                    seen.add(kk)
                    new.append(dict(r, filed=filed, as_on_basis=basis, as_on_eff=r['as_on'] or quarter_end_before(fdate).isoformat(),
                                    doc=doc, doc_title=title))
            c['readings'] = new
        ledger['built'] = ist.stamp()
        json.dump(ledger, open(LEDGER, 'w'), indent=1, ensure_ascii=False)
        a.signals = True
    if not a.signals:
        uni = {r['scrip']: r for r in json.load(open(os.path.join(DOCS, 'universe.json')))['rows']}
        scrips = a.scrips.split(',') if a.scrips else (list(uni) if a.universe else [])
        if a.limit:
            scrips = scrips[:a.limit]
        scrips = [(x, None) for x in scrips] + [('', y) for y in (a.nse.split(',') if a.nse else [])]
        if a.refresh:
            refresh_by_day(ledger, a.days_back)
            scrips = []
        if False:
            have = {k[4:] if k.startswith('NSE:') else (ledger['companies'][k].get('nse') or '') for k in ledger['companies']}
            probs, today = [], ist.today()
            for ix in ('equities', 'sme'):
                for r in updates._rows(f'https://www.nseindia.com/api/corporate-announcements?index={ix}&from_date={today - datetime.timedelta(days=30):%d-%m-%Y}&to_date={today:%d-%m-%Y}', probs, 'order filers'):
                    d = (r.get('desc') or '').lower()
                    if 'order' in d and ('bagging' in d or 'receiving' in d or 'award' in d) and r.get('symbol') and r['symbol'] not in have:
                        scrips.append(('', r['symbol'])); have.add(r['symbol'])
            if probs:
                print('order-filer scan problems:', '; '.join(probs)[:200])
        for i, (s, nsym) in enumerate(scrips, 1):
            key = s or 'NSE:' + nsym
            c = ledger['companies'].setdefault(key, dict(readings=[]))
            u = uni.get(s, {}) if s else {}
            c['name'] = u.get('name') or c.get('name') or nsym; c['nse'] = u.get('nse') or c.get('nse') or nsym
            days_c = a.days
            if a.refresh and c.get('read_through'):
                # daily: list only the filings since the last read (a few days), not the whole 15 months again
                days_c = max(3, (ist.today() - datetime.date.fromisoformat(c['read_through'])).days + 3)
            got, ndocs, status = read_company(s, days_c, c.get('read_through') if a.refresh else None, nse=nsym)
            have = {(r['doc'], r['value_cr'], r['as_on']) for r in c['readings']}
            new = [r for r in got if (r['doc'], r['value_cr'], r['as_on']) not in have]
            c['readings'] += new
            c['read_through'] = ist.today().isoformat()
            c['read_status'] = status
            print(f'[{i}/{len(scrips)}] {key} {c["name"]}: {ndocs} docs read, {len(new)} new readings, status {status[:40]}', flush=True)
            if i % 20 == 0:
                ledger['built'] = ist.stamp()
                json.dump(ledger, open(LEDGER, 'w'), indent=1, ensure_ascii=False)
        ledger['built'] = ist.stamp()
        json.dump(ledger, open(LEDGER, 'w'), indent=1, ensure_ascii=False)
    s = build_signals(ledger)
    for r in s['rows'][:25]:
        if r.get('yoy_pct') is not None:
            print(f"{(r['name'] or r.get('nse') or r['scrip'])[:30]:30s} {r['year_ago']['value_cr']:>9,.0f} ({r['year_ago']['as_on_eff']}) -> {r['latest']['value_cr']:>9,.0f} ({r['latest']['as_on_eff']})  {r['yoy_pct']:+.0f}%")


if __name__ == '__main__':
    main()
