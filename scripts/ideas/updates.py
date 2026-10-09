"""Queue the new exchange filings for every published idea, so the routine can keep each card current.

Usage: python3 scripts/ideas/updates.py [--since YYYY-MM-DD] [--ids ID,ID]
Reads docs/ideas/ideas.json. For each idea, lists the filings the company made AFTER the card's last check
(`updates_checked`, else the card's `date`) on BSE (per-scrip announcement API, 90-day windows, every page) and on
NSE (corporate-announcements by symbol; index=equities, or index=sme for an NSE Emerge name). Writes
docs/ideas/updates_queue.json: {built, ideas: [{id, name, since, status, items: [{exchange, date, subject, headline,
category, pdf}]}]}. Nothing is summarised here - the routine reads each queued filing and writes the card's
`updates[]` entry from the document (PLAYBOOK "Updates sweep"). A read that fails or stops short is marked in
`status`, never reported as "nothing filed".
"""
import json, os, sys, re, datetime, argparse, http.cookiejar, urllib.request, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.append(os.path.join(HERE, '..'))
import bse, bse_names, ist

DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
CHUNK = 90
NOISE = re.compile(r'newspaper|trading window|closure of trading|book closure|loss of share certificate|duplicate share|'
                   r'compliance certificate|certificate under regulation|reg(ulation)?\.? ?74|reconciliation of share capital|'
                   r'investor complaint|statement of investor|disclosure of voting|scrutinizer|clarification on price|'
                   r'price movement|spurt in volume', re.I)


def bse_items(scrip, d_from, d_to, problems):
    rows, seen = [], set()
    a = d_from
    while a <= d_to:
        b = min(a + datetime.timedelta(days=CHUNK - 1), d_to)
        got, reported = 0, None
        for p in range(1, 41):
            url = ('https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d&strCat=-1&strPrevDate=%s&strScrip=%s'
                   '&strSearch=P&strToDate=%s&strType=C&subcategory=-1' % (p, a.strftime('%Y%m%d'), scrip, b.strftime('%Y%m%d')))
            try:
                j = json.loads(bse._get(url))
            except Exception as e:
                problems.append(f'BSE {a}..{b} page {p} failed ({str(e)[:80]})'); break
            if 'Table' not in j:
                problems.append(f'BSE {a}..{b} refused: {j.get("Message") or j}'); break
            t = j.get('Table') or []
            if reported is None:
                try: reported = int((j.get('Table1') or [{}])[0].get('ROWCNT'))
                except (TypeError, ValueError, IndexError, AttributeError): pass
            for r in t:
                k = r.get('NEWSID')
                if k is None or k not in seen:
                    seen.add(k); rows.append(r); got += 1
            if len(t) < 50 or (reported is not None and got >= reported):
                break
        if reported is not None and got < reported:
            problems.append(f'BSE {a}..{b} read {got} of {reported}')
        a = b + datetime.timedelta(days=1)
    return [dict(exchange='BSE', key='bse:%s' % r.get('NEWSID'), date=(r.get('NEWS_DT') or '')[:10],
                 subject=bse_names.clean_ann_subject(r.get('NEWSSUB'), r.get('SCRIP_CD') or scrip),
                 headline=(r.get('HEADLINE') or '').strip()[:400], category=r.get('CATEGORYNAME') or '',
                 pdf=bse.attachment_url(r)) for r in rows]


_jar = None
NSE_CALLS = {'ok': 0, 'fail': 0}
def nse_get(url):
    try:
        r = _nse_get(url)
        NSE_CALLS['ok'] += 1
        return r
    except Exception:
        NSE_CALLS['fail'] += 1
        raise


def _nse_get(url):
    global _jar
    if _jar is None:
        _jar = http.cookiejar.CookieJar()
        op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_jar))
        for u in ('https://www.nseindia.com/', 'https://www.nseindia.com/companies-listing/corporate-filings-announcements'):
            try: op.open(urllib.request.Request(u, headers={'User-Agent': UA, 'Accept': 'text/html'}), timeout=25).read()
            except Exception: pass
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_jar))
    req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json, text/plain, */*',
                                               'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-announcements'})
    return json.loads(op.open(req, timeout=40).read().decode('utf-8', 'ignore'))


_isin_hint = {}


def nse_items(sym, sme, d_from, d_to, problems):
    out, seen = [], set()
    a = d_from
    while a <= d_to:
        b = min(a + datetime.timedelta(days=29), d_to)
        url = ('https://www.nseindia.com/api/corporate-announcements?index=%s&symbol=%s&from_date=%s&to_date=%s'
               % ('sme' if sme else 'equities', urllib.request.quote(sym), a.strftime('%d-%m-%Y'), b.strftime('%d-%m-%Y')))
        j = None
        for attempt in range(3):
            try:
                j = nse_get(url); break
            except Exception as e:
                err = str(e)[:80]; time.sleep(2 + 2 * attempt)
        if j is None:
            problems.append(f'NSE {a}..{b} failed ({err})')
        else:
            rows = j if isinstance(j, list) else (j.get('data') or [])
            for r in rows:
                if str(r.get('symbol') or '').upper() != sym.upper():
                    continue
                k = (r.get('an_dt'), r.get('desc'), r.get('attchmntFile'))
                if k in seen: continue
                seen.add(k)
                if r.get('sm_isin'): _isin_hint[sym.upper()] = r.get('sm_isin')
                dt = r.get('an_dt') or r.get('sort_date') or ''
                try: date = datetime.datetime.strptime(dt[:11].strip(), '%d-%b-%Y').date().isoformat()
                except Exception: date = dt[:10]
                out.append(dict(exchange='NSE', key=nse_ann_key(r), date=date, subject=(r.get('desc') or '').strip()[:200],
                                headline=(r.get('attchmntText') or '').strip()[:400], category=(r.get('desc') or '')[:80],
                                pdf=r.get('attchmntFile') or ''))
        a = b + datetime.timedelta(days=1)
    return out


def nse_board_items(sym, sme, d_from, d_to, problems):
    """NSE files board-meeting intimations in their OWN feed (corporate-board-meetings), not in corporate-announcements:
    Viviana's 06-Oct-2026 intimation of a 10-Oct fund-raising board meeting never appeared in the announcement feed
    and the 9-Oct sweep missed it. Read both."""
    url = ('https://www.nseindia.com/api/corporate-board-meetings?index=%s&symbol=%s'
           % ('sme' if sme else 'equities', urllib.request.quote(sym)))
    j = None
    for attempt in range(3):
        try:
            j = nse_get(url); break
        except Exception as e:
            err = str(e)[:80]; time.sleep(2 + 2 * attempt)
    if j is None:
        problems.append(f'NSE board meetings failed ({err})'); return []
    out = []
    for r in (j if isinstance(j, list) else (j.get('data') or [])):
        if str(r.get('bm_symbol') or '').upper() != sym.upper():
            continue
        ts = r.get('bm_timestamp') or ''
        try: date = datetime.datetime.strptime(ts[:11].strip(), '%d-%b-%Y').date()
        except Exception: continue
        if not (d_from <= date <= d_to):
            continue
        out.append(dict(exchange='NSE', key=nse_board_key(r), date=date.isoformat(),
                        subject=f"Board meeting on {r.get('bm_date') or '?'}: {(r.get('bm_purpose') or '').strip()}"[:200],
                        headline=(r.get('bm_desc') or '').strip()[:400], category='Board meeting',
                        pdf=r.get('attachment') or ''))
    return out


def nse_ann_key(r):
    return 'nse:' + str(r.get('attchmntFile') or r.get('seq_id') or (r.get('an_dt'), r.get('desc')))


def nse_board_key(r):
    return 'nsebm:%s|%s' % (r.get('bm_timestamp'), r.get('bm_purpose'))


def _d(txt):
    """'28-Sep-2026 16:18' / '21-JUL-2026 12:16:44' / '30-JUN-2026' -> date, else None."""
    m = re.match(r'\s*(\d{1,2})-([A-Za-z]{3})-(\d{4})', str(txt or ''))
    if not m:
        return None
    try:
        return datetime.datetime.strptime(f'{m.group(1)}-{m.group(2).title()}-{m.group(3)}', '%d-%b-%Y').date()
    except ValueError:
        return None


def _rows(url, problems, what):
    j = None
    for attempt in range(3):
        try:
            j = nse_get(url); break
        except Exception as e:
            err = str(e)[:80]; time.sleep(2 + 2 * attempt)
    if j is None:
        problems.append(f'NSE {what} failed ({err})'); return []
    return j if isinstance(j, list) else (j.get('data') or [])


def nse_other_items(sym, sme, d_from, d_to, problems):
    """Every other per-company NSE feed. Each of these is a filing type NSE does NOT repeat in the announcement feed
    (or repeats only sometimes): corporate actions, SAST Reg 29 disclosures, shareholding patterns, integrated
    financial results, annual reports, credit ratings, insider trading (PIT). Measured 2026-10-09 on VIVIANA."""
    ix = 'sme' if sme else 'equities'
    q = urllib.request.quote(sym)
    out = []
    def add(date, subject, headline, cat, pdf, key):
        if date and d_from <= date <= d_to:
            out.append(dict(exchange='NSE', key=key, date=date.isoformat(), subject=subject[:200], headline=(headline or '')[:400],
                            category=cat, pdf=pdf or ''))
    for r in _rows(f'https://www.nseindia.com/api/corporates-corporateActions?index={ix}&symbol={q}', problems, 'corporate actions'):
        if str(r.get('symbol') or '').upper() != sym.upper(): continue
        add(_d(r.get('caBroadcastDate')) or _d(r.get('exDate')), f"Corporate action: {r.get('subject') or ''} (ex {r.get('exDate')}, record {r.get('recDate')})",
            '', 'Corporate action', '', 'nseca:%s|%s' % (r.get('exDate'), r.get('subject')))
    for r in _rows(f'https://www.nseindia.com/api/corporate-sast-reg29?index={ix}&symbol={q}', problems, 'SAST'):
        if str(r.get('symbol') or '').upper() != sym.upper(): continue
        add(_d(r.get('timestamp') or r.get('sysTime')), f"SAST {r.get('regType') or ''}: {r.get('acquirerName')} {r.get('acqSaleType')} {r.get('noOfShareAcq') or r.get('noOfShareSale') or ''} sh, after {r.get('noOfShareAft')} ({r.get('totAftDiluted')}%)",
            f"{r.get('acquisitionMode') or ''} {r.get('acquirerDate') or ''}", 'SAST', r.get('attachement'), 'nsesast:%s' % r.get('application_no'))
    for r in _rows(f'https://www.nseindia.com/api/corporates-pit?index={ix}&symbol={q}&from_date={d_from:%d-%m-%Y}&to_date={d_to:%d-%m-%Y}', problems, 'insider trading'):
        if str(r.get('symbol') or '').upper() != sym.upper(): continue
        add(_d(r.get('date') or r.get('intimDt') or r.get('acqfromDt')), f"Insider trade: {r.get('acqName')} {r.get('tdpTransactionType') or ''} {r.get('secAcq') or ''} sh ({r.get('personCategory') or ''})",
            f"{r.get('acqMode') or ''} {r.get('acqfromDt') or ''}", 'Insider trading', r.get('xbrl') or '', 'nsepit:%s|%s|%s' % (r.get('date'), r.get('acqName'), r.get('secAcq')))
    for r in _rows(f'https://www.nseindia.com/api/corporate-share-holdings-master?index={ix}&symbol={q}', problems, 'shareholding'):
        add(_d(r.get('broadcastDate') or r.get('submissionDate')), f"Shareholding pattern {r.get('date')}: promoters {r.get('pr_and_prgrp')}%, public {r.get('public_val')}%"
            + (' (revised)' if str(r.get('revisedData') or 'N') != 'N' else ''), '', 'Shareholding', r.get('xbrl') or '', 'nseshp:%s|%s' % (r.get('recordId'), r.get('date')))
    for r in _rows(f'https://www.nseindia.com/api/integrated-filing-results?index={ix}&symbol={q}&period=Quarterly', problems, 'results'):
        if str(r.get('symbol') or '').upper() != sym.upper(): continue
        add(_d(r.get('broadcast_Date')), f"Results {r.get('qe_Date')}: {r.get('audited')} {r.get('consolidated')} ({r.get('type_Sub') or ''})",
            r.get('type') or '', 'Results', r.get('pdf_attach') or r.get('ixbrl') or '', 'nseres:%s' % r.get('seq_Id'))
    for r in _rows(f'https://www.nseindia.com/api/annual-reports?index={ix}&symbol={q}', problems, 'annual reports'):
        add(_d(r.get('broadcast_dttm')), f"Annual report {r.get('fromYr')}-{r.get('toYr')} ({r.get('submission_type')})", '', 'Annual report',
            r.get('fileName'), 'nsear:%s' % r.get('fileName'))
    for r in _rows(f'https://www.nseindia.com/api/corporate-credit-rating?index={ix}&symbol={q}', problems, 'credit ratings'):
        if str(r.get('Symbol') or '').upper() != sym.upper(): continue
        add(_d(r.get('ReportingDate') or r.get('DateofCR')), f"Credit rating: {r.get('NameOfCRAgency')} {r.get('CreditRating')} — {r.get('RatingAction')}",
            r.get('Subject') or '', 'Credit rating', '', 'nsecr:%s' % r.get('AppID'))
    return out


def bse_ca_items(scrip, d_from, d_to, problems):
    try:
        rows = bse.corporate_actions(scrip) or []
    except Exception as e:
        problems.append(f'BSE corporate actions failed ({str(e)[:80]})'); return []
    # bse.corporate_actions() returns (ex_date, factor, label) for bonus / split events: the price-changing actions.
    out = []
    for r in rows:
        try:
            d, factor, label = r[0], r[1], r[2]
        except Exception:
            continue
        if isinstance(d, datetime.date) and d_from <= d <= d_to:
            out.append(dict(exchange='BSE', key='bseca:%s|%s' % (d, label), date=d.isoformat(),
                            subject=f"Corporate action (ex-date): {label}"[:200], headline=f'price factor {factor}', category='Corporate action', pdf=''))
    return out


def second_reader(ideas, days, problems):
    """Read each whole day's filings (NSE announcements + NSE board meetings, both boards; BSE's full day list) and pick
    out every filing by a company we cover, matched by ISIN / NSE symbol / BSE code. Anything it finds that the
    per-company reads did not is a MISS, and is added to that idea's queue with found_by='daily-feed'."""
    by_isin, by_sym, by_scrip, by_name = {}, {}, {}, {}
    norm = lambda n: re.sub(r'\b(ltd|limited|pvt|private|india)\b|[^a-z0-9]', '', str(n or '').lower())
    for it in ideas:
        if it.get('name'): by_name[norm(it['name'])] = it['id']
        if it.get('isin'): by_isin[it['isin'].upper()] = it['id']
        if it.get('nse'): by_sym[it['nse'].upper()] = it['id']
        if re.fullmatch(r'\d{6}', str(it.get('scrip') or '')): by_scrip[str(it['scrip'])] = it['id']
    found = {}
    today = ist.today()
    for k in range(days):
        day = today - datetime.timedelta(days=k)
        if day.weekday() >= 5: continue
        for ix in ('equities', 'sme'):
            for r in _rows(f'https://www.nseindia.com/api/corporate-announcements?index={ix}&from_date={day:%d-%m-%Y}&to_date={day:%d-%m-%Y}', problems, f'daily announcements {day} {ix}'):
                iid = by_isin.get(str(r.get('sm_isin') or '').upper()) or by_sym.get(str(r.get('symbol') or '').upper()) or by_name.get(norm(r.get('sm_name')))
                if iid:
                    found.setdefault(iid, []).append(dict(exchange='NSE', key=nse_ann_key(r), date=day.isoformat(), subject=(r.get('desc') or '')[:200],
                        headline=(r.get('attchmntText') or '')[:400], category=(r.get('desc') or '')[:80], pdf=r.get('attchmntFile') or '', found_by='daily-feed'))
            for r in _rows(f'https://www.nseindia.com/api/corporate-board-meetings?index={ix}&from_date={day:%d-%m-%Y}&to_date={day:%d-%m-%Y}', problems, f'daily board meetings {day} {ix}'):
                iid = by_isin.get(str(r.get('sm_isin') or '').upper()) or by_sym.get(str(r.get('bm_symbol') or '').upper()) or by_name.get(norm(r.get('sm_name')))
                d2 = _d(r.get('bm_timestamp'))
                if iid and d2:
                    found.setdefault(iid, []).append(dict(exchange='NSE', key=nse_board_key(r), date=d2.isoformat(),
                        subject=f"Board meeting on {r.get('bm_date') or '?'}: {(r.get('bm_purpose') or '').strip()}"[:200],
                        headline=(r.get('bm_desc') or '')[:400], category='Board meeting', pdf=r.get('attachment') or '', found_by='daily-feed'))
        if by_scrip:
            try:
                rows = bse.announcements(day)
            except Exception as e:
                problems.append(f'BSE daily list {day} failed ({str(e)[:60]})'); rows = []
            if getattr(bse, 'last_announcements_capped', False) or getattr(bse, 'last_announcements_partial', False):
                problems.append(f'BSE daily list {day} incomplete')
            for r in rows:
                iid = by_scrip.get(str(r.get('SCRIP_CD') or ''))
                if iid:
                    found.setdefault(iid, []).append(dict(exchange='BSE', key='bse:%s' % r.get('NEWSID'), date=(r.get('NEWS_DT') or '')[:10],
                        subject=bse_names.clean_ann_subject(r.get('NEWSSUB'), r.get('SCRIP_CD')), headline=(r.get('HEADLINE') or '').strip()[:400],
                        category=r.get('CATEGORYNAME') or '', pdf=bse.attachment_url(r), found_by='daily-feed'))
    return found


def isin_of(it):
    """ISIN for the second reader: the card's own field, else the NSE announcement / BSE master records."""
    if it.get('isin'):
        return it['isin']
    try:
        for r in (bse.scrip_master() or []):
            if str(r.get('scrip') or r.get('SCRIP_CD') or '') == str(it.get('scrip') or '') and (r.get('isin') or r.get('ISIN_NUMBER')):
                return r.get('isin') or r.get('ISIN_NUMBER')
    except Exception:
        pass
    return ''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', help='override the per-idea start date (YYYY-MM-DD)')
    ap.add_argument('--ids', help='comma-separated idea ids to refresh (default: all)')
    ap.add_argument('--xcheck-days', type=int, default=7, help='days of whole-market feeds the second reader re-reads (0 = off)')
    ap.add_argument('--selftest', action='store_true', help='fail unless known filings are found (Viviana 06-Oct-2026 board-meeting intimation)')
    a = ap.parse_args()
    fn = os.path.join(DOCS, 'ideas.json')
    ideas = json.load(open(fn)).get('ideas', [])
    want = set(a.ids.split(',')) if a.ids else None
    today = ist.today()
    out, isin_seen = [], {}
    for it in ideas:
        if want and it['id'] not in want:
            continue
        # The queue always lists every filing since the CARD date (the company page's Filings tab shows it);
        # `new` marks the ones filed on or after the last sweep (`updates_checked`) - the sweep's work list.
        since = datetime.date.fromisoformat(a.since or it['date'])
        checked = it.get('updates_checked') or ''
        problems, items = [], []
        scrip = str(it.get('scrip') or '').strip()
        if re.fullmatch(r'\d{6}', scrip):
            items += bse_items(scrip, since, today, problems)
            items += bse_ca_items(scrip, since, today, problems)
        if it.get('nse'):
            sme_board = bool(it.get('sme')) and not re.fullmatch(r'\d{6}', scrip)
            items += nse_items(it['nse'], sme_board, since, today, problems)
            items += nse_board_items(it['nse'], sme_board, since, today, problems)
            items += nse_other_items(it['nse'], sme_board, since, today, problems)
        if it.get('nse') and _isin_hint.get(it['nse'].upper()):
            isin_seen[it['id']] = _isin_hint[it['nse'].upper()]
        # the same filing on both exchanges / two feeds: keep one copy, BSE first (it carries the category)
        seen, uniq = set(), []
        # A filing is identified by its own key (BSE NEWSID, NSE attachment / seq id, feed record id). Two filings on the
        # same day with the same subject are TWO filings (Om Power's two GETCO letters of 06-Oct-2026 were merged into one
        # by an old date+subject rule; the second reader caught it). The only cross-copy merge is the same filing
        # posted on BOTH exchanges: same date, same subject wording, different exchange.
        bse_subj = set()
        for x in sorted(items, key=lambda x: (x['exchange'] != 'BSE')):
            k = x.get('key') or (x['exchange'], x['date'], x['subject'])
            if k in seen: continue
            sj = (x['date'], re.sub(r'\W+', ' ', (x['subject'] or '')[:60]).lower())
            if x['exchange'] == 'NSE' and sj in bse_subj: continue
            if x['exchange'] == 'BSE': bse_subj.add(sj)
            seen.add(k); uniq.append(x)
        for x in uniq:
            x['routine'] = bool(NOISE.search((x['subject'] or '') + ' ' + (x['headline'] or '')))
            x['new'] = not checked or x['date'] >= checked
        uniq.sort(key=lambda x: x['date'], reverse=True)
        out.append(dict(id=it['id'], name=it.get('name'), ticker=it.get('ticker'), scrip=scrip, nse=it.get('nse') or '',
                        since=since.isoformat(), status='complete' if not problems else 'SUSPECT - ' + '; '.join(problems),
                        n=len(uniq), n_substantive=sum(1 for x in uniq if not x['routine']),
                        n_new=sum(1 for x in uniq if x['new'] and not x['routine']), checked=checked, items=uniq))
        print(f"{it['id']:28s} since {since}  {len(uniq):3d} filings ({out[-1]['n_substantive']} substantive, {out[-1]['n_new']} new since {checked or 'never'})  {out[-1]['status'][:80]}")
    # ---- second reader: whole-day feeds, matched to our companies; anything the per-company reads missed is a MISS ----
    misses, xprob = 0, []
    if a.xcheck_days > 0:
        sel = [dict(it, isin=isin_seen.get(it['id'], '')) for it in ideas if not want or it['id'] in want]
        found = second_reader(sel, a.xcheck_days, xprob)
        for q in out:
            have_keys = {x.get('key') for x in q['items']}
            have_subj = {(x['exchange'], x['date'], re.sub(r'\W+', ' ', (x['subject'] or '')[:60]).lower()) for x in q['items'] if not x.get('key')}
            for x in found.get(q['id'], []):
                if x['date'] < q['since']:
                    continue
                if x['key'] in have_keys or (x['exchange'], x['date'], re.sub(r'\W+', ' ', (x['subject'] or '')[:60]).lower()) in have_subj:
                    continue
                x['routine'] = bool(NOISE.search((x['subject'] or '') + ' ' + (x['headline'] or '')))
                x['new'] = not q['checked'] or x['date'] >= q['checked']
                q['items'].insert(0, x); have_keys.add(x['key']); misses += 1
                q['n'] += 1
                if not x['routine']:
                    q['n_substantive'] += 1; q['n_new'] += int(x['new'])
                q['status'] = ('SUSPECT - ' if q['status'] == 'complete' else q['status'] + '; ') + f"daily-feed second reader found {x['exchange']} filing {x['date']} '{x['subject'][:60]}' that the per-company read missed"
            q['items'].sort(key=lambda x: x['date'], reverse=True)
        print(f'second reader: {a.xcheck_days} days of whole-market feeds re-read, {misses} filing(s) the per-company reads missed'
              + (f'; feed problems: {"; ".join(xprob)[:300]}' if xprob else ''))
    if a.selftest:
        # Known filings each fix was made for. A run that cannot find them is not a complete read.
        cases = [('VIVIANA', lambda x: x['category'] == 'Board meeting' and x['date'] == '2026-10-06',
                  'Viviana 06-Oct-2026 board-meeting intimation (NSE board-meetings feed)'),
                 ('OMPOWER', None, 'Om Power: both BSE order letters of 06-Oct-2026 kept as two filings')]
        bad = 0
        for sym, test, label in cases:
            q = next((q for q in out if q['id'].endswith('-' + sym)), None)
            if q is None:
                print('SELFTEST SKIP -', label, '(idea not in this run)'); continue
            if test:
                ok = any(test(x) for x in q['items'])
            else:
                ok = sum(1 for x in q['items'] if x['exchange'] == 'BSE' and x['date'] == '2026-10-06' and not x.get('found_by')) >= 2
            print('SELFTEST', 'PASS' if ok else 'FAIL', '-', label)
            bad += not ok
        if bad:
            sys.exit(2)
    if NSE_CALLS['ok'] == 0 and NSE_CALLS['fail'] > 0:
        # NSE unreachable from this host (the research sandbox has blocked it before): a queue built without NSE would
        # silently drop every NSE-only filing. Keep the committed queue (ideas-feeds.yml builds it from Actions).
        print(f"NSE UNREACHABLE: {NSE_CALLS['fail']} NSE requests failed, none answered - docs/ideas/updates_queue.json "
              "LEFT UNCHANGED. Use the committed queue and check its `built` stamp is today.")
        sys.exit(3)
    json.dump(dict(built=ist.stamp(), second_reader_days=a.xcheck_days, second_reader_misses=misses,
                   second_reader_problems=(xprob if a.xcheck_days > 0 else []), ideas=out),
              open(os.path.join(DOCS, 'updates_queue.json'), 'w'), indent=1, ensure_ascii=False)
    print(f'wrote docs/ideas/updates_queue.json for {len(out)} ideas')


if __name__ == '__main__':
    main()
