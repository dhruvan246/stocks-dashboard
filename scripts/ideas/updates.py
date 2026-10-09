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
    return [dict(exchange='BSE', date=(r.get('NEWS_DT') or '')[:10],
                 subject=bse_names.clean_ann_subject(r.get('NEWSSUB'), r.get('SCRIP_CD') or scrip),
                 headline=(r.get('HEADLINE') or '').strip()[:400], category=r.get('CATEGORYNAME') or '',
                 pdf=bse.attachment_url(r)) for r in rows]


_jar = None
def nse_get(url):
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
                dt = r.get('an_dt') or r.get('sort_date') or ''
                try: date = datetime.datetime.strptime(dt[:11].strip(), '%d-%b-%Y').date().isoformat()
                except Exception: date = dt[:10]
                out.append(dict(exchange='NSE', date=date, subject=(r.get('desc') or '').strip()[:200],
                                headline=(r.get('attchmntText') or '').strip()[:400], category=(r.get('desc') or '')[:80],
                                pdf=r.get('attchmntFile') or ''))
        a = b + datetime.timedelta(days=1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', help='override the per-idea start date (YYYY-MM-DD)')
    ap.add_argument('--ids', help='comma-separated idea ids to refresh (default: all)')
    a = ap.parse_args()
    fn = os.path.join(DOCS, 'ideas.json')
    ideas = json.load(open(fn)).get('ideas', [])
    want = set(a.ids.split(',')) if a.ids else None
    today = ist.today()
    out = []
    for it in ideas:
        if want and it['id'] not in want:
            continue
        since = datetime.date.fromisoformat(a.since or it.get('updates_checked') or it['date'])
        problems, items = [], []
        scrip = str(it.get('scrip') or '').strip()
        if re.fullmatch(r'\d{6}', scrip):
            items += bse_items(scrip, since, today, problems)
        if it.get('nse'):
            items += nse_items(it['nse'], bool(it.get('sme')) and not re.fullmatch(r'\d{6}', scrip), since, today, problems)
        # the same filing on both exchanges: keep one copy, BSE first (it carries the category)
        seen, uniq = set(), []
        for x in sorted(items, key=lambda x: (x['exchange'] != 'BSE')):
            key = (x['date'], re.sub(r'\W+', ' ', (x['subject'] or '')[:60]).lower())
            if key in seen: continue
            seen.add(key); uniq.append(x)
        for x in uniq:
            x['routine'] = bool(NOISE.search((x['subject'] or '') + ' ' + (x['headline'] or '')))
        uniq.sort(key=lambda x: x['date'], reverse=True)
        out.append(dict(id=it['id'], name=it.get('name'), ticker=it.get('ticker'), scrip=scrip, nse=it.get('nse') or '',
                        since=since.isoformat(), status='complete' if not problems else 'SUSPECT - ' + '; '.join(problems),
                        n=len(uniq), n_substantive=sum(1 for x in uniq if not x['routine']), items=uniq))
        print(f"{it['id']:28s} since {since}  {len(uniq):3d} filings ({sum(1 for x in uniq if not x['routine'])} substantive)  {out[-1]['status'][:90]}")
    json.dump(dict(built=ist.stamp(), ideas=out), open(os.path.join(DOCS, 'updates_queue.json'), 'w'), indent=1, ensure_ascii=False)
    print(f'wrote docs/ideas/updates_queue.json for {len(out)} ideas')


if __name__ == '__main__':
    main()
