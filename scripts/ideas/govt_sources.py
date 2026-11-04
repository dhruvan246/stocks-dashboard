"""Government notices that never reach PIB: the Gazette, DGFT, RBI, CERC and more, read straight from each site.

PIB (govt.py) carries Cabinet decisions and ministry press releases. The legal instruments that change an order
book are published elsewhere and usually earlier or only there: a customs-duty or anti-dumping notification or a
quality control order goes into the Gazette, an import/export policy change is a DGFT notification, a tariff
order naming the company is CERC's. This reader lists every dated item from those sites, tags the ones whose
TITLE names a mapped sector (theme_map.json keywords, the same title-only rule as govt.py), and for CERC links
the petitioner to its listed symbol by exact company name. Nothing is guessed from a body.

  source   page read                                                  rows per run
  gazette  egazette.gov.in RecentUploads.aspx Category 1-5, 5 pages   ~500 (about a month)
  dgft     dgft.gov.in/CP/?opt= notification, public-notice, trade-notice  ~400
  rbi      rbi.org.in/pressreleases_rss.xml                           ~10
  cerc     cercind.gov.in/recent_orders.html                          ~550

Not read here, measured 2026-11-04: CPPP's home page shows a handful of random small tenders with no value and
GeM needs a login for its bid list, so neither is a list of decisions; BIS publishes no dated QCO list (its QCO
page is a product table) and the QCOs themselves are notified in the Gazette, which is read.

The Gazette and DEA servers leave the Let's Encrypt intermediates out of their handshake, so a strict client
fails ("unable to get local issuer certificate"). certs/letsencrypt_yr.pem holds those two certificates (checked
against ISRG Root X1 on 2026-11-04); verification stays on.

Usage: python3 scripts/ideas/govt_sources.py [--keep-days 45]
Writes docs/ideas/govt_sources.json  {built, sources:{src:{ok,rows,error}}, items:{id:{src,date,title,...}}}
Items persist for --keep-days, so a source that fails one run keeps what earlier runs read.
"""
import argparse, datetime, hashlib, html, http.cookiejar, json, os, re, ssl, sys, time, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ist
from govt import UA, MANDATE, amount_hit
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
OUT = os.path.join(DOCS, 'govt_sources.json')
LE_CHAIN = os.path.join(HERE, 'certs', 'letsencrypt_yr.pem')


def tls():
    # certifi when installed: this Mac's /etc/ssl/cert.pem lacks GlobalSign Root R46, which some sites chain to.
    # Plus the two Let's Encrypt intermediates that egazette.gov.in and dea.gov.in leave out of their handshake.
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    ctx.load_verify_locations(cafile=LE_CHAIN)
    return ctx


def opener(ctx=None):
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
                                       urllib.request.HTTPSHandler(context=ctx or tls()))


def fetch(op, url, data=None, referer=None, retries=3):
    last = None
    for i in range(retries):
        try:
            h = {'User-Agent': UA, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                 'Accept-Language': 'en-IN,en;q=0.9'}   # DGFT answered a bare Accept: */* with 403 from a GitHub runner (2026-11-05)
            if referer:
                h['Referer'] = referer
            if data is not None:
                h['Content-Type'] = 'application/x-www-form-urlencoded'
                data = data if isinstance(data, bytes) else urllib.parse.urlencode(data).encode()
            r = op.open(urllib.request.Request(url, data=data, headers=h), timeout=60)
            return r.geturl(), r.read().decode('utf-8', 'ignore')
        except Exception as e:
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f'GET failed {url}: {last}')


def clean(s):
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', s or ''))).strip()


def ymd(s, fmt):
    try:
        return datetime.datetime.strptime(s.strip(), fmt).strftime('%Y-%m-%d')
    except Exception:
        return None


def hidden(s):
    return {m.group(1): html.unescape(m.group(2))
            for m in re.finditer(r'<input type="hidden" name="([^"]+)" id="[^"]*" value="([^"]*)"', s)}


def gazette(pages=5):
    op = opener()
    home, _ = fetch(op, 'https://egazette.gov.in/')          # the session id lives in the path: /(S(...))/
    base = home.rsplit('/', 1)[0]
    out = []
    for cat in range(1, 6):
        u = f'{base}/RecentUploads.aspx?Category={cat}'
        _, s = fetch(op, u, referer=home)
        for p in range(2, pages + 2):
            f = lambda k: {m.group(1): clean(m.group(2)) for m in re.finditer(r'id="gvGazetteList_lbl_' + k + r'_(\d+)"[^>]*>(.*?)</span>', s, re.S)}
            sub, mi, dt, ug, iss, part = f('Subject'), f('Ministry'), f('PublishDate'), f('UGID'), f('IssueDate'), f('PartSection')
            for i in sub:
                if ug.get(i):
                    out.append(dict(src='gazette', id=ug[i], date=ymd(dt.get(i, ''), '%d-%b-%Y'), title=sub[i],
                                    ministry=mi.get(i), issued=ymd(iss.get(i, ''), '%d-%b-%Y'), part=part.get(i),
                                    url='https://egazette.gov.in/'))
            if f"Page${p}" not in s:
                break
            h = hidden(s)
            h.update(__EVENTTARGET='gvGazetteList', __EVENTARGUMENT=f'Page${p}')
            try:
                _, s = fetch(op, u, data=h, referer=u, retries=2)
            except RuntimeError:
                break                      # the grid 500s past its last page
    return out


def dgft():
    op, out = opener(), []
    for opt in ('notification', 'public-notice', 'trade-notice'):
        _, s = fetch(op, f'https://www.dgft.gov.in/CP/?opt={opt}')
        for r in re.findall(r'<tr[^>]*>(.*?)</tr>', s, re.S):
            td = re.findall(r'<td[^>]*>(.*?)</td>', r, re.S)
            if len(td) < 6:
                continue
            no, title, d = clean(td[1]), clean(td[3]), ymd(clean(td[4]), '%d/%m/%Y')
            href = re.search(r'href="([^"]+)"', r)
            if d and title:
                out.append(dict(src='dgft', id=f'{opt}:{no}', date=d, title=title, ministry=f'DGFT {opt.replace("-", " ")} {no}',
                                url=href.group(1).replace(' ', '%20') if href else f'https://www.dgft.gov.in/CP/?opt={opt}'))
    return out


def rbi():
    _, s = fetch(opener(), 'https://www.rbi.org.in/pressreleases_rss.xml')
    out = []
    for it in re.findall(r'<item>(.*?)</item>', s, re.S):
        t = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', it, re.S)
        l = re.search(r'<link>(.*?)</link>', it, re.S)
        d = re.search(r'<pubDate>(.*?)</pubDate>', it, re.S)
        date = None
        if d:
            m = re.search(r'(\d{1,2}) (\w{3}) (\d{4})', d.group(1))
            date = ymd(' '.join(m.groups()), '%d %b %Y') if m else None
        if t:
            out.append(dict(src='rbi', id=clean(l.group(1)) if l else clean(t.group(1)), date=date, title=clean(t.group(1)),
                            ministry='RBI', url=clean(l.group(1)) if l else 'https://www.rbi.org.in/'))
    return out


def cerc():
    _, s = fetch(opener(), 'https://cercind.gov.in/recent_orders.html')
    out = []
    for r in re.findall(r'<tr[^>]*>(.*?)</tr>', s, re.S | re.I):
        raw = re.findall(r'<td[^>]*>(.*?)</td>', r, re.S | re.I)
        td = [clean(x) for x in raw]
        if len(td) < 6 or not re.fullmatch(r'\d{2}\.\d{2}\.\d{4}', td[3]):
            continue
        a = re.search(r'href="([^"]+\.pdf)"', raw[2], re.I)
        pet = re.findall(r'<strong>(.*?)</strong>', raw[2], re.S | re.I)      # the petitioner is the bold tail of the subject cell
        title = clean(re.sub(r'<strong>.*?</strong>', ' ', raw[2], flags=re.S | re.I))
        out.append(dict(src='cerc', id=td[1], date=ymd(td[3], '%d.%m.%Y'), title=title[:500],
                        petitioner=clean(pet[-1]) if pet else None, category=td[5] or None, ministry=f'CERC {td[1]}',
                        url='https://cercind.gov.in/' + a.group(1) if a else 'https://cercind.gov.in/recent_orders.html'))
    return out


# Five ministries moved to one Next.js + WordPress platform (digifootprint) whose pages render empty without
# JavaScript; the page itself reads /cms/wp-json/document/documents, which answers plain GETs. Measured
# 2026-11-04: MoD's own lists are empty (it publishes through PIB); Power, DPIIT, MNRE and MeitY carry orders,
# notices, gazette copies and policies. The bare power host 302s to www.
WP_SITES = {'www.mod.gov.in': 'Ministry of Defence', 'www.powermin.gov.in': 'Ministry of Power', 'dpiit.gov.in': 'DPIIT',
            'mnre.gov.in': 'MNRE', 'www.meity.gov.in': 'MeitY'}
WP_CATS = ('press-release', 'orders-and-notices', 'gazette-notifications', 'acts-and-policy', 'policies-and-regulations',
           'guidelines', 'office-memorandum')


def ministries():
    op, out, errs = opener(), [], []
    for host, name in WP_SITES.items():
        for cat in WP_CATS:
            u = f'https://{host}/cms/wp-json/document/documents?document_category={cat}&limit=20&page=1&sort=acf&order=DESC&search='
            try:
                _, t = fetch(op, u, retries=2)
                d = json.loads(t)
            except Exception as e:
                errs.append(f'{host}/{cat}: {str(e)[:60]}')
                continue
            for p in (d.get('posts') or []) if isinstance(d, dict) else []:
                acf = p.get('acf_data') or {}
                date = ymd(acf.get('date') or '', '%d/%m/%Y') or (p.get('post_date') or '')[:10] or None
                title = clean(p.get('post_title') or acf.get('title'))
                if title and date:
                    out.append(dict(src='ministries', id=f"{host}:{p.get('ID')}", date=date, title=title,
                                    ministry=f'{name} · {cat.replace("-", " ")}', url=f'https://{host}/documents/{cat}'))
    if not out and errs:
        raise RuntimeError('; '.join(errs[:4]))
    return out


def dgtr():
    """Trade remedies: anti-dumping, countervailing and safeguard initiations and findings (home page 'What's new')."""
    _, s = fetch(opener(), 'https://www.dgtr.gov.in/en')   # the bare host 301s to http://, which times out from Python
    out = []
    for d, u, t in re.findall(r'<div class="notifi_list_inner">\s*<span>([^<]+)</span>\s*<span>\s*<a href="([^"]+)"[^>]*>(.*?)</a>', s, re.S):
        title, date = clean(t), ymd(clean(d), '%d %b %Y')
        if title and date:
            out.append(dict(src='dgtr', id=u, date=date, title=title, ministry='DGTR (trade remedies)',
                            url=u if u.startswith('http') else 'https://www.dgtr.gov.in' + u))
    return out


def coal():
    _, s = fetch(opener(), 'https://coal.gov.in/')
    out = []
    for u, t, d in re.findall(r'<div class="list-content"><a href="([^"]+)"[^>]*>(.*?)</a>[^<(]*\((\d{2}/\d{2}/\d{4})\)', s, re.S):
        out.append(dict(src='coal', id=u, date=ymd(d, '%d/%m/%Y'), title=clean(t), ministry='Ministry of Coal',
                        url=u if u.startswith('http') else 'https://coal.gov.in' + u))
    return out


def mopng():
    _, s = fetch(opener(), 'https://mopng.gov.in/en')
    out = []
    for d, u, t in re.findall(r'<span>Dt\.\s*([0-9-]+)\s*</span>\s*<p><a href="([^"]+)"[^>]*>(.*?)</a>', s, re.S):
        out.append(dict(src='mopng', id=u, date=ymd(d, '%d-%m-%Y'), title=clean(t), ministry='Ministry of Petroleum and Natural Gas',
                        url=u if u.startswith('http') else 'https://mopng.gov.in' + u))
    return out


def dea():
    _, s = fetch(opener(), 'https://dea.gov.in/')
    out = []
    for d, u, t in re.findall(r'<div class="circularDate[^"]*">.*?(\d{2}/\d{2}/\d{4})\s*</div>\s*<div class="circularBox[^"]*">\s*<a href="\s*([^"]+)"[^>]*>(.*?)</a>', s, re.S):
        u = u.strip()
        out.append(dict(src='dea', id=u, date=ymd(d, '%d/%m/%Y'), title=clean(t), ministry='Department of Economic Affairs',
                        url=u if u.startswith('http') else 'https://dea.gov.in' + u))
    return out


# What makes a notice worth a look: it changes a trade, tax or market rule, or records a project awarded by bidding.
# Measured on the 2026-11-04 read (1,607 rows): sector words alone tagged 149 railway land-acquisition gazettes,
# every TRAI release ("Telecom") and 86 routine CERC transmission true-ups, none of which moves an order book.
RULE = re.compile(r'\b(import policy|export policy|minimum import price|\bmip\b|anti[- ]?dumping|anti[- ]subsidy|countervailing|safeguard|'
                  r'customs (?:tariff|duty)|basic customs duty|quality control order|\bqco\b|production linked|\bpli\b|rodtep|'
                  r'duty drawback|prohibit(?:ed|ion)?|restrict(?:ed|ion)|embargo|free trade agreement|'
                  r'adoption of tariff|competitive bidding|auction|spectrum|mmdr|critical mineral|'
                  r'amendment\)? regulations|draft (?:rules|regulations|guidelines|policy)|suo[- ]?motu|repo rate|monetary policy|'
                  r'cash reserve ratio|\bcrr\b|\bslr\b|priority sector|foreign exchange management|fdi|\bgst\b|cgst)', re.I)
ROUTINE = re.compile(r'\b(land schedule|acquisition of land|right of user|pmp act|section 3\(1\)|section 6\(1\)|section 20[ae]|'
                     r'section 3[ad]\b|3d\b|bye[- ]election|recruitment rules|competent authority|corrigendum|'
                     r'variable rate (?:reverse )?repo|vrr|money market operations|lending facility|auction of (?:government|state)|'
                     r'treasury bills?|state government securities|t-bills?|g-sec|transmission tariff for|truing[- ]up|'
                     r'interview|young professional|independent consultants?|liaison officer)\b', re.I)


SOURCES = dict(gazette=gazette, dgft=dgft, rbi=rbi, cerc=cerc, dgtr=dgtr, ministries=ministries, coal=coal,
               mopng=mopng, dea=dea)


def norm_name(n):
    n = re.sub(r'[^a-z0-9 ]', ' ', (n or '').lower().replace('&', ' and '))
    n = re.sub(r'\b(the|limited|ltd|pvt|private)\b', ' ', n)
    return re.sub(r'\s+', ' ', n).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--keep-days', type=int, default=45)
    ap.add_argument('--only', help='comma list of sources (testing)')
    a = ap.parse_args()
    tm = json.load(open(os.path.join(DOCS, 'theme_map.json'), encoding='utf-8'))
    themes = {k: re.compile(r'\b(?:' + v['keywords'] + ')', re.I) for k, v in tm['themes'].items()}
    idx = json.load(open(os.path.join(HERE, '..', '..', 'docs', 'search_index.json'), encoding='utf-8'))
    names = {}
    for row in idx.get('s', []):
        names.setdefault(norm_name(row[1]), row)

    old = {}
    if os.path.exists(OUT):
        try:
            old = json.load(open(OUT, encoding='utf-8'))
        except Exception:
            old = {}
    items = {k: v for k, v in old.get('items', {}).items() if v.get('src') in SOURCES}   # a dropped source leaves no rows (TRAI, user 2026-11-04)
    status = {}
    for src, fn in SOURCES.items():
        if a.only and src not in a.only.split(','):
            status[src] = old.get('sources', {}).get(src, {'ok': False, 'error': 'not run'})
            continue
        try:
            rows = fn()
            status[src] = dict(ok=True, rows=len(rows), read_at=ist.stamp())
        except Exception as e:
            rows = []
            prev = old.get('sources', {}).get(src, {})
            status[src] = dict(ok=False, rows=0, error=str(e)[:300], last_ok=prev.get('read_at') if prev.get('ok') else prev.get('last_ok'))
            print(f'{src}: FAILED {e}', file=sys.stderr)
        for r in rows:
            if r.get('date'):
                key = hashlib.sha1(f"{r['src']}|{r['id']}|{r['title']}".encode()).hexdigest()[:16]
                r['first_seen'] = items.get(key, {}).get('first_seen') or ist.today().isoformat()
                items[key] = r
        print(f'{src}: {len(rows)} rows, {sum(1 for r in rows if r.get("date"))} dated')

    # classify every stored item, not only today's rows, so a change to these rules re-reads the whole history
    for r in items.values():
        r['themes'] = [k for k, rx in themes.items() if rx.search(r['title'])]
        r['theme_names'] = [tm['themes'][k]['name'] for k in r['themes']]
        r['title'] = r['title'][:400]
        hit = MANDATE.search(r['title']) or RULE.search(r['title'])
        r['rule'] = hit.group(0) if hit and not ROUTINE.search(r['title']) else None
        r['amount_cr'], r['amount_text'] = amount_hit(r['title'])
        if r.get('petitioner'):
            m = names.get(norm_name(r['petitioner']))
            r['company'] = dict(symbol=m[0], name=m[1], mcap_cr=m[3]) if m else None
        r['flag'] = bool(r['rule'])
    cut = (ist.today() - datetime.timedelta(days=a.keep_days)).isoformat()
    items = {k: v for k, v in items.items() if (v.get('date') or '') >= cut}
    out = dict(built=ist.stamp(), sources=status, keep_days=a.keep_days,
               source_pages={'gazette': 'https://egazette.gov.in/', 'dgft': 'https://www.dgft.gov.in/CP/?opt=notification',
                             'rbi': 'https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx',
                             'cerc': 'https://cercind.gov.in/recent_orders.html', 'dgtr': 'https://www.dgtr.gov.in/',
                             'ministries': 'https://www.powermin.gov.in/documents/orders-and-notices', 'coal': 'https://coal.gov.in/',
                             'mopng': 'https://mopng.gov.in/en', 'dea': 'https://dea.gov.in/'},
               items=dict(sorted(items.items(), key=lambda kv: (kv[1]['date'], kv[0]), reverse=True)))
    tmp = OUT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, OUT)
    print('rule changes:', {k: sum(1 for r in items.values() if r['src'] == k and r['flag']) for k in SOURCES})
    print(f'wrote {OUT}: {len(items)} items, sources ok: {[k for k, v in status.items() if v.get("ok")]}')


if __name__ == '__main__':
    main()
