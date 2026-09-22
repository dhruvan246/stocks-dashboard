"""Indian domestic commodity prices, daily, from public pages that publish them — the ones the newspapers quote.

Sources (each isolated: a failure is reported and the previous rows for that source are kept):
  metalbook   MetalBook home-page ticker: Indian steel and non-ferrous prices by city in Rs/kg with the day's %
              change — TMT, HRC, CRC, wire rod, channel, ingot, zinc, aluminium... (server-rendered, ~230 rows)
  ibja        India Bullion & Jewellers Association: gold 999/995/916/750/585 (Rs per 10 g), silver 999 (Rs per kg),
              platinum, AM and PM fixes. The official Indian bullion benchmark. Site has a broken certificate chain.
  rubber      Rubber Board of India: RSS4, RSS5, ISNR20, Latex 60% at Kottayam, Kochi, Agartala (Rs per 100 kg + US$)
  sugar       Chinimandi sugar spot rates by city and grade (S/30, M/30), Rs per quintal, with the day's change
  fuel        PPAC (Ministry of Petroleum): daily petrol and diesel in the four metros from the posted PDF
  te          Trading Economics: ~100 global commodities (HRC steel, iron ore, coking coal, steel scrap, PVC, PE, PP,
              soda ash, methanol, urea, DAP, sulphur, titanium, lithium, kraft pulp, freight index...) from each
              page's summary line: value, unit, date, day / month / year change. Their series are futures and CFD
              proxies (their 'steel' is Shanghai rebar in CNY), so they show direction, not the Mumbai print.

Usage: python3 scripts/ideas/india_spot.py [--te-only | --no-te]
Writes docs/ideas/india_spot.json and appends docs/ideas/india_spot_history.csv (one row per series per day, so
weekly and monthly changes accumulate for the sources that publish no history).
"""
import argparse, csv, datetime, html, json, os, re, ssl, sys, time, urllib.request, concurrent.futures as cf

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
OUT = os.path.join(DOCS, 'india_spot.json')
HIST = os.path.join(DOCS, 'india_spot_history.csv')
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36'
HDR = {'User-Agent': UA, 'Accept': 'text/html,application/json,*/*', 'Accept-Language': 'en-IN,en;q=0.9'}
LAX = ssl.create_default_context(); LAX.check_hostname = False; LAX.verify_mode = ssl.CERT_NONE

TE_SLUGS = ['hrc-steel', 'steel', 'scrap-steel', 'iron-ore', 'coking-coal', 'coal', 'manganese', 'silicon', 'magnesium',
            'cobalt', 'lithium', 'molybden', 'neodymium', 'uranium', 'titanium', 'rhodium', 'aluminum', 'copper', 'zinc',
            'lead', 'nickel', 'tin', 'gold', 'silver', 'platinum', 'palladium',
            'urea', 'di-ammonium', 'phosphorus', 'sulfur', 'soda-ash', 'methanol', 'styrene', 'polyethylene', 'polypropylene',
            'polyvinyl', 'naphtha', 'ethanol', 'bitumen', 'kraft-pulp', 'rubber', 'synthetic-rubber',
            'brent-crude-oil', 'crude-oil', 'natural-gas', 'eu-natural-gas', 'liquefied-natural-gas-japan-korea', 'propane',
            'cotton', 'sugar', 'coffee', 'cocoa', 'tea', 'wheat', 'rice', 'corn', 'soybeans', 'palm-oil', 'rapeseed-oil',
            'sunflower-oil', 'milk', 'wool', 'containerized-freight-index', 'solar', 'carbon']


def get(url, timeout=45, ctx=None, binary=False, retries=2):
    last = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HDR), timeout=timeout, context=ctx) as f:
                b = f.read()
                return b if binary else b.decode('utf-8', 'ignore')
        except Exception as e:
            last = e
            time.sleep(1 + i)
    raise RuntimeError(f'{url}: {last}')


def plain(txt):
    p = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', txt, flags=re.S)
    p = re.sub(r'<[^>]+>', ' ', p)
    return re.sub(r'\s+', ' ', html.unescape(p))


def num(s):
    try:
        return float(str(s).replace(',', ''))
    except Exception:
        return None


# ---------------------------------------------------------------- sources
def src_metalbook():
    h = get('https://www.metalbook.com/')
    # the ticker repeats one block per item: <svg map-pin>...</svg>City</span> ... font-medium ">PRODUCT<!-- --> <!-- -->SPEC</span>
    # <span ...>51.1 / kg</span> ... <span class="truncate">+<!-- -->Rs<!-- -->1.19<!-- -->% ...   (marquee duplicates each item)
    rows, seen = [], set()
    for chunk in h.split('lucide-map-pin')[1:]:
        chunk = chunk[:2500]
        city = re.search(r'</svg>\s*([A-Za-z .]+?)\s*</span>', chunk)
        prod = re.search(r'font-medium\s*">(.*?)</span>', chunk, re.S)
        px = re.search(r'>\s*([\d.,]+)\s*/\s*(kg|MT|ton|tonne|Kg|KG)\s*<', chunk)
        chg = re.search(r'truncate">\s*([+-])(?:<!--.*?-->)?\s*₹?(?:<!--.*?-->)?\s*([\d.]+)', chunk, re.S)
        if not (city and prod and px):
            continue
        name = re.sub(r'<!--.*?-->', '', prod.group(1))
        name = re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', '', name))).strip()
        key = (city.group(1).strip(), name)
        if key in seen:
            continue
        seen.add(key)
        c = num(chg.group(2)) if chg else None
        if c is not None and chg.group(1) == '-':
            c = -c
        rows.append(dict(city=city.group(1).strip(), name=name, price=num(px.group(1)), unit='Rs/' + px.group(2).lower(), chg_1d=c))
    if len(rows) < 20:
        raise RuntimeError(f'metalbook ticker parsed only {len(rows)} rows')
    return dict(source='MetalBook (metalbook.com) home-page ticker', url='https://www.metalbook.com/', rows=rows)


def src_ibja():
    h = get('https://ibjarates.com/', ctx=LAX)
    rows = []
    for k, label, unit in (('Gold999', 'gold 999', 'Rs/10g'), ('Gold995', 'gold 995', 'Rs/10g'), ('Gold916', 'gold 916 (22k)', 'Rs/10g'),
                           ('Gold750', 'gold 750 (18k)', 'Rs/10g'), ('Gold585', 'gold 585 (14k)', 'Rs/10g'),
                           ('Silver999', 'silver 999', 'Rs/kg'), ('Platinum999', 'platinum 999', 'Rs/10g')):
        am = re.search(r'id="lbl%s_AM"[^>]*>\s*([\d.,]+)' % k, h)
        pm = re.search(r'id="lbl%s_PM"[^>]*>\s*([\d.,]+)' % k, h)
        if am or pm:
            rows.append(dict(name=label, am=num(am.group(1)) if am else None, pm=num(pm.group(1)) if pm else None,
                             price=num((pm or am).group(1)), unit=unit))
    dm = re.search(r'(\d{2}/\d{2}/20\d\d)', plain(h))
    if len(rows) < 5:
        raise RuntimeError('ibja parsed too few rows')
    return dict(source='IBJA (ibjarates.com) daily AM/PM fix', url='https://ibjarates.com/', date=dm.group(1) if dm else None, rows=rows)


def src_rubber():
    h = get('https://rubberboard.gov.in/public', ctx=LAX)
    rows = []
    for loc, name in (('loc1', 'Kottayam'), ('loc2', 'Kochi'), ('loc3', 'Agartala')):
        m = re.search(r'id="%s".*?<table.*?</table>' % loc, h, re.S)
        if not m:
            continue
        for grade, inr, usd in re.findall(r'(RSS4|RSS5|ISNR20|Latex\(60%\))</i></td>.*?<i[^>]*>([\d.]+)</i></td>.*?<i[^>]*>([\d.]+)</i>', m.group(0), re.S):
            rows.append(dict(market=name, name=grade, price=num(inr), unit='Rs/100kg', usd_per_100kg=num(usd)))
    p = plain(h)
    dm = re.search(r'(\d{2}-\d{2}-20\d\d)', p)
    if len(rows) < 3:
        raise RuntimeError('rubber board parsed too few rows')
    return dict(source='Rubber Board of India (rubberboard.gov.in)', url='https://rubberboard.gov.in/public', date=dm.group(1) if dm else None, rows=rows)


def src_sugar():
    h = get('https://www.chinimandi.com/')
    p = plain(h)
    m = re.search(r'Sugar Spot Rates\s+(\d{2}/\d{2}/20\d\d\s+[\d.]+\s*[AP]M)', p)
    stamp = m.group(1) if m else None
    seg = p[m.start():m.start() + 3000] if m else p
    rows = []
    for city, grade, rate, chg in re.findall(r'\b(Delhi|Kanpur|Kolhapur|Kolkata|Muzaffarnagar|Ahmedabad|Bengaluru|Chennai|Mumbai|Hyderabad|Pune|Nagpur|Indore|Jaipur|Lucknow|Patna)\s+([SM]/\d+)\s+([\d,]+\.\d+)\s+(-?[\d,]+\.\d+)', seg):
        rows.append(dict(city=city, grade=grade, price=num(rate), unit='Rs/quintal', chg_1d=num(chg)))
    if len(rows) < 4:
        raise RuntimeError('chinimandi parsed too few rows')
    return dict(source='Chinimandi sugar spot rates', url='https://www.chinimandi.com/', date=stamp, rows=rows)


def src_fuel():
    import fitz
    h = get('https://ppac.gov.in/')
    links = re.findall(r'href="(https://ppac\.gov\.in/download\.php\?file=importantnews/\d+_PP_9_a_DailyPriceMSHSD_Metro_[\d.]+\.pdf)"', h)
    if not links:
        raise RuntimeError('no PPAC daily price PDF link on the home page')
    b = get(links[0], binary=True)
    d = fitz.open(stream=b, filetype='pdf')
    t = re.sub(r'\s+', ' ', d[0].get_text())
    m = re.search(r'Posted:\s*(\d{2}-\w{3}-\d{2}).*?(\d{2}-\w{3}-\d{2})\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\d{2}-\w{3}-\d{2})\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)', t)
    if not m:
        raise RuntimeError('PPAC PDF layout not recognised: ' + t[:120])
    rows = []
    for fuel, vals in (('petrol', m.groups()[2:6]), ('diesel', m.groups()[7:11])):
        for city, v in zip(('Delhi', 'Mumbai', 'Chennai', 'Kolkata'), vals):
            rows.append(dict(name=fuel, city=city, price=num(v), unit='Rs/litre'))
    return dict(source='PPAC, Ministry of Petroleum (ppac.gov.in) daily metro prices', url=links[0], date=m.group(2), rows=rows)


def te_one(slug):
    h = get(f'https://tradingeconomics.com/commodity/{slug}', timeout=35, retries=2)
    d = re.search(r'name="description" content="([^"]{0,400})', h)
    s = html.unescape(d.group(1)) if d else ''
    m = re.search(r'^(.*?) (?:rose|fell|traded flat|increased|decreased|climbed|dropped|was unchanged|edged (?:up|down)|remained flat)(?: to| at)? ([\d,.]+) ([A-Za-z$€]+(?:\s*Cents)?(?:\s*/\s*\d*\s*[A-Za-z]+)?(?:\s*oz\.?|\.oz)?) on (\w+ \d{1,2}, 20\d\d)', s)
    if not m:
        raise RuntimeError('unparsed: ' + s[:90])
    chg1d = re.search(r'(up|down) ([\d.]+)% from the previous day', s)
    chg1m = re.search(r'past month.*?(risen|fallen) ([\d.]+)%', s)
    # two phrasings: "is up 34.87% compared to the same time last year" / "is still 7.56% lower than a year ago"
    y1 = re.search(r'\b(up|down) ([\d.]+)% compared to the same time last year', s)
    y2 = re.search(r'([\d.]+)% (higher|lower) than a year ago', s)
    chg1y = (y1.group(1), y1.group(2)) if y1 else ((y2.group(2), y2.group(1)) if y2 else None)
    def sgn(mm, negs=('down', 'fallen', 'lower')):
        if not mm:
            return None
        g = mm if isinstance(mm, tuple) else (mm.group(1), mm.group(2))
        v = num(g[1])
        return -v if (v is not None and g[0] in negs) else v
    return dict(slug=slug, name=m.group(1).strip(), price=num(m.group(2)), unit=m.group(3), date=m.group(4),
                chg_1d=sgn(chg1d), chg_1m=sgn(chg1m), chg_1y=sgn(chg1y), summary=s[:220])


def src_te(slugs):
    rows, errs = [], []
    with cf.ThreadPoolExecutor(6) as ex:
        for slug, fut in [(s, ex.submit(te_one, s)) for s in slugs]:
            try:
                rows.append(fut.result())
            except Exception as e:
                errs.append(f'{slug}: {str(e)[:60]}')
    if len(rows) < 10:
        raise RuntimeError(f'trading economics parsed only {len(rows)} ({errs[:3]})')
    return dict(source='Trading Economics (tradingeconomics.com) commodity pages', url='https://tradingeconomics.com/commodities',
                rows=rows, errors=errs)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--no-te', action='store_true')
    ap.add_argument('--te-only', action='store_true')
    a = ap.parse_args()
    old = {}
    if os.path.exists(OUT):
        try:
            old = json.load(open(OUT)).get('sources') or {}
        except Exception:
            old = {}
    plan = [] if a.te_only else [('metalbook', src_metalbook), ('ibja', src_ibja), ('rubber', src_rubber), ('sugar', src_sugar), ('fuel', src_fuel)]
    if not a.no_te:
        plan.append(('te', lambda: src_te(TE_SLUGS)))
    sources, status = dict(old), {}
    for key in old:                                   # rows carried over from the previous file, not refetched this run
        if key not in [k for k, _ in plan]:
            status[key] = f'kept from previous run ({old[key].get("fetched", "?")}), {len(old[key].get("rows", []))} rows'
    for key, fn in plan:
        try:
            res = fn()
            res['fetched'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M IST')
            sources[key] = res
            status[key] = f'ok, {len(res["rows"])} rows'
        except Exception as e:
            status[key] = f'FAILED ({str(e)[:90]}) - previous rows kept' if key in old else f'FAILED ({str(e)[:90]})'
    if not sources:
        raise SystemExit('nothing fetched and nothing to keep: ' + json.dumps(status))
    # history: one row per series per day, so 1w/1m changes can be computed later for sources with no history of their own
    today = datetime.date.today().isoformat()
    seen = set()
    if os.path.exists(HIST):
        for r in csv.DictReader(open(HIST)):
            seen.add((r['date'], r['source'], r['series']))
    new = 0
    with open(HIST, 'a', newline='') as f:
        w = csv.writer(f)
        if os.path.getsize(HIST) == 0:
            w.writerow(['date', 'source', 'series', 'price', 'unit'])
        for key, res in sources.items():
            if not status.get(key, '').startswith('ok'):
                continue
            for r in res['rows']:
                series = ' | '.join(str(r.get(k)) for k in ('city', 'market', 'name', 'grade', 'slug') if r.get(k))
                if r.get('price') is None or (today, key, series) in seen:
                    continue
                w.writerow([today, key, series, r['price'], r.get('unit', '')]); new += 1
    out = dict(built=datetime.datetime.now().strftime('%Y-%m-%d %H:%M IST'), status=status, sources=sources)
    json.dump(out, open(OUT, 'w'), indent=1, ensure_ascii=False)
    print('india_spot:', json.dumps(status, indent=1)); print(f'history rows appended: {new}')


if __name__ == '__main__':
    main()
