"""Indian domestic commodity prices, daily, from public pages that publish them — the ones the newspapers quote.

Sources (each isolated: a failure is reported and the previous rows for that source are kept):
  metalbook   MetalBook home-page ticker: Indian steel and non-ferrous prices by city in Rs/kg with the day's %
              change — TMT, HRC, CRC, wire rod, channel, ingot, zinc, aluminium... (server-rendered, ~230 rows)
  ibja        India Bullion & Jewellers Association: gold 999/995/916/750/585 (Rs per 10 g), silver 999 (Rs per kg),
              platinum, AM and PM fixes. The official Indian bullion benchmark. Site has a broken certificate chain.
  rubber      Rubber Board of India: RSS4, RSS5, ISNR20, Latex 60% at Kottayam, Kochi, Agartala (Rs per 100 kg + US$)
  sugar       Chinimandi sugar spot rates by city and grade (S/30, M/30), Rs per quintal, with the day's change
  fuel        PPAC (Ministry of Petroleum): daily petrol and diesel in the four metros from the posted PDF
  minsteel    the Ministry of Steel's monthly Mumbai retail TMT / HRC / CRC (Rs/t incl. GST), from the committed
              docs/ideas/minsteel_mumbai.json that minsteel.py builds from its reports (Nov-2021 onward)
  nmdc        NMDC's administered iron-ore price (lump and fines, Rs per tonne), read from the price letters it
              files with BSE. It revises roughly monthly, so this is a dated series, not a daily print, and the
              whole history back to 2015 is kept in docs/ideas/nmdc_history.json.
  te          Trading Economics: ~100 global commodities (HRC steel, iron ore, coking coal, steel scrap, PVC, PE, PP,
              soda ash, methanol, urea, DAP, sulphur, titanium, lithium, kraft pulp, freight index...) from each
              page's summary line: value, unit, date, day / month / year change. Their series are futures and CFD
              proxies (their 'steel' is Shanghai rebar in CNY), so they show direction, not the Mumbai print.

  guidechem   Guidechem GuideTrends: ~80 Chinese domestic chemical quotes in CNY/t (H-acid, vinyl sulphone, phenol,
              acetone, caustic soda, sulphuric acid, aniline, R134a, TiO2, carbon black...). An aggregator of supplier and
              market quotes: direction data, each row dated by the page's own 'Updated' and named by the row it is from.
  cai         Cotton Association of India daily spot rates by growth and grade (Rs per candy of 356 kg).
  spices      Spices Board: small-cardamom e-auction averages, large cardamom, Kochi pepper (Rs/kg), each dated.
  moil        MOIL's monthly manganese-ore price letters filed with BSE: an index chained from the stated % revisions
              (ferro grades Mn>=44% and <44%) and the EMD basic price (Rs/t); history in docs/ideas/moil_history.json.

Usage: python3 scripts/ideas/india_spot.py [--te-only | --no-te | --only guidechem,cai]
Writes docs/ideas/india_spot.json and appends docs/ideas/india_spot_history.csv (one row per series per day, so
weekly and monthly changes accumulate for the sources that publish no history).
"""
import argparse, csv, datetime, gzip, html, json, os, re, ssl, sys, time, urllib.request, concurrent.futures as cf

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)          # the sibling bse.py, for the NMDC filings
import ist                        # IST stamps: a naive now() on a UTC runner was labelled IST (runbook 144e)
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
OUT = os.path.join(DOCS, 'india_spot.json')
HIST = os.path.join(DOCS, 'india_spot_history.csv')
# Older prints recovered from archives (e.g. dated snapshots of a source page). Kept apart from HIST so a
# recorded print and a recovered one never blur: columns date,source,series,price,unit,via - `via` says where
# the price was read. A recorded print outranks a recovered one on the same date.
BACKFILL = os.path.join(DOCS, 'india_backfill.csv.gz')   # gzipped: 6 MB of text, rewritten only by india_backfill.py
# Recorded rows later found wrong are never deleted from HIST: they are listed here with the reason and skipped
# when the history is built. columns date,source,series,price,unit,reason (runbook 144a-v).
RETRACTED = os.path.join(DOCS, 'india_retracted.csv')
# The same idea for the RECOVERED rows (india_backfill.csv): values the source itself got wrong (a one-day decimal
# slip in the Rubber Board's database). Each ledger applies ONLY to its own file: a recorded row and a recovered row
# can share date, series and price, and one ledger for both once hid a correct recovered point (runbook 144a-vii).
BACKFILL_RETRACTED = os.path.join(DOCS, 'india_backfill_retracted.csv')
# What the page charts when a price is clicked: one dated series per print, with its stats.
HIST_JSON = os.path.join(DOCS, 'india_history.json.gz')


MON = {m: i for i, m in enumerate(['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], 1)}


def iso_date(s):
    """A source's own date in any of the forms they print it, as YYYY-MM-DD; None when unreadable.
    21/09/2026 (IBJA) · 22/09/2026 4.30 PM (sugar) · 03-09-2026 (Rubber Board) · 21-Sep-26 / 5-Sep-26 (PPAC) ·
    September 22, 2026 (Trading Economics) · w.e.f. 2026-09-09 (NMDC) · 2026-09-16 (MetalBook)"""
    s = (s or '').strip()
    try:
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})', s)
        if m:
            return datetime.date(int(m[1]), int(m[2]), int(m[3])).isoformat()
        m = re.search(r'(?<!\d)(\d{1,2})[/-](\d{1,2})[/-](\d{4})', s)
        if m:
            return datetime.date(int(m[3]), int(m[2]), int(m[1])).isoformat()
        m = re.search(r'(?<!\d)(\d{1,2})-([A-Za-z]{3})-(\d{2})(?!\d)', s)
        if m and m[2].lower() in MON:
            return datetime.date(2000 + int(m[3]), MON[m[2].lower()], int(m[1])).isoformat()
        m = re.search(r'([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})', s)
        if m and m[1][:3].lower() in MON:
            return datetime.date(int(m[3]), MON[m[1][:3].lower()], int(m[2])).isoformat()
    except ValueError:
        return None
    return None


def in_force(path, ledger_path):
    """The rows of an append-only CSV minus the ones its ledger retracts. A ledger line names a row by (date, source,
    series, price) and removes the EARLIEST row with those values - the file only grows, so the retracted row is
    always the older one. Without that, a correctly re-recorded row that happens to carry the same values as a
    retracted one (IBJA's 22-Sep fix: first stamped wrong, then recorded right) was hidden too."""
    if not os.path.exists(path):
        return []
    opener = (lambda p: gzip.open(p, 'rt', newline='')) if path.endswith('.gz') else (lambda p: open(p, newline=''))
    left = {}
    if os.path.exists(ledger_path):
        for r in csv.DictReader(open(ledger_path)):
            k = (r['date'], r['source'], r['series'], r['price'])
            left[k] = left.get(k, 0) + 1
    out = []
    for r in csv.DictReader(opener(path)):
        k = (r.get('date'), r.get('source'), r.get('series'), r.get('price'))
        if left.get(k):
            left[k] -= 1
            continue
        out.append(r)
    return out


def series_key(r):
    """The one name a print goes by - in the CSV, the history file, signals.json and the page."""
    return ' | '.join(str(r.get(k)) for k in ('city', 'market', 'name', 'grade', 'slug') if r.get(k))
import os as _o, sys as _s; _s.path.insert(0, _o.path.dirname(_o.path.dirname(_o.path.abspath(__file__))))
import bse_headers  # noqa: E402 — BSE hosts (NMDC filings) get the honest header set, replacing UA below (§190)
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
            'sunflower-oil', 'milk', 'wool', 'containerized-freight-index', 'solar', 'carbon',
            # added 2026-10-12 (each measured parsing on that day): minor metals, secondary aluminium, CNY iron ore,
            # refined products and Urals (India's main crude grade), and the farm series TE carries that we lacked
            'cobalt-hydroxide', 'gallium', 'germanium', 'indium', 'tellurium', 'scrap-aluminum', 'aluminum-alloy',
            'iron-ore-cny', 'gasoline', 'heating-oil', 'urals-oil', 'uk-natural-gas', 'canola', 'barley', 'oat',
            'orange-juice', 'cheese', 'butter']


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
    """MetalBook's Indian steel and metal prices, from the price records embedded in its home page.

    NOT the ticker markup: the ticker prints each item as PRODUCT, PRICE, CHANGE, then CITY, and the old parser
    split on the city pin and read the city first - so every product took the NEXT item's city. Checked on
    2026-09-23 against these records, 12 of 41 published rows were right, 12 carried another city's price and
    17 were city/product pairs the site does not list (runbook 144a-v). The records also carry what the ticker
    hid: the date MetalBook priced each item (`price_date`, often days old) and the prices before it.
    Data vendor per the records: SteelMint (BigMint).
    """
    h = get('https://www.metalbook.com/')
    t = h.replace('\\\\"', '\x00').replace('\\"', '"')
    rows, seen = [], set()
    for m in re.findall(r'\{"_id":\{"product_name".*?"created_by":"[^"]*"\}', t):
        try:
            o = json.loads(m.replace('\x00', '\\"'))
            pid = o['_id']
            city = (o.get('location') or pid.get('location') or '').strip()
            name = re.sub(r'\s+', ' ', f"{pid.get('product_name', '')} {pid.get('grade') or ''}").strip()
            per_t = float(o['price_per_ton'])
            when = datetime.datetime.utcfromtimestamp(o['price_date'] / 1000) + datetime.timedelta(hours=5, minutes=30)
        except (ValueError, KeyError, TypeError):
            continue
        if not city or not name or (city, name) in seen:
            continue
        seen.add((city, name))
        prev = [float(x) for x in (o.get('prices') or [])[1:2] if x]
        rows.append(dict(city=city, name=name, price=round(per_t / 1000, 2), unit='Rs/kg', date=when.strftime('%Y-%m-%d'),
                         prev=round(prev[0] / 1000, 2) if prev else None,
                         chg_prev=round(100 * (per_t / prev[0] - 1), 2) if prev else None,
                         grade_class=pid.get('primary_or_secondary'), vendor=o.get('source')))
    if len(rows) < 20:
        raise RuntimeError(f'metalbook price records parsed only {len(rows)} rows')
    return dict(source='MetalBook (metalbook.com) price records, data from SteelMint', url='https://www.metalbook.com/', rows=rows)

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
    # The date of the headline fix is the chart point that carries the same gold 999 PM price. The FIRST date in
    # the page text is the top row of a past-rates table (the previous day), which is what this used to read, so
    # the 22-Sep fix was recorded under 21 September (runbook 144a-v). No match: no date (the run date is used).
    date = None
    g = next((r for r in rows if r['name'] == 'gold 999'), None)
    ch = ibja_chart(h)
    if g and ch.get('gold 999'):
        hits = [d for d, v in ch['gold 999'] if v == g['pm']]
        date = hits[-1] if hits else None
    if len(rows) < 5:
        raise RuntimeError('ibja parsed too few rows')
    return dict(source='IBJA (ibjarates.com) daily AM/PM fix', url='https://ibjarates.com/', date=date, rows=rows)


def ibja_chart(h):
    """IBJA's own chart data on its home page: ~85 trading days of the PM fix for gold 999, gold 916 and silver 999,
    as {series name: [(YYYY-MM-DD, Rs)]}."""
    out = {}
    for hid, series in (('HdnGold', (('purity999', 'gold 999'), ('purity916', 'gold 916 (22k)'))), ('HdnSilver', (('silverRate', 'silver 999'),))):
        m = re.search(r'id="%s"[^>]*value="([^"]*)"' % hid, h)
        if not m:
            continue
        try:
            j = json.loads(html.unescape(m.group(1)))
        except ValueError:
            continue
        labels = [iso_date(x) for x in j.get('labels') or []]
        for k, name in series:
            vals = j.get(k) or []
            if len(vals) == len(labels):
                out[name] = [(d, float(v)) for d, v in zip(labels, vals) if d and v]
    return out


def src_rubber():
    h = get('https://rubberboard.gov.in/public', ctx=LAX)
    rows = []
    for loc, name in (('loc1', 'Kottayam'), ('loc2', 'Kochi'), ('loc3', 'Agartala')):
        m = re.search(r'id="%s".*?<table.*?</table>' % loc, h, re.S)
        if not m:
            continue
        for grade, inr, usd in re.findall(r'(RSS4|RSS5|ISNR20|Latex\(60%\))</i></td>.*?<i[^>]*>([\d.]+)</i></td>.*?<i[^>]*>([\d.]+)</i>', m.group(0), re.S):
            rows.append(dict(market=name, name=grade, price=num(inr), unit='Rs/100kg', usd_per_100kg=num(usd)))
    # The price date is the one labelled right above the tables ('Domestic market on 22-09-2026, per 100 kg').
    # The FIRST date on the page is a news item ('03-09-2026- ...'), which is what this used to read, so the
    # 22-Sep prices were recorded under 3 September (runbook 144a-v).
    i = h.find('id="loc1"')
    ds = re.findall(r'(\d{2}-\d{2}-20\d\d)', plain(h[max(0, i - 3000):i])) if i > 0 else []
    if len(rows) < 3:
        raise RuntimeError('rubber board parsed too few rows')
    return dict(source='Rubber Board of India (rubberboard.gov.in)', url='https://rubberboard.gov.in/public', date=ds[-1] if ds else None, rows=rows)


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
    # PPAC prints days 1-9 without a leading zero ("5-Sep-26"), so the day is one or two digits
    d_ = r'\d{1,2}-\w{3}-\d{2}'
    m = re.search(r'Posted:\s*(' + d_ + r').*?(?<![\d-])(' + d_ + r')\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(' + d_ + r')\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)', t)
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


# ---------------------------------------------------------------- NMDC iron ore (from its own BSE filings)
# NMDC sets an administered price for Indian iron ore and files a letter with BSE every time it revises
# (roughly monthly). That makes it the one Indian steel-chain price with a real, dated, free history: this
# reads those filings back to 2015 into docs/ideas/nmdc_history.json and adds only what it has not read yet.
# The filing's own tax basis CHANGES between eras (2023-24 letters say the price INCLUDES royalty/DMF/NMET,
# the 2026 ones say it excludes them), so the basis of every row is stored beside the number and never
# silently compared across the break.
NMDC_SCRIP = '526371'
NMDC_HIST = os.path.join(DOCS, 'nmdc_history.json')
NMDC_ANN = ('https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d&strCat=-1&strPrevDate=%s'
            '&strScrip=' + NMDC_SCRIP + '&strSearch=P&strToDate=%s&strType=C&subcategory=-1')
NMDC_ATT = 'https://www.bseindia.com/xml-data/corpfiling/AttachLive/%s'
NMDC_PAT = re.compile(r'price[s]?\s+of\s+iron\s+ore', re.I)
# Bump when nmdc_parse/nmdc_wef change: every stored letter read by an older parser is read again ONCE, so a
# parser fix reaches the rows already on file instead of only the next month's letter.
#   2 = four-digit price + 'Baila' grades   3 = w.e.f. nearest the filing, no '20206'   4 = OCR-split basis
NMDC_PARSER = 4
# the letters call the grades 'Lump Ore'/'Fines' up to 2024 and 'Baila Lump'/'Baila Fines' after it
NMDC_GRADES = (('lump', r'(?:Baila\s+)?Lump(?:\s*Ore)?', 'Iron ore lump (65.5%, 10-40mm)'),
               ('fines', r'(?:Baila\s+)?Fines', 'Iron ore fines (64%, -10mm)'))
MONTHS = {}
for _i, _m in enumerate(['January', 'February', 'March', 'April', 'May', 'June',
                         'July', 'August', 'September', 'October', 'November', 'December'], 1):
    MONTHS[_m.lower()] = _i
    MONTHS[_m.lower()[:3]] = _i


def nmdc_wef(text, fallback):
    """The 'with effect from' date the letter names, as (YYYY-MM-DD, note).

    Every w.e.f. date in the subject AND the body is a candidate, and the one nearest the filing date wins.
    Two guards, both from real filings: BSE's subject for the 2026-01-09 letter reads "w.e.f. 09Th January
    20206" (the body says 2026), and taking the first four digits of '20206' filed a 2026 price under
    January 2020 - so a year must not run on into another digit. And a letter announces the price in force
    now, so a candidate more than 60 days from the filing date is rejected; with none left the filing date
    stands and the note says so.
    """
    t = re.sub(r'\s+', ' ', text or '')
    wef = r'w\.?\s*e\.?\s*f\.?\s*:?\s*'
    cands = []
    for m in re.finditer(wef + r'(\d{1,2})\s*[./-]\s*(\d{1,2})\s*[./-]\s*(\d{4}|\d{2})(?!\d)', t, re.I):
        y = int(m.group(3))
        cands.append((y + 2000 if y < 100 else y, int(m.group(2)), int(m.group(1))))
    for m in re.finditer(wef + r'(\d{1,2})\s*(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\.?,?\s+(\d{4})(?!\d)', t, re.I):
        if MONTHS.get(m.group(2).lower()):
            cands.append((int(m.group(3)), MONTHS[m.group(2).lower()], int(m.group(1))))
    for m in re.finditer(wef + r'([A-Za-z]{3,9})\.?\s+(\d{1,2})\s*(?:st|nd|rd|th)?,?\s+(\d{4})(?!\d)', t, re.I):
        if MONTHS.get(m.group(1).lower()):
            cands.append((int(m.group(3)), MONTHS[m.group(1).lower()], int(m.group(2))))
    try:
        f = datetime.date.fromisoformat(fallback)
    except (TypeError, ValueError):
        return fallback, 'filing date unreadable'
    ok = []
    for y, mo, d in cands:
        try:
            ok.append(datetime.date(y, mo, d))
        except ValueError:
            continue
    if not ok:
        return fallback, 'no w.e.f. date in the letter; filing date used'
    best = min(ok, key=lambda x: abs((x - f).days))
    if abs((best - f).days) > 60:
        return fallback, f'w.e.f. {best} is {abs((best - f).days)} days from the filing; filing date used'
    return best.isoformat(), ''


def nmdc_parse(text, subject, filing_date):
    """Lump and fines rupees per tonne out of one price letter, with the tax basis it states."""
    body = re.sub(r'\s+', ' ', text or '')
    out = {}
    for key, rx, _name in NMDC_GRADES:
        # The letters use four shapes for the same line, so anchor on the "per ton" that all of them end
        # with rather than on any one separator:
        #   "Lump Ore (65.53, 6-40mm) @Rs. 2,850/- per ton"      (2019, and the OCR of the scanned ones)
        #   "Lump Ore (65.5%, 10-40mm) @ ₹ 5,400/- per ton."     (2026)
        #   "Baila Lump (65.5%, 10-40 mm) - ₹ 5,450/- Per Ton."  (2026, a dash where the @ used to be)
        #   "Baila Lump (65.5%, 10-40mm) – ₹ 6,100- Per Ton."    (2025, including the filing's own missing slash)
        # The number needs four digits: the OCR of the scans breaks '3,100' into '3, 100', and matching a
        # bare three-digit run there silently produced a price of Rs 100 a tonne.
        m = re.search(rx + r'[^\n]{0,60}?(?:₹|Rs\.?|INR|@)\s*(\d{1,2}[,\s]{0,2}\d{3})\s*/?\s*-?\s*(?:per|PER)\s*(?:ton|mt|wmt)',
                      body, re.I)
        out[key] = float(re.sub(r'[^\d]', '', m.group(1))) if m else None
    basis = None
    # the OCR of the scans splits words ("exc luding Royalty" in the 2020-01-02 letter), so allow one space
    # inside each word rather than miss the basis and leave the row undated in its tax era
    w = lambda word: r'\s?'.join(word)
    if re.search(w('inclusive') + r'\s+of\s+Royalty|' + w('including') + r'\s+Royalty', body, re.I):
        basis = 'includes royalty, DMF and NMET'
    elif re.search(w('exclusive') + r'\s+of\s+Royalty|' + w('excluding') + r'\s+Royalty', body, re.I):
        basis = 'excludes royalty, DMF and NMET'
    note = re.search(r'(Note\s*:.{0,260}?)(?:Please take note|Thanking you|$)', body, re.I)
    wef, wef_note = nmdc_wef(subject + ' ' + body, filing_date)
    return dict(wef=wef, wef_note=wef_note, lump=out['lump'], fines=out['fines'],
                basis=basis, note=re.sub(r'\s+', ' ', note.group(1)).strip()[:240] if note else '')


def nmdc_filings(d_from, d_to):
    """NMDC's 'Prices of Iron Ore w.e.f. ...' filings in a window. BSE serves 50 rows a page whatever
    window is asked for, so callers walk the range in short windows rather than trusting one call."""
    import bse
    out = {}
    for page in (1, 2):
        rows = json.loads(bse._get(NMDC_ANN % (page, d_from.strftime('%Y%m%d'), d_to.strftime('%Y%m%d')),
                                   sleep=0.4)).get('Table') or []
        for r in rows:
            if NMDC_PAT.search((r.get('NEWSSUB') or '') + ' ' + (r.get('HEADLINE') or '')):
                out[r['NEWS_DT'][:10]] = dict(date=r['NEWS_DT'][:10], att=(r.get('ATTACHMENTNAME') or '').strip(),
                                              subject=re.sub(r'\s+', ' ', r.get('NEWSSUB') or '').strip())
        if len(rows) < 50:
            break
    return list(out.values())


def nmdc_read(f):
    """One filing's PDF -> parsed record. Never raises: an unreadable filing is recorded as unread."""
    import bse
    if not f['att']:
        return dict(f, lump=None, fines=None, basis=None, note='', wef=f['date'], wef_note='',
                    error='the BSE row carries no attachment')
    try:
        data = bse.get_attachment(NMDC_ATT % f['att'], timeout=90)
        if data[:4] != b'%PDF':
            return dict(f, lump=None, fines=None, basis=None, note='', wef=f['date'], error='attachment is not a PDF')
        import fitz
        doc = fitz.open(stream=data, filetype='pdf')
        txt = '\n'.join(doc[i].get_text() for i in range(min(3, len(doc))))
    except Exception as e:
        return dict(f, lump=None, fines=None, basis=None, note='', wef=f['date'], error=str(e)[:110])
    rec = dict(f, **nmdc_parse(txt, f['subject'], f['date']), parser=NMDC_PARSER)
    if rec['lump'] is None and rec['fines'] is None:
        rec['error'] = 'PDF has no readable price line (older filings are scans with no text layer)'
    return rec


def src_nmdc(days=200, since=None):
    """NMDC's administered iron-ore price, and the whole dated history of its revisions."""
    hist = {}
    if os.path.exists(NMDC_HIST):
        try:
            hist = {r['date']: r for r in (json.load(open(NMDC_HIST)).get('filings') or [])}
        except Exception:
            hist = {}
    d_to = ist.today()
    d_from = since or (d_to - datetime.timedelta(days=days))
    wins, d = [], d_from
    while d < d_to:                       # quarters: one BSE call cannot return more than 50 rows
        nxt = min(d + datetime.timedelta(days=90), d_to)
        wins.append((d, nxt))
        d = nxt + datetime.timedelta(days=1)
    seen, win_err = {}, 0
    for a, b in wins:
        try:
            for f in nmdc_filings(a, b):
                seen[f['date']] = f
        except Exception as e:
            win_err += 1
            print(f'nmdc: window {a}..{b} failed ({str(e)[:70]})')
    if win_err == len(wins) and not hist:
        raise RuntimeError(f'every BSE window failed ({win_err})')
    def implausible(r):
        # a price letter announces the price in force now; a w.e.f. far from the filing date is a misread
        try:
            return abs((datetime.date.fromisoformat(r['wef']) - datetime.date.fromisoformat(r['date'])).days) > 60
        except (KeyError, TypeError, ValueError):
            return True
    def stale(r):
        return (r.get('parser') or 0) < NMDC_PARSER
    for r in list(hist.values()):
        if (r.get('lump') is not None or r.get('fines') is not None) and (implausible(r) or stale(r)) and r.get('att'):
            seen.setdefault(r['date'], dict(date=r['date'], att=r['att'], subject=r.get('subject', '')))
    added, unread = 0, 0
    for f in sorted(seen.values(), key=lambda r: r['date']):
        prev = hist.get(f['date'])
        if prev and (prev.get('lump') is not None or prev.get('fines') is not None) and not implausible(prev) and not stale(prev):
            continue                      # already read by this parser, priced and dated sensibly; never refetch
        rec = nmdc_read(f)
        hist[f['date']] = rec
        if rec.get('lump') is not None or rec.get('fines') is not None:
            added += 1
        else:
            unread += 1
    rows_h = sorted(hist.values(), key=lambda r: r['date'])
    # A re-filing ("Resubmission: Prices of Iron Ore w.e.f. 08-10-2020", filed the next day) repeats a letter
    # already on record. The ORIGINAL keeps the row; the re-filing stays in the file marked dup_of and is
    # never counted twice. Should a re-filing ever state a different price, the original still stands and
    # the disagreement is written onto it rather than resolved by guessing.
    first_by_wef = {}
    for r in rows_h:
        r.pop('dup_of', None)
        if r.get('lump') is None and r.get('fines') is None:
            continue
        o = first_by_wef.get(r['wef'])
        if o is None:
            first_by_wef[r['wef']] = r
            r.pop('refiling_differs', None)
            continue
        r['dup_of'] = o['date']
        diff = [f'{leg} {o[leg]:.0f} vs {r[leg]:.0f}' for leg in ('lump', 'fines')
                if o.get(leg) is not None and r.get(leg) is not None and o[leg] != r[leg]]
        if diff:
            o['refiling_differs'] = f"re-filing of {r['date']} states " + ', '.join(diff)
    priced = [r for r in rows_h if (r.get('lump') is not None or r.get('fines') is not None) and not r.get('dup_of')]
    json.dump(dict(built=ist.stamp(),
                   source='NMDC Limited price letters filed with BSE under LODR Regulation 30 (scrip %s)' % NMDC_SCRIP,
                   note='NMDC administers this price; it changes only when NMDC files a revision, so the series is '
                        'dated by the filing, not daily. The tax basis is stated per row and changes between eras.',
                   filings_found=len(rows_h), priced=len(priced),
                   refilings=sum(1 for r in rows_h if r.get('dup_of')),
                   unread=sum(1 for r in rows_h if r.get('lump') is None and r.get('fines') is None),
                   filings=rows_h), open(NMDC_HIST, 'w'), indent=1, ensure_ascii=False)
    if not priced:
        raise RuntimeError('no NMDC price filing could be read')
    priced.sort(key=lambda r: r['wef'])   # by the date the price took effect, not the date it was filed
    last = priced[-1]
    rows = []
    for key, _rx, name in NMDC_GRADES:
        if last.get(key) is None:
            continue
        before = [r for r in priced[:-1] if r.get(key) is not None]
        prev_v = before[-1][key] if before else None
        rows.append(dict(market='NMDC (administered)', name=name, price=last[key], unit='Rs/tonne', step=True,
                         wef=last.get('wef'), basis=last.get('basis'), filed=last['date'],
                         chg_rev=round(100 * (last[key] / prev_v - 1), 2) if prev_v else None,
                         prev=prev_v, prev_date=before[-1]['wef'] if before else None,
                         history=sorted([r['wef'], r[key], r.get('basis')] for r in priced if r.get(key) is not None)))
    # Every BSE window can fail while the committed history still yields rows. That is NOT a clean
    # read: no revision filed since the last run could have been seen. Say so instead of "ok".
    degraded = (f'BSE unreachable, all {win_err} window(s) failed - these rows are the committed '
                f'series re-published; a revision filed since the last run would NOT have been seen') if win_err == len(wins) else None
    return dict(source='NMDC Limited price letters filed with BSE (scrip %s)' % NMDC_SCRIP,
                url='https://www.bseindia.com/stock-share-price/nmdc-ltd/nmdc/%s/corp-announcements/' % NMDC_SCRIP,
                date='w.e.f. ' + (last.get('wef') or last['date']), rows=rows, degraded=degraded,
                revisions=len(priced), since=priced[0]['wef'],
                unread=sum(1 for r in rows_h if r.get('lump') is None and r.get('fines') is None))


# ---------------------------------------------------------------- Ministry of Steel, monthly Mumbai retail
def src_minsteel():
    """The official monthly Mumbai retail price of TMT, HRC and CRC (Rs/t incl. GST), read from
    docs/ideas/minsteel_mumbai.json, which minsteel.py builds from the Ministry of Steel's monthly reports. No
    network here: the reports are monthly. Each row carries its whole series, like NMDC."""
    j = json.load(open(os.path.join(DOCS, 'minsteel_mumbai.json')))
    rows = []
    for n in ('TMT', 'HRC', 'CRC'):
        s = (j.get('series') or {}).get(n) or []
        if not s:
            continue
        d, v, report = s[-1]
        prev = s[-2] if len(s) > 1 else None
        rows.append(dict(city='Mumbai', name=j['specs'][n], price=v, unit='Rs/tonne', date=d,
                         prev=prev[1] if prev else None, prev_date=prev[0] if prev else None,
                         chg_prev=round(100 * (v / prev[1] - 1), 2) if prev else None,
                         history=[[x[0], x[1]] for x in s], history_via='Ministry of Steel monthly report',
                         report=report))
    if not rows:
        raise RuntimeError('minsteel_mumbai.json holds no series')
    return dict(source='Ministry of Steel monthly report (steel.gov.in): Mumbai retail price incl. GST',
                url='https://steel.gov.in/monthly-summary', date=rows[0]['date'], rows=rows)


# ---------------------------------------------------------------- Guidechem: Chinese domestic chemical quotes
# Guidechem's "GuideTrends" pages (guidechem.com/price/en/<CAS>.html, server-rendered, no login, robots.txt allows
# them) print a headline price per chemical with its own "Updated" date, the day-on-day change, a 3-month average
# and the market row the headline comes from (data-brand = region/producer, data-spec = grade). It is an
# AGGREGATOR of supplier and market quotes inside China, in CNY: direction data, labelled as such - never the
# Indian price and never a benchmark assessment. The headline's row identity is part of the series name, so if
# Guidechem switches the headline to another region or grade that is a new series, not a fake jump.
# Measured 2026-10-12: the dye intermediates (H-acid, vinyl sulphone, beta naphthol, J acid) are 'Locally Made'
# supplier quotes. Guidechem's H-acid (41,000 CNY/t, flat 38-41k since August) does NOT show the Aug-Sep 2026
# spike the industry assessors reported in the news (SCI99 95 CNY/kg on 28-Aug; Baiinfo ~150,000 CNY/t on 1-Sep):
# SCI99 (price page shows '请登录' in every cell) and Baiinfo (slider captcha) are login-walled, 100ppi/SunSirs
# answer a JavaScript bot check and echemi an Aliyun WAF - none of them is read here. So the H-acid row carries
# a caveat that the page shows; treat it as one supplier-quote series that disagrees with the assessors.
GC_URL = 'https://www.guidechem.com/price/en/%s.html'
GC_ITEMS = [   # (CAS, our name, caveat or '') - every CAS here was fetched and parsed on 2026-10-12
    # dye intermediates
    ('90-20-0', 'H-acid', 'Guidechem supplier quote; it shows no rise while SCI99 (95 CNY/kg, 28-Aug-2026) and Baiinfo '
                          '(~150,000 CNY/t, 1-Sep-2026) reported a spike in the news - unconfirmed, read for direction only'),
    ('2494-89-5', 'Vinyl sulphone (para base ester)', ''), ('135-19-3', 'Beta naphthol', ''), ('87-02-5', 'J acid', ''),
    ('130-13-2', 'Sodium naphthionate', ''), ('91-20-3', 'Naphthalene', ''), ('108-45-2', 'm-Phenylenediamine', ''),
    ('123-30-8', 'Para-aminophenol', ''),
    # benzene chain: nitro/chloro aromatics, aniline
    ('71-43-2', 'Benzene', ''), ('108-88-3', 'Toluene', ''), ('62-53-3', 'Aniline', ''), ('98-95-3', 'Nitrobenzene', ''),
    ('108-90-7', 'Chlorobenzene', ''), ('100-00-5', 'Para-nitrochlorobenzene', ''), ('88-73-3', 'Ortho-nitrochlorobenzene', ''),
    ('106-46-7', 'Para-dichlorobenzene', ''), ('95-50-1', 'Ortho-dichlorobenzene', ''), ('106-47-8', 'Para-chloroaniline', ''),
    ('88-74-4', 'Ortho-nitroaniline', ''), ('95-80-7', '2,4-Diaminotoluene', ''),
    # phenol, acetone, epoxy, PU
    ('108-95-2', 'Phenol', ''), ('67-64-1', 'Acetone', ''), ('80-05-7', 'Bisphenol A', ''), ('106-89-8', 'Epichlorohydrin', ''),
    ('101-68-8', 'MDI', ''), ('78-93-3', 'MEK', ''), ('67-63-0', 'Isopropyl alcohol', ''),
    # chlor-alkali and acids
    ('1310-73-2', 'Caustic soda', ''), ('7647-01-0', 'Hydrochloric acid', ''), ('7681-52-9', 'Sodium hypochlorite', ''),
    ('75-09-2', 'Methylene chloride', ''), ('67-66-3', 'Chloroform', ''), ('7664-93-9', 'Sulphuric acid', ''),
    ('7704-34-9', 'Sulphur', ''), ('7722-84-1', 'Hydrogen peroxide', ''), ('497-19-8', 'Soda ash', ''),
    ('7757-82-6', 'Sodium sulphate', ''),
    # fluorochemicals
    ('7789-75-5', 'Fluorspar', ''), ('7664-39-3', 'Hydrofluoric acid', ''), ('811-97-2', 'R134a', ''), ('75-45-6', 'R22', ''),
    ('9002-84-0', 'PTFE', ''),
    # fertiliser and phosphorus chain
    ('57-13-6', 'Urea', ''), ('6484-52-2', 'Ammonium nitrate', ''), ('7783-20-2', 'Ammonium sulphate', ''),
    ('7722-76-1', 'Monoammonium phosphate', ''), ('7783-28-0', 'Diammonium phosphate', ''), ('7447-40-7', 'Potassium chloride', ''),
    ('7778-80-5', 'Potassium sulphate', ''), ('7757-79-1', 'Potassium nitrate', ''), ('12185-10-3', 'Yellow phosphorus', ''),
    ('7758-29-4', 'Sodium tripolyphosphate', ''),
    # solvents, oxo, plasticisers, intermediates, polymers
    ('67-56-1', 'Methanol', ''), ('64-19-7', 'Acetic acid', ''), ('141-78-6', 'Ethyl acetate', ''), ('71-36-3', 'n-Butanol', ''),
    ('104-76-7', '2-Ethylhexanol', ''), ('117-81-7', 'DOP', ''), ('85-44-9', 'Phthalic anhydride', ''),
    ('108-31-6', 'Maleic anhydride', ''), ('124-04-9', 'Adipic acid', ''), ('68-12-2', 'DMF', ''), ('110-86-1', 'Pyridine', ''),
    ('107-13-1', 'Acrylonitrile', ''), ('105-60-2', 'Caprolactam', ''), ('100-42-5', 'Styrene', ''), ('107-21-1', 'MEG', ''),
    ('100-21-0', 'PTA', ''), ('25038-59-9', 'PET (filament grade)', ''), ('9002-86-2', 'PVC', ''), ('108-78-1', 'Melamine', ''),
    # pigments, rubber chemicals, agro, pharma, battery and silicon
    ('13463-67-7', 'Titanium dioxide', ''), ('1333-86-4', 'Carbon black', ''), ('1071-83-6', 'Glyphosate', ''),
    ('138261-41-3', 'Imidacloprid', ''), ('1912-24-9', 'Atrazine', ''), ('551-16-6', '6-APA', ''),
    ('554-13-2', 'Lithium carbonate', ''), ('1310-66-3', 'Lithium hydroxide', ''), ('7440-48-4', 'Cobalt', ''),
    ('7440-21-3', 'Silicon metal', ''),
]


def gc_one(cas):
    """One Guidechem price page -> the headline quote with its own date and row identity."""
    s = get(GC_URL % cas, timeout=40, retries=2)
    hd = re.search(r'class="i_rd_x1[^"]*">\s*<span[^>]*><em>([\d.,]+)</em>([^<]+)</span>\s*<i[^>]*>Updated:\s*(\d{4}-\d{2}-\d{2})</i>', s)
    if not hd:
        raise RuntimeError('no headline price on the page')
    h1 = re.search(r'<h1[^>]*>(.*?)</h1>', s, re.S)
    dod = re.search(r'Price change \(DoD\):\s*([-+]?[\d.,]+)', s)
    avg = re.search(r'Average price \(3M\):\s*([\d.,]+)', s)
    lvl = re.search(r'Price Level\(1Y\):\s*<i[^>]*>([^<]*)</i>', s)
    row = re.search(r'class="gzs_drf2[^"]*"[^>]*data-brand="([^"]*)"\s+data-spec="([^"]*)"', s)
    unit = re.sub(r'\s+', '', hd.group(2)).replace('TON', 't').replace('KG', 'kg')
    price = num(hd.group(1))
    if not price or unit not in ('CNY/t', 'CNY/kg'):
        raise RuntimeError(f'unexpected headline {hd.group(1)} {hd.group(2)}')
    if unit == 'CNY/kg':                    # one unit across the source; a CNY/kg headline is the same price x1000
        price, unit = round(price * 1000, 2), 'CNY/t'
    d1 = num(dod.group(1)) if dod else None
    prev = price - d1 * (1000 if hd.group(2).strip().endswith('KG') else 1) if d1 else None
    brand = html.unescape(row.group(1)).strip() if row else ''
    spec = re.sub(r'\s+', ' ', html.unescape(row.group(2))).strip()[:90] if row else ''
    return dict(cas=cas, source_name=html.unescape(h1.group(1)).strip() if h1 else None, price=price, unit=unit,
                date=hd.group(3), market=brand or None, grade=spec or None,
                chg_1d=round(100 * (price / prev - 1), 2) if prev else (0.0 if d1 == 0 else None),
                avg_3m=num(avg.group(1)) if avg else None, level_1y=lvl.group(1).strip() if lvl else None,
                url=GC_URL % cas)


def src_guidechem(items=None):
    rows, errs = [], []
    items = items or GC_ITEMS
    with cf.ThreadPoolExecutor(4) as ex:
        for (cas, name, caveat), fut in [(it, ex.submit(gc_one, it[0])) for it in items]:
            try:
                r = fut.result()
                r['name'] = name
                if caveat:
                    r['caveat'] = caveat
                rows.append(r)
            except Exception as e:
                errs.append(f'{name} ({cas}): {str(e)[:70]}')
    if len(rows) < max(3, len(items) // 2):
        raise RuntimeError(f'guidechem parsed only {len(rows)} of {len(items)} ({errs[:3]})')
    return dict(source='Guidechem GuideTrends (guidechem.com) - Chinese domestic quotes, CNY, aggregator',
                url='https://www.guidechem.com/price/', rows=rows, errors=errs)


# ---------------------------------------------------------------- CAI: Indian cotton spot rates
# The Cotton Association of India fixes and registers daily spot rates by growth area and grade (Rs per candy of
# 356 kg, ex-gin, excluding tax). The page loads them from a JSON endpoint that takes the date as a form field;
# no rates are published on Sundays and holidays, so the latest published day within a week is read. Each row
# carries both the current and the previous cotton season; the season label is part of the series name.
CAI_URL = 'https://caionline.in/details/cai/spot-rates/by/date'


def _post_form(url, fields, timeout=40):
    bnd = '----stocksdash' + str(int(time.time() * 1000))
    body = ''.join(f'--{bnd}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n' for k, v in fields.items()) + f'--{bnd}--\r\n'
    req = urllib.request.Request(url, data=body.encode(), headers=dict(HDR, **{'Content-Type': f'multipart/form-data; boundary={bnd}',
                                                                              'X-Requested-With': 'XMLHttpRequest'}))
    with urllib.request.urlopen(req, timeout=timeout) as f:
        return f.read().decode('utf-8', 'ignore')


def src_cai():
    day, j = ist.today(), None
    for _ in range(8):
        j = json.loads(_post_form(CAI_URL, {'date': day.strftime('%d-%m-%Y')}))
        if j.get('list'):
            break
        day -= datetime.timedelta(days=1)
    if not j or not j.get('list'):
        raise RuntimeError('CAI published no spot rates in the last 8 days')
    pub = (j.get('master') or {}).get('published_date') or day.isoformat()
    rows = []
    for r in j['list']:
        base = f"{r.get('grade_standard', '')} {r.get('staple', '')}".strip()
        for fld, season in (('per_candy', j.get('current_season')), ('p_per_candy', j.get('previous_season'))):
            v = num(r.get(fld))
            if v:
                rows.append(dict(market=r.get('growth'), name=f'Cotton {base}', grade=f'season {season}', price=v,
                                 unit='Rs/candy (356 kg)', date=pub, micronaire=r.get('micronaire'),
                                 strength_gpt=r.get('strength_gpt')))
    if len(rows) < 3:
        raise RuntimeError(f'CAI list had {len(j["list"])} rows but only {len(rows)} priced')
    return dict(source='Cotton Association of India (caionline.in) daily spot rates', url='https://caionline.in/details/cai/spot-rates',
                date=pub, rows=rows, title_date=j.get('title_date'))


# ---------------------------------------------------------------- Spices Board: cardamom auctions, pepper
# The Spices Board page prints (a) the latest small-cardamom e-auctions with the auctioneer, quantity sold and
# max/average Rs/kg, (b) large-cardamom market prices, and (c) a 'Current Market Price of Key Spices' table
# (pepper at Kochi, garbled and ungarbled, Rs/kg). Each item states its own date.
SPICES_URL = 'https://www.indianspices.com/marketing/price/domestic/current-market-price.html'


def src_spices():
    h = get(SPICES_URL, ctx=LAX, timeout=60)
    p = plain(h)
    rows, seen = [], set()
    for d, auc, sold, mx, avg in re.findall(r'Spice:\s*Small Cardamom\s*,\s*Date of Auction:\s*(\d{2}-\w{3}-\d{4}),\s*Auctioneer:\s*([^,]+(?:,[^,]*?Ltd)?)\s*,'
                                            r'\s*No\.of lots:\s*[\d.]+,\s*Qty Arrived \(Kgs\):\s*[\d.]+,\s*Qty Sold \(Kgs\):\s*([\d.]+),'
                                            r'\s*Max Price \(Rs\./Kg\):\s*([\d.]+),\s*Avg\. Price \(Rs\./Kg\):\s*([\d.]+)', p):
        k = ('sc', d, auc.strip())
        if k in seen:
            continue
        seen.add(k)
        rows.append(dict(market=auc.strip().title(), name='Small cardamom (e-auction avg)', price=num(avg), max_price=num(mx),
                         qty_sold_kg=num(sold), unit='Rs/kg', date=d))
    for d, mkt, typ, pr in re.findall(r'Spice:\s*Large Cardamom\s*,\s*Date:\s*(\d{2}-\w{3}-\d{4}),\s*Market:\s*([^,]+),\s*Type:\s*([^,]+),\s*Price \(Rs\./Kg\):\s*([\d.]+)', p):
        k = ('lc', d, mkt.strip(), typ.strip())
        if k in seen:
            continue
        seen.add(k)
        rows.append(dict(market=mkt.strip(), name='Large cardamom', grade=typ.strip(), price=num(pr), unit='Rs/kg', date=d))
    i = h.find('Current Market Price of Key Spices')
    if i > 0:
        t = h[i:]; t = t[:t.find('</table>')]
        for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', t, re.S):
            c = [re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', '', x))).strip() for x in re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)]
            if len(c) >= 9 and re.match(r'\d{2}-\w{3}-\d{4}$', c[0]) and num(c[8]):
                rows.append(dict(market=c[2].title(), name=c[1], grade=c[4], price=num(c[8]), unit='Rs/kg', date=c[0], via=c[5]))
    if len(rows) < 2:
        raise RuntimeError(f'spices board parsed only {len(rows)} rows')
    return dict(source='Spices Board India (indianspices.com) auction and market prices', url=SPICES_URL, rows=rows)


# ---------------------------------------------------------------- MOIL manganese ore (from its own BSE filings)
# MOIL fixes Indian manganese-ore prices monthly and files each revision with BSE ("Information relating to fixation
# of price of different grades of Manganese Ore ... w.e.f. 01.10.2026"). Unlike NMDC the letters state the CHANGE
# ("increased by 5% on the prices prevailing since 01.09.2026"), not the rupee price, except for EMD (electrolytic
# manganese dioxide, Rs per tonne). So the ore series here is an INDEX chained from MOIL's own stated monthly %
# changes: 100 = the prices prevailing before the first letter of the unbroken monthly run, and the chain stops at
# any month whose letter is missing or unreadable rather than bridging it. Read back to 2023 into
# docs/ideas/moil_history.json; only letters not yet read are fetched.
MOIL_SCRIP = '533286'
MOIL_HIST = os.path.join(DOCS, 'moil_history.json')
MOIL_ANN = ('https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d&strCat=-1&strPrevDate=%s'
            '&strScrip=' + MOIL_SCRIP + '&strSearch=P&strToDate=%s&strType=C&subcategory=-1')
# 'Fixation Of Different Grades Of Manganese Ore' (Oct-2025) drops the word 'price', so match on 'fixation'
MOIL_PAT = re.compile(r'(?:price|fixation)\s+of\s+(?:price\s+of\s+)?(?:different\s+grades\s+of\s+)?manganese\s+ore|price\s+fixation\s+of\s+manganese', re.I)
MOIL_PARSER = 3   # 3 = month from the items' 'midnight of a/b' dates; subject and 'effective from' both carried typos


def moil_parse(text, subject, filed):
    t = re.sub(r'\s+', ' ', text or '')
    def move(rx):
        m = re.search(rx + r'.{0,160}?have been (increased|decreased|continued)(?: by ([\d.]+)\s*%)?', t, re.I)
        if not m:
            return None
        if m.group(1).lower() == 'continued':
            return 0.0
        v = num(m.group(2))
        return None if v is None else (v if m.group(1).lower() == 'increased' else -v)
    hi = move(r'Ferro grades of manganese ore with Manganese content of Mn-?\s*44% and above')
    lo = move(r'other Ferro grades of manganese ore with Manganese content below Mn-?\s*44%')
    # 'basic price Rs.1,80,000/- PMT of EMD' / 'basic price of EMD of Rs.1,80,000/- PMT' / 'basic price of Rs.205000/- PMT of EMO'
    emd = re.search(r'basic price (?:of (?:EMD of )?)?Rs\.?\s*([\d,]{5,})\s*/-\s*PMT', t, re.I)
    # The month a letter prices: the body's 'effective from' date when it states one (a subject has carried the
    # previous month by mistake), else any w.e.f. date; among candidates the first-of-month nearest the filing.
    f0 = datetime.date.fromisoformat(filed)
    def cands(rx, txt):
        out = []
        for d, m, y in re.findall(rx, txt, re.I):
            try:
                c = datetime.date(int(y), int(m), int(d))
            except ValueError:
                continue
            if c.day == 1 and abs((c - f0).days) <= 45:
                out.append(c)
        return out
    # Each item repeats 'w.e.f. midnight of 31.07.2025/01.08.2025', so the most frequent second date is the vote;
    # the subject and the 'effective from' line have each carried the previous month by mistake (1-May-2025 subject,
    # 1-Aug-2025 body), so they are only fallbacks.
    mid = cands(r'midnight\s+of\s*\d{2}\.\d{2}\.\d{4}\s*/\s*(\d{2})\.(\d{2})\.(\d{4})', t)
    if mid:
        wef = max(set(mid), key=mid.count).isoformat()
    else:
        pool = cands(r'effective\s+from\s*(\d{2})\.(\d{2})\.(\d{4})', t) or cands(r'w\.e\.f\.?\s*(\d{2})\.(\d{2})\.(\d{4})', subject + ' ' + t)
        wef = min(pool, key=lambda c: abs((c - f0).days)).isoformat() if pool else None
    return dict(wef=wef, ferro_44_up=hi, ferro_below_44=lo, emd=num(emd.group(1)) if emd else None,
                revised=bool(re.search(r'\brevised\b', subject + ' ' + t[:600], re.I)))


def src_moil(days=200, since=None):
    import bse
    hist = {}
    if os.path.exists(MOIL_HIST):
        try:
            hist = {r['key']: r for r in (json.load(open(MOIL_HIST)).get('letters') or [])}
        except Exception:
            hist = {}
    d_to = ist.today()
    d = since or (d_to - datetime.timedelta(days=days))
    found, win_err, wins = {}, 0, 0
    while d < d_to:
        nxt = min(d + datetime.timedelta(days=90), d_to)
        wins += 1
        try:
            for page in (1, 2):
                rows = json.loads(bse._get(MOIL_ANN % (page, d.strftime('%Y%m%d'), nxt.strftime('%Y%m%d')), sleep=0.4)).get('Table') or []
                for r in rows:
                    s = re.sub(r'\s+', ' ', (r.get('NEWSSUB') or '') + ' ' + (r.get('HEADLINE') or ''))
                    att = (r.get('ATTACHMENTNAME') or '').strip()
                    if MOIL_PAT.search(s) and att:
                        found[att] = dict(key=att, date=r['NEWS_DT'][:10], att=att, subject=s.strip()[:300])
                if len(rows) < 50:
                    break
        except Exception as e:
            win_err += 1
            print(f'moil: window {d}..{nxt} failed ({str(e)[:70]})')
        d = nxt + datetime.timedelta(days=1)
    if wins and win_err == wins and not hist:
        raise RuntimeError(f'every BSE window failed ({win_err})')
    import fitz
    for att, f in sorted(found.items(), key=lambda kv: kv[1]['date']):
        if att in hist and hist[att].get('parser') == MOIL_PARSER:
            continue
        try:
            b = bse.get_attachment('https://www.bseindia.com/xml-data/corpfiling/AttachLive/' + att, timeout=90)
            doc = fitz.open(stream=b, filetype='pdf')
            txt = '\n'.join(doc[i].get_text() for i in range(min(3, len(doc))))
            rec = dict(f, **moil_parse(txt, f['subject'], f['date']), parser=MOIL_PARSER)
        except Exception as e:
            rec = dict(f, wef=None, ferro_44_up=None, ferro_below_44=None, emd=None, error=str(e)[:110], parser=MOIL_PARSER)
        hist[att] = rec
    letters = sorted(hist.values(), key=lambda r: (r.get('wef') or '', r['date']))
    # one letter per month: a later filing for the same w.e.f. (a 'Revised' letter, or a re-filing) supersedes
    by_wef = {}
    for r in letters:
        if r.get('wef'):
            by_wef[r['wef']] = r
    months = sorted(by_wef)
    json.dump(dict(built=ist.stamp(), source='MOIL Limited price letters filed with BSE (scrip %s)' % MOIL_SCRIP,
                   note='Letters state % changes on the prices prevailing the month before; EMD is stated in Rs/PMT.',
                   letters=letters), open(MOIL_HIST, 'w'), indent=1, ensure_ascii=False)
    if not months:
        raise RuntimeError('no MOIL price letter could be read')

    def chain(field):
        """The latest unbroken monthly run of stated % moves, as an index; [] when the latest month is unreadable."""
        run = []
        for w in reversed(months):
            v = by_wef[w].get(field)
            if v is None:
                break
            if run:
                a, b = datetime.date.fromisoformat(w), datetime.date.fromisoformat(run[-1][0])
                if (b.year * 12 + b.month) - (a.year * 12 + a.month) != 1:
                    break
            run.append((w, v))
        run.reverse()
        if not run:
            return [], None
        pts, idx = [], 100.0
        first = datetime.date.fromisoformat(run[0][0])
        base = (first.replace(day=1) - datetime.timedelta(days=1)).replace(day=1).isoformat()
        pts.append([base, 100.0])
        for w, v in run:
            idx = idx * (1 + v / 100)
            pts.append([w, round(idx, 2)])
        return pts, base
    rows = []
    for field, name in (('ferro_44_up', 'Manganese ore, ferro grades Mn 44% and above'),
                        ('ferro_below_44', 'Manganese ore, ferro grades below Mn 44%')):
        pts, base = chain(field)
        if len(pts) < 2:
            continue
        last = by_wef[pts[-1][0]]
        rows.append(dict(market='MOIL (administered)', name=name + ' (index)', price=pts[-1][1], step=True,
                         unit=f'index, 100 = prices prevailing in {base[:7]}', wef=pts[-1][0], filed=last['date'],
                         chg_rev=last.get(field), history=pts, history_via='MOIL price letter'))
    emd = [[w, by_wef[w]['emd']] for w in months if by_wef[w].get('emd')]
    if emd:
        last = by_wef[emd[-1][0]]
        prev = emd[-2][1] if len(emd) > 1 else None
        rows.append(dict(market='MOIL (administered)', name='EMD (electrolytic manganese dioxide), basic price', price=emd[-1][1],
                         unit='Rs/tonne', step=True, wef=emd[-1][0], filed=last['date'],
                         chg_rev=round(100 * (emd[-1][1] / prev - 1), 2) if prev else None, history=emd,
                         history_via='MOIL price letter'))
    if not rows:
        raise RuntimeError(f'{len(months)} MOIL letters read but none yields a series')
    degraded = (f'BSE unreachable, all {win_err} window(s) failed - committed letters re-published') if wins and win_err == wins else None
    return dict(source='MOIL Limited price letters filed with BSE (scrip %s)' % MOIL_SCRIP,
                url='https://www.bseindia.com/stock-share-price/moil-ltd/moil/%s/corp-announcements/' % MOIL_SCRIP,
                date='w.e.f. ' + months[-1], rows=rows, degraded=degraded, letters=len(letters), months=len(months),
                unread=sum(1 for r in letters if not r.get('wef')))


# ---------------------------------------------------------------- history for click-to-chart
def build_history(sources):
    """One dated series per Indian print (and per Trading Economics proxy), written to india_history.json.gz.

    Points come from three places and each keeps its provenance: what this site has recorded daily
    (india_spot_history.csv, from 2026-09-22), older prints recovered from archives (india_backfill.csv,
    `via` names the archive copy), and NMDC's own revision history (its filings, with the tax basis each
    was stated on). Stats are computed here with signals.hist_stats - the one definition the page reads.
    """
    from signals import hist_stats
    pts = {}
    for path, tag, ledger in ((BACKFILL, None, BACKFILL_RETRACTED), (HIST, 'recorded', RETRACTED)):   # recorded last: wins a tie
        for r in in_force(path, ledger):
            try:
                pts.setdefault((r['source'], r['series']), {})[r['date']] = (float(r['price']), tag or r.get('via') or 'archive')
            except (ValueError, KeyError, TypeError):
                continue
    out = {}
    for src, res in sources.items():
        for r in res.get('rows', []):
            key = r.get('key') or series_key(r)
            if r.get('history'):
                series = [list(x) for x in r['history']]
                via = [r.get('history_via') or 'filing'] * len(series)
            else:
                d = pts.get((src, key), {})
                series = [[dt, d[dt][0]] for dt in sorted(d)]
                via = [d[dt][1] for dt in sorted(d)]
            if not series:
                continue
            out[f'{src}|{key}'] = dict(src=src, key=key, unit=r.get('unit'), p=series,
                                       via=via[0] if len(set(via)) == 1 else via,      # one label, or one per point
                                       stats=hist_stats(series, step=bool(r.get('step'))) if len(series) >= 2 else None,
                                       recorded_from=next((x[0] for x, v in zip(series, via) if v == 'recorded'), None),
                                       archived=sum(1 for v in via if str(v).lower().startswith('wayback')))
    blob = json.dumps(dict(built=ist.stamp(), series=out),
                      separators=(',', ':'), ensure_ascii=False).encode()
    with open(HIST_JSON, 'wb') as fh:
        with gzip.GzipFile(fileobj=fh, mode='wb', mtime=0) as gz:   # mtime=0: same data, same bytes
            gz.write(blob)
    return len(out), sum(len(v['p']) for v in out.values())


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--no-te', action='store_true')
    ap.add_argument('--te-only', action='store_true')
    ap.add_argument('--nmdc-days', type=int, default=200, help='how far back to look for unread NMDC price filings')
    ap.add_argument('--nmdc-since', default='', help='YYYY-MM-DD: sweep NMDC filings from here (one-off backfill)')
    ap.add_argument('--moil-days', type=int, default=200, help='how far back to look for unread MOIL price letters')
    ap.add_argument('--moil-since', default='', help='YYYY-MM-DD: sweep MOIL letters from here (one-off backfill)')
    ap.add_argument('--only', default='', help='comma list of sources to fetch this run (the rest are kept from the previous file)')
    a = ap.parse_args()
    old = {}
    if os.path.exists(OUT):
        try:
            old = json.load(open(OUT)).get('sources') or {}
        except Exception:
            old = {}
    since = datetime.date.fromisoformat(a.nmdc_since) if a.nmdc_since else None
    msince = datetime.date.fromisoformat(a.moil_since) if a.moil_since else None
    plan = [] if a.te_only else [('metalbook', src_metalbook), ('ibja', src_ibja), ('rubber', src_rubber),
                                 ('sugar', src_sugar), ('fuel', src_fuel),
                                 ('nmdc', lambda: src_nmdc(days=a.nmdc_days, since=since)),
                                 ('minsteel', src_minsteel), ('guidechem', src_guidechem), ('cai', src_cai),
                                 ('spices', src_spices), ('moil', lambda: src_moil(days=a.moil_days, since=msince))]
    if not a.no_te:
        plan.append(('te', lambda: src_te(TE_SLUGS)))
    if a.only:
        want = {x.strip() for x in a.only.split(',') if x.strip()}
        plan = [(k, f) for k, f in plan if k in want]
    sources, status = dict(old), {}
    for key in old:                                   # rows carried over from the previous file, not refetched this run
        if key not in [k for k, _ in plan]:
            status[key] = f'kept from previous run ({old[key].get("fetched", "?")}), {len(old[key].get("rows", []))} rows'
    for key, fn in plan:
        try:
            res = fn()
            res['fetched'] = ist.stamp()
            sources[key] = res
            status[key] = f'ok, {len(res["rows"])} rows'
            if res.get('degraded'):     # rows came back, but not from a live read - never report that as "ok" alone
                status[key] += ' - DEGRADED: ' + res['degraded']
        except Exception as e:
            status[key] = f'FAILED ({str(e)[:90]}) - previous rows kept' if key in old else f'FAILED ({str(e)[:90]})'
    if not sources:
        raise SystemExit('nothing fetched and nothing to keep: ' + json.dumps(status))
    # history: one row per series per day, so 1w/1m changes can be computed later for sources with no history of their own
    today = ist.today().isoformat()
    # A print is recorded under the date its SOURCE states (MetalBook prices each item on its own day, often
    # a week back; Rubber Board's page can lag a fortnight). Stamping the run date instead made a flat line of
    # fake daily points out of one unchanged print. No stated date, or one in the future: the run date, marked.
    seen = {(r['date'], r['source'], r['series']) for r in in_force(HIST, RETRACTED)}   # a retracted row is not a record
    new = 0
    with open(HIST, 'a', newline='') as f:
        w = csv.writer(f)
        if os.path.getsize(HIST) == 0:
            w.writerow(['date', 'source', 'series', 'price', 'unit'])
        for key, res in sources.items():
            if not status.get(key, '').startswith('ok'):
                continue
            for r in res['rows']:
                series = series_key(r)
                d = iso_date(r.get('date') or r.get('wef') or res.get('date') or '')
                if not d or d > today:
                    d = today
                if r.get('price') is None or (d, key, series) in seen:
                    continue
                seen.add((d, key, series))
                w.writerow([d, key, series, r['price'], r.get('unit', '')]); new += 1
    for res in sources.values():
        for r in res.get('rows', []):
            r['key'] = series_key(r)
    out = dict(built=ist.stamp(), status=status, sources=sources)
    json.dump(out, open(OUT, 'w'), indent=1, ensure_ascii=False)
    print('india_spot:', json.dumps(status, indent=1)); print(f'history rows appended: {new}')
    try:
        n_series, n_pts = build_history(sources)
        print(f'india_history.json.gz: {n_series} series, {n_pts} dated points')
    except Exception as e:                                    # never lose the day's prices over the chart file
        print(f'india_history.json.gz NOT rebuilt: {str(e)[:160]}')


if __name__ == '__main__':
    main()
