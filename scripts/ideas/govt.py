"""Government announcements that are big enough to move an order book, and who they land on.

The Press Information Bureau publishes 40-80 releases a day and almost all of it is noise: awards, greetings,
inaugurations, observances. The 2026-09-22 study (runbook 144c) measured what actually paid: Cabinet
approvals, Budget allocations and procurement mandates with outlays in the tens of thousands of crores, in
sectors where the money becomes somebody's order book.

  programme (date)                        basket median   market median   share that made 5x
  defence indigenisation list (Aug-2020)      17.4x           4.3x            76% vs 44%
  smart meters / RDSS (Jun-2021)              12.0x           3.2x           100% vs 28%
  Jal Jeevan Mission (Feb-2021)                6.7x           3.7x            50% vs 35%
  railways + Vande Bharat (Feb-2022)           4.4x           3.0x            44% vs 24%
  PM Surya Ghar rooftop solar (Feb-2024)       1.8x           2.0x             0% vs  6%   <- a household
                                                                                              subsidy is not
                                                                                              an order book
So the gate here is deliberately narrow. A release must look like a DECISION (approved, sanctioned, launched,
allocated, awarded, notified) and carry either a large rupee outlay or a procurement mandate, in a sector we
have a beneficiary map for. Everything else is dropped and counted, never published.

Usage: python3 scripts/ideas/govt.py [--date YYYY-MM-DD] [--backfill-days 14] [--min-cr 1000]
Writes docs/ideas/govt.json  {built, scanned, kept, releases:[...]}  (today's day, as before)
   and docs/ideas/govt_history.json  {days:{date:{scanned,kept,unmapped,read_at}}, releases:{prid:{...,date}}}
A run reads today AND every day in the last --backfill-days that the history has not read yet (PIB's own
date picker, a postback on allRel.aspx), so a day the job failed is filled on the next run, never lost
(2026-11-04: nine stopped runs 26-Oct..3-Nov had left those days unread).
"""
import argparse, datetime, html, http.cookiejar, json, os, re, sys, time, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ist                        # IST stamps: a naive now() on a UTC runner was labelled IST (runbook 144e)
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36'
LIST = 'https://www.pib.gov.in/allRel.aspx?reg=3&lang=1'
BODY = 'https://pib.gov.in/PressReleaseIframePage.aspx?PRID=%s'

# a release must look like a decision, not an event
DECISION = re.compile(r'\b(cabinet (?:committee )?(?:approve|clears|nod)|ccea approve|approves?|approved|sanction(?:s|ed|ing)?|'
                      r'launch(?:es|ed)?|allocat(?:es|ed|ion)|notifie[sd]|award(?:s|ed)|signs? (?:an? )?(?:contract|agreement|mou)|'
                      r'clears?|tender|invites bids|policy|scheme|mission|outlay|budget|procurement|indigenis|'
                      r'production linked incentive|\bpli\b|viability gap|capital outlay|contract worth)\b', re.I)
# and must not be one of the daily rituals
NOISE = re.compile(r'\b(english rendering|text of (?:the )?(?:pm|prime minister)|congratulatcondol|greet|felicitat|celebrat|observ(?:es|ance)|diwas|divas|awards? ceremony|'
                   r'film award|exhibition|webinar|workshop|seminar|swachhata|cleanliness|yoga day|walkathon|'
                   r'photo caption|clarification|fact check|rashtrapati|visits|meets|inaugurat(?:es|ed) an? (?:exhibition|event)|'
                   r'address(?:es|ed) (?:the )?(?:gathering|students)|quiz|essay|poster|pledge|anniversar)\b', re.I)
# Retrospectives quote big numbers about decisions taken years ago. 2026-09-24: "Coal Distribution Over
# the Years: From Allocation to Auction" (Rs 7,500 cr in the body) was kept because "Allocation" is a
# decision verb. A backgrounder is a story about the past, not an order book.
BACKGROUNDER = re.compile(r'\b(over the (?:years|decades)|explainer|backgrounder|fact ?sheet|year[- ]?ender|'
                          r'(?:a )?look (?:back )?at|journey (?:of|from)|story of|(?:a )?decade of|\d+ years of|'
                          r'milestones?|achievements? (?:of|in|under)|background note|from \w+ to \w+:|'
                          r'transforming|transformation of|then and now|retrospect)\b', re.I)

# NOTE: 'lakh cr' and 'lakh crore' must be tried BEFORE plain 'cr' or 'Rs 1.39 lakh cr' reads as Rs 1.39 cr
AMT = re.compile(r'(?:rs\.?|inr|₹|rupees)\s*([\d,]+(?:\.\d+)?)\s*(lakh\s+crores?|lakh\s+cr\b|crores?|cr\b|lakhs?|billion|bn\b|trillion)', re.I)
MULT = {'lakh crore': 100000, 'lakh crores': 100000, 'lakh cr': 100000, 'crore': 1, 'crores': 1, 'cr': 1,
        'lakh': .01, 'lakhs': .01, 'billion': 100, 'bn': 100, 'trillion': 100000}

# procurement mandates carry no rupee figure but reshape an order book all the same
MANDATE = re.compile(r'\b(indigenisation list|positive list|negative import list|import embargo|local content|'
                     r'make in india (?:category|procurement)|emergency procurement|domestic content requirement|'
                     r'\balmm\b|approved list of models|quality control order|anti-dumping|safeguard duty|'
                     r'minimum import price|production linked incentive|\bpli\b)\b', re.I)


def get(url, timeout=60, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': '*/*'})
            with urllib.request.urlopen(req, timeout=timeout) as f:
                return f.read().decode('utf-8', 'ignore')
        except Exception as e:
            last = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f'GET failed {url}: {last}')


def amount_hit(text):
    """Largest rupee outlay in the text -> (Rs crore, the matched text), or (None, None)."""
    best, hit = None, None
    for m in AMT.finditer(text or ''):
        try:
            v = float(m.group(1).replace(',', '')) * MULT.get(re.sub(r'\s+', ' ', m.group(2).lower().strip()), 0)
        except Exception:
            continue
        if v and (best is None or v > best):
            best, hit = v, m.group(0).strip()
    return (round(best, 1), hit) if best else (None, None)


def amount_cr(text):
    """Largest rupee outlay in the text, in Rs crore."""
    return amount_hit(text)[0]


def listing_page(day=None):
    """allRel.aspx HTML for today, or for `day` (a date) via the page's own day/month/year postback."""
    if day is None:
        return get(LIST)
    last = None
    for i in range(3):
        try:
            op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            first = op.open(urllib.request.Request(LIST, headers={'User-Agent': UA}), timeout=60).read().decode('utf-8', 'ignore')
            form = dict(re.findall(r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', first))
            form.update({'ctl00$ContentPlaceHolder1$ddlday': str(day.day), 'ctl00$ContentPlaceHolder1$ddlMonth': str(day.month),
                         'ctl00$ContentPlaceHolder1$ddlYear': str(day.year), 'ctl00$ContentPlaceHolder1$ddlMinistry': '0',
                         'ctl00$Bar1$ddlregion': '3', 'ctl00$Bar1$ddlLang': '1', '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlday'})
            page = op.open(urllib.request.Request(LIST, data=urllib.parse.urlencode(form).encode(),
                           headers={'User-Agent': UA, 'Content-Type': 'application/x-www-form-urlencoded'}), timeout=60).read().decode('utf-8', 'ignore')
            # the page echoes the day it served; refuse a page for some other day
            sel = dict((k, re.search(r'<select name="ctl00\$ContentPlaceHolder1\$%s".*?<option selected="selected" value="(\d+)"' % k, page, re.S)) for k in ('ddlday', 'ddlMonth', 'ddlYear'))
            got = tuple(int(m.group(1)) if m else -1 for m in sel.values())
            if got != (day.day, day.month, day.year):
                raise RuntimeError(f'PIB served {got} for {day}')
            return page
        except Exception as e:
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f'PIB listing for {day} failed: {last}')


def listing(day=None):
    """One day's English releases: [(prid, ministry, title)] — the page groups titles under ministry headings."""
    page = listing_page(day)
    out = []
    # split on the ministry headings so each link inherits the ministry above it
    parts = re.split(r'<h3[^>]*>(.*?)</h3>', page, flags=re.S)
    cur = ''
    for i, chunk in enumerate(parts):
        if i % 2 == 1:
            cur = html.unescape(re.sub(r'<[^>]+>', '', chunk)).strip()
            continue
        for m in re.finditer(r'<a[^>]*PRID=(\d+)[^>]*>(.*?)</a>', chunk, re.S):
            title = html.unescape(re.sub(r'<[^>]+>', '', m.group(2))).strip()
            if title:
                out.append((m.group(1), cur, re.sub(r'\s+', ' ', title)))
    return out


def body_text(prid):
    try:
        b = get(BODY % prid, timeout=45, retries=2)
    except Exception:
        return ''
    b = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', b, flags=re.S | re.I)
    m = re.search(r'<div[^>]*class="[^"]*innner-page-main-about-us-content-right-part[^"]*"[^>]*>(.*)', b, re.S)
    b = m.group(1) if m else b
    t = html.unescape(re.sub(r'<[^>]+>', ' ', b))
    return re.sub(r'\s+', ' ', t)[:6000]


def process_day(rels, tm, themes, a):
    """Gate one day's releases. -> (kept, unmapped, dropped, opened)"""
    kept, unmapped, opened = [], [], 0
    dropped = {'noise': 0, 'backgrounder': 0, 'no decision verb': 0, 'no theme': 0, 'too small': 0}
    seen_titles = set()
    for prid, ministry, title in rels:
        # PIB posts one decision under several ministries with the same title (Cabinet + MNRE): keep the first.
        tkey = re.sub(r'\s+', ' ', title).strip().lower()
        if tkey in seen_titles:
            dropped['duplicate'] = dropped.get('duplicate', 0) + 1
            continue
        seen_titles.add(tkey)
        if NOISE.search(title):
            dropped['noise'] += 1
            continue
        if BACKGROUNDER.search(title):
            dropped['backgrounder'] += 1
            continue
        verb = DECISION.search(title)
        if not verb:
            dropped['no decision verb'] += 1
            continue
        hit = [k for k, (v, rx) in themes.items() if rx.search(title)]
        theme_in = 'title' if hit else None
        body_hint = []
        text = title
        amt, amt_text, amt_in = *amount_hit(title), 'title'
        if opened < a.max_bodies and (not hit or amt is None):
            text = title + ' ' + body_text(prid)
            opened += 1
            # the title names the programme; the body is read for the amount and, only when the title named no
            # sector, for the sector. Body-wide matching tagged GEC-III 'solar' from the phrase "non-solar hour".
            # 2026-11-04: a sector found ONLY in the body was wrong 5 times in 6 over 14 days (a farm-app launch
            # tagged green hydrogen, a PM speech tagged solar). The body may suggest a sector; it never attaches
            # stocks. Such a release goes to the unmapped list with the hint shown.
            body_hint = [] if hit else [k for k, (v, rx) in themes.items() if rx.search(text)]
            if amt is None:
                amt, amt_text = amount_hit(text)
                amt_in = 'body' if amt is not None else None
        mandate = MANDATE.search(text)
        if not mandate and (amt is None or amt < a.min_cr):
            dropped['too small'] += 1
            continue
        # Why it passed, from the file itself: the verb, the figure and where the figure was found.
        why = dict(verb=verb.group(0), amount_text=amt_text, amount_in=amt_in,
                   mandate=mandate.group(0) if mandate else None, theme_in=theme_in)
        if not hit:
            # big enough to matter but in a sector we have no beneficiary map for. Never drop this
            # silently: the run researches it from the universe and adds the names to theme_map.json.
            unmapped.append(dict(prid=prid, ministry=ministry, title=title,
                                 url=f'https://pib.gov.in/PressReleasePage.aspx?PRID={prid}',
                                 outlay_cr=amt, mandate=bool(mandate), why=why,
                                 body_mentions=[tm['themes'][k]['name'] for k in body_hint]))
            dropped['no theme'] += 1
            continue
        names = []
        for k in hit:
            for c in tm['themes'][k]['companies']:
                names.append(dict(c, theme=k))
        seen, uniq = set(), []
        for c in sorted(names, key=lambda c: -(c.get('n_order_filings') or 0)):
            if c['symbol'] in seen:
                continue
            seen.add(c['symbol'])
            uniq.append(c)
        kept.append(dict(prid=prid, ministry=ministry, title=title,
                         url=f'https://pib.gov.in/PressReleasePage.aspx?PRID={prid}',
                         themes=hit, theme_names=[tm['themes'][k]['name'] for k in hit],
                         history=[tm['themes'][k]['history'] for k in hit],
                         outlay_cr=amt, mandate=bool(mandate), why=why,
                         small_caps=[c for c in uniq if c.get('mcap_cr') and 200 <= c['mcap_cr'] <= 7500][:15],
                         companies=uniq[:40]))
    kept.sort(key=lambda r: -(r['outlay_cr'] or 0))
    return kept, unmapped, dropped, opened


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--min-cr', type=float, default=1000.0, help='rupee outlay below which a release is noise')
    ap.add_argument('--max-bodies', type=int, default=25, help='how many candidate releases to open in full, per day')
    ap.add_argument('--date', help='read this day (YYYY-MM-DD) instead of today; govt.json is then left alone')
    ap.add_argument('--backfill-days', type=int, default=14, help='also read any of the last N days the history has not read')
    ap.add_argument('--keep-days', type=int, default=120, help='history window kept in govt_history.json')
    a = ap.parse_args()

    tm = json.load(open(os.path.join(DOCS, 'theme_map.json')))
    # A keyword must START a word: bare 'port' matched 'support' (every MSP release was tagged shipbuilding on
    # 2026-09-30) and 'hal' matched 'shall'. Suffixes stay open so 'railway' still finds 'railways'; a keyword
    # that must also END a word carries its own \\b in theme_map.json (ports?\\b, hal\\b).
    themes = {k: (v, re.compile(r'\b(?:' + v['keywords'] + ')', re.I)) for k, v in tm['themes'].items()}

    hfn = os.path.join(DOCS, 'govt_history.json')
    hist = {'days': {}, 'releases': {}}
    if os.path.exists(hfn):
        try:
            hist = json.load(open(hfn))
        except Exception:
            pass
    stamp = ist.stamp()
    today = datetime.date.fromisoformat(stamp[:10])
    if a.date:
        todo = [datetime.date.fromisoformat(a.date)]
    else:
        todo = [today] + [today - datetime.timedelta(days=i) for i in range(1, a.backfill_days + 1)
                          if (today - datetime.timedelta(days=i)).isoformat() not in hist['days']]

    today_out, failed = None, []
    for day in todo:
        is_today = (day == today and not a.date)
        try:
            rels = listing(None if is_today else day)
        except Exception as e:
            failed.append(day.isoformat())
            print(f'govt: PIB unreachable for {day} ({str(e)[:130]}); that day stays UNREAD and is retried next run.')
            continue
        kept, unmapped, dropped, opened = process_day(rels, tm, themes, a)
        ds = day.isoformat()
        # a day is only marked read once the day is over; today is re-read by every run
        hist['days'][ds] = dict(scanned=len(rels), kept=len(kept), unmapped=len(unmapped), opened=opened,
                                read_at=stamp, final=day < today)
        for r in kept + unmapped:
            hist['releases'][r['prid']] = dict(r, date=ds, unmapped=r in unmapped)
        print(f'govt {ds}: {len(rels)} releases scanned, {opened} opened in full, {len(kept)} kept, {len(unmapped)} big but unmapped  (dropped: {dropped})')
        for r in unmapped:
            print(f'  [UNMAPPED Rs {r["outlay_cr"] or 0:,.0f} cr] {r["title"][:90]}')
        for r in kept:
            amt = f'Rs {r["outlay_cr"]:,.0f} cr' if r['outlay_cr'] else ('mandate' if r['mandate'] else '-')
            print(f'  [{amt}] {r["ministry"][:28]:28s} {r["title"][:80]}  themes {r["themes"]} ({r["why"]["theme_in"]})')
        if is_today:
            today_out = dict(built=stamp, source='Press Information Bureau, English releases (pib.gov.in/allRel.aspx)',
                             gate=f'a decision verb in the title, a mapped sector, and either a procurement mandate or an outlay of at least Rs {a.min_cr:.0f} cr',
                             scanned=len(rels), opened=opened, kept=len(kept), dropped=dropped, releases=kept,
                             unmapped_releases=unmapped)
    # a day with "final": false (today, read before it ended) is re-read by the next run's backfill
    for d, v in list(hist['days'].items()):
        if not v.get('final') and d < today.isoformat():
            hist['days'].pop(d)
    cut = (today - datetime.timedelta(days=a.keep_days)).isoformat()
    hist['days'] = {d: v for d, v in sorted(hist['days'].items()) if d >= cut}
    hist['releases'] = {k: v for k, v in sorted(hist['releases'].items(), key=lambda kv: (kv[1]['date'], kv[0])) if v['date'] >= cut}
    hist.update(built=stamp, source='Press Information Bureau, English releases, one listing per day (allRel.aspx date picker)',
                unread_days=failed, keep_days=a.keep_days)
    json.dump(hist, open(hfn, 'w'), indent=1, ensure_ascii=False)
    if today_out is not None:
        json.dump(today_out, open(os.path.join(DOCS, 'govt.json'), 'w'), indent=1, ensure_ascii=False)
    elif not a.date:
        # PIB is blocked outright on some networks (2026-09-23/24). Leave govt.json exactly as it is -
        # a stale file the page can detect beats a half-written one - and say so in the run log.
        print('govt: today could not be read; docs/ideas/govt.json LEFT UNCHANGED. The government lane did NOT run for today.')
    print(f'govt: history now {len(hist["days"])} days read, {len(hist["releases"])} releases kept; unread this run: {failed or "none"}')


if __name__ == '__main__':
    main()
