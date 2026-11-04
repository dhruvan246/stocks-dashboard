"""Government X (Twitter) accounts as a second reader of decisions, so a decision PIB's list missed is still seen.

x.com itself refuses an unauthenticated reader (HTTP 402, 2026-11-04), so posts are read through FxTwitter's
public API (api.fxtwitter.com/2/profile/<handle>/statuses, an unofficial mirror; 20 posts a page with a cursor).
Each account is read back to --days or --max-pages, whichever comes first.

A post is kept when it reads like a decision (govt.py's DECISION verbs, not its NOISE list) and carries an outlay
of at least --min-cr, a procurement mandate, or a mapped sector word. Posts never attach stocks: they are short
and promotional, and a sector guessed from words outside a release title was wrong 5 times in 6 (govt.py,
2026-11-04). Instead each kept post is matched to the PIB lane (govt_history.json) by date (same or next IST day)
and either the same rupee outlay or most of the same significant words; the PIB card carries the companies. A
post with no match is flagged 'not in the PIB lane' — the reason this reader exists.

Usage: python3 scripts/ideas/govt_x.py [--days 3] [--max-pages 4] [--keep-days 30]
Writes docs/ideas/govt_x.json  {built, accounts:{handle:{ok,posts,error}}, posts:{id:{handle,date,text,url,...}}}
"""
import argparse, datetime, json, os, re, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ist
from govt import UA, DECISION, NOISE, MANDATE, amount_hit
DOCS = os.path.join(HERE, '..', '..', 'docs', 'ideas')
OUT = os.path.join(DOCS, 'govt_x.json')
API = 'https://api.fxtwitter.com/2/profile/%s/statuses'

# Each handle was opened on FxTwitter on 2026-11-04 and its profile name and follower count checked; BEL_CorpCom is
# the account bel-india.in links to. Not listed: Mines, SEBI and Jal Shakti — no handle tried resolved to an account
# their own site or ministry names (SEBI_updates exists but sebi.gov.in links no X account).
HANDLES = ['narendramodi', 'PIB_India', 'MIB_India', 'nsitharaman', 'DefenceMinIndia', 'rajnathsingh',
           'SpokespersonMoD', 'RailMinIndia', 'AshwiniVaishnaw', 'GoI_MeitY', 'MinOfPower', 'mnreindia',
           'CoalMinistry', 'DPIITGoI', 'dgftindia', 'cbic_india', 'PiyushGoyal', 'MORTHIndia', 'nitin_gadkari',
           'shipmin_india', 'MoCA_GoI', 'DoT_India', 'TRAI', 'RBI', 'NITIAayog', 'InvestIndia', 'PetroleumMin',
           'HardeepSPuri', 'MoHUA_India', 'DrSJaishankar', 'INSPACeIND', 'isro', 'HALHQBLR', 'GeM_India',
           'PMOIndia', 'FinMinIndia', 'SteelMinIndia', 'MoHFW_INDIA', 'DoC_GoI', 'BEL_CorpCom']

# A sector word alone needs a hard decision verb: on 2026-11-04 'policy', 'mission', 'launch' and 'clear' with a
# sector word kept 25 event and summit posts (RE Summit panels, a HAL PF-trust award, a Transport Mela).
STRONG = re.compile(r'\b(cabinet|approv(?:es|ed|al)|sanction(?:s|ed)|allocat(?:es|ed)|award(?:s|ed) (?:a |the )?contract|notifie[sd]|'
                    r'signs? (?:a |an )?(?:contract|agreement)|contract (?:worth|for))\b', re.I)


def keep(text, min_cr, themes):
    """-> (verb, amount, amount_text, mandate, theme keys) or None"""
    if NOISE.search(text):
        return None
    verb = DECISION.search(text)
    amt, amt_text = amount_hit(text)
    mandate = MANDATE.search(text)
    hit = [k for k, rx in themes.items() if rx.search(text)]
    strong = STRONG.search(text)
    if not verb or not ((amt and amt >= min_cr) or mandate or (strong and hit)):
        return None
    return (strong or verb).group(0), amt, amt_text, mandate.group(0) if mandate else None, hit


STOP = set('the a an of to in for and on with by at from is are be as this that its our india indian government '
           'cabinet approves approved scheme crore rs lakh will has have new under its it'.split())


def get_json(url, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=45) as f:
                return json.loads(f.read().decode('utf-8', 'ignore'))
        except Exception as e:
            last = e
            time.sleep(2 * (i + 1))
    raise RuntimeError(f'GET failed {url}: {last}')


def ist_day(created):
    t = datetime.datetime.strptime(created, '%a %b %d %H:%M:%S %z %Y')
    return t.astimezone(ist.IST).strftime('%Y-%m-%d')


def words(t):
    return {w for w in re.findall(r'[a-z][a-z0-9-]{2,}', (t or '').lower()) if w not in STOP}


def read_account(h, since, max_pages):
    posts, cursor = [], None
    for _ in range(max_pages):
        d = get_json(API % h + (f'?cursor={cursor}' if cursor else ''))
        res = d.get('results') or []
        for p in res:
            if p.get('reposted_by') or not p.get('created_at'):
                continue
            posts.append(p)
        cursor = (d.get('cursor') or {}).get('bottom')
        if not res or not cursor or ist_day(res[-1]['created_at']) < since:
            break
        time.sleep(1)
    return posts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=3)
    ap.add_argument('--max-pages', type=int, default=4)
    ap.add_argument('--keep-days', type=int, default=30)
    ap.add_argument('--min-cr', type=float, default=1000)
    ap.add_argument('--only', help='comma list of handles (testing)')
    a = ap.parse_args()
    tm = json.load(open(os.path.join(DOCS, 'theme_map.json'), encoding='utf-8'))
    themes = {k: re.compile(r'\b(?:' + v['keywords'] + ')', re.I) for k, v in tm['themes'].items()}
    hist = {}
    try:
        hist = json.load(open(os.path.join(DOCS, 'govt_history.json'), encoding='utf-8'))
    except Exception:
        pass
    pib = list((hist.get('releases') or {}).values())
    old = {}
    try:
        old = json.load(open(OUT, encoding='utf-8'))
    except Exception:
        pass
    posts = old.get('posts', {})
    accounts = {}
    since = (ist.today() - datetime.timedelta(days=a.days)).isoformat()
    for h in (a.only.split(',') if a.only else HANDLES):
        try:
            got = read_account(h, since, a.max_pages)
            accounts[h] = dict(ok=True, posts=len(got), read_at=ist.stamp())
        except Exception as e:
            got = []
            prev = old.get('accounts', {}).get(h, {})
            accounts[h] = dict(ok=False, posts=0, error=str(e)[:200], last_ok=prev.get('read_at') if prev.get('ok') else prev.get('last_ok'))
            print(f'{h}: FAILED {e}', file=sys.stderr)
        kept = 0
        for p in got:
            text = p.get('text') or ''
            day = ist_day(p['created_at'])
            g = keep(text, a.min_cr, themes) if day >= since else None
            if not g:
                continue
            verb, amt, amt_text, mandate, hit = g
            posts[p['id']] = dict(handle=h, date=day, created=p['created_at'], url=p.get('url'), text=text[:700],
                                  verb=verb, amount_cr=amt, amount_text=amt_text, mandate=mandate,
                                  mentions=[tm['themes'][k]['name'] for k in hit])
            kept += 1
        print(f'{h}: {len(got)} posts read, {kept} kept')
        time.sleep(1)

    posts = {k: v for k, v in posts.items() if keep(v['text'], a.min_cr, themes)}   # stored posts face today's gate too
    # match every stored post to the PIB lane: same or next IST day, and the same outlay or most of the words
    for p in posts.values():
        d0 = datetime.date.fromisoformat(p['date'])
        near = [r for r in pib if r.get('date') and abs((datetime.date.fromisoformat(r['date']) - d0).days) <= 1]
        pw, best = words(p['text']), None
        for r in near:
            same_amt = p.get('amount_cr') and r.get('outlay_cr') and abs(p['amount_cr'] - r['outlay_cr']) <= 0.01 * r['outlay_cr']
            rw = words(r.get('title'))
            overlap = len(pw & rw) / max(1, len(rw))
            score = 1.0 if same_amt else overlap
            if score >= 0.5 and (best is None or score > best[0]):
                best = (score, r)
        p['pib'] = dict(title=best[1].get('title'), url=best[1].get('url'), date=best[1].get('date'),
                        how='same outlay' if best[0] == 1.0 and p.get('amount_cr') else f'{round(best[0] * 100)}% of title words') if best else None

    cut = (ist.today() - datetime.timedelta(days=a.keep_days)).isoformat()
    posts = {k: v for k, v in posts.items() if v['date'] >= cut}
    for h, v in old.get('accounts', {}).items():
        accounts.setdefault(h, v)
    out = dict(built=ist.stamp(), source='api.fxtwitter.com (unofficial X mirror)', days=a.days, accounts=accounts,
               posts=dict(sorted(posts.items(), key=lambda kv: kv[1]['created'] and datetime.datetime.strptime(kv[1]['created'], '%a %b %d %H:%M:%S %z %Y').timestamp(), reverse=True)))
    tmp = OUT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, OUT)
    print(f'wrote {OUT}: {len(posts)} posts, {sum(1 for p in posts.values() if not p["pib"])} not in the PIB lane, '
          f'accounts ok {sum(1 for v in accounts.values() if v.get("ok"))}/{len(accounts)}')


if __name__ == '__main__':
    main()
