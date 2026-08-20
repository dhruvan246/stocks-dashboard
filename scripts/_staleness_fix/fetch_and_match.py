#!/usr/bin/env python3
"""Staleness campaign (DATA_RUNBOOK §102/§103, PLAN_QUANTMAC_FIXES.md P1/P2) — for every symbol
in target_list.json, fetch its full BSE announcement history over the target date span and
match each target quarter-end to its REAL filing date, so the quarter-end+45d placeholder
(apply_agg_pat_fills.py's CONVENTION default) can be replaced with the truth.

v2 (2026-08-20, P1 pilot): the v1 FILESTATUS='U' filter is WRONG and was never run at scale —
a dedicated verification pass found 4 of 5 genuine pre-2015 results disclosures carry
FILESTATUS='N', and 'U' shows up on unrelated rows (e.g. insider-trading disclosures). Dropped.

Method, per symbol:
1. Query AnnSubCategoryGetData with strCat=-1 (ALL categories) — the 'Result' category tag
   itself is unreliable pre-~2012 (HINDUNILVR's genuine 2009 result disclosure is tagged
   CATEGORYNAME='Others'; by 2013 CANFINHOME's is correctly 'Result' — the boundary is fuzzy,
   so never filter on it, only use it as a confidence signal).
2. Candidate = 'result' in NEWSSUB.lower() (fallback HEADLINE) AND an "ended <date>" phrase
   extracts AND the text does NOT match an intimation phrase (a future-tense board-meeting
   notice for the SAME quarter, not the disclosure itself — confirmed via CANFINHOME/DHANI
   verification: intimations say "will be held", not "has announced"/"informed BSE about").
3. Extract the quarter-end from NEWSSUB (fallback HEADLINE), handling both "Month Day, Year"
   and "DDth Month, Year" forms.
4. Match against target (qe, basis) pairs for that symbol. Multiple candidates for the same
   qe (e.g. a same-quarter "Updates on Financial Results" follow-up days later, confirmed via
   CANFINHOME 2012-12-31: real disclosure Jan-19, an "Updates" row Jan-21): prefer one whose
   text names the matching basis (standalone/consolidated); otherwise take the EARLIEST (first
   public disclosure is what a backtest could have seen; a later "Updates" row is a restatement
   the market already had the substance of).
5. No match -> left alone, recorded as not-found. NEVER guess.

Scripcode resolution: bse_scrips.json['by_id'] first; falls back to scripts/_bse_master_all.json
(scrip_id field) for symbols absent from the first map — confirmed needed for ALFALAVAL/DHANI,
both of which sit only in the master list under their era name (DHANI filed as "Indiabulls
Securities" in 2014).

Resumable: skips symbols already present in the output file.

Output: scripts/_staleness_fix/fetch_results.json
  { SYMBOL: { "matches": {"qe|basis": [newsDt, newssub]}, "candidates_seen": N,
              "scripcode": N or null, "scripcode_src": "by_id"|"master_all"|null,
              "error": str or null } }
"""
import json, re, os, sys, time, datetime
import urllib.request, urllib.error, gzip, http.cookiejar

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
TARGET_LIST = os.path.join(HERE, 'target_list.json')
OUT_PATH = os.path.join(HERE, 'fetch_results.json')
PROGRESS_PATH = os.path.join(HERE, 'progress.log')

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'
MONTHS = {m.lower(): i for i, m in enumerate(
    ['January','February','March','April','May','June','July','August','September','October','November','December'], 1)}
MONTHS.update({m.lower()[:3]: i for i, m in enumerate(
    ['January','February','March','April','May','June','July','August','September','October','November','December'], 1)})

INTIMATION_PHRASES = [
    'will be held', 'shall be held', 'is scheduled to be held', 'opted to submit',
    'intimation for consideration', 'board meeting intimation',
]

def _req(u, ref='https://www.bseindia.com/corporates/ann.html'):
    return urllib.request.Request(u, headers={'User-Agent': UA, 'Accept': '*/*',
                                               'Referer': ref, 'Origin': 'https://www.bseindia.com'})

_opener = None
def opener():
    global _opener
    if _opener is None:
        _opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        _opener.open(_req('https://www.bseindia.com/'), timeout=30).read()
    return _opener

def get(u, timeout=70):
    r = opener().open(_req(u), timeout=timeout)
    raw = r.read()
    if r.headers.get('Content-Encoding') == 'gzip':
        raw = gzip.decompress(raw)
    return raw.decode('utf-8', 'replace')

def get_json_retry(u, attempts=5):
    last = None
    for i in range(attempts):
        try:
            return json.loads(get(u))
        except Exception as e:
            last = e
            time.sleep(6 * (i + 1))
    raise last

DATE_RE = re.compile(
    r'ended\s+(?:(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)|([A-Za-z]+)\s+(\d{1,2})),?\s*(\d{4})',
    re.IGNORECASE)

def extract_qe(text):
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    if m.group(2):
        day, mon_name, year = int(m.group(1)), m.group(2).lower(), int(m.group(5))
    else:
        mon_name, day, year = m.group(3).lower(), int(m.group(4)), int(m.group(5))
    mon = MONTHS.get(mon_name)
    if not mon:
        return None
    try:
        datetime.date(year, mon, day)
    except ValueError:
        return None
    return year * 10000 + mon * 100 + day

def is_candidate(row):
    sub = (row.get('NEWSSUB') or '')
    head = (row.get('HEADLINE') or '')
    text = (sub + ' ' + head).lower()
    if 'result' not in text:
        return False
    if any(p in text for p in INTIMATION_PHRASES):
        return False
    return True

def load_scripcode_maps():
    by_id = json.load(open(os.path.join(ROOT, 'scripts', 'bse_scrips.json')))['by_id']
    master = json.load(open(os.path.join(ROOT, 'scripts', '_bse_master_all.json')))
    master_by_scripid = {}
    for row in master:
        sid = row.get('scrip_id')
        cd = row.get('SCRIP_CD')
        if sid and cd and sid not in master_by_scripid:
            try:
                master_by_scripid[sid] = int(cd)
            except (TypeError, ValueError):
                pass
    return by_id, master_by_scripid

def resolve_scripcode(sym, by_id, master_by_scripid):
    if sym in by_id:
        return by_id[sym], 'by_id'
    if sym in master_by_scripid:
        return master_by_scripid[sym], 'master_all'
    return None, None

def fetch_symbol_rows(scripcode, d1, d2):
    out = []
    page = 1
    while True:
        u = ('https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?'
             f'pageno={page}&strCat=-1&strPrevDate={d1}&strToDate={d2}'
             f'&strScrip={scripcode}&strSearch=P&strType=C&subcategory=-1')
        j = get_json_retry(u)
        tbl = j.get('Table', []) or []
        t1 = j.get('Table1', [])
        total = (t1[0].get('ROWCNT') if t1 else 0) or 0
        for row in tbl:
            if is_candidate(row):
                out.append(row)
        if not tbl or page * 50 >= total:
            break
        page += 1
    return out

def match_targets(rows, targets):
    by_qe = {}
    for row in rows:
        qe = extract_qe(row.get('NEWSSUB')) or extract_qe(row.get('HEADLINE'))
        if qe is None:
            continue
        by_qe.setdefault(qe, []).append(row)
    matches = {}
    for qe, basis, _old in targets:
        cands = by_qe.get(qe)
        if not cands:
            continue
        key = f'{qe}|{basis}'
        basis_word = 'standalone' if basis == 'std' else 'consolidated'
        named = [r for r in cands if basis_word in ((r.get('NEWSSUB') or '') + (r.get('HEADLINE') or '')).lower()]
        pool = named if named else cands
        best = min(pool, key=lambda r: r.get('NEWS_DT') or '9999')
        matches[key] = [best.get('NEWS_DT'), best.get('NEWSSUB')]
    return matches

def main(target_list_path=TARGET_LIST, out_path=OUT_PATH, progress_path=PROGRESS_PATH, limit=None):
    targets = json.load(open(target_list_path))
    by_id, master_by_scripid = load_scripcode_maps()
    results = json.load(open(out_path)) if os.path.exists(out_path) else {}
    symbols = list(targets.keys())
    if limit:
        symbols = symbols[:limit]
    done = 0
    t0 = time.time()
    for sym in symbols:
        if sym in results:
            continue
        rows_targets = targets[sym]
        scripcode, src = resolve_scripcode(sym, by_id, master_by_scripid)
        entry = {'scripcode': scripcode, 'scripcode_src': src, 'candidates_seen': 0, 'matches': {}, 'error': None}
        if scripcode is None:
            entry['error'] = 'no-scripcode'
        else:
            qes = [r[0] for r in rows_targets]
            d1 = (datetime.date(qes[0] // 10000, (qes[0] // 100) % 100, qes[0] % 100)
                  - datetime.timedelta(days=10)).strftime('%Y%m%d')
            d2 = (datetime.date(qes[-1] // 10000, (qes[-1] // 100) % 100, qes[-1] % 100)
                  + datetime.timedelta(days=120)).strftime('%Y%m%d')
            try:
                rows = fetch_symbol_rows(scripcode, d1, d2)
                entry['candidates_seen'] = len(rows)
                entry['matches'] = match_targets(rows, rows_targets)
            except Exception as e:
                entry['error'] = f'{type(e).__name__}: {e}'
        results[sym] = entry
        done += 1
        if done % 10 == 0 or done == len(symbols):
            json.dump(results, open(out_path, 'w'))
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            remaining = len(symbols) - done
            eta_min = (remaining / rate / 60) if rate > 0 else -1
            with open(progress_path, 'a') as f:
                f.write(f'{datetime.datetime.now().isoformat()} done={done}/{len(symbols)} '
                        f'rate={rate:.2f}/s eta_min={eta_min:.1f}\n')
        time.sleep(0.3)
    json.dump(results, open(out_path, 'w'))
    with open(progress_path, 'a') as f:
        f.write(f'{datetime.datetime.now().isoformat()} COMPLETE done={len(results)}\n')

if __name__ == '__main__':
    main()
