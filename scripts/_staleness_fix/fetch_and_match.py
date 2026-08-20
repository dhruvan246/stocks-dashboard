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

def get_json_retry(u, attempts=3, log=None):
    last = None
    for i in range(attempts):
        try:
            return json.loads(get(u, timeout=30))
        except Exception as e:
            last = e
            if log:
                log(f'    retry {i+1}/{attempts} after {type(e).__name__}: {e}')
            time.sleep(4 * (i + 1))
    raise last

# Anchor word before the date: "ended" (most common) or "for" (INDORAMA's genuine 2009-06-30
# disclosure: "Financial Results for Jun 30, 2009" — no "ended" anywhere). Two date forms, both
# seen live: "ended June 30, 2016" / "ended 31st December, 2022" (month-name) and "ended
# 30.09.2019" (numeric DD.MM.YYYY/DD/MM/YYYY/DD-MM-YYYY, Indian day-month-year order — GEOJITFSL's
# real Sep-2019 disclosure uses ONLY this form).
#
# ⚠️ finditer(), not search() — a combined annual+quarterly announcement carries TWO dates
# ("results for the year ended March 31, 2010 & ... for the quarter ended June 30, 2010", ESSAROIL
# 2010-07-27 real filing): search() silently returns only the FIRST (the annual one), so the
# quarter actually being targeted never matches even though the announcement plainly has it.
# Every one of these three gaps (numeric dates, "for" not just "ended", multi-date text) was found
# in the P1 pilot 2026-08-20 by asking why 2015+/2020+ match rates read LOWER than the older,
# supposedly-worse-covered era — the opposite of what BSE's improving digital records would predict.
DATE_RE_NAMED = re.compile(
    r'(?:ended|for)\s+(?:(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)|([A-Za-z]+)\s+(\d{1,2})),?\s*(\d{4})',
    re.IGNORECASE)
DATE_RE_NUMERIC = re.compile(r'(?:ended|for)\s+(\d{1,2})[./-](\d{1,2})[./-](\d{4})', re.IGNORECASE)

def extract_all_qes(text):
    """-> set of YYYYMMDD ints, every date-after-ended/for phrase found in text."""
    if not text:
        return set()
    out = set()
    for m in DATE_RE_NAMED.finditer(text):
        if m.group(2):
            day, mon_name, year = int(m.group(1)), m.group(2).lower(), int(m.group(5))
        else:
            mon_name, day, year = m.group(3).lower(), int(m.group(4)), int(m.group(5))
        mon = MONTHS.get(mon_name)
        if mon:
            try:
                datetime.date(year, mon, day)
                out.add(year * 10000 + mon * 100 + day)
            except ValueError:
                pass
    for m in DATE_RE_NUMERIC.finditer(text):
        day, mon, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            datetime.date(year, mon, day)
            out.add(year * 10000 + mon * 100 + day)
        except ValueError:
            pass
    return out

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

MAX_PAGES_PER_SYMBOL = 120   # 6,000 rows / ~20yr history at 50/page is generous; a symbol
                             # needing more almost certainly means a MISRESOLVED scripcode
                             # (e.g. landed on an unrelated, high-announcement-volume company)

def fetch_symbol_rows(scripcode, d1, d2, log=None):
    # BSE's endpoint silently breaks when strToDate is even ONE DAY in the future: it returns
    # Table1=None and a single Table row with every field null, instead of an error — found live
    # 2026-08-20 (INGERRAND/TMPV/RAMANEWS/SOUTHWEST all read "0 candidates" this way; the true
    # cause was a future-dated query, not an absence of filings). Never ask past today.
    today = datetime.date.today().strftime('%Y%m%d')
    if d2 > today:
        d2 = today
    out = []
    page = 1
    while True:
        if log:
            log(f'    page {page} (d1={d1} d2={d2})')
        u = ('https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?'
             f'pageno={page}&strCat=-1&strPrevDate={d1}&strToDate={d2}'
             f'&strScrip={scripcode}&strSearch=P&strType=C&subcategory=-1')
        j = get_json_retry(u, log=log)
        tbl = j.get('Table', []) or []
        t1 = j.get('Table1', [])
        # The degenerate-response signature: Table1 missing/empty but Table not empty (one null row).
        # Treat it as a hard failure, never as "zero results" — an empty Table1 with rows present
        # is never a legitimate answer from this endpoint (confirmed: every good response we've
        # seen, including genuinely empty date windows, carries a real Table1 ROWCNT).
        if not t1 and tbl:
            raise RuntimeError(f'degenerate BSE response (Table1 empty, {len(tbl)} null-ish Table '
                                f'rows) at page {page} d1={d1} d2={d2} — likely a future-dated or '
                                f'otherwise malformed query, not a real empty result')
        total = (t1[0].get('ROWCNT') if t1 else 0) or 0
        for row in tbl:
            if is_candidate(row):
                out.append(row)
        if not tbl or page * 50 >= total:
            break
        page += 1
        if page > MAX_PAGES_PER_SYMBOL:
            if log:
                log(f'    ABORT: exceeded {MAX_PAGES_PER_SYMBOL} pages ({total} total rows) — '
                    f'suspected misresolved scripcode, not a real announcement history')
            raise RuntimeError(f'page cap exceeded (total={total} rows)')
    return out

def match_targets(rows, targets):
    by_qe = {}
    for row in rows:
        # union, not first-match-wins: a combined annual+quarterly announcement carries two
        # dates and both are real (see extract_all_qes docstring) — index the row under EVERY
        # date it mentions so whichever one a target actually needs still finds it.
        qes = extract_all_qes(row.get('NEWSSUB')) | extract_all_qes(row.get('HEADLINE'))
        for qe in qes:
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
    def log(msg):
        with open(progress_path, 'a') as f:
            f.write(f'{datetime.datetime.now().isoformat()} {msg}\n')

    done = 0
    t0 = time.time()
    for sym in symbols:
        if sym in results:
            continue
        log(f'START {sym} ({done+1}/{len(symbols)})')
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
                rows = fetch_symbol_rows(scripcode, d1, d2, log=log)
                entry['candidates_seen'] = len(rows)
                entry['matches'] = match_targets(rows, rows_targets)
                log(f'  DONE {sym}: {len(rows)} candidates, {len(entry["matches"])}/{len(rows_targets)} matched')
            except Exception as e:
                entry['error'] = f'{type(e).__name__}: {e}'
                log(f'  FAILED {sym}: {entry["error"]}')
        results[sym] = entry
        done += 1
        if done % 5 == 0 or done == len(symbols):
            json.dump(results, open(out_path, 'w'))
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            remaining = len(symbols) - done
            eta_min = (remaining / rate / 60) if rate > 0 else -1
            log(f'CHECKPOINT done={done}/{len(symbols)} rate={rate:.2f}/s eta_min={eta_min:.1f}')
        time.sleep(0.3)
    json.dump(results, open(out_path, 'w'))
    with open(progress_path, 'a') as f:
        f.write(f'{datetime.datetime.now().isoformat()} COMPLETE done={len(results)}\n')

if __name__ == '__main__':
    main()
