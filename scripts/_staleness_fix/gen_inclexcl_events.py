#!/usr/bin/env python3
"""Parse NSE's IndexInclExcl.xls (Nifty 500 sheet) into scripts/_n500_inclexcl_events.json —
the authoritative dated inclusion/exclusion register build_membership_v2.py merges in.

WHY (PLAN_QUANTMAC_FIXES.md finding 1, DATA_RUNBOOK §102): scripts/_changelog.json holds only
74 events starting 2015-03-23, so the backward membership walk never rolls pre-2015 joiners out
of the past — 24 quantmac-flagged trades entered stocks that NSE had excluded years earlier
(SANDESH excluded 2009-03-27 still screenable in 2010; PCBL excluded 2002-01-17 screenable in
2017). This file is NSE's own register: 2,495 dated events, 1998-08-01 .. 2020-09-14, one sheet
per index; we parse Nifty 500.

Name→symbol mapping: exact normalized-name match against search_index / NSE EQUITY_L / BSE
master (fed only with names whose scrip_id exists in our fundamentals), then difflib fuzzy at
0.90 MINUS an explicit blacklist (fuzzy produced provably-wrong pairs: "Indian Petrochemicals
Corporation"→IGPL and "BPL Engineering"→HBLENGINE are DIFFERENT companies — a wrong mapping
injects a dead company's exclusion onto a live symbol, worse than no mapping), plus manual
entries for names whose current ticker drifted (PCBL, CRESTANI, STYRENIX, SMLMAH).

Unmapped names are RECORDED, not guessed: mostly 1998-2005 era companies outside our tracked
universe. Their missing exclusions can leave ghosts only for symbols we don't track.

Output: {"events": [[iso_date, symbol, "inc"|"exc"], ...] (mapped only, current-name space),
         "name_map": {xls_name: symbol}, "unmapped": [names...], "source": "..."}
"""
import json, re, os, csv, difflib, collections
import xlrd, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
OUT = os.path.join(SCRIPTS, '_n500_inclexcl_events.json')

# fuzzy pairs verified WRONG by hand (different companies) — never map these
FUZZY_BLACKLIST = {
    'Indian Petrochemicals Corporation Ltd.',   # IPCL (merged into RIL) != IGPL (I G Petrochemicals)
    'BPL Engineering Ltd.',                     # != HBL Engineering
    'BIL Industries Ltd.',                      # != ITL Industries
}
# current-ticker drift the automatic passes cannot see (each verified against the xls rows +
# our rename map by hand, 2026-08-23)
MANUAL = {
    'Phillips Carbon Black Ltd.': 'PCBL',
    'Crest Animation Studios Ltd.': 'CRESTANI',
    'INEOS Styrolution India Ltd.': 'STYRENIX',
    'Styrolution ABS (India) Ltd.': 'STYRENIX',
    'SML Isuzu Ltd.': 'SMLMAH',
}


def norm(x):
    x = (x or '').lower()
    x = re.sub(r'\(.*?\)', ' ', x)
    x = x.replace('pharmaceuticals', 'pharma').replace('laboratories', 'lab')
    x = re.sub(r'\b(ltd|limited|india|indian|the|company|co|corp|corporation|pvt|private|and|&|of)\b', ' ', x)
    return re.sub(r'[^a-z0-9]', '', x)


def parse_date(v, datemode):
    if isinstance(v, float):
        return datetime.datetime(*xlrd.xldate_as_tuple(v, datemode)).date().isoformat()
    s = str(v).strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    m = re.match(r'^(\d{1,2})-(\d{1,2})-(\d{4})$', s)
    if m:   # NSE prints DD-MM-YYYY (verified: PCBL "17-01-2002" == the known 2002-01-17 exclusion)
        return f'{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}'
    return None


def main():
    wb = xlrd.open_workbook(os.path.join(HERE, 'IndexInclExcl.xls'))
    sh = wb.sheet_by_name('Nifty 500')
    raw = []
    for r in range(1, sh.nrows):
        d = parse_date(sh.cell_value(r, 1), wb.datemode)
        name = str(sh.cell_value(r, 2)).strip()
        desc = str(sh.cell_value(r, 3)).strip().lower()
        if d and name:
            raw.append((d, name, 'inc' if 'inclusion' in desc else 'exc'))
    print(f'xls rows parsed: {len(raw)}')

    cand = {}
    def feed(name, sym, src):
        k = norm(name)
        if k and k not in cand:
            cand[k] = (sym, src)
    for row in json.load(open(os.path.join(ROOT, 'docs', 'search_index.json')))['s']:
        feed(row[1], row[0], 'search_index')
    for r in csv.DictReader(open(os.path.join(HERE, 'nse_equity_l.csv'))):
        feed(r['NAME OF COMPANY'], r['SYMBOL'].strip(), 'nse')
    fund_syms = set(json.load(open(os.path.join(SCRIPTS, 'fundamentals.json'))).keys())
    for r in json.load(open(os.path.join(SCRIPTS, '_bse_master_all.json'))):
        sid = r.get('scrip_id')
        if sid and sid in fund_syms:
            for f in ('Scrip_Name', 'Issuer_Name'):
                feed(r.get(f), sid, 'bse_master')

    keys = list(cand.keys())
    names = sorted({x[1] for x in raw})
    name_map, unmapped, fuzzy_used = {}, [], []
    for n in names:
        if n in MANUAL:
            name_map[n] = MANUAL[n]; continue
        k = norm(n)
        if k in cand:
            name_map[n] = cand[k][0]; continue
        if n in FUZZY_BLACKLIST:
            unmapped.append(n); continue
        hits = difflib.get_close_matches(k, keys, n=1, cutoff=0.90)
        if hits:
            name_map[n] = cand[hits[0]][0]; fuzzy_used.append((n, cand[hits[0]][0]))
        else:
            unmapped.append(n)
    print(f'names: {len(names)}  mapped: {len(name_map)} ({len(fuzzy_used)} fuzzy)  unmapped: {len(unmapped)}')

    events = sorted([d, name_map[n], k] for d, n, k in raw if n in name_map)
    # sanity: no symbol may have two OPPOSITE events on the same date
    bad = [g for g, c in collections.Counter((d, s) for d, s, _ in events).items() if c > 1]
    for d, s in bad:
        ks = {k for dd, ss, k in events if dd == d and ss == s}
        if len(ks) > 1:
            print(f'  ⚠️ CONFLICT same-day inc+exc: {s} {d} — dropping both (ambiguous)')
            events = [e for e in events if not (e[0] == d and e[1] == s)]
    json.dump({'events': events, 'name_map': name_map, 'unmapped': unmapped,
               'source': 'NSE IndexInclExcl.xls (saved 2020-09-22), Nifty 500 sheet, parsed 2026-08-23'},
              open(OUT, 'w'), indent=0)
    print(f'wrote {OUT}: {len(events)} mapped events')


if __name__ == '__main__':
    main()
