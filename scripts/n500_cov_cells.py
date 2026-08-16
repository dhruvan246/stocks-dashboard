#!/usr/bin/env python3
"""N500 COVERAGE-100 CAMPAIGN — build and check the exhaustive per-stock work queue.

Campaign doc: scripts/N500_COVERAGE_100_CAMPAIGN.md  (Phase 0 tool)

WHAT IT DOES
  Turns "param X is 91.5%" into "these exact symbols, on these exact month-ends".
  Input is `--explain` output from build_coverage_matrix.js, which names the missing
  symbols from the SAME vm scan that wrote the payload — so the queue can never drift
  from the page (§92: measure THROUGH the engine, never re-implement it).

  build:  emits scripts/n500_cov_queue.json — one row per (param, symbol)
  --check: re-asserts every parity gate against the CURRENT payload

PARITY GATE (hard stop, campaign §3)
  For each param:  Σ(queue row month-counts)  ==  payload missing  ==  Σ(den − count)
  where den = members − na, exactly as docs/coverage.html:346-350 computes it.
  Roll members that never reached factorsAt (no price row at all) are carried in the
  explain file's `__norow` bucket and counted against EVERY param — omitting them
  would silently shrink the queue below the page's own numbers.

NO ASSUMPTIONS (campaign golden rule)
  This tool proposes a class ONLY where the proposal is itself a measurement:
    ebit/op/rev -> C1-candidate  when sf_revop holds the slot null in EVERY quarter
                   C4-candidate  when the slot is non-null in at least one quarter
  Everything else is emitted `unclassified`. A proposal is NOT a verdict: `class` stays
  null and `status` stays "open" until Phase 2 records per-name evidence. SPICEJET — an
  airline sitting in the never-has-ebit set — is why no category shortcut is allowed.

USAGE
  python3 scripts/n500_cov_cells.py build \
      [--explain scripts/n500_cov_explain.json] [--out scripts/n500_cov_queue.json]
  python3 scripts/n500_cov_cells.py --check
"""
import argparse, gzip, json, os, re, sys, collections

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, 'docs')
SCRIPTS = os.path.join(ROOT, 'scripts')
FROM = '2020-01-01'
UNIVERSE = 'nifty-500'


def load_payload():
    U = json.load(open(os.path.join(DOCS, 'coverage', f'{UNIVERSE}.json')))
    return U


def payload_missing(U):
    """Per-param missing counts and denominators, page math verbatim."""
    out = {}
    for pi, pk in enumerate(U['paramKeys']):
        na_arr = U['na'][pi]
        miss = den = 0
        per_date = {}
        for di, d in enumerate(U['dates']):
            if d < FROM:
                continue
            mem, c = U['members'][di], U['params'][pi][di]
            if c < 0 or mem < 0:                      # "–" no roll at this date
                continue
            na = na_arr[di] if (isinstance(na_arr, list) and na_arr[di] > 0) else 0
            dn = max(0, mem - na)
            m = max(0, dn - c)
            den += dn
            miss += m
            if m:
                per_date[d] = m
        out[pk] = {'missing': miss, 'den': den, 'per_date': per_date}
    return out


# ---------- sf_revop measurement (the only auto-proposed class) ----------
def revop_evidence():
    """symbol -> {field -> (n_quarters, n_with_value)} straight from sf_revop."""
    REVOP = json.load(open(os.path.join(DOCS, 'sf_revop.json')))
    src = open(os.path.join(DOCS, 'backtest-engine.js')).read()
    m = re.search(r'FUND_ALIAS\s*=\s*(\{.*?\})\s*;', src, re.S)
    alias = json.loads(re.sub(r'(\w+)\s*:', r'"\1":', m.group(1)).replace("'", '"')) if m else {}
    slot = {'rev': (1, 0), 'op': (3, 2), 'ebit': (8, 7)}

    def present(cell, f):
        ci, si = slot[f]
        return (len(cell) > ci and cell[ci] is not None) or (len(cell) > si and cell[si] is not None)

    def stats(sym, f):
        rmap = REVOP.get(sym) or REVOP.get(alias.get(sym, ''), None)
        if not rmap:
            return (0, 0, None)
        n = len(rmap)
        k = sum(1 for c in rmap.values() if present(c, f))
        # alias shadowing: a direct key that wins over an alias key holding MORE data
        shadow = None
        ali = alias.get(sym)
        if ali and sym in REVOP and ali in REVOP:
            d_q = {q for q, c in REVOP[sym].items() if present(c, f)}
            a_q = {q for q, c in REVOP[ali].items() if present(c, f)}
            extra = sorted(a_q - d_q)
            if extra:
                shadow = {'alias': ali, 'quarters': extra}
        return (n, k, shadow)
    return stats


def build(explain_path, out_path):
    ex = json.load(open(explain_path))
    if ex.get('universe') != UNIVERSE:
        sys.exit(f'explain file is for {ex.get("universe")}, expected {UNIVERSE}')
    U = load_payload()
    pm = payload_missing(U)
    rvstats = revop_evidence()

    # ---- invert byDate -> (param, symbol) -> [months]
    cells = collections.defaultdict(lambda: collections.defaultdict(list))
    norow = collections.defaultdict(list)      # symbol -> [months] (missing EVERYTHING)
    for date, params in sorted(ex['byDate'].items()):
        if date < FROM:
            continue
        for pk, syms in params.items():
            if pk == '__norow':
                for s in syms:
                    norow[s].append(date)
                continue
            for s in syms:
                cells[pk][s].append(date)

    rows = []
    for pk in sorted(cells):
        for sym, months in sorted(cells[pk].items()):
            proposed, note = 'unclassified', ''
            if pk in ('rev', 'op', 'ebit'):
                nq, nk, shadow = rvstats(sym, pk)
                if nq == 0:
                    proposed = 'unclassified'
                    note = f'no sf_revop rows for {sym}'
                elif nk == 0:
                    proposed = 'C1-candidate'
                    note = f'{pk} slot null in all {nq} sf_revop quarters — needs a filing read to confirm the format lacks the line'
                else:
                    proposed = 'C4-candidate'
                    note = f'{pk} present in {nk} of {nq} sf_revop quarters'
                if shadow:
                    proposed = 'C4-candidate'
                    note += f" · ALIAS SHADOW: {len(shadow['quarters'])} {pk} quarters live under {shadow['alias']} ({', '.join(shadow['quarters'][:5])})"
            rows.append({
                'param': pk, 'symbol': sym, 'n': len(months), 'months': months,
                'class_proposed': proposed, 'class': None, 'evidence': '',
                'status': 'open', 'ledger': '', 'note': note,
            })

    for sym, months in sorted(norow.items()):
        rows.append({
            'param': '__norow', 'symbol': sym, 'n': len(months), 'months': months,
            'class_proposed': 'unclassified', 'class': None, 'evidence': '',
            'status': 'open', 'ledger': '',
            'note': 'roll member with no factorsAt row — missing EVERY parameter at these dates',
        })

    parity = check_parity(rows, pm, verbose=True)
    doc = {
        'campaign': 'N500_COVERAGE_100',
        'universe': UNIVERSE, 'from': FROM,
        'payload_updated': U['updated'], 'payload_dataEnd': U['dataEnd'],
        'explain_generated': ex.get('generated'),
        'n_rows': len(rows),
        'parity': parity,
        'rows': rows,
    }
    with open(out_path, 'w') as f:
        json.dump(doc, f, indent=1)
    print(f'\nwrote {out_path} · {len(rows)} rows · '
          f'{sum(r["n"] for r in rows if r["param"] != "__norow")} member-date cells')
    return 0 if parity['ok'] else 1


def check_parity(rows, pm, verbose=False):
    """Σ(queue months) + norow contribution == payload missing, per param. Hard stop."""
    norow_dates = collections.Counter()
    for r in rows:
        if r['param'] == '__norow':
            for d in r['months']:
                norow_dates[d] += 1
    by_param = collections.defaultdict(int)
    for r in rows:
        if r['param'] != '__norow':
            by_param[r['param']] += r['n']

    results, ok = {}, True
    gap_params = sorted(p for p, v in pm.items() if v['missing'] > 0)
    for pk in gap_params:
        want = pm[pk]['missing']
        got = by_param.get(pk, 0) + sum(norow_dates.values())
        good = (got == want)
        ok &= good
        results[pk] = {'queue': got, 'payload': want, 'ok': good}
    # a param with zero missing must have zero queue rows
    for pk, n in by_param.items():
        if pm.get(pk, {}).get('missing', 0) == 0 and n:
            results[pk] = {'queue': n, 'payload': 0, 'ok': False}
            ok = False
    if verbose:
        print(f"{'param':14s} {'queue':>8s} {'payload':>8s}  parity")
        for pk in sorted(results):
            r = results[pk]
            print(f"{pk:14s} {r['queue']:8d} {r['payload']:8d}  {'OK' if r['ok'] else 'MISMATCH'}")
        print(f"\nPARITY {'PASS' if ok else 'FAIL'} · {len(gap_params)} params with gaps · "
              f"{sum(pm[p]['missing'] for p in gap_params):,} missing cells total")
    return {'ok': bool(ok), 'params': results}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', nargs='?', default='build', choices=['build'])
    ap.add_argument('--explain', default=os.path.join(SCRIPTS, 'n500_cov_explain.json'))
    ap.add_argument('--out', default=os.path.join(SCRIPTS, 'n500_cov_queue.json'))
    ap.add_argument('--check', action='store_true', help='re-assert parity of the existing queue')
    a = ap.parse_args()
    if a.check:
        q = json.load(open(a.out))
        pm = payload_missing(load_payload())
        r = check_parity(q['rows'], pm, verbose=True)
        return 0 if r['ok'] else 1
    return build(a.explain, a.out)


if __name__ == '__main__':
    sys.exit(main())
