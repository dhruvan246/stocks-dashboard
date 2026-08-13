'use strict';
/* ============================================================================
 * scripts/agg_tools/_first_bar.json — SYM -> the date of the symbol's FIRST TRADED BAR.
 *
 * WHY THIS FILE EXISTS
 *   An announce date must never precede the day the stock first traded. The
 *   backtest engine selects a quarter with `q[annIdx] <= dateInt`
 *   (docs/backtest-engine.js), so a stored ann date is a claim about when a
 *   number was PUBLIC — and for a pre-listing quarter the qe+45d convention
 *   (runbook §52, apply_agg_pat_fills.py) fabricates one. SBICARD's Mar-2019
 *   quarter was written with ann 2019-05-15 against a first bar of 2020-03-16:
 *   ten months of claimed availability before the company had a tape. §91c is
 *   the same failure from the other side (ann = 0 read as "since the epoch").
 *
 *   The floor is `max(qe+45d, first bar)`:
 *     * it can never CREATE a look-ahead — for an IPO the prospectus is public
 *       before listing and for a demerger the scheme is public before the
 *       record date, so the true public date is at or before the first bar;
 *     * it can never HIDE a cell from a date that mattered — nothing can hold
 *       or screen a stock before its first bar, so no earlier date is reachable.
 *   It is still a CONVENTION, not a filing date, and every ledger entry says so.
 *
 * WHY A SIDECAR AND NOT A LOOKUP
 *   The only full-depth tape is sf_stock_data.bin — ~193 MB gz, past GitHub's
 *   cap, so the in-repo copy is a frozen snapshot and the live one is a release
 *   asset. A python applier cannot afford to parse it; ~120 KB of dates can be
 *   read in a millisecond. ⚠️ docs/dash_slim.bin is NOT a substitute: it is
 *   trimmed to `recentCutoffOff` (measured 2026-08-13: KPITTECH's series there
 *   starts at offset 10931 = 2021-12-31, not its real first bar 2019-04-22), so
 *   reading first bars off it returns the CUTOFF for every symbol — one number
 *   wearing 4,727 disguises.
 *
 *   node scripts/build_first_bar_map.js [--bin auto|<path>] [--out scripts/agg_tools/_first_bar.json]
 * ========================================================================== */
const fs = require('fs');
const path = require('path');
const os = require('os');
const vm = require('vm');
const zlib = require('zlib');
const { execFileSync } = require('child_process');

const ROOT = path.dirname(__dirname);
const DOCS = path.join(ROOT, 'docs');
const RELEASE_BIN = 'https://github.com/dhruvan246/stocks-dashboard/releases/download/data/sf_stock_data.bin';
const ARGV = process.argv.slice(2);
const arg = (n, d) => { const i = ARGV.indexOf('--' + n); return i >= 0 ? ARGV[i + 1] : d; };
const OUT = path.resolve(ROOT, arg('out', 'scripts/agg_tools/_first_bar.json'));
const BIN_ARG = arg('bin', 'auto');
const log = (...a) => console.log('[first-bar]', ...a);

const readGz = p => JSON.parse(zlib.gunzipSync(fs.readFileSync(p)).toString('utf8'));

function resolveBin() {
  if (BIN_ARG !== 'auto') { log('using bin', BIN_ARG); return readGz(path.resolve(ROOT, BIN_ARG)); }
  const cache = path.join(os.tmpdir(), 'sf_stock_data_live.bin');
  let ok = false;
  if (fs.existsSync(cache)) {
    try { zlib.gunzipSync(fs.readFileSync(cache).slice(0, 1024)); } catch (e) { ok = e.code === 'Z_BUF_ERROR'; }
    ok = ok || fs.statSync(cache).size > 50e6;
  }
  if (!ok) {
    log('downloading LIVE release asset (the in-repo bin is a frozen snapshot)…');
    execFileSync('curl', ['-fsSL', '-o', cache, RELEASE_BIN], { stdio: 'inherit' });
  } else {
    log('reusing cached live bin', cache, (fs.statSync(cache).size / 1e6).toFixed(0) + 'MB');
  }
  return readGz(cache);
}

function main() {
  const D = readGz(path.join(DOCS, 'dash_slim.bin'));
  const RAW = resolveBin();
  log('bin end =', RAW.end, '· symbols =', Object.keys(RAW.data).length);

  /* The engine's own _sfNorm decodes the series — never a second copy of that
   * decoder here (the coverage matrix learned the same lesson, §92). */
  const ctx = vm.createContext({
    console, Date, Math, JSON, Object, Array, Number, String, Set, Map, isFinite, parseInt, parseFloat,
    location: { hostname: 'build', protocol: 'file:' },
    localStorage: { getItem: () => null, setItem: () => {} },
    fetch: async () => ({ ok: false, json: async () => ({}) }),
  });
  vm.runInContext(fs.readFileSync(path.join(DOCS, 'backtest-engine.js'), 'utf8'), ctx,
    { filename: 'backtest-engine.js' });
  ctx.__RAW = RAW; ctx.__STARTTS = D.startTs;

  const json = vm.runInContext(`(function(){
    START_TS = __STARTTS;
    const out = {};
    for (const sym in __RAW.data) {
      const r = _sfNorm(__RAW.data[sym], __STARTTS);
      const d = r.ser && r.ser.d;
      if (d && d.length) out[sym] = isoOff(d[0]);
      delete __RAW.data[sym];
    }
    return JSON.stringify(out);
  })()`, ctx);
  const map = JSON.parse(json);
  const n = Object.keys(map).length;
  fs.writeFileSync(OUT, JSON.stringify({
    _doc: 'SYM -> first traded bar (ISO). Measured through the engine\'s own _sfNorm on the LIVE ' +
          'sf_stock_data.bin. Regenerate: node scripts/build_first_bar_map.js. Used as the FLOOR ' +
          'for announce dates written by convention — see the header of this builder and §99.',
    _bin_end: RAW.end,
    _symbols: n,
    first_bar: map,
  }, null, 1) + '\n');
  log(`${n} symbols -> ${path.relative(ROOT, OUT)}`);
  for (const s of ['KPITTECH', 'SBICARD', 'RELIANCE', 'NSLNISP']) log('  ' + s + ' ' + map[s]);
}

main();
