'use strict';
/* ============================================================================
 * Bake docs/coverage/*.json — the payload behind docs/coverage.html (private).
 *
 * WHAT IT MEASURES
 *   For every month-end and every universe: how many of that date's members
 *   carry a usable value for each screening parameter. "Usable" is decided by
 *   the LIVE ENGINE, not by a re-implementation: this script loads
 *   docs/backtest-engine.js verbatim into a Node `vm` context, injects the
 *   same globals loadCore()/loadSF()/loadFund()/loadShp() would have set, and
 *   asks factorsAt() + fieldVal() — the exact accessors a backtest screens on.
 *   So a cell here is "what a strategy could actually see at that date".
 *
 *   Two families come from files the engine does not read (revenue/margins from
 *   sf_revop, sector classification): those are counted here directly and the
 *   rule is documented per-parameter in the payload (`rule`), never guessed at
 *   render time.
 *
 * ENGINE-IN-VM GOTCHA (learned the hard way, keep it):
 *   the engine declares its globals with `let`, which in Node's vm does NOT
 *   attach to the contextified sandbox as a settable property. Inject with an
 *   in-context assignment (`SERIES = …` inside runInContext) — never
 *   `sandbox.SERIES = …`. All runInContext calls against one context share a
 *   single global lexical scope, so this works.
 *
 * DATA FRESHNESS:
 *   docs/sf_stock_data.bin in this repo is a FROZEN snapshot (the real ~193 MB
 *   file is past GitHub's 100 MB cap and is never committed). Default is
 *   therefore --bin auto → download the LIVE release asset. Pass an explicit
 *   --bin path in CI, where a freshly appended bin already exists on disk.
 *
 *   node --max-old-space-size=12288 scripts/build_coverage_matrix.js \
 *        [--bin auto|<path>] [--out docs/coverage] [--from 2002-01] [--dates N]
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

/* ---- args ---- */
const ARGV = process.argv.slice(2);
function arg(name, dflt) { const i = ARGV.indexOf('--' + name); return i >= 0 ? ARGV[i + 1] : dflt; }
const OUT_DIR = path.resolve(ROOT, arg('out', 'docs/coverage'));
const BIN_ARG = arg('bin', 'auto');
const FROM = arg('from', '2002-01');          // true daily bars start 2002-01-02; earlier is weekly
const MAX_DATES = +arg('dates', 0) || 0;      // 0 = all (a small N is the dev/smoke setting)

const log = (...a) => console.log('[coverage]', ...a);

/* ============================================================================
 * PARAMETERS — every screening input the site holds, grouped into the families
 * the page shows as columns. `k` is the engine field name (fieldVal key) unless
 * `src` says otherwise. `rule` is what the page prints when you ask "what does
 * this cell count?" — it is the honest definition, not a label.
 * ========================================================================== */
const FAMILIES = [
  { id: 'price', label: 'Price', note: 'A tradeable bar at the date. Every other column is gated on this one.', params: [
    { k: 'price', rule: 'Close ≤ the rebalance date, with a bar in the last 14 days (28 pre-2002) — the engine\'s entry-freshness gate.' },
    { k: 'chg', rule: '1-month price change % (needs a close 30 days earlier).' },
  ] },
  { id: 'band52', label: '52W band', note: 'Trailing-365d high/low, from true intraday H/L where the bhavcopy carries them.', params: [
    { k: 'd52', rule: 'Distance below the 52w high %.' },
    { k: 'd52_low_pct', rule: 'Distance above the 52w low %.' },
    { k: 'rangePos', rule: '52w range position — null when high == low.' },
    { k: 'daysHigh', rule: 'Days since the 52w high.' },
  ] },
  { id: 'momentum', label: 'Momentum', note: 'Trailing returns — each needs a close that far back.', params: [
    { k: 'ret1m', rule: 'Return over 30 days.' },
    { k: 'ret3m', rule: 'Return over 91 days.' },
    { k: 'ret6m', rule: 'Return over 182 days.' },
    { k: 'ret12m', rule: 'Return over 365 days.' },
    { k: 'accel', rule: 'This month\'s return minus last month\'s (needs 60 days).' },
    { k: 'riskMom', rule: '3m return ÷ volatility (needs both).' },
    { k: 'rsNifty', rule: '6m return minus Nifty\'s 6m return — needs nifty.json to reach back that far.' },
  ] },
  { id: 'trend', label: 'Trend', note: 'Moving averages and the industry ranking built from them.', params: [
    { k: 'dma50', rule: 'Distance from the 50-day SMA %.' },
    { k: 'dma200', rule: 'Distance from the 200-day SMA %.' },
    { k: 'indRank', rule: 'Industry momentum rank 1-10 — assigned to every screened row, so this tracks Price.' },
  ] },
  { id: 'risk', label: 'Risk', note: 'Volatility, beta and drawdown over 90-180 day windows.', params: [
    { k: 'vol', rule: 'Annualised volatility — needs >2 returns in the trailing 90 days.' },
    { k: 'beta', rule: 'Beta vs Nifty over 120 days — needs >5 paired stock/Nifty returns, so it is 0 before nifty.json starts.' },
    { k: 'mdd6', rule: 'Max drawdown over 182 days.' },
    { k: 'upPct', rule: 'Share of up-days over 90 days.' },
  ] },
  { id: 'osc', label: 'Oscillators', note: 'Need enough consecutive daily bars — the pre-2002 weekly era cannot supply them.', params: [
    { k: 'rsi', rule: 'Wilder RSI(14) — needs 15 bars before the date.' },
    { k: 'macd', rule: 'MACD histogram — needs >35 closes in the trailing 320 days.' },
    { k: 'stoch', rule: 'Stochastic %K(14) — needs closes in the trailing 21 days.' },
    { k: 'bollB', rule: 'Bollinger %b — needs >1 close in the trailing 28 days.' },
  ] },
  { id: 'liquidity', label: 'Liquidity', note: 'Turnover and share-volume, from the bhavcopy.', params: [
    { k: 'turnover', zeroIsNull: true, rule: '20-day average daily turnover. turnAvgAt() returns 0 — never null — when nothing is recorded, so a plain non-null test would read 100% forever; this column counts > 0 only.' },
    { k: 'turnSurge', rule: '5d ÷ 90d turnover — null when the 90d average is 0.' },
    { k: 'volSurge', rule: '5d ÷ 90d share volume — null when the series carries no volume array at all.' },
  ] },
  { id: 'delivery', label: 'Delivery', note: 'Delivered-quantity %, backfilled to 2002.', params: [
    { k: 'delivPct', rule: '20-day average delivery % — null when no bar in the window carries a delivery figure.' },
  ] },
  { id: 'pat', label: 'PAT (point-in-time)', note: 'Quarterly net profit, visible only from its filing date — no look-ahead.', params: [
    { k: 'profitYoyPct', rule: 'Net-profit YoY % for the latest quarter FILED on or before the date, vs the same quarter a year earlier. Null when the year-ago base is exactly 0.' },
    { k: 'profitBase', rule: 'The year-ago quarter\'s net profit itself.' },
    { k: 'profitAccel', rule: 'This quarter\'s YoY minus last quarter\'s YoY.' },
    { k: 'profitStreak', rule: 'Consecutive +YoY quarters — 0 is a real answer, so every row with a filed quarter counts.', zeroOk: true },
  ] },
  { id: 'patttm', label: 'PAT TTM', note: 'Needs 8 consecutive quarters, so it lags the plain YoY by ~2 years.', params: [
    { k: 'profitTTM', rule: 'Last 4 filed quarters vs the 4 before them — needs all 8 present.' },
  ] },
  { id: 'drift', label: 'Post-result drift', note: 'Price move since the last earnings date.', params: [
    { k: 'postDrift', rule: 'Return from the close on the last filing date to the date — needs both a filed quarter and a close on that filing day.' },
  ] },
  { id: 'revenue', label: 'Revenue & margins', src: 'revop', note: 'From sf_revop, made point-in-time with sf_fundamentals\' filing date for the same quarter.', params: [
    { k: 'rev', src: 'revop', rule: 'Revenue (consolidated, else standalone) for the latest quarter filed on or before the date.' },
    { k: 'op', src: 'revop', rule: 'Operating profit (consolidated, else standalone) for that same quarter.' },
    { k: 'ebit', src: 'revop', rule: 'EBIT for that same quarter — the sparsest slot in the file.' },
  ] },
  { id: 'fii', label: 'FII', note: 'Quarterly shareholding filings, visible from their submission date.', params: [
    { k: 'fiiPct', rule: 'FII holding % from the latest SHP filing submitted on or before the date.' },
    { k: 'fiiChgPp', rule: 'QoQ change in pp — needs the calendar-previous quarter too, and is deliberately never computed across the Sep-2022 SEBI reclassification.' },
  ] },
  { id: 'dii', label: 'DII', note: 'Same filings as FII.', params: [
    { k: 'diiPct', rule: 'DII holding % from that same filing.' },
    { k: 'diiChgPp', rule: 'QoQ change in pp — same previous-quarter and Sep-2022 rules as FII.' },
  ] },
  { id: 'industry', label: 'Industry', src: 'meta', note: 'Classification is a CURRENT attribute, not point-in-time — it answers "is this stock classified today", asked of that date\'s members.', params: [
    { k: 'industry', src: 'meta', rule: 'The dataset\'s industry for the symbol is known (not "Unknown"/"Other").' },
  ] },
  { id: 'composite', label: 'QM composite', note: 'Derived ranking — needs TTM growth, 12m return and volatility together.', params: [
    { k: 'composite', rule: 'Cross-sectional z(profitTTM) + z(ret12m) − z(vol); null when any input is missing.' },
  ] },
];
// flat parameter list, in column order
const PARAMS = [];
FAMILIES.forEach(f => f.params.forEach(p => PARAMS.push({ ...p, fam: f.id })));
const NP = PARAMS.length;

/* Parameters with NO point-in-time history at all — stated on the page rather than
 * rendered as a column of zeroes (mcap is verified min=max=0 in survivorship-free mode). */
const NO_HISTORY = [
  { k: 'mcap', why: 'Always 0 in survivorship-free mode — the NSE bhavcopy carries no market cap. Use Turnover for size.' },
  { k: 'hist_mcap', why: 'Same: always 0. Never filter or sort on it.' },
  { k: 'promoter / public holding', why: 'shareholding.json holds only the last 8 quarters; the engine-facing shp_engine.json carries FII/DII only, so there is no long history to count.' },
];

/* ============================================================================
 * UNIVERSES
 * ========================================================================== */
const LIQUID_FLOOR = 100;   // ₹ lacs of 20d average daily turnover = ₹1 crore/day

function readJSON(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function readGz(p) { return JSON.parse(zlib.gunzipSync(fs.readFileSync(p))); }

function resolveBin() {
  if (BIN_ARG !== 'auto') {
    const p = path.resolve(ROOT, BIN_ARG);
    log('using bin', p);
    return readGz(p);
  }
  const cache = path.join(os.tmpdir(), 'sf_stock_data_live.bin');
  if (!fs.existsSync(cache) || fs.statSync(cache).size < 5e7) {
    log('downloading LIVE release asset (the in-repo bin is a frozen snapshot)…');
    execFileSync('curl', ['-sL', '-o', cache, RELEASE_BIN], { stdio: 'inherit' });
  } else {
    log('reusing cached live bin', cache, (fs.statSync(cache).size / 1e6).toFixed(0) + 'MB');
  }
  return readGz(cache);
}

/* ---- month-end date list, snapped to the last trading day (same rule as simulate()) ---- */
function monthEnds(firstISO, lastISO) {
  const out = [];
  let y = +firstISO.slice(0, 4), m = +firstISO.slice(5, 7);
  const ey = +lastISO.slice(0, 4), em = +lastISO.slice(5, 7);
  while (y < ey || (y === ey && m <= em)) {
    out.push(new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10));
    m++; if (m > 12) { m = 1; y++; }
  }
  return out;
}

/* ============================================================================
 * MAIN
 * ========================================================================== */
function main() {
  const t0 = Date.now();

  log('reading dash_slim.bin…');
  const D = readGz(path.join(DOCS, 'dash_slim.bin'));
  const IDXH = D.indicesHistory || {}, FNOH = D.fnoHistory || [], START_TS = D.startTs;

  log('reading sf bin…');
  const RAW = resolveBin();
  log('  bin end =', RAW.end, '· symbols =', Object.keys(RAW.data).length);

  const NIFTY = (readJSON(path.join(DOCS, 'nifty.json')).px) || {};
  const REVOP = readJSON(path.join(DOCS, 'sf_revop.json'));
  const FUNDJ = readJSON(path.join(DOCS, 'sf_fundamentals.json'));

  /* ---- context: the live engine, verbatim ---- */
  const ctx = vm.createContext({
    console, Date, Math, JSON, Object, Array, Number, String, Set, Map, isFinite, parseInt, parseFloat,
    location: { hostname: 'build', protocol: 'file:' },
    localStorage: { getItem: () => null, setItem: () => {} },
    // loadFund()/loadShp() run verbatim against local files — no second copy of the
    // FUND_ALIAS merge to keep in sync.
    fetch: async (url) => {
      const f = path.join(DOCS, String(url).replace(/^\.\//, ''));
      return { ok: fs.existsSync(f), json: async () => readJSON(f) };
    },
  });
  vm.runInContext(fs.readFileSync(path.join(DOCS, 'backtest-engine.js'), 'utf8'), ctx,
    { filename: 'backtest-engine.js' });

  // hand the raw payload over WITHOUT copying it through the sandbox boundary twice
  ctx.__RAW = RAW; ctx.__D = { idxh: IDXH, fnoh: FNOH, startTs: START_TS, nifty: NIFTY, end: RAW.end };

  log('normalising series through the engine\'s own _sfNorm…');
  vm.runInContext(`
    START_TS = __D.startTs; IDXH = __D.idxh; FNOH = __D.fnoh; NIFTY = __D.nifty;
    (function () {
      const ser = {}, meta = {}, turn = {};
      for (const sym in __RAW.data) {
        const r = _sfNorm(__RAW.data[sym], __D.startTs);
        ser[sym] = r.ser; turn[sym] = { d: r.ser.d, t: r.t };
        meta[sym] = _sfMeta(sym, __RAW.meta[sym] || {}, r.lastClose);
        delete __RAW.data[sym];                       // free as we go — the live bin is ~193 MB gz
      }
      SERIES = ser; META = meta; TURN = turn;
      SF_END_OFF = Math.floor((Date.parse(__D.end + 'T00:00:00Z') / 1000 - __D.startTs) / 86400);
    })();
  `, ctx);
  ctx.__RAW = null;
  if (global.gc) global.gc();

  vm.runInContext('__loaded = (async () => { await loadFund(); await loadShp(); return [Object.keys(FUND).length, Object.keys(SHPD).length]; })();', ctx);

  return ctx.__loaded.then(([nf, ns]) => {
    log(`engine ready · ${Object.keys(vm.runInContext('META', ctx)).length} symbols · FUND ${nf} · SHPD ${ns}`);
    run(ctx, { IDXH, FNOH, START_TS, REVOP, FUNDJ, end: RAW.end, t0 });
  });
}

function run(ctx, C) {
  const { IDXH, FNOH, START_TS, REVOP, FUNDJ, end } = C;

  /* ---- revenue visibility index: QE -> filing date, from sf_fundamentals ------------------
   * sf_revop carries no filing date of its own. sf_fundamentals does (idx2 std / idx4 con) for
   * the SAME quarter, so a revenue cell becomes visible on the day that quarter's result was
   * announced. Quarters with no fundamentals row have no known filing date and are NOT counted
   * (an unknown date is not a licence to assume "always visible").
   *
   * ⚠️ `> 0`, NOT `!= null` (runbook §91c). ann = 0 is the documented "announce date UNKNOWN"
   * sentinel, and `0 != null` is TRUE in JS — that exact test put a 23-year look-ahead into four
   * production selection loops. A falsy sentinel needs a truthy test. Here it would also shadow
   * the good basis: min(0, 20240515) = 0, dropping a quarter whose other basis IS dated. */
  const REV_INDEX = {};                    // SYM -> [[annInt, qeStr], …] ascending by annInt
  for (const sym in REVOP) {
    const fRows = FUNDJ[sym] || [];
    const annOf = {};                       // qeInt -> earliest known announce date
    for (const q of fRows) {
      const a = [q[2], q[4]].filter(x => x > 0);
      if (a.length) annOf[q[0]] = Math.min(...a);
    }
    const out = [];
    for (const qe in REVOP[sym]) {
      const ann = annOf[+qe];
      if (ann) out.push([ann, qe]);
    }
    if (out.length) { out.sort((a, b) => a[0] - b[0]); REV_INDEX[sym] = out; }
  }
  const FUND_ALIAS = vm.runInContext('FUND_ALIAS', ctx);
  const revFor = sym => REV_INDEX[sym] || (FUND_ALIAS[sym] ? REV_INDEX[FUND_ALIAS[sym]] : null) || null;
  const revopFor = sym => REVOP[sym] || (FUND_ALIAS[sym] ? REVOP[FUND_ALIAS[sym]] : null) || null;

  /* ---- dates ---- */
  const dayOff = d => vm.runInContext(`dayOff(${JSON.stringify(d)})`, ctx);
  let dates = monthEnds(FROM + '-01', end.slice(0, 7));
  // trading-day snap: the engine's own convention (simulate()), so a month-end on a
  // weekend/holiday is priced off the same bar a real rebalance would use.
  const tdset = vm.runInContext(`(function(){const s=new Set();
    for (const r of ['RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','ITC','SBIN','LT']) { const x=SERIES[r]; if (x&&x.d) for (const o of x.d) s.add(o); }
    return [...s].sort((a,b)=>a-b); })()`, ctx);
  const idxLE = (arr, v) => { let lo = 0, hi = arr.length - 1, a = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (arr[m] <= v) { a = m; lo = m + 1; } else hi = m - 1; } return a; };
  const endOff = dayOff(end);
  dates = dates.map(d => {
    const o0 = dayOff(d), i = idxLE(tdset, Math.min(o0, endOff));
    return i < 0 ? null : { iso: vm.runInContext(`isoOff(${tdset[i]})`, ctx), off: tdset[i], label: d };
  }).filter(Boolean);
  // a month whose snap lands in an earlier month has no trading day yet — drop it
  dates = dates.filter(d => d.iso.slice(0, 7) === d.label.slice(0, 7));
  if (MAX_DATES) dates = dates.slice(-MAX_DATES);
  log(`scanning ${dates.length} month-ends · ${dates[0].iso} → ${dates[dates.length - 1].iso}`);

  /* ---- universes ---- */
  const firstSnap = list => (list && list.length) ? list.map(s => s.effectiveDate).sort()[0] : null;
  const UNIVERSES = [
    { slug: 'all', name: 'All listed', kind: 'all', note: 'Every symbol in the survivorship-free dataset that is tradeable at the date (delisted names included, DVR shares excluded). The denominator is that count — there is no external roll to check against.' },
    { slug: 'liquid', name: 'Liquid (≥₹1cr/day)', kind: 'liquid', note: 'All listed, filtered to ≥₹1 crore of 20-day average daily turnover at the date.' },
    { slug: 'fno', name: 'F&O stocks', kind: 'index', index: '__FNO__', from: firstSnap(FNOH), note: 'NSE derivatives eligibility as of the date.' },
  ];
  Object.keys(IDXH).forEach(n => UNIVERSES.push({
    slug: n.toLowerCase().replace(/[^a-z0-9]+/g, '-'), name: n, kind: 'index', index: n,
    from: firstSnap(IDXH[n]), note: 'Point-in-time index membership from indicesHistory.',
  }));

  /* Membership sets per universe per date — via the engine's own membersAsOf, so the roll is
   * resolved exactly as a backtest resolves it.
   *
   * ⚠️ ONE DELIBERATE DEPARTURE. lastSnap() ends `best || list[0]`: asked for a date BEFORE the
   * first snapshot it hands back the EARLIEST roll rather than nothing. For a backtest that is a
   * reasonable fallback; for a coverage chart it is fabricated membership — only Nifty 500's roll
   * reaches 2002, so every other index would show 13-19 years of "members" it never had (Nifty
   * Healthcare's roll starts 2021-03-31). Dates before a universe's first snapshot are recorded
   * as -1 = no roll, and the page prints them as "–". */
  const memberSets = {};
  for (const u of UNIVERSES) {
    if (u.kind !== 'index') continue;
    memberSets[u.slug] = dates.map(d => {
      if (u.from && d.iso < u.from) return null;
      const arr = vm.runInContext(`(function(){const s = membersAsOf(${JSON.stringify(u.index)}, ${JSON.stringify(d.iso)}); return s ? [...s] : null;})()`, ctx);
      return arr ? new Set(arr) : null;
    });
  }

  /* ---- the scan ------------------------------------------------------------------------
   * One factorsAt() call per date over the WHOLE market (indexName null), then every universe
   * is a mask over those rows. Costs one expensive pass instead of 30. */
  const zero = () => new Int32Array(NP);
  const counts = {};   // slug -> [date][param] = covered count
  const members = {};  // slug -> [date] = universe size at that date
  for (const u of UNIVERSES) { counts[u.slug] = dates.map(zero); members[u.slug] = new Int32Array(dates.length); }

  const CFG = {
    indexName: null, mcapFloor: 0, earnBasis: 'con', sortBy: 'composite',
    // factorsAt never APPLIES filters (screenAsOf does) — listing one field per family here is
    // simply how needsTech/needsFund/needsShp are switched on for every family at once.
    filters: [{ field: 'fiiPct' }, { field: 'profitTTM' }, { field: 'ret12m' }],
  };
  ctx.__CFG = CFG;

  const revopIdx = { rev: [1, 0], op: [3, 2], ebit: [8, 7] };   // [con, std] slots in sf_revop
  let lastLog = 0;
  for (let di = 0; di < dates.length; di++) {
    const d = dates[di];
    ctx.__OFF = d.off;
    // pull only what we need across the boundary: symbol + one flag per engine parameter
    const rows = vm.runInContext(`(function(){
      const rows = factorsAt(__OFF, __CFG), keys = ${JSON.stringify(PARAMS.filter(p => !p.src).map(p => p.k))};
      const zeroIsNull = ${JSON.stringify(PARAMS.filter(p => !p.src).map(p => !!p.zeroIsNull))};
      const out = new Array(rows.length);
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i], flags = new Array(keys.length);
        for (let j = 0; j < keys.length; j++) {
          const v = fieldVal(r, keys[j]);
          const ok = (v != null && typeof v === 'number' && isFinite(v)) && !(zeroIsNull[j] && v === 0);
          flags[j] = ok ? 1 : 0;
        }
        out[i] = [r.sym, flags, r.turnover || 0, (r.ind && r.ind !== 'Other' && r.ind !== 'Unknown') ? 1 : 0];
      }
      return out;
    })()`, ctx);

    const dateInt = +d.iso.replace(/-/g, '');
    // engine-parameter column positions, in PARAMS order
    const engineCols = []; PARAMS.forEach((p, i) => { if (!p.src) engineCols.push(i); });
    const iIndustry = PARAMS.findIndex(p => p.k === 'industry');
    const iTurnover = PARAMS.findIndex(p => p.k === 'turnover');
    const revCols = ['rev', 'op', 'ebit'].map(k => PARAMS.findIndex(p => p.k === k && p.src === 'revop'));

    // per-row flags for the non-engine families, computed once per row per date
    const perRow = rows.map(([sym, flags, turnover, indKnown]) => {
      const rv = [0, 0, 0];
      const ridx = revFor(sym), rmap = revopFor(sym);
      if (ridx && rmap) {
        let qe = null;
        for (let i = ridx.length - 1; i >= 0; i--) if (ridx[i][0] <= dateInt) { qe = ridx[i][1]; break; }
        const cell = qe ? rmap[qe] : null;
        if (cell) ['rev', 'op', 'ebit'].forEach((k, j) => {
          const [ci, si] = revopIdx[k];
          rv[j] = (cell[ci] != null || cell[si] != null) ? 1 : 0;
        });
      }
      return { sym, flags, turnover, indKnown, rv };
    });

    for (const u of UNIVERSES) {
      const cnt = counts[u.slug][di];
      const set = u.kind === 'index' ? memberSets[u.slug][di] : null;
      if (u.kind === 'index' && !set) { members[u.slug][di] = -1; continue; }   // -1 = no roll yet
      let n = 0;
      for (const r of perRow) {
        if (set && !set.has(r.sym)) continue;
        if (u.kind === 'liquid' && !(r.turnover >= LIQUID_FLOOR)) continue;
        n++;
        for (let j = 0; j < engineCols.length; j++) if (r.flags[j]) cnt[engineCols[j]]++;
        if (r.indKnown) cnt[iIndustry]++;
        for (let j = 0; j < 3; j++) if (r.rv[j]) cnt[revCols[j]]++;
      }
      // an INDEX universe's denominator is the roll itself, not what survived the price gate —
      // that difference is exactly what the Price column is there to show.
      members[u.slug][di] = set ? [...set].filter(s => !s.includes('DVR')).length : n;
    }

    if (Date.now() - lastLog > 5000 || di === dates.length - 1) {
      lastLog = Date.now();
      log(`  ${di + 1}/${dates.length} ${d.iso} · ${rows.length} rows · ${((Date.now() - C.t0) / 1000).toFixed(0)}s`);
    }
  }

  writeOut(UNIVERSES, dates, counts, members, C);
}

/* ============================================================================
 * FLAGS + OUTPUT
 * ========================================================================== */
// A date is flagged when its coverage sits below half its own neighbourhood median (that median
// being a real population, >200), or — when the universe has a known roll — under 90% of that
// date's members while the neighbours clear it. Smoke detector, not a gate.
function medianOf(a) {
  const v = a.filter(x => x >= 0).slice().sort((x, y) => x - y);
  if (!v.length) return 0;
  const h = v.length >> 1;
  return v.length % 2 ? v[h] : (v[h - 1] + v[h]) / 2;
}
const NEIGH = 6;   // ±6 month-ends

const ROLL_FLOOR = 0.9;     // an index date should carry data for ≥90% of its own members
const ROLL_MARGIN = 0.03;   // …and must miss the neighbours by ≥3pp before it counts as a hole

// `hasRoll` — only an index universe has a membership list to check a 90% floor against. For
// "All listed"/"Liquid" the denominator IS the screened count, so that test would just re-flag
// structural facts (a new listing has no 12-month return) every single month.
//
// Both checks require the LEFT neighbourhood to be a real population. Without that, the first
// month of every ramp reads as a crater: FII/DII coverage starting at 0 in 2002 is a floor, not
// a hole, and a page that shouts about it teaches you to ignore it.
function computeFlags(series, memberArr, hasRoll) {
  const flags = [];
  for (let i = 0; i < series.length; i++) {
    const v = series[i]; if (v < 0) continue;
    const lo = Math.max(0, i - NEIGH), hi = Math.min(series.length - 1, i + NEIGH);
    const left = [], neigh = [];
    for (let k = lo; k <= hi; k++) { if (k === i || series[k] < 0) continue; neigh.push(series[k]); if (k < i) left.push(series[k]); }
    const med = medianOf(neigh), lmed = medianOf(left);
    if (lmed <= 200) continue;                       // nothing established before this date
    if (med > 200 && v < med / 2) { flags.push({ i, why: 'half', med: Math.round(med) }); continue; }
    if (!hasRoll) continue;
    const m = memberArr[i];
    if (m > 200 && v < ROLL_FLOOR * m) {
      const nb = []; for (let k = lo; k <= hi; k++) if (k !== i && memberArr[k] > 0 && series[k] >= 0) nb.push(series[k] / memberArr[k]);
      const nmed = medianOf(nb);
      if (nmed >= ROLL_FLOOR && v / m < nmed - ROLL_MARGIN) flags.push({ i, why: 'roll', med: Math.round(m * nmed) });
    }
  }
  return flags;
}

function writeOut(UNIVERSES, dates, counts, members, C) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const stamp = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' IST';

  const famMeta = FAMILIES.map(f => ({
    id: f.id, label: f.label, note: f.note,
    params: f.params.map(p => ({ k: p.k, rule: p.rule })),
  }));

  const index = {
    updated: stamp, dataEnd: C.end, from: dates[0].label, to: dates[dates.length - 1].label,
    nDates: dates.length, nParams: NP, liquidFloorCr: LIQUID_FLOOR / 100,
    families: famMeta, noHistory: NO_HISTORY,
    universes: [],
  };

  for (const u of UNIVERSES) {
    const cnt = counts[u.slug], mem = members[u.slug];
    // A family cell is the WEAKEST parameter in that family. Derived, not shipped: the page
    // recomputes it from `params` with the same one-line min. Shipping it too would be ~25% more
    // bytes AND a second source for one number — the exact split that lets two views disagree.
    const famSeries = FAMILIES.map(f => {
      const idxs = f.params.map(p => PARAMS.findIndex(q => q.k === p.k && q.fam === f.id));
      return dates.map((_, di) => {
        if (mem[di] < 0) return -1;
        let lo = Infinity;
        idxs.forEach(pi => { const v = cnt[di][pi]; if (v < lo) lo = v; });
        return lo === Infinity ? 0 : lo;
      });
    });
    const flagsByFam = {};
    let nFlags = 0;
    FAMILIES.forEach((f, fi) => {
      const fl = computeFlags(famSeries[fi], Array.from(mem), u.kind === 'index');
      if (fl.length) { flagsByFam[f.id] = fl; nFlags += fl.length; }
    });

    const payload = {
      slug: u.slug, name: u.name, kind: u.kind, note: u.note, rollFrom: u.from || null,
      updated: stamp, dataEnd: C.end,
      dates: dates.map(d => d.iso), months: dates.map(d => d.label),
      members: Array.from(mem),
      params: PARAMS.map((p, pi) => dates.map((_, di) => (mem[di] < 0 ? -1 : cnt[di][pi]))),
      paramKeys: PARAMS.map(p => p.k), paramFam: PARAMS.map(p => p.fam),
      flags: flagsByFam,
    };
    fs.writeFileSync(path.join(OUT_DIR, u.slug + '.json'), JSON.stringify(payload));
    const firstLive = Array.from(mem).findIndex(x => x > 0);
    index.universes.push({
      slug: u.slug, name: u.name, kind: u.kind, rollFrom: u.from || null,
      firstDate: firstLive >= 0 ? dates[firstLive].label : null,
      lastMembers: mem[mem.length - 1], nFlags,
    });
    log(`  wrote ${u.slug}.json · ${mem[mem.length - 1]} members now · ${nFlags} flags`);
  }

  fs.writeFileSync(path.join(OUT_DIR, 'index.json'), JSON.stringify(index));
  log(`done in ${((Date.now() - C.t0) / 1000 / 60).toFixed(1)} min → ${OUT_DIR}`);
}

main();
