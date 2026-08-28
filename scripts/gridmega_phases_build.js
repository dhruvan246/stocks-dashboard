// Build docs/strategy_phases<variant>.json for the Strategy Phases Lab from the
// per-window mega-grid artifacts (DATA_RUNBOOK §7.4).
//
//   node scripts/gridmega_phases_build.js [variantTag]      // '' | _h5 | _r3 | _h3 | _fno_h3
//
// Inputs, per window (11 of them — 4 phases + 7 calendar years):
//   scripts/_gridmega_top_<start>_<end><vtag>.json   DONE MARKER. A window counts only when this
//                                                    exists; the CSV alone may be a partial file
//                                                    from a run still in flight.
//   scripts/_gridmega_all_<start>_<end><vtag>.csv.gz every combo's cagr/maxDD/winRate, written in
//                                                    the grid's enumeration order — so row N is the
//                                                    SAME strategy in every window, and the merge
//                                                    is a positional join, no key matching.
//
// The CSVs are used ONLY to rank and select. Every number the page displays is then re-simulated
// exactly via the grid's SELECT mode, so totRet is a real finalV and not a 2dp CAGR raised to a
// fractional power.
'use strict';
const fs = require('fs'), zlib = require('zlib'), path = require('path'), readline = require('readline');
const { execFileSync } = require('child_process');

const ROOT = path.resolve(__dirname, '..');
const S = p => path.join(ROOT, 'scripts', p);
const VTAG = process.argv[2] || '';
const END = process.env.GRID_END || null;          // required: the data end date these grids used

// `card` = the tile title, `short` = the column header in the consistency table.
// The BEAR phase is the only window here that is not carved out of the post-Covid bull run —
// measured peak 2018-01-23 to trough 2020-03-23, Nifty 500 -36.8%. Without it the ⭐ ranking
// was "consistent across four slices of one bull market": scored against it, 0 of the top 10
// best-in-all-4 strategies made money and their median was -55%, worse than the index.
// `metric` picks what the tile and the consistency table show for a phase: 'cagr' for multi-year
// windows (a 22-year total return is an unreadable 7,250x), 'tot' for the short slices.
// `long` is the 22-year run — the only window spanning MULTIPLE cycles (2008, 2018-20, 2022, 2025),
// so it is the strongest single test in the lab. `full` stays phase 0: the page treats phase 0 as
// the headline and the source of the "Full DD %" column, so re-ordering would silently change both.
// Caveat carried from the coverage probe: every factor in this window is >=98% covered in its worst
// year EXCEPT `composite` (9% in 2004, 82% by 2008) — composite-based winners here are effectively
// post-2008 results.
const PHASES = [
  { key: 'full', label: 'Full cycle',    card: 'Entire cycle',    short: 'Full',  metric: 'cagr', start: '2020-03-31', end: 'END' },
  { key: 'long', label: 'Since 2004',    card: 'Since 2004 (22y)', short: '22y',  metric: 'cagr', start: '2004-03-31', end: 'END' },
  { key: 'bear', label: '2018-20 bear',  card: 'Jan-18 → Mar-20', short: 'Bear',  start: '2018-01-23', end: '2020-03-23' },
  { key: 'w1',   label: 'Covid recovery', card: 'Covid → Sep-21', short: 'Covid', start: '2020-03-31', end: '2021-09-30' },
  { key: 'w2',   label: '2023-24 bull',  card: 'Mar-23 → Sep-24', short: '23-24', start: '2023-03-31', end: '2024-09-30' },
  { key: 'w3',   label: '2026 YTD-ish',  card: 'Mar-26 → today',  short: '26',    start: '2026-03-31', end: 'END' },
];
const YEARS = [
  { key: 'y2020', label: '2020 (Mar→Dec)', start: '2020-03-31', end: '2020-12-31' },
  { key: 'y2021', label: '2021',           start: '2020-12-31', end: '2021-12-31' },
  { key: 'y2022', label: '2022',           start: '2021-12-31', end: '2022-12-31' },
  { key: 'y2023', label: '2023',           start: '2022-12-31', end: '2023-12-31' },
  { key: 'y2024', label: '2024',           start: '2023-12-31', end: '2024-12-31' },
  { key: 'y2025', label: '2025',           start: '2024-12-31', end: '2025-12-31' },
  { key: 'y2026', label: '2026 YTD',       start: '2025-12-31', end: 'END' },
];
const TOP_PHASE = 1000, TOP_YEAR = 1000, TOP_CONSIST = 100;

if (!END) { console.error('set GRID_END=<yyyy-mm-dd> (the data end the grids were run with)'); process.exit(1); }
const resolve = w => ({ ...w, end: w.end === 'END' ? END : w.end });
const WINDOWS = [...PHASES, ...YEARS].map(resolve);
// MERGE_BASES=std,con ranks BOTH earnings bases in ONE pool, so a card's top-1000 is the best
// 1000 (strategy × basis) pairs rather than the best 1000 of whichever basis you happened to
// build. Every row then carries which basis produced it. Default = the single EARN_BASIS.
const EARN_BASIS = process.env.EARN_BASIS || 'con';
const BASES = (process.env.MERGE_BASES || EARN_BASIS).split(',').map(s => s.trim()).filter(Boolean);
for (const b of BASES) if (!['std', 'con', 'conOnly'].includes(b)) {
  console.error('MERGE_BASES entries must be std|con|conOnly, got ' + JSON.stringify(b)); process.exit(1); }
if (new Set(BASES).size !== BASES.length) { console.error('MERGE_BASES has a duplicate: ' + BASES.join(',')); process.exit(1); }
const tagOf = (w, b) => w.start + '_' + w.end + VTAG + '_' + b;

// ---- gate: every window must have its DONE marker, for EVERY basis being merged
const missing = [];
for (const w of WINDOWS) for (const b of BASES)
  if (!fs.existsSync(S('_gridmega_top_' + tagOf(w, b) + '.json'))) missing.push(tagOf(w, b));
if (missing.length) {
  console.error('NOT READY — ' + missing.length + ' window/basis pair(s) have no _gridmega_top_ marker:');
  missing.forEach(t => console.error('   ' + t));
  process.exit(2);
}

// ---- gate: each marker must stamp the basis whose file it is. The marker is written by the same
// run that wrote the CSV, so this catches a mislabelled or clobbered artifact before its numbers
// reach the page under the wrong basis label.
for (const w of WINDOWS) for (const b of BASES) {
  const mk = JSON.parse(fs.readFileSync(S('_gridmega_top_' + tagOf(w, b) + '.json'), 'utf8'));
  const got = mk.earnBasis || 'con';
  if (got !== b) {
    console.error('BASIS MISMATCH ' + w.key + ' [' + b + ']: marker says ' + got);
    process.exit(5);
  }
}

// Which grid rows actually READ earnings. The engine's needsFund() (backtest-engine.js) says a
// strategy consults fundamentals only if its sort field or one of its filters is a FUND field —
// so a strategy touching none of them scores IDENTICALLY under std and con. Its con copy is a
// pure duplicate and must never occupy a second slot in a card. Derived from the CSV's own
// sortBy/filters text, so it cannot drift from the enumeration that produced the row.
const FUND_FIELDS = ['profitYoyPct', 'profitBase', 'profitAccel', 'profitTTM', 'profitStreak', 'postDrift', 'composite'];
const FUND_SET = new Set(FUND_FIELDS);
const FUND_RE = new RegExp('(^|[^A-Za-z])(' + FUND_FIELDS.join('|') + ')([^A-Za-z]|$)');
let FUNDFLAG = null;          // Uint8Array over the N grid rows, 1 = basis-dependent
let NROWS = 0;

// Factors barred from the published cards (user policy, 2026-08-28). A strategy is dropped from
// the pool entirely — every phase card, every year card, both consistency cards — if it names one
// of these in its sort field OR any filter:
//   indRank    — the BSE industry map is a CURRENT snapshot, so in a survivorship-free universe
//                delisted names carry ind='Unknown' (31.9% of the universe in 2004 vs 0.6% today).
//                They pool into ONE pseudo-industry whose shared rank was <=3 in 41 of 268 months,
//                admitting up to 163 unrelated stocks through `indRank<=3` in a single clump.
//   composite  — coverage is 15% in 2004 and only ~82% by 2008, so a composite winner in the 22-year
//                window is really a post-2008 result carrying a 22-year label.
// Set EXCLUDE_FIELDS= (empty) to publish them again, or list other fields to bar.
const EXCLUDE_FIELDS = (process.env.EXCLUDE_FIELDS !== undefined ? process.env.EXCLUDE_FIELDS : 'indRank,composite')
  .split(',').map(s => s.trim()).filter(Boolean);
const EXCL_SET = new Set(EXCLUDE_FIELDS);
const EXCL_RE = EXCLUDE_FIELDS.length
  ? new RegExp('(^|[^A-Za-z])(' + EXCLUDE_FIELDS.join('|') + ')([^A-Za-z]|$)') : null;
let EXCLFLAG = null;          // Uint8Array over the N grid rows, 1 = barred from the cards

// Read one window/basis CSV → its CAGR column. On the very first call it also derives FUNDFLAG.
async function readCagr(file, deriveFlags) {
  const cagr = [], flags = deriveFlags ? [] : null, excl = deriveFlags ? [] : null;
  const rl = readline.createInterface({
    input: fs.createReadStream(file).pipe(zlib.createGunzip()), crlfDelay: Infinity });
  let first = true;
  for await (const line of rl) {
    if (first) { first = false; continue; }              // header
    if (!line) continue;
    // sortBy,dir,filters,cagr,maxDD,winRate,avgPicks — filters never contains a comma (the grid
    // writes ';' between atoms), so field 3 is always CAGR.
    const c1 = line.indexOf(','), c2 = line.indexOf(',', c1 + 1), c3 = line.indexOf(',', c2 + 1);
    const c4 = line.indexOf(',', c3 + 1);
    cagr.push(+line.slice(c3 + 1, c4));
    if (flags) {
      const sortBy = line.slice(0, c1), fil = line.slice(c2 + 1, c3);
      flags.push((FUND_SET.has(sortBy) || FUND_RE.test(fil)) ? 1 : 0);
      excl.push(EXCL_RE && (EXCL_SET.has(sortBy) || EXCL_RE.test(fil)) ? 1 : 0);
    }
  }
  if (flags) {
    FUNDFLAG = Uint8Array.from(flags); NROWS = FUNDFLAG.length;
    EXCLFLAG = Uint8Array.from(excl);
    const nDep = flags.reduce((a, b) => a + b, 0), nEx = excl.reduce((a, b) => a + b, 0);
    console.error('basis-dependent rows: ' + nDep.toLocaleString() + ' of ' + NROWS.toLocaleString() +
                  ' (' + (nDep / NROWS * 100).toFixed(1) + '%) — the rest score identically under every basis');
    if (EXCLUDE_FIELDS.length)
      console.error('barred by EXCLUDE_FIELDS=' + EXCLUDE_FIELDS.join(',') + ': ' + nEx.toLocaleString() +
                    ' of ' + NROWS.toLocaleString() + ' rows (' + (nEx / NROWS * 100).toFixed(1) + '%)');
  }
  return cagr;
}

// The merged index space: position g → (basis BASES[GB[g]], grid row GR[g]). Basis 0 contributes
// every row; each later basis contributes only its basis-DEPENDENT rows, which is the dedupe.
let GB = null, GR = null, M = 0;
function buildGlobalIndex() {
  // EXCLUDE_FIELDS rows never enter the pool, so they cannot reach ANY card — that is the point:
  // dropping them at ranking time is what lets the remaining strategies move up into the top 1000.
  const keep = i => !(EXCLFLAG && EXCLFLAG[i]);
  let nKeep = 0, nDep = 0;
  for (let i = 0; i < NROWS; i++) if (keep(i)) { nKeep++; if (FUNDFLAG[i]) nDep++; }
  M = nKeep + (BASES.length - 1) * nDep;
  GB = new Uint8Array(M); GR = new Int32Array(M);
  let g = 0;
  for (let i = 0; i < NROWS; i++) if (keep(i)) { GB[g] = 0; GR[g] = i; g++; }
  for (let k = 1; k < BASES.length; k++) for (let i = 0; i < NROWS; i++) if (keep(i) && FUNDFLAG[i]) { GB[g] = k; GR[g] = i; g++; }
  if (g !== M) throw new Error('global index build mismatch: ' + g + ' vs ' + M);
  console.error('merged pool: ' + M.toLocaleString() + ' (strategy × basis) pairs across ' + BASES.join('+') +
                (EXCLUDE_FIELDS.length ? '  [' + (NROWS - nKeep).toLocaleString() + ' strategies barred]' : ''));
}

// ---- pass 1: rank the merged pool for one window.
// Keeps one Int32Array of ranks per window; the CAGR column is freed after ranking.
async function rankWindow(w) {
  const per = [];
  for (const b of BASES) {
    const arr = await readCagr(S('_gridmega_all_' + tagOf(w, b) + '.csv.gz'), FUNDFLAG === null);
    if (arr.length !== NROWS) throw new Error('row count ' + arr.length + ' != ' + NROWS + ' for ' + tagOf(w, b));
    per.push(Float32Array.from(arr));
  }
  if (GB === null) buildGlobalIndex();
  const C = new Float32Array(M);
  for (let g = 0; g < M; g++) C[g] = per[GB[g]][GR[g]];
  // A plain Array (not a TypedArray) because Array.prototype.sort is spec-stable: ties then keep
  // the grid's enumeration order, so the same input always yields the same ranking.
  const ord = new Array(M); for (let i = 0; i < M; i++) ord[i] = i;
  ord.sort((a, b) => C[b] - C[a]);
  const rank = new Int32Array(M);
  for (let k = 0; k < M; k++) rank[ord[k]] = k + 1;
  const top = ord.slice(0, Math.max(TOP_PHASE, TOP_YEAR));
  console.error('  ranked ' + w.key + ': ' + M.toLocaleString() + ' pairs, best CAGR ' + C[ord[0]].toFixed(2) +
                ' [' + BASES[GB[ord[0]]] + ']');
  return { n: M, rank, top };
}

(async function () {
  console.error('variant "' + (VTAG || 'r5 (default)') + '" — ranking ' + WINDOWS.length + ' windows');
  const R = {};
  let N = null;
  for (const w of WINDOWS) {
    R[w.key] = await rankWindow(w);
    if (N === null) N = R[w.key].n;
    else if (R[w.key].n !== N) { console.error('ROW COUNT MISMATCH ' + w.key + ': ' + R[w.key].n + ' vs ' + N); process.exit(3); }
  }

  // ---- consistency rankings: minimax of a strategy's rank across the 4 phases / the 7 years
  const worstOver = keys => {
    const out = new Int32Array(N);
    for (let i = 0; i < N; i++) { let m = 0; for (const k of keys) { const r = R[k].rank[i]; if (r > m) m = r; } out[i] = m; }
    return out;
  };
  const topSmallest = (arr, take) => {
    const idx = new Array(N); for (let i = 0; i < N; i++) idx[i] = i;
    idx.sort((a, b) => arr[a] - arr[b]);
    return idx.slice(0, take);
  };
  const worst4 = worstOver(PHASES.map(p => p.key));
  const worstY = worstOver(YEARS.map(y => y.key));
  const all4Idx = topSmallest(worst4, TOP_CONSIST);
  const allYIdx = topSmallest(worstY, TOP_CONSIST);

  // ---- the union that the page ships
  const union = new Set();
  PHASES.forEach(p => R[p.key].top.slice(0, TOP_PHASE).forEach(i => union.add(i)));
  YEARS.forEach(y => R[y.key].top.slice(0, TOP_YEAR).forEach(i => union.add(i)));
  all4Idx.forEach(i => union.add(i)); allYIdx.forEach(i => union.add(i));
  const sel = [...union].sort((a, b) => a - b);
  console.error('selected ' + sel.length.toLocaleString() + ' distinct strategies for the payload');
  // Each basis re-simulates only ITS OWN selected rows, so the SELECT run's env basis always
  // matches the rows it is scoring.
  const selRowsByBasis = BASES.map(() => []);
  sel.forEach(g => selRowsByBasis[GB[g]].push(GR[g]));
  BASES.forEach((b, k) => console.error('  selection: ' + selRowsByBasis[k].length.toLocaleString() + ' rows on ' + b));

  // ---- pass 2: exact metrics for the selection, window by window, through the grid's SELECT mode
  const RUN = S('_gridmega_run.js');
  const baseEnv = { ...process.env };
  if (VTAG === '_h5') { baseEnv.TOPN = '5'; baseEnv.METHOD = 'hold'; }
  else if (VTAG === '_r3') { baseEnv.TOPN = '3'; baseEnv.METHOD = 'reset'; }
  else if (VTAG === '_h3') { baseEnv.TOPN = '3'; baseEnv.METHOD = 'hold'; }
  else if (VTAG === '_fno_h3') { baseEnv.TOPN = '3'; baseEnv.METHOD = 'hold'; baseEnv.UNIVERSE = '__FNO__'; }
  const SELF = BASES.map((b, k) => {
    const f = S('_gridmega_selidx' + (VTAG || '_r5') + '_' + b + '.json');
    fs.writeFileSync(f, JSON.stringify(selRowsByBasis[k]));
    return f;
  });
  // SEL[windowKey][basisIdx] = Map(gridRow -> exact metrics)
  const SEL = {}, BENCH = {};
  for (const w of WINDOWS) {
    SEL[w.key] = [];
    for (let k = 0; k < BASES.length; k++) {
      const b = BASES[k];
      const out = S('_gridmega_sel_' + tagOf(w, b) + '.json');
      if (!fs.existsSync(out)) {
        console.error('  re-simulating ' + selRowsByBasis[k].length + ' [' + b + '] strategies over ' + w.key + '…');
        execFileSync(process.execPath, ['--max-old-space-size=3072', RUN, w.start, w.end],
                     { env: { ...baseEnv, SELECT_FILE: SELF[k], EARN_BASIS: b }, stdio: ['ignore', 'ignore', 'inherit'] });
      }
      const j = JSON.parse(fs.readFileSync(out, 'utf8'));
      if (j.rows.length !== selRowsByBasis[k].length) {
        console.error('SELECT returned ' + j.rows.length + ' of ' + selRowsByBasis[k].length + ' for ' + w.key + ' [' + b + ']');
        process.exit(4);
      }
      const m = new Map(); for (const r of j.rows) m.set(r.i, r);
      SEL[w.key].push(m);
      if (k === 0) BENCH[w.key] = { bench: j.bench, years: j.years };
    }
  }
  const metric = (wkey, g) => {
    const r = SEL[wkey][GB[g]].get(GR[g]);
    if (!r) throw new Error('no SELECT row for grid row ' + GR[g] + ' [' + BASES[GB[g]] + '] in ' + wkey);
    return r;
  };

  // ---- assemble the payload the page reads
  const pos = new Map(); sel.forEach((g, k) => pos.set(g, k));
  const combos = sel.map(g => {
    const ref = metric('full', g);
    return {
      s: ref.s, d: ref.d, f: ref.f,
      // Which earnings basis produced these numbers — 'any' when the strategy reads NO earnings
      // factor at all, because then every basis yields this exact row and calling it 'std' would
      // invent a distinction the data does not have.
      b: FUNDFLAG[GR[g]] ? BASES[GB[g]] : 'any',
      p: PHASES.map(p => { const r = metric(p.key, g); return [r.cagr, r.tot, r.dd, r.win]; }),
    };
  });
  const cards = { all4: all4Idx.map(i => ({ i: pos.get(i), worst: worst4[i], ranks: PHASES.map(p => R[p.key].rank[i]) })),
                  allY: allYIdx.map(i => ({ i: pos.get(i), worst: worstY[i], yrets: YEARS.map(y => Math.round(metric(y.key, i).tot)) })) };
  PHASES.forEach(p => { cards[p.key] = R[p.key].top.slice(0, TOP_PHASE).map(i => pos.get(i)); });

  // The grid derives `bench` from nifty500.json, whose DAILY series only starts 2012-01-02 — so any
  // window beginning earlier (the 2004 phase) comes back {totRet:null, cagr:null}, and the page then
  // crashes on `.toFixed()`. Recover it from docs/index_monthly.json, which carries NIFTY 500
  // month-end closes back to 1995. (Runbook §7.1b trap 4.)
  const IDXM = (() => {
    try { const j = JSON.parse(fs.readFileSync(path.join(ROOT, 'docs', 'index_monthly.json'), 'utf8'));
          const n5 = (j.indices || []).find(i => i.key === 'NIFTY 500');
          return n5 ? n5.closes : null; } catch (e) { return null; }
  })();
  const monthClose = iso => { if (!IDXM) return null;
    const a = IDXM[iso.slice(0, 4)]; if (!a) return null;
    const v = a[+iso.slice(5, 7) - 1]; return (v == null) ? null : v; };
  for (const p of PHASES) {
    const b = BENCH[p.key].bench;
    if (b && b.cagr != null) continue;
    const e = resolve(p).end;
    // month-end closes: use the start month and the last COMPLETE month before the end
    const em = new Date(Date.parse(e + 'T00:00:00Z')); em.setUTCDate(0);
    const a0 = monthClose(p.start), a1 = monthClose(em.toISOString().slice(0, 10));
    if (a0 && a1) {
      const yrs = BENCH[p.key].years;
      BENCH[p.key].bench = { totRet: +((a1 / a0 - 1) * 100).toFixed(1),
                             cagr: +(((a1 / a0) ** (1 / yrs) - 1) * 100).toFixed(2),
                             src: 'index_monthly (daily NIFTY500 starts 2012)' };
      console.error('  bench for "' + p.key + '" recovered from index_monthly: ' +
                    BENCH[p.key].bench.cagr + '% CAGR');
    } else {
      console.error('  WARNING: no benchmark for phase "' + p.key + '" — page must tolerate null');
    }
  }

  const universeLabel = VTAG === '_fno_h3' ? 'F&O stocks' : 'Nifty 500';
  const basket = 'top-' + (VTAG.includes('3') ? 3 : 5) + ' · ' + (VTAG.includes('h') ? 'hold' : 'reset');
  const BLABEL = { std: 'standalone', con: 'consolidated', conOnly: 'consolidated-only' };
  const basisLabel = BASES.length > 1
    ? ' · ' + BASES.map(b => BLABEL[b]).join(' + ') + ' earnings, ranked together'
    : BASES[0] === 'con' ? '' : ' · ' + BLABEL[BASES[0]] + ' earnings';
  const out = {
    built: new Date().toISOString().slice(0, 10),
    totalCombos: N,
    universe: universeLabel + ' · monthly · ' + basket + basisLabel,
    earnBasis: BASES.length > 1 ? 'merged' : BASES[0],
    bases: BASES,
    excluded: EXCLUDE_FIELDS,        // factors barred from the cards — the page must say so
    dataEnd: END,
    phases: PHASES.map(p => ({ key: p.key, label: p.label, card: p.card, short: p.short,
      metric: p.metric || 'tot', start: p.start, end: resolve(p).end,
      years: BENCH[p.key].years, bench: BENCH[p.key].bench })),
    combos,
    cards,
    years: YEARS.map(y => ({ key: y.key, label: y.label, start: y.start, end: resolve(y).end,
      years: BENCH[y.key].years, bench: { totRet: BENCH[y.key].bench.totRet },
      rows: R[y.key].top.slice(0, TOP_YEAR).map(i => { const r = metric(y.key, i);
        return { i: pos.get(i), cg: r.cagr, tot: r.tot, dd: r.dd, win: r.win }; }) })),
  };
  const DST = path.join(ROOT, 'docs', 'strategy_phases' + VTAG + '.json');
  fs.writeFileSync(DST, JSON.stringify(out));
  console.error('WROTE ' + path.relative(ROOT, DST) + '  ' + (fs.statSync(DST).size / 1e6).toFixed(2) + ' MB  ' +
                combos.length + ' combos, best full-cycle CAGR ' + combos[cards.full[0]].p[0][0]);
})();
