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

const PHASES = [
  { key: 'full', label: 'Full cycle',    start: '2020-03-31', end: 'END' },
  { key: 'w1',   label: 'Covid recovery', start: '2020-03-31', end: '2021-09-30' },
  { key: 'w2',   label: '2023-24 bull',  start: '2023-03-31', end: '2024-09-30' },
  { key: 'w3',   label: '2026 YTD-ish',  start: '2026-03-31', end: 'END' },
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
const tagOf = w => w.start + '_' + w.end + VTAG;

// ---- gate: every window must have its DONE marker
const missing = WINDOWS.filter(w => !fs.existsSync(S('_gridmega_top_' + tagOf(w) + '.json')));
if (missing.length) {
  console.error('NOT READY — ' + missing.length + ' window(s) still have no _gridmega_top_ marker:');
  missing.forEach(w => console.error('   ' + w.key + '  ' + tagOf(w)));
  process.exit(2);
}

// ---- pass 1: read each window's CSV, rank every combo by CAGR
// Keeps one Int32Array of ranks per window (~18 MB each); the CAGR column is freed after ranking.
async function rankWindow(w) {
  const file = S('_gridmega_all_' + tagOf(w) + '.csv.gz');
  const cagr = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(file).pipe(zlib.createGunzip()), crlfDelay: Infinity });
  let first = true;
  for await (const line of rl) {
    if (first) { first = false; continue; }              // header
    if (!line) continue;
    // sortBy,dir,filters,cagr,maxDD,winRate,avgPicks — filters never contains a comma (grid
    // replaces them with ';'), so the 4th field is always CAGR.
    let c = 0, i = -1;
    for (let k = 0; k < line.length && c < 3; k++) if (line.charCodeAt(k) === 44) { c++; i = k; }
    let j = line.indexOf(',', i + 1);
    cagr.push(+line.slice(i + 1, j));
  }
  const n = cagr.length;
  const C = Float32Array.from(cagr); cagr.length = 0;
  // A plain Array (not a TypedArray) because Array.prototype.sort is spec-stable: ties then keep
  // the grid's enumeration order, so the same input always yields the same ranking.
  const ord = new Array(n); for (let i = 0; i < n; i++) ord[i] = i;
  ord.sort((a, b) => C[b] - C[a]);
  const rank = new Int32Array(n);
  for (let k = 0; k < n; k++) rank[ord[k]] = k + 1;
  const top = ord.slice(0, Math.max(TOP_PHASE, TOP_YEAR));
  console.error('  ranked ' + w.key + ': ' + n.toLocaleString() + ' combos, best CAGR ' + C[ord[0]].toFixed(2));
  return { n, rank, top };
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
  const SELF = S('_gridmega_selidx' + (VTAG || '_r5') + '.json');
  fs.writeFileSync(SELF, JSON.stringify(sel));

  // ---- pass 2: exact metrics for the selection, window by window, through the grid's SELECT mode
  const RUN = S('_gridmega_run.js');
  const env = { ...process.env, SELECT_FILE: SELF };
  if (VTAG === '_h5') { env.TOPN = '5'; env.METHOD = 'hold'; }
  else if (VTAG === '_r3') { env.TOPN = '3'; env.METHOD = 'reset'; }
  else if (VTAG === '_h3') { env.TOPN = '3'; env.METHOD = 'hold'; }
  else if (VTAG === '_fno_h3') { env.TOPN = '3'; env.METHOD = 'hold'; env.UNIVERSE = '__FNO__'; }
  const SEL = {};
  for (const w of WINDOWS) {
    const out = S('_gridmega_sel_' + tagOf(w) + '.json');
    if (!fs.existsSync(out)) {
      console.error('  re-simulating ' + sel.length + ' strategies over ' + w.key + '…');
      execFileSync(process.execPath, ['--max-old-space-size=3072', RUN, w.start, w.end],
                   { env, stdio: ['ignore', 'ignore', 'inherit'] });
    }
    const j = JSON.parse(fs.readFileSync(out, 'utf8'));
    if (j.rows.length !== sel.length) { console.error('SELECT returned ' + j.rows.length + ' of ' + sel.length + ' for ' + w.key); process.exit(4); }
    SEL[w.key] = j;
  }

  // ---- assemble the payload the page reads
  const pos = new Map(); sel.forEach((rowIdx, k) => pos.set(rowIdx, k));
  const ref = SEL.full.rows;
  const combos = sel.map((rowIdx, k) => ({
    s: ref[k].s, d: ref[k].d, f: ref[k].f,
    p: PHASES.map(p => { const r = SEL[p.key].rows[k]; return [r.cagr, r.tot, r.dd, r.win]; }),
  }));
  const cards = { all4: all4Idx.map(i => ({ i: pos.get(i), worst: worst4[i], ranks: PHASES.map(p => R[p.key].rank[i]) })),
                  allY: allYIdx.map(i => ({ i: pos.get(i), worst: worstY[i], yrets: YEARS.map(y => Math.round(SEL[y.key].rows[pos.get(i)].tot)) })) };
  PHASES.forEach(p => { cards[p.key] = R[p.key].top.slice(0, TOP_PHASE).map(i => pos.get(i)); });

  const universeLabel = VTAG === '_fno_h3' ? 'F&O stocks' : 'Nifty 500';
  const basket = 'top-' + (VTAG.includes('3') ? 3 : 5) + ' · ' + (VTAG.includes('h') ? 'hold' : 'reset');
  const out = {
    built: new Date().toISOString().slice(0, 10),
    totalCombos: N,
    universe: universeLabel + ' · monthly · ' + basket,
    dataEnd: END,
    phases: PHASES.map(p => ({ key: p.key, label: p.label, start: p.start, end: resolve(p).end,
      years: SEL[p.key].years, bench: SEL[p.key].bench })),
    combos,
    cards,
    years: YEARS.map(y => ({ key: y.key, label: y.label, start: y.start, end: resolve(y).end,
      years: SEL[y.key].years, bench: { totRet: SEL[y.key].bench.totRet },
      rows: R[y.key].top.slice(0, TOP_YEAR).map(i => { const r = SEL[y.key].rows[pos.get(i)];
        return { i: pos.get(i), cg: r.cagr, tot: r.tot, dd: r.dd, win: r.win }; }) })),
  };
  const DST = path.join(ROOT, 'docs', 'strategy_phases' + VTAG + '.json');
  fs.writeFileSync(DST, JSON.stringify(out));
  console.error('WROTE ' + path.relative(ROOT, DST) + '  ' + (fs.statSync(DST).size / 1e6).toFixed(2) + ' MB  ' +
                combos.length + ' combos, best full-cycle CAGR ' + combos[cards.full[0]].p[0][0]);
})();
