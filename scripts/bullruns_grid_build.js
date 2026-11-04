#!/usr/bin/env node
'use strict';
/* ============================================================================
   bullruns_grid_build.js — turn the bull-leg grids into docs/bull_runs<suffix>.json
   (DATA_RUNBOOK §7.4b). Run scripts/bullruns_grid_run.py first.

     GRID_END=<sf end> node --max-old-space-size=8192 scripts/bullruns_grid_build.js "Nifty 500"       → bull_runs.json
     GRID_END=<sf end> node --max-old-space-size=8192 scripts/bullruns_grid_build.js "Nifty Next 50"   → bull_runs_nn50.json

   POOL — one candidate per (combo, basis) that actually differs: a combo whose sort or any filter is
   a FUND field is scored under std AND con (two candidates); every other combo reads no earnings and
   is ONE candidate ('any'), taken from the std grid — and the build REQUIRES its std and con CAGR to
   agree, which proves the two grids line up row for row. indRank and composite are barred (sort or
   filter). This rule reproduces the 2026-08-28 build's pool exactly (5,439,680 from 4,440,600 combos).

   RANK — per leg, competition rank on the grid CAGR (2dp, best = 1; ties share a place). A view's
   score is its WORST placing across its legs; the top 1,000 (tie-break: sum of placings, then grid
   order) are re-scored EXACTLY with SELECT_FILE in every leg + the live leg, and those exact totals,
   drawdowns and benchmark returns are what the page prints.
   ==========================================================================*/
const fs = require('fs'), path = require('path'), zlib = require('zlib'), cp = require('child_process');
const HERE = __dirname, ROOT = path.join(HERE, '..');
const UNI = process.argv[2] || 'Nifty 500';
const UPRE = { 'Nifty 500': '', 'Nifty Next 50': 'nn50_' }[UNI];
const OUTF = { 'Nifty 500': 'bull_runs.json', 'Nifty Next 50': 'bull_runs_nn50.json' }[UNI];
if (UPRE == null) throw new Error('unknown universe ' + UNI);
const BENCH_NAME = UNI === 'Nifty 500' ? 'NIFTY 500' : 'NIFTY NEXT 50';

const LEGS = [
  { k: '2013-15', label: 'Aug-13 → Mar-15', s: '2013-08-28', e: '2015-03-03' },
  { k: '2016-18', label: 'Feb-16 → Aug-18', s: '2016-02-25', e: '2018-08-31' },
  { k: '2020-21', label: 'Mar-20 → Oct-21', s: '2020-03-23', e: '2021-10-18' },
  { k: '2022-24', label: 'Jun-22 → Sep-24', s: '2022-06-20', e: '2024-09-26' },
];
const CUR_S = '2026-03-30';
const VIEWS = { all4: { label: 'All four bull runs', keys: ['2013-15', '2016-18', '2020-21', '2022-24'] },
                last2: { label: 'Latest two bull runs', keys: ['2020-21', '2022-24'] } };
const FUND = new Set(['profitYoyPct', 'profitBase', 'profitAccel', 'profitTTM', 'profitStreak', 'postDrift', 'composite']);
const BAR = new Set(['indRank', 'composite']);
const TOP = 1000;
const log = m => process.stderr.write(m + '\n');
const tagOf = (s, e, b) => s + '_' + e + '_' + UPRE + 'h3_' + b;

function readGrid(tag) {
  const mk = path.join(HERE, '_gridmega_top_' + tag + '.json');
  if (!fs.existsSync(mk)) throw new Error('marker missing — grid not finished: ' + mk);
  const lines = zlib.gunzipSync(fs.readFileSync(path.join(HERE, '_gridmega_all_' + tag + '.csv.gz'))).toString('utf8').split('\n');
  if (lines[lines.length - 1] === '') lines.pop();
  if (lines[0] !== 'sortBy,dir,filters,cagr,maxDD,winRate,avgPicks') throw new Error('bad header ' + tag);
  return lines.slice(1);
}

function main() {
  // ---- load: descriptors from the first grid, cagr arrays from all 8
  let desc = null, N = 0;
  const cagr = {};   // `${leg}|${basis}` -> Float64Array
  for (const L of LEGS) for (const b of ['std', 'con']) {
    const rows = readGrid(tagOf(L.s, L.e, b));
    if (!desc) {
      N = rows.length;
      desc = rows.map(r => { const p = r.split(','); return { s: p[0], d: p[1], f: p[2] }; });
    }
    if (rows.length !== N) throw new Error('ROW COUNT MISMATCH ' + tagOf(L.s, L.e, b) + ' ' + rows.length + ' vs ' + N);
    const a = new Float64Array(N);
    rows.forEach((r, i) => {
      const p = r.split(',');
      if (p[0] !== desc[i].s || p[1] !== desc[i].d || p[2] !== desc[i].f) throw new Error('row ' + i + ' differs in ' + tagOf(L.s, L.e, b));
      a[i] = +p[3];
    });
    cagr[L.k + '|' + b] = a;
    log('loaded ' + tagOf(L.s, L.e, b) + ' · ' + N.toLocaleString() + ' rows');
  }

  // ---- pool
  const cand = [];   // {i, b}
  let anyMismatch = 0;
  for (let i = 0; i < N; i++) {
    const fields = desc[i].f.split(' & ').map(t => t.match(/^(\w+)/)[1]);
    if (BAR.has(desc[i].s) || fields.some(f => BAR.has(f))) continue;
    if (FUND.has(desc[i].s) || fields.some(f => FUND.has(f))) { cand.push({ i, b: 'std' }, { i, b: 'con' }); continue; }
    for (const L of LEGS) if (cagr[L.k + '|std'][i] !== cagr[L.k + '|con'][i]) anyMismatch++;
    cand.push({ i, b: 'any' });
  }
  if (anyMismatch) throw new Error(anyMismatch + ' earnings-free cells differ between std and con grids — grids do not line up');
  const P = cand.length;
  log('pool ' + P.toLocaleString() + ' candidates from ' + N.toLocaleString() + ' combos per basis (earnings-free std==con verified)');

  // ---- per-leg competition rank
  const ranks = {};
  for (const L of LEGS) {
    const v = new Float64Array(P);
    cand.forEach((c, j) => { v[j] = cagr[L.k + '|' + (c.b === 'any' ? 'std' : c.b)][c.i]; });
    const ord = Array.from({ length: P }, (_, j) => j).sort((x, y) => v[y] - v[x]);
    const r = new Int32Array(P);
    for (let p = 0; p < P; p++) r[ord[p]] = (p > 0 && v[ord[p]] === v[ord[p - 1]]) ? r[ord[p - 1]] : p + 1;
    ranks[L.k] = r;
    log('ranked ' + L.k);
  }

  // ---- top 1000 per view
  const tops = {};
  for (const [vk, V] of Object.entries(VIEWS)) {
    const worst = new Int32Array(P), sum = new Float64Array(P);
    for (let j = 0; j < P; j++) { let w = 0, s = 0; for (const k of V.keys) { const r = ranks[k][j]; if (r > w) w = r; s += r; } worst[j] = w; sum[j] = s; }
    const ord = Array.from({ length: P }, (_, j) => j).sort((x, y) => worst[x] - worst[y] || sum[x] - sum[y] || x - y);
    tops[vk] = ord.slice(0, TOP).map(j => ({ j, worst: worst[j] }));
    log(vk + ': best worst-rank ' + worst[ord[0]].toLocaleString() + ' · row 1000 ' + worst[ord[TOP - 1]].toLocaleString());
  }

  // ---- exact re-scoring: SELECT every chosen row in every leg + the live leg
  const runner = path.join(HERE, '_gridmega_run.js');
  if (!fs.existsSync(runner)) throw new Error('runner missing — run bullruns_grid_run.py first');
  const end = process.env.GRID_END;   // the sf end the grids were staged at (same as gridmega_phases_build.js)
  if (!/^\d{4}-\d\d-\d\d$/.test(end || '')) throw new Error('set GRID_END=<sf end the grids were staged at>');
  const want = { std: new Set(), con: new Set() };
  for (const t of Object.values(tops)) for (const { j } of t) { const c = cand[j]; want[c.b === 'any' ? 'std' : c.b].add(c.i); }
  const WIN = LEGS.map(L => [L.k, L.s, L.e]).concat([['cur', CUR_S, end]]);
  const sel = {};   // `${win}|${basis}` -> Map(i -> row) ; bench `${win}`
  const bench = {};
  for (const b of ['std', 'con']) {
    if (!want[b].size) continue;
    const sf = path.join(HERE, '_gridmega_selidx_bull_' + UPRE + b + '.json');
    fs.writeFileSync(sf, JSON.stringify([...want[b]]));
    for (const [wk, s, e] of WIN) {
      const tag = tagOf(s, e, b), out = path.join(HERE, '_gridmega_sel_' + tag + '.json');
      log('SELECT ' + tag + ' (' + want[b].size + ' rows)…');
      cp.execFileSync('node', ['--max-old-space-size=4096', runner, s, e], { cwd: ROOT, stdio: ['ignore', 'ignore', 'inherit'],
        env: Object.assign({}, process.env, { SELECT_FILE: sf, MAIN_ONLY: '', TOPN: '3', METHOD: 'hold', UNIVERSE: UNI, EARN_BASIS: b }) });
      const J = JSON.parse(fs.readFileSync(out, 'utf8'));
      if (J.rows.length !== want[b].size) throw new Error('SELECT returned ' + J.rows.length + '/' + want[b].size + ' for ' + tag);
      sel[wk + '|' + b] = new Map(J.rows.map(r => [r.i, r]));
      if (bench[wk] == null) bench[wk] = J.bench.totRet;
      else if (bench[wk] !== J.bench.totRet) throw new Error('benchmark disagrees across bases for ' + wk);
      // the SELECT row must carry the same descriptor and (to the grid's 2dp) the same CAGR as the grid row
      if (wk !== 'cur') for (const r of J.rows) {
        const d = desc[r.i];
        if (r.s !== d.s || r.d !== d.d || r.f !== d.f) throw new Error('SELECT descriptor drift at row ' + r.i);
        if (Math.abs(r.cagr - cagr[wk + '|' + b][r.i]) > 0.011) throw new Error('SELECT cagr ' + r.cagr + ' vs grid ' + cagr[wk + '|' + b][r.i] + ' row ' + r.i + ' ' + tag);
      }
    }
  }

  // ---- assemble
  const months = (s, e) => Math.round((Date.parse(e) - Date.parse(s)) / (30.4375 * 864e5));
  const legMeta = {};
  for (const L of LEGS) legMeta[L.k] = { label: L.label, s: L.s, e: L.e, bench: bench[L.k], months: months(L.s, L.e) };
  const views = {};
  for (const [vk, V] of Object.entries(VIEWS)) {
    const rows = tops[vk].map(({ j, worst }) => {
      const c = cand[j], b = c.b === 'any' ? 'std' : c.b, d = desc[c.i];
      const pick = k => sel[k + '|' + b].get(c.i);
      return { s: d.s, d: d.d, f: d.f, b: c.b, worst,
               ranks: V.keys.map(k => ranks[k][j]), rets: V.keys.map(k => pick(k).tot),
               cur: pick('cur').tot, dd: Math.max(...V.keys.map(k => pick(k).dd)) };
    });
    const ord = rows.map((r, i) => i).sort((a, b) => rows[b].cur - rows[a].cur || a - b);
    ord.forEach((i, p) => { rows[i].curRank = p + 1; });
    views[vk] = { label: V.label, keys: V.keys, rows };
  }
  const R = {
    built: new Date().toLocaleDateString('en-CA'), dataEnd: end, universe: UNI, benchName: BENCH_NAME,
    pool: P, totalCombos: N, excluded: [...BAR], bases: ['std', 'con'],
    basket: UNI + ' · monthly · top-3 · hold · standalone + consolidated, ranked together',
    legMeta, cur: { label: 'Mar-26 → today', s: CUR_S, e: end, bench: bench.cur, months: months(CUR_S, end) }, views,
  };
  fs.writeFileSync(path.join(ROOT, 'docs', OUTF), JSON.stringify(R));
  log('wrote docs/' + OUTF + ' · pool ' + P.toLocaleString() + ' · legs ' + LEGS.map(L => L.k + ' ' + bench[L.k] + '%').join(', ') +
      ' · live ' + bench.cur + '%');
  for (const [vk, V] of Object.entries(views)) {
    const c = V.rows.map(r => r.cur).sort((a, b) => a - b);
    log('  ' + vk + ': worst ' + V.rows[0].worst.toLocaleString() + ' · live median ' + c[c.length >> 1] + '% · ' +
        V.rows.filter(r => r.cur > R.cur.bench).length + '/1000 beat the index');
  }
}
main();
