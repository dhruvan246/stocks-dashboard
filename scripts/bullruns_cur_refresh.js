#!/usr/bin/env node
'use strict';
/* ============================================================================
   bullruns_cur_refresh.js — roll the LIVE ("Now") column of docs/bull-runs.html
   forward to the latest data, for the 2 x 1000 grid rows in docs/bull_runs.json.

   The four completed legs and the rankings on them are NOT touched — only the
   read-only live column (cur, curRank), the live leg's end/bench/months, and
   built/dataEnd. Before anything is written it:
     1. re-measures the live leg off the NIFTY 500 daily close with the same 13%
        reversal filter: the trough must still be the low since it, and no 13% fall
        from the high since may have happened (else the leg has ENDED — stop and ask);
     2. replays a sample of rows over the OLD window and requires the stored cur
        to the decimal, so the row -> cfg mapping is proven, not assumed.

   Saved-strategy cards (docs/bull_runs_saved.json) are refreshed separately:
   scripts/bullruns_saved.js (CUR.e) then scripts/bullruns_saved_build.js.

     node --max-old-space-size=8192 scripts/bullruns_cur_refresh.js [--write]
   ==========================================================================*/
const fs = require('fs'), vm = require('vm'), path = require('path');
const SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
const ROOT = path.join(__dirname, '..');
const FILE = path.join(ROOT, 'docs', 'bull_runs.json');
const WRITE = process.argv.includes('--write');
const REVERSAL = 0.13, CAPITAL = 100000;

globalThis.location = { hostname: 'dhruvan246.github.io', protocol: 'https:' };
globalThis.localStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };
const _fetch = globalThis.fetch;
globalThis.fetch = (url, opt) => {
  const u = String(url), o = Object.assign({}, opt || {}); delete o.cache;
  return _fetch(u.startsWith('./') ? SITE + u.slice(2) : u, o);
};
const log = m => process.stderr.write(m + '\n');

/* "d52<=10 & profitYoyPct>50" -> [{field,op,val}] — the page's own row notation */
function parseFilters(f) {
  if (!f) return [];
  return f.split('&').map(t => {
    const m = t.trim().match(/^(\w+)\s*(<=|>=|<|>|==|=)\s*(-?[\d.]+)$/);
    if (!m) throw new Error('cannot parse filter atom ' + JSON.stringify(t));
    return { field: m[1], op: m[2], val: +m[3] };
  });
}
const cfgOf = (r, s, e) => ({ start: s, end: e, indexName: 'Nifty 500', freq: 1, lookback: 1, topN: 3,
  capital: CAPITAL, mode: 'sf', earnBasis: r.b, mcapFloor: 0, method: 'hold',
  sortBy: r.s, dir: r.d, filters: parseFilters(r.f) });

async function main() {
  const R = JSON.parse(fs.readFileSync(FILE, 'utf8'));
  const code = await (await _fetch(SITE + 'backtest-engine.js?t=' + Date.now())).text();
  vm.runInThisContext(code + '\n;globalThis.__E={simulate,loadEngineData,getSF:()=>SF,getN5:()=>NIFTY500};',
    { filename: 'backtest-engine.js' });
  const E = globalThis.__E;
  await E.loadEngineData(m => { if (m) log('  ' + m); });
  const END = E.getSF().end;
  log('engine bars to ' + END);

  /* 1. re-measure the live leg */
  const N5 = E.getN5(), days = Object.keys(N5).filter(d => d >= R.cur.s && d <= END).sort();
  const lo = N5[R.cur.s];
  if (lo == null) throw new Error('no NIFTY 500 close on the stored trough ' + R.cur.s);
  let hi = lo, hiD = R.cur.s, minAfter = Infinity, maxFall = 0, fallD = null;
  for (const d of days) {
    const v = N5[d];
    if (v < minAfter && d > R.cur.s) minAfter = v;
    if (v > hi) { hi = v; hiD = d; }
    const fall = 1 - v / hi; if (fall > maxFall) { maxFall = fall; fallD = d; }
  }
  log('live leg: trough ' + R.cur.s + ' ' + lo + ' · high ' + hiD + ' ' + hi + ' · last ' + days[days.length - 1] +
      ' ' + N5[days[days.length - 1]] + ' · deepest fall from a running high ' + (maxFall * 100).toFixed(1) + '% on ' + fallD +
      ' · lowest close after trough ' + minAfter);
  if (minAfter < lo) throw new Error('a close below the trough exists after it — the trough moved; re-measure by hand');
  if (maxFall >= REVERSAL) throw new Error('a ' + (maxFall * 100).toFixed(1) + '% fall from the high — the live leg has ENDED; ask the user');

  /* 2. replay a sample over the OLD window — a REPORT, not a gate: fundamentals/SHP heals since the
     last build legitimately move old-window returns; the benchmark (prices only) must not move */
  const OLD_E = R.cur.e;
  for (const [vk, V] of Object.entries(R.views)) {
    const sample = V.rows.filter((_, i) => i % 50 === 0), diffs = [];
    for (const r of sample) {
      const res = E.simulate(cfgOf(r, R.cur.s, OLD_E));
      const got = +((res.finalV / CAPITAL - 1) * 100).toFixed(1);
      const b5 = res.bench500, bv = +((b5[b5.length - 1][1] / CAPITAL - 1) * 100).toFixed(1);
      if (bv !== R.cur.bench) throw new Error('old-window benchmark ' + bv + ' != stored ' + R.cur.bench);
      diffs.push(+(got - r.cur).toFixed(1));
      if (got !== r.cur) log('  replay ' + vk + ' ' + r.b + ' ' + r.d + '-' + r.s + ' [' + r.f + '] stored ' + r.cur + ' now ' + got);
    }
    log(vk + ': ' + diffs.filter(x => x === 0).length + '/' + sample.length + ' sampled rows replay exactly over ' +
        R.cur.s + '→' + OLD_E + ' · diffs ' + JSON.stringify(diffs));
  }
  /* control: saved strategy 280826-08 was run by bullruns_saved.js with the user's OWN stored cfg
     over the same old window (cur 36.3) — if it moved too, the drift is the data, not this mapping */
  const ctl = { s: 'diiPct', d: 'low', b: 'std', f: 'd52<=10 & profitYoyPct>25 & profitStreak>=2' };
  const cres = E.simulate(cfgOf(ctl, R.cur.s, OLD_E));
  log('control 280826-08: stored 36.3 · now ' + ((cres.finalV / CAPITAL - 1) * 100).toFixed(1));

  /* 3. roll forward */
  let bench = null;
  const out = {};
  for (const [vk, V] of Object.entries(R.views)) {
    const cur = V.rows.map((r, i) => {
      const res = E.simulate(cfgOf(r, R.cur.s, END));
      if (!res || !isFinite(res.finalV)) throw new Error('no finalV for ' + vk + ' row ' + i);
      const b5 = res.bench500, bl = b5 && b5.length ? b5[b5.length - 1][1] : null;
      const bv = bl == null ? null : +((bl / CAPITAL - 1) * 100).toFixed(1);
      if (bench == null) bench = bv; else if (bv !== bench) throw new Error('benchmark disagrees: ' + bv + ' vs ' + bench);
      if (i % 200 === 0) log('  ' + vk + ' ' + i + '/' + V.rows.length);
      return res.finalV;
    });
    /* curRank: ordinal 1..1000 within the view on the unrounded value, best = 1 — the stored ranks
       are a permutation of 1..1000 (no shared places), so ties are broken, not shared */
    const order = cur.map((v, i) => i).sort((a, b) => cur[b] - cur[a] || a - b), rank = [];
    order.forEach((i, p) => { rank[i] = p + 1; });
    out[vk] = { cur: cur.map(v => +((v / CAPITAL - 1) * 100).toFixed(1)), rank };
  }
  for (const [vk, V] of Object.entries(R.views)) {
    const o = out[vk], sorted = o.cur.slice().sort((a, b) => a - b);
    log(vk + ': median now ' + sorted[sorted.length >> 1] + '% · ' + o.cur.filter(v => v > bench).length +
        '/1000 beat the index ' + bench + '%');
  }
  if (!WRITE) { log('dry run — pass --write to update ' + FILE); return; }
  for (const [vk, V] of Object.entries(R.views)) V.rows.forEach((r, i) => { r.cur = out[vk].cur[i]; r.curRank = out[vk].rank[i]; });
  const months = Math.round((Date.parse(END) - Date.parse(R.cur.s)) / (30.4375 * 864e5));
  Object.assign(R.cur, { e: END, bench, months });
  R.built = new Date().toLocaleDateString('en-CA'); R.dataEnd = END;   // built = local (IST) date, not UTC
  fs.writeFileSync(FILE, JSON.stringify(R));
  log('wrote ' + FILE + ' — live leg ' + R.cur.s + '→' + END + ', index +' + bench + '%, ' + months + ' months');
}
main().catch(e => { log('FATAL ' + (e && e.stack || e)); process.exit(1); });
