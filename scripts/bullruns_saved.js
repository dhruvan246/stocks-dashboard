#!/usr/bin/env node
'use strict';
/* ============================================================================
   bullruns_saved.js — run the SHARED saved strategies through the measured bull runs.

   Feeds the two "Your saved strategies" cards on docs/bull-runs.html. Same five
   windows the grid cards use (four completed legs + the live one), same measured
   trough/peak dates, but the population is the user's own saved list instead of
   the 5.44M-combo grid — so "rank" here means rank WITHIN the saved set.

   Nothing re-implements the engine: it fetches docs/backtest-engine.js from the
   live site and calls simulate(), exactly like scripts/portfolio_model.js.

     node --max-old-space-size=8192 scripts/bullruns_saved.js
   ==========================================================================*/
const fs = require('fs'), vm = require('vm'), path = require('path');
const SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
const ROOT = path.join(__dirname, '..');

/* the five windows — measured off the NIFTY 500 daily close with a 13% reversal
   filter (see docs/bull_runs.json legMeta; re-derived and verified 2026-08-28). */
const LEGS = [
  { k: '2013-15', s: '2013-08-28', e: '2015-03-03' },
  { k: '2016-18', s: '2016-02-25', e: '2018-08-31' },
  { k: '2020-21', s: '2020-03-23', e: '2021-10-18' },
  { k: '2022-24', s: '2022-06-20', e: '2024-09-26' },
];
const CUR = { k: 'cur', s: '2026-03-30', e: '2026-08-24' };   // end matches the grid cards' dataEnd
const ALL = LEGS.concat([CUR]);

globalThis.location = { hostname: 'dhruvan246.github.io', protocol: 'https:' };
globalThis.localStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };
const _fetch = globalThis.fetch;
globalThis.fetch = (url, opt) => {
  const u = String(url), o = Object.assign({}, opt || {}); delete o.cache;
  return _fetch(u.startsWith('./') ? SITE + u.slice(2) : u, o);
};
const log = m => process.stderr.write(m + '\n');

async function savedStrategies() {
  const r = await _fetch(SUPA + 'bt_strats_public', { method: 'POST', headers: {
    apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' }, body: '{}' });
  if (!r.ok) throw new Error('bt_strats_public HTTP ' + r.status);
  const j = JSON.parse(await r.text());
  if (!Array.isArray(j) || !j.length) throw new Error('no saved strategies came back');
  return j;
}

async function main() {
  const saved = await savedStrategies();
  log('saved strategies (live): ' + saved.length);

  const code = await (await _fetch(SITE + 'backtest-engine.js?t=' + Date.now())).text();
  vm.runInThisContext(code + '\n;globalThis.__E={simulate,strategyLabel,loadEngineData,ensureHistoryFor,getSF:()=>SF};',
    { filename: 'backtest-engine.js' });
  const E = globalThis.__E;
  const t0 = Date.now();
  await E.loadEngineData(m => { if (m) log('  ' + m); });
  log('engine data loaded in ' + Math.round((Date.now() - t0) / 1000) + 's · bars to ' + E.getSF().end);
  await E.ensureHistoryFor(LEGS[0].s);
  log('history ensured back to ' + LEGS[0].s);

  /* Per-window sanity: the engine may pull `start` to a nearby trading bar. Record what it
     actually used and what the NIFTY 500 did over that same span, so the cards can be checked
     against the measured legs instead of trusted. */
  const seen = {};
  const rows = [];
  saved.forEach((it, n) => {
    const base = it.cfg || {};
    const rec = { id: it.id, ts: it.ts, name: E.strategyLabel(base), cfg: base, r: {}, dd: {} };
    for (const w of ALL) {
      const cfg = Object.assign({}, base, { start: w.s, end: w.e, mode: 'sf' });
      const res = E.simulate(cfg);
      const cap = base.capital || 100000;
      if (!res || !isFinite(res.finalV)) throw new Error('simulate returned no finalV for ' + it.id + ' @ ' + w.k);
      rec.r[w.k] = +((res.finalV / cap - 1) * 100).toFixed(1);
      rec.dd[w.k] = +res.maxDD.toFixed(1);
      const b5 = res.bench500, last = b5 && b5.length ? b5[b5.length - 1][1] : null;
      const s = seen[w.k] || (seen[w.k] = { effStart: {}, bench: {} });
      s.effStart[res.effStart] = (s.effStart[res.effStart] || 0) + 1;
      if (last != null) { const bv = +((last / cap - 1) * 100).toFixed(1); s.bench[bv] = (s.bench[bv] || 0) + 1; }
    }
    rows.push(rec);
    log('  [' + (n + 1) + '/' + saved.length + '] ' + ALL.map(w => w.k + '=' + rec.r[w.k] + '%').join(' '));
  });

  log('--- window sanity (effective start / NIFTY 500 over the window, as the ENGINE saw them) ---');
  const legMeta = {};
  for (const w of ALL) {
    const s = seen[w.k];
    const es = Object.keys(s.effStart), bs = Object.keys(s.bench).map(Number);
    log('  ' + w.k + ' asked ' + w.s + '→' + w.e + ' | engine effStart ' + es.join(',') +
        ' | bench500 ' + bs.join(',') + '%');
    if (es.length !== 1) throw new Error(w.k + ': strategies disagree on effStart — ' + es.join(','));
    if (bs.length !== 1) throw new Error(w.k + ': strategies disagree on the benchmark — ' + bs.join(','));
    legMeta[w.k] = { s: w.s, e: w.e, effStart: es[0], bench: bs[0] };
  }

  fs.writeFileSync(path.join(ROOT, 'scripts', '_bullsaved_raw.json'),
    JSON.stringify({ built: new Date().toISOString().slice(0, 10), dataEnd: E.getSF().end,
                     n: rows.length, legs: ALL, legMeta, rows }));
  log('wrote scripts/_bullsaved_raw.json — ' + rows.length + ' strategies × ' + ALL.length + ' windows');
}
main().catch(e => { log('FATAL ' + (e && e.stack || e)); process.exit(1); });
