#!/usr/bin/env node
'use strict';
/* Pre-buy sanity check: how many UNIQUE stocks do the ⭐ favourite strategies buy this rebalance,
   on whatever data is currently live. Runs the site's own engine under Node (same loader the
   nightly portfolio_model.js uses) + bt-identity.js to resolve the favourites, then screens each
   at the latest close — exactly the all-picks page's Rebalance screen. Prints a plain report.
     node check_fav_uniques.js
*/
const vm = require('vm');
const SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
globalThis.location = { hostname: 'dhruvan246.github.io', protocol: 'https:' };
const _fetch = globalThis.fetch;
globalThis.fetch = (url, opt) => { const u = String(url); const o = Object.assign({}, opt || {}); delete o.cache;
  return _fetch(u.startsWith('./') ? SITE + u.slice(2) : u, o); };
const log = m => process.stdout.write(m + '\n');

async function rpc(fn, body) {
  const r = await _fetch(SUPA + fn, { method: 'POST', headers: {
    apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const t = (await r.text()).trim(); if (!r.ok) throw new Error('supabase ' + fn + ' ' + r.status + ' ' + t.slice(0, 150));
  return t ? JSON.parse(t) : null;
}

async function main() {
  // 1. engine + identity helpers into scope
  const eng = await (await _fetch(SITE + 'backtest-engine.js?t=' + Date.now())).text();
  vm.runInThisContext(eng + '\n;globalThis.__E={factorsAt,passFilters,fieldVal,loadEngineData,dayOff,isoOff,getSF:()=>SF,getMETA:()=>META};',
    { filename: 'backtest-engine.js' });
  const ident = await (await _fetch(SITE + 'bt-identity.js?t=' + Date.now())).text();
  vm.runInThisContext(ident + '\n;globalThis.__ID={identityKey,bakeGroups};', { filename: 'bt-identity.js' });
  const E = globalThis.__E, ID = globalThis.__ID;
  await E.loadEngineData(() => {});
  const SF = E.getSF(), META = E.getMETA();
  const off = E.dayOff(SF.end);

  // 2. the 8 favourites -> full cfgs (newest saved version per identity)
  const kv = await rpc('sw_kv_get', { k: 'SETTINGS' });
  const favEntry = kv.find(x => x && x.k === 'bt_fav_strategies');
  const favs = favEntry && favEntry.v ? JSON.parse(favEntry.v) : [];
  const saved = await rpc('bt_strats_public', {});
  const groups = ID.bakeGroups(saved);

  // 3. screen each at the latest close (== all-picks Rebalance mode)
  const counts = {}, perFav = []; let unmatched = 0;
  favs.forEach((fk, i) => {
    const rep = groups.get(fk); if (!rep) { unmatched++; return; }
    const cfg = rep.cfg;
    let rows = E.factorsAt(off, cfg).filter(r => r.rsi != null && E.passFilters(r, cfg.filters));
    rows = rows.filter(r => E.fieldVal(r, cfg.sortBy) != null);
    rows.sort((a, b) => { const x = E.fieldVal(a, cfg.sortBy), y = E.fieldVal(b, cfg.sortBy); return cfg.dir === 'high' ? y - x : x - y; });
    const picks = rows.slice(0, cfg.topN).map(r => META[r.tkr].symbol || r.tkr);
    perFav.push({ n: i + 1, sortBy: cfg.sortBy, dir: cfg.dir, picks });
    picks.forEach(s => counts[s] = (counts[s] || 0) + 1);
  });

  const uniq = Object.keys(counts).sort((a, b) => counts[b] - counts[a] || (a < b ? -1 : 1));
  const shared = uniq.filter(s => counts[s] > 1).map(s => s + '×' + counts[s]);
  log('════════ FAVOURITE-STRATEGY BUY LIST — as of close ' + SF.end + ' ════════');
  log('strategies: ' + favs.length + (unmatched ? ' (' + unmatched + ' unresolved!)' : '') + '   ·   slots: ' + perFav.reduce((s, x) => s + x.picks.length, 0) + '   ·   UNIQUE STOCKS: ' + uniq.length);
  log('');
  perFav.forEach(f => log('  #' + f.n + '  ' + f.sortBy + '/' + f.dir + ':  ' + f.picks.join(', ')));
  log('');
  log('  shared across strategies: ' + (shared.join('  ') || 'none'));
  log('  all unique: ' + uniq.join(', '));
  // machine-readable tail for the scheduled report
  log('\nRESULT_JSON=' + JSON.stringify({ asOf: SF.end, unique: uniq.length, list: uniq, shared: shared, unmatched: unmatched }));
}
main().catch(e => { log('CHECK FAILED: ' + (e && e.stack || e)); process.exit(1); });
