#!/usr/bin/env node
// A/B the ⭐ favourite strategies through the site's own engine with two Nifty 500 roster files (DATA_RUNBOOK §132f).
// usage: node scripts/ab_favs.js docs/backtest-engine.js docs/bt-identity.js favs.json strats_public.json out.json [candidate_indices_history.json]
//   favs.json        = the bt_fav_strategies value from sw_kv_get(SETTINGS)   (see docs/sw-sync.js; anon key in scripts/check_fav_uniques.js)
//   strats_public    = the bt_strats_public RPC payload                       (docs/bt-sync.js)
// Loads the LIVE data (sf-data origin) like the page; call ensureHistoryFor(start) before each simulate (pre-2019 starts).
// Baskets are rebs[].holds[].sym — there is no picks field; a diff on a missing field compares empty lists and lies (§132f).
'use strict';
const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
const [engPath, idPath, favsPath, stratsPath, outPath, candPath] = process.argv.slice(2);
const SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
globalThis.location = { hostname: 'dhruvan246.github.io', protocol: 'https:', href: SITE };
globalThis.localStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };
const _fetch = globalThis.fetch;
globalThis.fetch = (url, opt) => { const u = String(url); const o = Object.assign({}, opt || {}); delete o.cache; return _fetch(u.startsWith('./') ? SITE + u.slice(2) : u, o); };
const eng = fs.readFileSync(engPath, 'utf8');
vm.runInThisContext(eng + '\n;globalThis.__E={simulate,loadEngineData,getIDXH:()=>IDXH,setIDXH:(h)=>{IDXH=h;},getSF:()=>SF,ensureHistoryFor:ensureHistoryFor};', { filename: 'backtest-engine.js' });
vm.runInThisContext(fs.readFileSync(idPath, 'utf8') + '\n;globalThis.__ID={identityKey,bakeGroups};', { filename: 'bt-identity.js' });
const E = globalThis.__E, ID = globalThis.__ID;
function summarize(res) {
  const eq = res.equity || []; const v0 = eq.length ? eq[0][1] : null, v1 = eq.length ? eq[eq.length - 1][1] : null;
  const yrs = eq.length > 1 ? (new Date(eq[eq.length - 1][0]) - new Date(eq[0][0])) / 31557600000 : 0;
  const cagr = (v0 && v1 && yrs > 0) ? (Math.pow(v1 / v0, 1 / yrs) - 1) * 100 : null;
  let peak = -Infinity, mdd = 0; for (const [, v] of eq) { if (v > peak) peak = v; const dd = (v - peak) / peak * 100; if (dd < mdd) mdd = dd; }
  const rebs = res.rebs || []; const baskets = rebs.map(r => [r.date, (Array.isArray(r.holds) ? r.holds : Object.keys(r.holds || {})).map(p => (p && (p.tkr || p.symbol || p.sym)) || p).sort()]);
  const tradeKeys = (res.trades || []).map(t => [t.tkr || t.symbol || t.sym, t.entryDate || t.entry || t.in, t.exitDate || t.exit || t.out].join('|'));
  const trades = (res.trades || []).length;
  const top = {}; for (const k of Object.keys(res)) { const v = res[k]; if (typeof v === 'number') top[k] = v; }
  return { final: v1, cagr, maxDD: mdd, trades, nReb: rebs.length, months: eq.length, top, baskets, tradeKeys, rebKeys: rebs.length ? Object.keys(rebs[0]) : [], holdsSample: rebs.length ? JSON.stringify(rebs[0].holds).slice(0, 200) : null, tradeSample: (res.trades && res.trades[0]) ? JSON.stringify(res.trades[0]).slice(0, 200) : null };
}
(async () => {
  const t0 = Date.now();
  await E.loadEngineData(() => {});
  const SF = E.getSF(); console.error('engine', (eng.match(/const ENGINE_VER='(e[0-9]+)'/)||[])[1], 'data end', SF.end, 'loaded in', ((Date.now() - t0) / 1000) | 0, 's');
  const favs = JSON.parse(fs.readFileSync(favsPath, 'utf8')); const saved = JSON.parse(fs.readFileSync(stratsPath, 'utf8'));
  const groups = ID.bakeGroups(saved);
  const cfgs = favs.map(fk => { const rep = groups.get(fk); return rep ? { key: fk, cfg: rep.cfg, name: rep.name || (rep.cfg && rep.cfg.name) } : { key: fk, cfg: null }; });
  console.error('favourites resolved:', cfgs.filter(c => c.cfg).length, '/', favs.length);
  const base = E.getIDXH(); const liveN500 = base['Nifty 500'];
  const out = { engine: (eng.match(/const ENGINE_VER='(e[0-9]+)'/)||[])[1], dataEnd: SF.end, live_n500_snaps: liveN500.length, runs: {} };
  const variants = [['A_live', null]]; if (candPath) variants.push(['B_candidate', JSON.parse(fs.readFileSync(candPath, 'utf8'))['Nifty 500']]);
  for (const [label, roster] of variants) {
    const h = Object.assign({}, base); if (roster) h['Nifty 500'] = roster; E.setIDXH(h);
    out.runs[label] = { n500_snaps: h['Nifty 500'].length, strategies: [] };
    for (const c of cfgs) {
      if (!c.cfg) { out.runs[label].strategies.push({ key: c.key, error: 'unresolved' }); continue; }
      const t1 = Date.now(); let s; try { if (E.ensureHistoryFor) await E.ensureHistoryFor(c.cfg.start); } catch (e) { console.error('ensureHistoryFor failed', e && e.message); }
      try { s = summarize(E.simulate(c.cfg)); } catch (e) { s = { error: String(e && e.message || e) }; }
      s.key = c.key; s.name = c.name; s.secs = (Date.now() - t1) / 1000; out.runs[label].strategies.push(s);
      console.error(label, c.key.slice(0, 60), 'CAGR', s.cagr && s.cagr.toFixed(2), 'maxDD', s.maxDD && s.maxDD.toFixed(1), 'trades', s.trades, s.secs + 's');
    }
  }
  fs.writeFileSync(outPath, JSON.stringify(out));
  console.error('wrote', outPath);
})().catch(e => { console.error('FAILED', e && e.stack || e); process.exit(1); });
