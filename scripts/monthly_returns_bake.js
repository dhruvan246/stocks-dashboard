#!/usr/bin/env node
'use strict';
/* ============================================================================
   monthly_returns_bake.js — the month-by-month return of EVERY saved strategy
   from a fixed anchor (default 31 Mar 2020) to the latest close, baked to
   docs/monthly_returns.json for the All Picks "Monthly returns" card.

   Runs the site's OWN backtest engine (backtest-engine.js, fetched LIVE) under
   Node against the live survivorship-free data — the same simulate() the
   Backtest page runs, so the numbers cannot drift from the page's. Nothing is
   re-implemented; a strategy's monthly return series is just consecutive ratios
   of simulate().equity (which the engine already marks to market every month,
   whatever the rebalance frequency).

     node --max-old-space-size=8192 scripts/monthly_returns_bake.js --out docs/monthly_returns.json
     node scripts/monthly_returns_bake.js --slice 1/4 --out /tmp/mr_1.json   (one worker of four)
     node scripts/monthly_returns_bake.js --limit 2                          (quick pipeline test)

   Grid (monthsBetween in the engine): months[0] = the anchor month-end (entry,
   no return), months[1..] = each following month-end, last = the data end (a
   PARTIAL current month). So return column k is labelled by months[k+1].
   ==========================================================================*/
const fs = require('fs'), vm = require('vm');
const SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';   // public in the repo (docs/sw-sync.js)
const REFS = ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'ITC', 'SBIN', 'LT'];   // engine's trading-day calendar
const START = '2020-03-31';   // a real month-END so the strategy grid and the benchmark coincide (see the four-way-sweep note)
const CAP = 1000000;          // returns are capital-independent (ratios); a round number keeps the log readable

const argv = process.argv.slice(2);
const arg = k => { const i = argv.indexOf(k); return i >= 0 ? argv[i + 1] : null; };
const OUT = arg('--out');
const SLICE = arg('--slice');           // "i/N" — 1-based worker index / worker count
const LIMIT = arg('--limit') ? +arg('--limit') : null;

/* ---- browser shims: the engine is DOM-free but reads location at load and fetches by RELATIVE url. */
globalThis.location = { hostname: 'dhruvan246.github.io', protocol: 'https:' };
const _fetch = globalThis.fetch;
globalThis.fetch = (url, opt) => {
  const u = String(url); const o = Object.assign({}, opt || {}); delete o.cache;   // undici rejects cache:'reload'
  return _fetch(u.startsWith('./') ? SITE + u.slice(2) : u, o);
};
const log = m => process.stderr.write(m + '\n');
const istNow = () => new Date().toLocaleString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false })
  .replace(/(\d+)\/(\d+)\/(\d+),?\s+(\d+):(\d+):\d+/, '$3-$2-$1 $4:$5 IST');

async function rpc(fn, body) {
  const r = await _fetch(SUPA + fn, { method: 'POST', headers: {
    apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' },
    body: JSON.stringify(body) });
  const t = (await r.text()).trim();
  if (!r.ok) throw new Error('supabase ' + fn + ' HTTP ' + r.status + ' ' + t.slice(0, 200));
  return t ? JSON.parse(t) : null;
}
const round = (x, d) => (x == null || !isFinite(x)) ? null : +(+x).toFixed(d);
// "2026-08-28" -> "Aug 2026"; partial-month flag handled by the caller
const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const monLabel = d => MON[+d.slice(5, 7) - 1] + " '" + d.slice(2, 4);

async function loadLive(file, exposeSrc) {
  const code = await (await _fetch(SITE + file + '?t=' + Date.now())).text();
  vm.runInThisContext(code + '\n;' + exposeSrc, { filename: file });
  return code.length;
}

async function main() {
  // 1) the shared saved-strategy list — the same public RPC saved-strategies.html reads
  const records = (await rpc('bt_strats_public', {}) || []).filter(s => s && s.cfg);
  log(records.length + ' raw shared records');

  // 2) the LIVE shared helpers + engine (NOT the checkout — see feedback-verify-against-the-version-that-ships)
  await loadLive('bt-identity.js', 'globalThis.__ID={identityKey,ruleKey,bakeGroups,saveSerials};');
  await loadLive('bt-names.js', 'globalThis.__N={strategyEnglish:(typeof strategyEnglish!=="undefined")?strategyEnglish:null};');
  const engBytes = await loadLive('backtest-engine.js',
    'globalThis.__E={simulate,factorsAt,passFilters,fieldVal,loadEngineData,ensureHistoryFor,strategyLabel,getSF:()=>SF,getMETA:()=>META,getSERIES:()=>SERIES};');
  const ID = globalThis.__ID, N = globalThis.__N, E = globalThis.__E;

  // 3) cards = one rep per identityKey (exactly what saved-strategies renders); serial = its DDMMYY-NN tag
  const reps = ID.bakeGroups(records);       // Map identityKey -> newest rep record
  const serials = ID.saveSerials(records);   // Map identityKey -> save serial
  let cards = [...reps.entries()].map(([k, rep]) => ({ key: k, cfg: rep.cfg, serial: serials.get(k) || '' }));
  cards.sort((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0);   // stable order → deterministic slicing
  if (SLICE) { const [i, n] = SLICE.split('/').map(Number); cards = cards.filter((_, idx) => idx % n === (i - 1)); }
  if (LIMIT) cards = cards.slice(0, LIMIT);
  log((reps.size) + ' cards total · processing ' + cards.length + (SLICE ? ' (slice ' + SLICE + ')' : '') + (LIMIT ? ' (limit ' + LIMIT + ')' : ''));

  // 4) engine data (live bins) — once per process
  const t0 = Date.now();
  await E.loadEngineData(m => { if (m) log('  ' + m); });
  const SF = E.getSF();
  log('engine data loaded in ' + Math.round((Date.now() - t0) / 1000) + 's · bars to ' + SF.end + ' · engine ' + engBytes + ' B');

  let months = null, benchRets = null;   // shared grid + Nifty-500 monthly returns (identical across cards)
  const out = [];
  for (const card of cards) {
    const cfg = Object.assign({}, card.cfg, { start: START, end: SF.end, mode: 'sf', capital: CAP });
    await E.ensureHistoryFor(cfg.start);
    const res = E.simulate(cfg);
    const eq = res.equity;                  // [[monthDate, value], ...]  months[0]=anchor (=CAP), then each month-end, last=SF.end
    const dates = eq.map(p => p[0]);
    const rets = []; for (let i = 1; i < eq.length; i++) rets.push(eq[i - 1][1] > 0 ? round((eq[i][1] / eq[i - 1][1] - 1) * 100, 2) : null);
    // grid + benchmark: capture once from the first card (identical window for all)
    if (!months) {
      months = dates;
      const b = res.bench500 || [];
      benchRets = [];
      for (let i = 1; i < b.length; i++) benchRets.push((b[i - 1] && b[i] && b[i - 1][1] > 0 && b[i][1] != null) ? round((b[i][1] / b[i - 1][1] - 1) * 100, 2) : null);
    } else if (dates.length !== months.length) {
      throw new Error('grid mismatch: ' + card.serial + ' has ' + dates.length + ' points vs master ' + months.length);
    }
    // SANITY: the STORED (2dp-rounded) monthly returns, compounded, must reproduce the full-window
    // total — within the rounding drift that ~77 two-decimal factors accumulate (a real off-by-one or
    // sign bug blows past 1%). The strong, INDEPENDENT check is total vs the baked `waves` cycle ret,
    // run as a separate step after the merge (see the four-way-sweep oracle).
    let comp = 1; for (const r of rets) if (r != null) comp *= (1 + r / 100);
    const total = res.finalV / CAP;
    if (total > 0.01 && Math.abs(comp / total - 1) > 0.01) throw new Error('compounding sanity FAILED for ' + card.serial + ': ' + comp.toFixed(6) + ' vs ' + total.toFixed(6));
    const name = (N.strategyEnglish && N.strategyEnglish(cfg)) || E.strategyLabel(cfg);
    out.push({ key: card.key, serial: card.serial, name, topN: cfg.topN,
      basis: cfg.earnBasis || 'con', method: cfg.method || 'hold',
      total: round((total - 1) * 100, 2), cagr: round(res.cagr, 2), maxDD: round(res.maxDD, 2), rets });
    log(card.serial + '  ' + name.slice(0, 46).padEnd(46) + '  ' + rets.length + ' mo  total ' + round((total - 1) * 100, 1) + '%  (oracle ok)');
  }

  const payload = { generated: istNow(), dataEnd: SF.end, start: START,
    months: (months || []).slice(1),                    // return columns = months[1..] (months[0] is the anchor/entry)
    anchorEnd: (months || [])[0] || START,
    labels: (months || []).slice(1).map(monLabel),
    partialLast: ((months || [])[months ? months.length - 1 : 0] || '') !== '',   // last col is a partial month unless data lands on a month-end
    benchName: 'Nifty 500', benchRets: benchRets || [],
    engineBytes: engBytes, cards: out };
  if (OUT) { fs.writeFileSync(OUT + '.tmp', JSON.stringify(payload)); fs.renameSync(OUT + '.tmp', OUT); log('wrote ' + OUT + ' (' + fs.statSync(OUT).size + ' B, ' + out.length + ' cards, ' + payload.months.length + ' months)'); }
  else process.stdout.write(JSON.stringify(payload));
}
main().catch(e => { log('FAILED: ' + (e && e.stack || e)); process.exit(1); });
