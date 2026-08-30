#!/usr/bin/env node
'use strict';
/* ============================================================================
   portfolio_model.js — the STRATEGY basket + today's picks for the private
   portfolio page's "You vs the strategy" and "Rebalance calendar" cards.

   Runs the site's OWN backtest engine (backtest-engine.js, fetched live) under
   Node against the live survivorship-free data, for every portfolio that carries
   a `strategy` + `cycle`. One script, two homes — so the nightly cloud job and the
   Mac's Publish.command can never produce a different model.json:

     CLOUD  (GitHub Actions):   PF_HOLDINGS_TOKEN in env, no --out
              reads holdings from the token-addressed supabase row, and pushes
              the model back to <token>.model (last 8 rebalances, ≤4 KB column).
     LOCAL  (deploy.sh):        --holdings ./holdings.json --out ./model.json
              reads/writes plain files; push_holdings.py publishes the row.

   Nothing here re-implements the engine — it calls simulate() / factorsAt() /
   passFilters() exactly as the Backtest page does, so it cannot drift.

     node --max-old-space-size=8192 scripts/portfolio_model.js            (cloud)
     node scripts/portfolio_model.js --holdings h.json --out model.json   (local)
   ==========================================================================*/
const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
const SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';   // public in the repo (docs/sw-sync.js)
const OWNER = 'sw_owner_8Kq2Lm9Xp4Rt7v';                          // public write secret, same as push_holdings.py
const REFS = ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'ITC', 'SBIN', 'LT'];   // engine's trading-day calendar
const CAP = 4096, KEEP_REBS = 8;   // the pf_feed column cap, and how many rebalances fit under it

const argv = process.argv.slice(2);
const arg = k => { const i = argv.indexOf(k); return i >= 0 ? argv[i + 1] : null; };
const HOLD_TOKEN = (process.env.PF_HOLDINGS_TOKEN || '').trim();
const OUT_FILE = arg('--out');
const HOLD_FILE = arg('--holdings');
const CLOUD = !OUT_FILE && !!HOLD_TOKEN;   // no local output path + a token → publish to supabase

/* ---- browser shims: the engine is DOM-free but reads location at load, caches in IndexedDB
   (absent here → its helpers catch and fall through to a plain fetch) and fetches by RELATIVE url. */
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
async function loadHoldings() {
  if (HOLD_FILE) return JSON.parse(fs.readFileSync(HOLD_FILE, 'utf8'));
  if (HOLD_TOKEN) {
    const row = await rpc('pf_feed_get', { token: HOLD_TOKEN });
    if (!row || !row.z) throw new Error('holdings row not found — is PF_HOLDINGS_TOKEN right, and has push_holdings.py run?');
    return JSON.parse(zlib.gunzipSync(Buffer.from(row.z, 'base64')).toString());
  }
  throw new Error('no holdings source: set PF_HOLDINGS_TOKEN (cloud) or pass --holdings <file> (local)');
}
async function publish(out) {
  /* The strategies no longer fit one 4 KB pf_feed row (8 monthly strategies ≈ 2-3 rows), so the
     model is split: row `<token>.model` = metadata + as many strategies as pack under the cap
     (+ `parts: N` when more follow), rows `.model2`… = `{strategies:{…}}` with the rest. The page
     fetches row 1 and, when `parts` says so, the extra rows — merging all `strategies` together.
     push_holdings.py (the Mac-side publisher) splits identically — keep the two in lockstep. */
  const strats = {};
  for (const k in out.strategies) { const st = Object.assign({}, out.strategies[k]); st.rebs = (st.rebs || []).slice(-KEEP_REBS); strats[k] = st; }
  const head = Object.assign({}, out); delete head.strategies;
  const packB = doc => zlib.gzipSync(Buffer.from(JSON.stringify(doc)), { level: 9 }).toString('base64');
  const buildDoc = (sub, first, n) => {
    const doc = first ? Object.assign({}, head, { strategies: sub }) : { strategies: sub };
    if (first && n > 1) doc.parts = n;
    return doc;
  };
  /* PF_SPLIT_TEST=1 halves the split threshold so the multi-row path can be exercised with small
     real data (same hook as push_holdings.py); the per-row write guard below stays at the real CAP. */
  const lim = (process.env.PF_SPLIT_TEST === '1' ? CAP / 2 : CAP) - 32, groups = []; let cur = {};
  for (const k in strats) {                       // greedy: new row the moment the packed row would overflow
    cur[k] = strats[k];                           // (trial n=99 packs to the same length as any 2-digit count)
    if (packB(buildDoc(cur, !groups.length, 99)).length > lim && Object.keys(cur).length > 1) {
      delete cur[k]; groups.push(cur); cur = {}; cur[k] = strats[k];
    }
  }
  groups.push(cur);
  const docs = groups.map((g, i) => buildDoc(g, i === 0, groups.length));
  for (let i = 0; i < docs.length; i++) {
    const blob = packB(docs[i]);
    if (blob.length + 32 > CAP) throw new Error('one model row still ' + blob.length + ' B > the ' + CAP + ' B cap (strategies: ' +
      Object.keys(groups[i]).join(',') + ') — lower KEEP_REBS');
    const token = HOLD_TOKEN + '.model' + (i ? String(i + 1) : '');
    const ok = await rpc('pf_feed_set', { secret: OWNER, token, payload: { z: blob } });
    if (ok !== true) throw new Error('supabase rejected the model write (' + token + '): ' + JSON.stringify(ok));
    log('pushed model row ' + (i ? '<token>.model' + (i + 1) : '<token>.model') + ' — ' + blob.length + ' B of ' + CAP +
        ' (' + Object.keys(groups[i]).length + ' of ' + Object.keys(strats).length + ' strategies × last ' + KEEP_REBS + ' rebalances' +
        (docs.length > 1 ? ', row ' + (i + 1) + '/' + docs.length : '') + ')');
  }
}

function anchorFor(cycle) {
  /* Earliest cycle month whose 365-day lookback still sits inside the recent data (deepFrom
     2019-01-01): Feb-2020 onward needs no deep history. The basket at any rebalance is the
     screen's fresh top-N, so the anchor only moves weights, never names. */
  const m = cycle.filter(c => c >= 2).sort((a, b) => a - b)[0] || cycle[0];
  return '2020-' + String(m).padStart(2, '0') + '-01';
}
function cmp(x, op, v) { return op === '>' ? x > v : op === '>=' ? x >= v : op === '<' ? x < v : op === '<=' ? x <= v : x === v; }
function round(x, d) { return (x == null || !isFinite(x)) ? null : +(+x).toFixed(d); }

async function main() {
  const doc = await loadHoldings();
  const pfs = (doc.portfolios || []).filter(p => p.strategy && p.cycle && p.cycle.length);
  if (!pfs.length) { log('no portfolio carries a strategy + cycle — nothing to do'); process.exit(2); }
  log((CLOUD ? 'CLOUD' : 'LOCAL') + ' mode · ' + pfs.length + ' strategies · fetching the live engine…');

  const code = await (await _fetch(SITE + 'backtest-engine.js?t=' + Date.now())).text();
  vm.runInThisContext(code + '\n;globalThis.__E={simulate,factorsAt,passFilters,fieldVal,loadEngineData,ensureHistoryFor,'
    + 'markPrice,priceAt,dayOff,isoOff,idxLE,strategyLabel,getSF:()=>SF,getMETA:()=>META,getSERIES:()=>SERIES};',
    { filename: 'backtest-engine.js' });
  const E = globalThis.__E;
  const t0 = Date.now();
  await E.loadEngineData(m => { if (m) log('  ' + m); });
  const SF = E.getSF(), META = E.getMETA(), SERIES = E.getSERIES();
  log('engine data loaded in ' + Math.round((Date.now() - t0) / 1000) + 's · bars to ' + SF.end + ' · ' + Object.keys(SERIES).length + ' symbols');

  const tdset = new Set(); for (const r of REFS) { const s = SERIES[r]; if (s && s.d) for (const o of s.d) tdset.add(o); }
  const td = [...tdset].sort((a, b) => a - b);
  const snapTD = o => { const i = E.idxLE(td, o); return i < 0 ? o : td[i]; };
  const latestOff = td[td.length - 1];
  const symToTkr = {}; for (const t in META) symToTkr[META[t].symbol || t] = t;

  const out = { generated: istNow(), dataEnd: SF.end, picksAsOf: E.isoOff(latestOff), engineBytes: code.length, strategies: {} };
  for (const p of pfs) {
    const cfg = Object.assign({}, p.strategy, { start: anchorFor(p.cycle), end: SF.end, mode: 'sf' });
    await E.ensureHistoryFor(cfg.start);
    const res = E.simulate(cfg);
    // the screen as of the latest close — the three lines simulate() runs at a rebalance
    let rows = E.factorsAt(latestOff, cfg).filter(r => r.rsi != null && E.passFilters(r, cfg.filters));
    rows = rows.filter(r => E.fieldVal(r, cfg.sortBy) != null);
    rows.sort((a, b) => { const x = E.fieldVal(a, cfg.sortBy), y = E.fieldVal(b, cfg.sortBy); return cfg.dir === 'high' ? y - x : x - y; });
    const rankOf = {}; rows.forEach((r, i) => rankOf[r.tkr] = i + 1);
    // read every factor through fieldVal(): some are derived on access, not stored on the row
    const pick = r => ({ sym: META[r.tkr].symbol || r.tkr, f: round(E.fieldVal(r, cfg.sortBy), 1), d52: round(E.fieldVal(r, 'd52'), 1), lo: round(E.fieldVal(r, 'd52_low_pct'), 1), px: r.price });
    const picks = rows.slice(0, cfg.topN).map((r, i) => Object.assign({ rank: i + 1 }, pick(r)));
    /* monthsBetween() makes the final point `end` itself, so when the data ends inside a cycle
       month the engine re-screens at the last bar (a rebalance dated e.g. 21 Aug). That is not a
       rebalance that has happened — it is the same "if it were today" screen as `picks`. Keep
       only genuine calendar month-end rebalances. */
    const isMonthEnd = d => new Date(Date.parse(d + 'T00:00:00Z') + 864e5).getUTCDate() === 1;
    const realRebs = res.rebs.filter(r => isMonthEnd(r.date));
    const last = realRebs[realRebs.length - 1];
    // where does each stock in the model's CURRENT basket stand on today's screen?
    const standing = last.holds.map(h => {
      const tkr = symToTkr[h.sym]; const rk = rankOf[tkr];
      if (rk) return { sym: h.sym, rank: rk };
      const all = E.factorsAt(latestOff, cfg); const r = all.find(x => x.tkr === tkr);
      if (!r) return { sym: h.sym, rank: null, fail: null };
      const f = (cfg.filters || []).find(f => { const x = E.fieldVal(r, f.field); return x == null || !cmp(x, f.op, f.val); });
      return { sym: h.sym, rank: null, fail: f ? { field: f.field, op: f.op, val: f.val, x: round(E.fieldVal(r, f.field), 1) } : null };
    });
    const rebs = realRebs.map(r => {
      const off = snapTD(E.dayOff(r.date));
      return { d: r.date, td: E.isoOff(off), ret: round(r.ret, 2), val: Math.round(r.val), cashWt: round(r.cashWt, 2),
               h: r.holds.map(h => [h.sym, round(h.wt, 2), E.priceAt(symToTkr[h.sym], off), h.isNew ? 1 : 0]) };
    });
    out.strategies[p.id] = {
      name: p.name, cycle: p.cycle, anchor: cfg.start, label: E.strategyLabel(cfg), topN: cfg.topN,
      final: { val: Math.round(res.finalV), cagr: round(res.cagr, 2), years: round(res.years, 2), benchCagr: round(res.benchCagr, 2) },
      rebs, picks: { asOf: E.isoOff(latestOff), qualifying: rows.length, rows: picks }, standing
    };
    log(`${p.name}: ${rebs.length} rebalances · last ${last.date} [${last.holds.map(h => h.sym).join(', ')}] · picks now [${picks.map(x => x.sym).join(', ')}] of ${rows.length} qualifying`);
  }

  if (CLOUD) { await publish(out); }
  else if (OUT_FILE) { fs.writeFileSync(OUT_FILE + '.tmp', JSON.stringify(out)); fs.renameSync(OUT_FILE + '.tmp', OUT_FILE); log('wrote ' + OUT_FILE + ' (' + fs.statSync(OUT_FILE).size + ' B)'); }
  else { process.stdout.write(JSON.stringify(out)); }
}
main().catch(e => { log('FAILED: ' + (e && e.stack || e)); process.exit(1); });
