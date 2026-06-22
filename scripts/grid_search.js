// Reusable strategy grid-search — runs the REAL backtest engine in Node (no browser freeze).
// It is APPENDED to docs/backtest-engine.js (shares its module scope, so it can set the engine
// globals SF/META/SERIES/IDXH/FNOH/NIFTY/FUND and call activateSF()/simulate()).
// Build + run:
//   cat docs/backtest-engine.js scripts/grid_search.js > scripts/_grid_run.js && node scripts/_grid_run.js
//   (pass `validate` as arg to just check 2 combos against the live site first)
// Edit `base` (window/universe/freq/topN) and `FSETS` (filters) below for a different search.
// Writes ranked top-25 (by CAGR and by risk-adjusted CAGR/maxDD) to scripts/_gridresult.json.
(function () {
  const fs = require('fs'), zlib = require('zlib'), path = require('path');
  const ROOT = path.resolve(__dirname, '..');
  const GZ = p => JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, p))));
  const J = p => JSON.parse(fs.readFileSync(path.join(ROOT, p), 'utf8'));

  // ---- loadCore() equivalent ----
  const SD = GZ('docs/stock_data.bin');
  IDXH = SD.indicesHistory || {}; FNOH = SD.fnoHistory || []; START_TS = SD.startTs;
  NIFTY = (J('docs/nifty.json').px) || {};

  // ---- loadSF() equivalent (mirrors backtest-engine.js loadSF) ----
  const SFD = GZ('docs/sf_stock_data.bin');
  const ts = START_TS, ser = {}, turn = {}, meta = {};
  for (const sym in SFD.data) {
    const o = SFD.data[sym], n = o.d.length, d = new Array(n), p = new Array(n), t = new Array(n);
    const hasHL = o.h && o.l, h = hasHL ? new Array(n) : null, l = hasHL ? new Array(n) : null;
    for (let i = 0; i < n; i++) {
      const y = o.d[i];
      const off = Math.floor((Date.UTC(Math.floor(y / 10000), (Math.floor(y / 100) % 100) - 1, y % 100) / 1000 - ts) / DAY);
      d[i] = off; p[i] = Math.round(o.c[i] * 100); t[i] = o.t[i] || 0;
      if (hasHL) { h[i] = Math.round(o.h[i] * 100); l[i] = Math.round(o.l[i] * 100); }
    }
    ser[sym] = { d, p }; turn[sym] = { d, t }; const sm = SFD.meta[sym] || {};
    if (hasHL) { ser[sym].h = h; ser[sym].l = l; } else if (o.hb && o.lb) { ser[sym].hb = o.hb; ser[sym].lb = o.lb; }
    if (o.v) ser[sym].v = o.v;
    if (o.dv) ser[sym].dv = hasHL ? o.dv : o.dv.map(x => x / 10);
    meta[sym] = { symbol: sym, name: sm.name || sym, industry: sm.ind || 'Other', sector: sm.ind || 'Other', mcap: 0, latest: o.c[n - 1], alive: sm.alive, raw: sm.raw || null };
  }
  const endOff = Math.floor((Date.parse((SFD.end || '2024-01-01') + 'T00:00:00Z') / 1000 - ts) / DAY);
  SF = { meta, series: ser, turn, startTs: ts, endOff, start: SFD.start, end: SFD.end,
         nDead: Object.values(meta).filter(m => !m.alive).length, nTot: Object.keys(meta).length };
  activateSF();
  FUND = J('docs/sf_fundamentals.json');
  console.error('loaded: stocks=' + SF.nTot + ' end=' + SF.end + ' fund=' + Object.keys(FUND).length);

  // ===== EDIT HERE for a different search =====
  const base = { start: '2024-09-30', end: '2026-03-31', indexName: 'Nifty 500', freq: 1, lookback: 1, topN: 5, capital: 100000, mode: 'sf', earnBasis: 'con', mcapFloor: 0 };
  const FSETS = [[], [{ field: 'profitYoyPct', op: '>', val: 0 }], [{ field: 'profitYoyPct', op: '>', val: 25 }],
    [{ field: 'rsi', op: '<', val: 70 }], [{ field: 'rsi', op: '>=', val: 40 }], [{ field: 'd52', op: '<=', val: 10 }],
    [{ field: 'd52', op: '<=', val: 25 }], [{ field: 'turnover', op: '>', val: 50 }],
    [{ field: 'profitYoyPct', op: '>', val: 0 }, { field: 'rsi', op: '<', val: 70 }]];
  // ============================================

  const run = cfg => { try { const r = simulate(cfg); return (r && isFinite(r.cagr)) ? { sortBy: cfg.sortBy, dir: cfg.dir, method: cfg.method, filters: cfg.filters, cagr: r.cagr, maxDD: r.maxDD, finalV: r.finalV, benchCagr: r.benchCagr } : null; } catch (e) { return null; } };

  if (process.argv[2] === 'validate') {
    const a = run({ ...base, sortBy: 'changePercent', dir: 'high', method: 'reset', filters: [] });
    console.log(JSON.stringify({ changePct_high_reset: a && +a.cagr.toFixed(2), bench: a && +a.benchCagr.toFixed(2) }));
    return;
  }

  const SORTS = FIELDS.map(f => f.v), DIRS = ['high', 'low'], METHODS = ['hold', 'reset'];
  const t0 = Date.now(); const results = []; let done = 0;
  const s1 = []; for (const s of SORTS) for (const d of DIRS) for (const m of METHODS) s1.push({ ...base, sortBy: s, dir: d, method: m, filters: [] });
  for (const cfg of s1) { const r = run(cfg); if (r) results.push(r); if (++done % 20 === 0) console.error('stage1 ' + done + '/' + s1.length + '  ' + ((Date.now() - t0) / 1000 | 0) + 's'); }
  const top = results.slice().sort((a, b) => b.cagr - a.cagr).slice(0, 12);
  const s2 = []; for (const tt of top) for (let fi = 1; fi < FSETS.length; fi++) s2.push({ ...base, sortBy: tt.sortBy, dir: tt.dir, method: tt.method, filters: FSETS[fi] });
  done = 0; for (const cfg of s2) { const r = run(cfg); if (r) results.push(r); if (++done % 20 === 0) console.error('stage2 ' + done + '/' + s2.length); }
  results.sort((a, b) => b.cagr - a.cagr);
  const out = { window: [base.start, base.end], universe: base.indexName, topN: base.topN, freq: base.freq,
    bench: results.length ? +results[0].benchCagr.toFixed(2) : null, totalCombos: results.length,
    topByCAGR: results.slice(0, 25), topByRiskAdj: results.slice().sort((a, b) => (b.cagr / (b.maxDD || 1)) - (a.cagr / (a.maxDD || 1))).slice(0, 10) };
  fs.writeFileSync(path.join(ROOT, 'scripts/_gridresult.json'), JSON.stringify(out, null, 1));
  console.error('DONE combos=' + results.length + ' bestCAGR=' + results[0].cagr.toFixed(2) + ' bench=' + out.bench + ' in ' + ((Date.now() - t0) / 1000 | 0) + 's');
})();
