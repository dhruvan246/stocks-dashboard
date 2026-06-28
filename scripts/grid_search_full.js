// EXHAUSTIVE strategy grid-search — every ranking factor × both directions × every filter set
// (NO greedy pruning; unlike grid_search.js). Appended to docs/backtest-engine.js (shares scope).
// Build + run:
//   cat docs/backtest-engine.js scripts/grid_search_full.js > scripts/_gridfull_run.js && node scripts/_gridfull_run.js
// Writes ranked top-40 by CAGR and top-20 by risk-adj (CAGR/maxDD) to scripts/_gridfull_result.json.
(function () {
  const fs = require('fs'), zlib = require('zlib'), path = require('path');
  const ROOT = path.resolve(__dirname, '..');
  const GZ = p => JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(ROOT, p))));
  const J = p => JSON.parse(fs.readFileSync(path.join(ROOT, p), 'utf8'));
  const SD = GZ('docs/stock_data.bin');
  IDXH = SD.indicesHistory || {}; FNOH = SD.fnoHistory || []; START_TS = SD.startTs;
  NIFTY = (J('docs/nifty.json').px) || {};
  const SFD = GZ('docs/sf_stock_data.bin');
  const ts = START_TS, ser = {}, turn = {}, meta = {};
  for (const sym in SFD.data) {
    const o = SFD.data[sym], n = o.d.length, d = new Array(n), p = new Array(n), t = new Array(n);
    const hasHL = o.h && o.l, h = hasHL ? new Array(n) : null, l = hasHL ? new Array(n) : null;
    for (let i = 0; i < n; i++) { const y = o.d[i];
      const off = Math.floor((Date.UTC(Math.floor(y/10000),(Math.floor(y/100)%100)-1,y%100)/1000 - ts)/DAY);
      d[i]=off; p[i]=Math.round(o.c[i]*100); t[i]=o.t[i]||0;
      if (hasHL){h[i]=Math.round(o.h[i]*100);l[i]=Math.round(o.l[i]*100);} }
    ser[sym]={d,p}; turn[sym]={d,t}; const sm=SFD.meta[sym]||{};
    if (hasHL){ser[sym].h=h;ser[sym].l=l;} else if(o.hb&&o.lb){ser[sym].hb=o.hb;ser[sym].lb=o.lb;}
    if(o.v)ser[sym].v=o.v; if(o.dv)ser[sym].dv=hasHL?o.dv:o.dv.map(x=>x/10);
    meta[sym]={symbol:sym,name:sm.name||sym,industry:sm.ind||'Other',sector:sm.ind||'Other',mcap:0,latest:o.c[n-1],alive:sm.alive,raw:sm.raw||null}; }
  const endOff = Math.floor((Date.parse((SFD.end||'2024-01-01')+'T00:00:00Z')/1000 - ts)/DAY);
  SF={meta,series:ser,turn,startTs:ts,endOff,start:SFD.start,end:SFD.end,nDead:Object.values(meta).filter(m=>!m.alive).length,nTot:Object.keys(meta).length};
  activateSF(); FUND = J('docs/sf_fundamentals.json');
  console.error('loaded: stocks='+SF.nTot+' end='+SF.end+' fund='+Object.keys(FUND).length);

  // ===== search space =====
  const START = process.argv[2] || '2023-03-31';
  const base = { start:START, end:'2026-06-25', indexName:'Nifty 500', freq:1, lookback:1, topN:5, capital:100000, mode:'sf', earnBasis:'con', mcapFloor:0 };
  const FSETS = [
    [],
    [{field:'profitYoyPct',op:'>',val:0}],
    [{field:'profitYoyPct',op:'>',val:25}],
    [{field:'rsi',op:'<',val:70}],
    [{field:'rsi',op:'>=',val:40}],
    [{field:'d52',op:'<=',val:10}],
    [{field:'d52',op:'<=',val:25}],
    [{field:'rangePos',op:'>=',val:70}],
    [{field:'dma200',op:'>',val:0}],
    [{field:'turnover',op:'>',val:50}],
    [{field:'vol',op:'<',val:45}],
    [{field:'d52',op:'<=',val:10},{field:'profitYoyPct',op:'>',val:0}],   // user's filtered-mdd6 combo
    [{field:'profitYoyPct',op:'>',val:0},{field:'rsi',op:'<',val:70}],
    [{field:'profitYoyPct',op:'>',val:0},{field:'dma200',op:'>',val:0},{field:'turnover',op:'>',val:50}],
  ];
  const SORTS = FIELDS.map(f => f.v), DIRS = ['high','low'];
  const lbl = {}; FIELDS.forEach(f => lbl[f.v] = f.l);
  const fdesc = fs2 => fs2.length ? fs2.map(f=>f.field+f.op+f.val).join(' & ') : '(none)';
  const run = cfg => { try { const r = simulate(cfg); return (r && isFinite(r.cagr)) ? { sortBy:cfg.sortBy, dir:cfg.dir, method:cfg.method, topN:cfg.topN, filters:cfg.filters, cagr:+r.cagr.toFixed(2), maxDD:+r.maxDD.toFixed(1), finalV:Math.round(r.finalV), benchCagr:+r.benchCagr.toFixed(2) } : null; } catch(e){ return null; } };

  const t0 = Date.now(); const results = []; let done = 0;
  // ---- EXHAUSTIVE: every sort × dir × filterset (method=reset, topN=5) ----
  const grid = [];
  for (const s of SORTS) for (const dir of DIRS) for (const fset of FSETS) grid.push({ ...base, sortBy:s, dir, method:'reset', filters:fset });
  console.error('exhaustive combos: '+grid.length);
  for (const cfg of grid) { const r = run(cfg); if (r) results.push(r); if (++done % 40 === 0) console.error('grid '+done+'/'+grid.length+'  '+((Date.now()-t0)/1000|0)+'s'); }

  // ---- sweep method=hold + topN on the top 15 distinct winners ----
  results.sort((a,b)=>b.cagr-a.cagr);
  const top = results.slice(0, 15);
  const sweep = [];
  for (const tt of top) {
    sweep.push({ ...base, sortBy:tt.sortBy, dir:tt.dir, method:'hold', filters:tt.filters });
    for (const n of [3,8,10]) { sweep.push({ ...base, topN:n, sortBy:tt.sortBy, dir:tt.dir, method:'reset', filters:tt.filters }); }
  }
  done = 0; for (const cfg of sweep) { const r = run(cfg); if (r) results.push(r); if (++done % 20 === 0) console.error('sweep '+done+'/'+sweep.length); }

  results.sort((a,b)=>b.cagr-a.cagr);
  const fmt = r => ({ strategy: r.dir+'-'+r.sortBy+' ('+r.method+', top'+r.topN+')  filters: '+fdesc(r.filters), factor: lbl[r.sortBy], cagr: r.cagr, maxDD: r.maxDD, finalV: r.finalV, raw: r });
  const out = { window:[base.start,base.end], universe:base.indexName, freq:base.freq, defaultTopN:base.topN,
    bench: results.length ? results[0].benchCagr : null, totalCombos: results.length,
    topByCAGR: results.slice(0,40).map(fmt),
    topByRiskAdj: results.slice().sort((a,b)=>(b.cagr/(b.maxDD||1))-(a.cagr/(a.maxDD||1))).slice(0,20).map(fmt) };
  const outName = 'scripts/_gridfull_result_' + START + '.json';
  fs.writeFileSync(path.join(ROOT, outName), JSON.stringify(out, null, 1));
  fs.writeFileSync(path.join(ROOT,'scripts/_gridfull_result.json'), JSON.stringify(out, null, 1));
  console.error('DONE combos='+results.length+' bestCAGR='+results[0].cagr+' bench='+out.bench+' in '+((Date.now()-t0)/1000|0)+'s');
})();
