// MEGA-EXHAUSTIVE strategy grid-search — every sort factor × both directions ×
// EVERY 1-, 2- and 3-filter combination from a ~73-atom filter library (distinct fields),
// plus a 4th-filter + topN/method refinement sweep on the winners, dual out-of-sample
// validation, and a final re-verification of the top strategies through the REAL simulate().
//
// Feasible because factor rows are computed ONCE per rebalance date (cached + presorted),
// then each combo only filters + walks the presorted order — verified EXACT vs simulate().
//
// Build + run (appended to the engine, shares scope):
//   cat scripts/gridmega_shim.js docs/backtest-engine.js scripts/grid_search_mega.js > scripts/_gridmega_run.js
//   node --max-old-space-size=3072 scripts/_gridmega_run.js            # full run
//   PARITY_ONLY=1 node scripts/_gridmega_run.js                        # parity + ETA only
// argv: [start] [end].  env: MAIN_ONLY=1 (grid + top-2000 only) · EVAL_FILE=x.json ·
//   SELECT_FILE=x.json (exact metrics for a set of grid row indices, for the Phases Lab build) ·
//   TOPN=3|5 · METHOD=reset|hold · UNIVERSE='Nifty 500'|__FNO__  (the Phases Lab basket variants).
// Reads LIVE data from scripts/_live/ (p1_new.bin p2_new.bin fund_live.json shp_live.json
// nifty_live.json nifty500_live.json stock_data_live.bin) — stage it with
// `python3 scripts/gridmega_fetch_live.py` (runbook §7.0/§7.4).
// Writes scripts/_gridmega_result_<start><vtag>.json (+ checkpoint + full CSV.gz of every combo).
(function () {
  const fs = require('fs'), zlib = require('zlib'), path = require('path');
  const ROOT = path.resolve(__dirname, '..');
  const LIVE = p => path.join(ROOT, 'scripts', '_live', p);
  const GZf = f => JSON.parse(zlib.gunzipSync(fs.readFileSync(f)));
  const Jf = f => JSON.parse(fs.readFileSync(f, 'utf8'));

  // ===== data load (LIVE files only) =====
  const SD = GZf(LIVE('stock_data_live.bin'));
  IDXH = SD.indicesHistory || {}; FNOH = SD.fnoHistory || []; START_TS = SD.startTs;
  NIFTY = Jf(LIVE('nifty_live.json')).px || {};
  NIFTY500 = Jf(LIVE('nifty500_live.json')).px || {};
  const A = GZf(LIVE('p1_new.bin')), B = GZf(LIVE('p2_new.bin'));
  const SFD = { data: { ...A.data, ...B.data }, meta: { ...(A.meta || {}), ...(B.meta || {}) },
                end: B.end || A.end, start: A.start || B.start };
  if (SFD.data.ZOMATO || !SFD.data.ETERNAL) throw new Error('rename sanity failed (ZOMATO/ETERNAL)');
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
  activateSF();
  FUND = Jf(LIVE('fund_live.json'));
  SHPD = Jf(LIVE('shp_live.json'));
  // merge renamed tickers' SHP eras under the CURRENT key (mirror of loadShp())
  for (const old in FUND_ALIAS) {
    if (!SHPD[old]) continue;
    const nw = FUND_ALIAS[old], by = {};
    (SHPD[nw] || []).concat(SHPD[old]).forEach(q => { if (!by[q[0]] || q[3] > by[q[0]][3]) by[q[0]] = q; });
    SHPD[nw] = Object.values(by).sort((a, b) => a[0] - b[0]);
  }
  console.error('loaded: stocks='+SF.nTot+' end='+SF.end+' fund='+Object.keys(FUND).length+' shp='+Object.keys(SHPD).length);

  // ===== window / schedule =====
  const START = process.argv[2] || '2020-03-31';
  const END = process.argv[3] || SFD.end;                       // optional end override (phase grids)
  // Basket variant — the Phases Lab ships one grid per basket shape (runbook §7.4).
  //   TOPN   basket size the grid itself searches over (default 5)
  //   METHOD 'reset' = sell all + rebuy monthly | 'hold' = ride winners (exits replaced, winners kept)
  //   UNIVERSE 'Nifty 500' (default) or '__FNO__' for the F&O universe
  // VTAG keeps each variant's CSV/cache/top artifacts separate; the default variant keeps the
  // legacy un-suffixed names so old artifacts stay readable.
  const TOPN = +(process.env.TOPN || 5);
  const METHOD = process.env.METHOD || 'reset';
  const UNIVERSE = process.env.UNIVERSE || 'Nifty 500';
  // EARN_BASIS: 'con' (consolidated, falls back to standalone — the engine/UI default),
  // 'std' (standalone only), 'conOnly' (consolidated only, no fallback). Flows into every
  // profit* factor via factorsAt AND into simulate()/simFast re-scoring via BASECFG.
  const EARN_BASIS = process.env.EARN_BASIS || 'con';
  // Validate loudly. A malformed TOPN (e.g. zsh not word-splitting `env $v`, so TOPN became
  // "3 METHOD=hold") yields NaN, and `target.length < NaN` is false for every candidate — so
  // every basket comes out EMPTY and the whole 4.44M grid "succeeds" in 12 seconds with NaN
  // CAGRs. That cost a silent bad run once; never again.
  if (!Number.isInteger(TOPN) || TOPN < 1 || TOPN > 50)
    throw new Error('TOPN must be an integer 1-50, got ' + JSON.stringify(process.env.TOPN));
  if (METHOD !== 'reset' && METHOD !== 'hold')
    throw new Error('METHOD must be reset|hold, got ' + JSON.stringify(METHOD));
  if (UNIVERSE !== '__FNO__' && !/^Nifty \d+$/.test(UNIVERSE))
    throw new Error('UNIVERSE must be __FNO__ or "Nifty <n>", got ' + JSON.stringify(UNIVERSE));
  if (EARN_BASIS !== 'con' && EARN_BASIS !== 'std' && EARN_BASIS !== 'conOnly')
    throw new Error('EARN_BASIS must be con|std|conOnly, got ' + JSON.stringify(process.env.EARN_BASIS));
  const VTAG = (TOPN === 5 && METHOD === 'reset' && UNIVERSE === 'Nifty 500') ? ''
             : '_' + (UNIVERSE === '__FNO__' ? 'fno_' : '') + (METHOD === 'hold' ? 'h' : 'r') + TOPN;
  // Artifact naming. The basis is part of the TAG (2026-08-27): the two bases sweep the SAME
  // 4,440,600 combos, so without it a `con` run silently overwrites the `std` CSV/marker it is
  // meant to be merged with. Callers that build this tag themselves — gridmega_phases_run.py's
  // resume check and gridmega_phases_build.js's tagOf — must use the identical suffix.
  const TAG = (process.argv[3] ? START + '_' + END : START) + VTAG + '_' + EARN_BASIS;
  const CAPITAL = 100000;
  const BASECFG = { start:START, end:END, indexName:UNIVERSE, freq:1, lookback:1, topN:TOPN,
                    capital:CAPITAL, mode:'sf', earnBasis:EARN_BASIS, mcapFloor:0, method:METHOD };
  // trading-day snap — identical to simulate()
  const _tdset = new Set();
  for (const r of ['RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','ITC','SBIN','LT']) { const s = SERIES[r]; if (s && s.d) for (const o of s.d) _tdset.add(o); }
  const _td = [..._tdset].sort((a,b)=>a-b);
  const snapTD = o => { const i = idxLE(_td, o); return i < 0 ? o : _td[i]; };
  const schedule = (start, end) => { const M = monthsBetween(start, end); return { M, O: M.map(md => snapTD(dayOff(md))) }; };
  const MAIN = schedule(START, END);
  const OOS1 = schedule('2023-03-31', END);            // post-COVID regime
  const OOS2 = schedule('2021-06-30', '2024-06-30');   // mid-cycle, excludes crash-bottom AND latest year
  const yearsOf = (s, e) => (Date.parse(e) - Date.parse(s)) / (365.25 * 864e5);
  const benchCagrOf = (s, e) => { const a = nearestIdx(NIFTY500, s), b = nearestIdx(NIFTY500, e);
    return (a && b) ? (Math.pow(b / a, 1 / yearsOf(s, e)) - 1) * 100 : null; };

  // ===== factor cache: ALL factors at every rebalance date, computed once =====
  const FULLCFG = { ...BASECFG, sortBy:'composite', filters:[{field:'fiiPct',op:'>',val:-1}] }; // triggers tech+fund+shp+composite
  const ALLOFFS = [...new Set([...MAIN.O, ...OOS1.O, ...OOS2.O])].sort((a,b)=>a-b);
  for (const o of [...OOS1.O, ...OOS2.O]) if (!MAIN.O.includes(o)) console.error('note: OOS off '+o+' ('+isoOff(o)+') outside main window set');
  // Factor rows are universe-filtered (rowsAt → membersAsOf), so the cache is per-UNIVERSE —
  // and per-BASIS: every profit* factor and resultAge comes out of factorsAt already carrying
  // cfg.earnBasis, so a 'std' run must never read a 'con' cache (or vice versa).
  // topN/method never reach factorsAt, so all basket sizes share one cache file.
  const UTAG = (UNIVERSE === 'Nifty 500' ? '' : '_fno') + (EARN_BASIS === 'con' ? '' : '_' + EARN_BASIS);
  const CACHE_FILE = path.join(ROOT, 'scripts', '_gridmega_cache_' + START + '_' + END + UTAG + '.json.gz');
  let CACHE = new Map();
  if (fs.existsSync(CACHE_FILE)) {
    const raw = GZf(CACHE_FILE);
    for (const k in raw) CACHE.set(+k, raw[k]);
    console.error('factor cache loaded from disk: ' + CACHE.size + ' dates');
  } else {
    const t0 = Date.now();
    ALLOFFS.forEach((off, i) => { CACHE.set(off, factorsAt(off, FULLCFG));
      if ((i+1) % 10 === 0) console.error('factors ' + (i+1) + '/' + ALLOFFS.length + '  ' + ((Date.now()-t0)/1000|0) + 's'); });
    const dump = {}; for (const [k, v] of CACHE) dump[k] = v;
    // write-then-rename: basket variants for the same window+universe share this cache and may run
    // concurrently, so a reader must never see a half-written file. Duplicate builds are harmless
    // (the content is deterministic); a torn read would not be.
    const tmp = CACHE_FILE + '.' + process.pid + '.tmp';
    fs.writeFileSync(tmp, zlib.gzipSync(JSON.stringify(dump), { level: 6 }));
    fs.renameSync(tmp, CACHE_FILE);
    console.error('factor cache built in ' + ((Date.now()-t0)/1000|0) + 's → ' + CACHE_FILE);
  }

  // ===== sort fields / filter atoms =====
  const SORTFIELDS = FIELDS.map(f => f.v).filter(v => v !== 'mcap' && v !== 'hist_mcap'); // mcap is 0 in SF mode
  const DIRS = ['high', 'low'];
  const ATOMS = [
    ['rsi','<',30],['rsi','<',45],['rsi','>=',55],['rsi','>=',70],
    ['d52','<=',5],['d52','<=',10],['d52','<=',25],
    ['d52_low_pct','<=',30],['d52_low_pct','>=',50],['d52_low_pct','>=',100],
    ['indRank','<=',3],['indRank','>=',8],
    ['profitYoyPct','>',0],['profitYoyPct','>',25],['profitYoyPct','>',50],
    ['profitBase','>',0],['profitBase','>=',10],
    ['profitTTM','>',0],['profitTTM','>',25],
    ['profitAccel','>',0],
    ['profitStreak','>=',2],['profitStreak','>=',4],
    ['postDrift','>',0],['postDrift','>',5],
    ['composite','>',0],['composite','>=',2],
    ['fiiPct','>=',5],['fiiPct','>=',15],
    ['fiiChgPp','>',0],['fiiChgPp','>=',0.5],
    ['diiPct','>=',5],['diiPct','>=',15],
    ['diiChgPp','>',0],['diiChgPp','>=',0.5],
    ['ret1m','>',0],['ret1m','>',5],
    ['ret3m','>',0],['ret3m','>',10],
    ['ret6m','>',0],['ret6m','>',20],
    ['ret12m','>',0],['ret12m','>',30],
    ['accel','>',0],
    ['dma50','>',0],['dma50','<',0],
    ['dma200','>',0],['dma200','<',0],
    ['rangePos','>=',70],['rangePos','>=',90],['rangePos','<=',30],
    ['daysHigh','<=',30],['daysHigh','>=',180],
    ['vol','<',30],['vol','<',45],['vol','>=',60],
    ['riskMom','>',0],['riskMom','>=',0.5],
    ['beta','<',1],['beta','>=',1.2],
    ['mdd6','<=',15],['mdd6','<=',25],
    ['upPct','>=',55],
    ['turnover','>=',100],['turnover','>=',2000],
    ['turnSurge','>=',1.5],['volSurge','>=',1.5],
    ['delivPct','>=',40],['delivPct','>=',60],
    ['macd','>',0],
    ['stoch','>=',80],['stoch','<=',20],
    ['bollB','>=',80],['bollB','<=',20],
  ].map(([field, op, val]) => ({ field, op, val }));
  const FILTFIELDS = [...new Set(ATOMS.map(a => a.field))];
  const FFI = {}; FILTFIELDS.forEach((f, i) => FFI[f] = i);
  const OPC = { '>':0, '>=':1, '<':2, '<=':3 };
  const CATOMS = ATOMS.map(a => ({ fi: FFI[a.field], opc: OPC[a.op], val: a.val, field: a.field, op: a.op }));

  // per-off structures: FV (rounded filter values per field, NaN=missing) + presorted row order per sort field/dir
  const OFFDATA = new Map(); // off -> { R, FV: [Float64Array per FILTFIELDS], ORD: Map('field|dir' -> Int32Array) }
  {
    const t0 = Date.now();
    // DROP_UNCLASSIFIED=1 — measurement mode for the `indRank` defect (2026-08-12).
    // The BSE industry map is a CURRENT snapshot, so in a survivorship-free universe the further
    // back you go the more delisted names carry ind='Unknown': 31.9% of the universe in 2004,
    // 0.6% today. indRank pools them into ONE pseudo-industry, whose average return then earns a
    // single shared rank — and in 41 of 268 months that rank was <=3, admitting up to 163
    // unrelated stocks through an `indRank<=3` filter in one clump.
    // This flag drops unclassified rows and re-ranks industries without them, so the SAME strategy
    // can be scored with and without the contamination. It deliberately does NOT change the engine.
    const DROP_UNC = !!process.env.DROP_UNCLASSIFIED;
    const UNCLASSIFIED = new Set(['Unknown', 'Uncategorized', 'Other', '', null, undefined]);
    // IND_OVERRIDE=<json {symbol: bseSector}> — backfilled industries for stocks BSE no longer
    // classifies (delisted names lose their classification, so a survivorship-free universe shows
    // 32% 'Unknown' in 2004 falling to 0.6% today). Source: 13 archived NSE Nifty-500 constituent
    // CSVs from the Wayback Machine, which carry a point-in-time Industry column; NSE's ~72-value
    // taxonomy is folded into BSE's 23 via a mapping LEARNED from 846 stocks that have both.
    // Applied BEFORE indRank is re-derived, so the fills actually change the industry ranking.
    // IND_OVERRIDE may point at either the tracked scripts/industry_backfill.json (which wraps the
    // data in {map, provenance, _caveats}) or a bare {symbol: sector} file.
    const OVR = (() => {
      if (!process.env.IND_OVERRIDE) return null;
      const j = JSON.parse(fs.readFileSync(process.env.IND_OVERRIDE, 'utf8'));
      return j && j.map ? j.map : j;
    })();
    let ovrHits = 0;
    let droppedTot = 0, droppedOffs = 0;
    for (const off of ALLOFFS) {
      let R = CACHE.get(off);
      if (OVR) {
        for (const r of R) {
          if (UNCLASSIFIED.has(r.ind) && OVR[r.sym]) { r.ind = OVR[r.sym]; ovrHits++; }
        }
        // industries changed ⇒ the cached indRank is stale; re-derive it exactly as the engine does
        const byInd = {}; R.forEach(r => { (byInd[r.ind] = byInd[r.ind] || []).push(r.chg); });
        const indAvg = Object.entries(byInd).map(([k, v]) => [k, v.reduce((a, b) => a + b, 0) / v.length])
                             .sort((a, b) => b[1] - a[1]);
        const map = {}; indAvg.forEach(([k], i) => { map[k] = Math.min(10, 1 + Math.floor(i / Math.max(1, indAvg.length / 10))); });
        R.forEach(r => { r.indRank = map[r.ind] || 10; });
      }
      if (DROP_UNC) {
        const keep = R.filter(r => !UNCLASSIFIED.has(r.ind));
        if (keep.length !== R.length) { droppedTot += R.length - keep.length; droppedOffs++; }
        R = keep;
        // re-derive indRank over the surviving industries, same decile formula as the engine
        const byInd = {}; R.forEach(r => { (byInd[r.ind] = byInd[r.ind] || []).push(r.chg); });
        const indAvg = Object.entries(byInd).map(([k, v]) => [k, v.reduce((a, b) => a + b, 0) / v.length])
                             .sort((a, b) => b[1] - a[1]);
        const map = {}; indAvg.forEach(([k], i) => { map[k] = Math.min(10, 1 + Math.floor(i / Math.max(1, indAvg.length / 10))); });
        R.forEach(r => { r.indRank = map[r.ind] || 10; });
      }
      const n = R.length;
      const FV = FILTFIELDS.map(f => { const a = new Float64Array(n);
        for (let i = 0; i < n; i++) { const x = fieldVal(R[i], f); a[i] = (x == null || typeof x !== 'number') ? NaN : Math.round(x * 1e6) / 1e6; }
        return a; });
      const ORD = new Map();
      for (const sfld of SORTFIELDS) {
        const idx = [];
        for (let i = 0; i < n; i++) { if (R[i].rsi == null) continue; const x = fieldVal(R[i], sfld); if (x == null || typeof x !== 'number') continue; idx.push(i); }
        const hi = idx.slice().sort((a, b) => fieldVal(R[b], sfld) - fieldVal(R[a], sfld));
        const lo = idx.slice().sort((a, b) => fieldVal(R[a], sfld) - fieldVal(R[b], sfld));
        ORD.set(sfld + '|high', Int32Array.from(hi)); ORD.set(sfld + '|low', Int32Array.from(lo));
      }
      OFFDATA.set(off, { R, FV, ORD });
    }
    if (DROP_UNC) {
      if (!droppedTot) throw new Error('DROP_UNCLASSIFIED set but NOTHING was dropped — the flag is a no-op (stale _gridmega_run.js?)');
      console.error('DROP_UNCLASSIFIED: removed ' + droppedTot + ' unclassified rows across ' + droppedOffs + ' of ' + ALLOFFS.length + ' dates');
    }
    if (OVR) {
      // Dated guard (runbook: date the sanity floor). The 200 backfilled names are all long-
      // delisted, so 0 fills is the TRUE value for a window whose offs never reach the
      // delisted-heavy era — measured 2026-08-25: the 2026-03-31 window fills exactly 0 while
      // the 2004 window fills ~18k. Zero is only evidence of a broken mechanism (key mismatch,
      // stale _gridmega_run.js) when the window actually spans the era the override exists for.
      if (!ovrHits && START < '2016-01-01')
        throw new Error('IND_OVERRIDE set but NOTHING was filled in a pre-2016 window — the flag is a no-op (key mismatch or stale _gridmega_run.js?)');
      console.error('IND_OVERRIDE: filled ' + ovrHits + ' stock-months from ' + Object.keys(OVR).length + ' backfilled symbols');
    }
    console.error('presort built in ' + ((Date.now()-t0)/1000|0) + 's');
  }

  // ===== the fast simulator — EXACT replica of simulate() for freq=1 =====
  // atoms: array of CATOMS entries; returns metrics only.
  function simFast(sortField, dir, atoms, topN, method, SCHED) {
    const { M, O } = SCHED, nM = M.length, ordKey = sortField + '|' + dir;
    const na = atoms.length;
    const pos = new Map(); let cash = 0, started = false, lastRebVal = CAPITAL;
    const equity = new Float64Array(nM);
    let sumPicks = 0, wins = 0, nRebRet = 0;
    for (let mi = 0; mi < nM; mi++) {
      const off = O[mi], D = OFFDATA.get(off);
      let mv;
      if (started) { mv = cash; for (const [t, u] of pos) { const p = markPrice(t, off); if (p != null) mv += u * p; } }
      else mv = CAPITAL;
      equity[mi] = mv;
      // freq=1 → every month rebalances (mi=0 included)
      const R = D.R, FV = D.FV, ord = D.ORD.get(ordKey);
      const target = [];
      for (let k = 0; k < ord.length && target.length < topN; k++) {
        const ri = ord[k]; let ok = true;
        for (let a = 0; a < na; a++) { const at = atoms[a], v = FV[at.fi][ri];
          const pass = at.opc === 0 ? v > at.val : at.opc === 1 ? v >= at.val : at.opc === 2 ? v < at.val : v <= at.val;
          if (!pass) { ok = false; break; } }
        if (ok) target.push(R[ri]);
      }
      sumPicks += target.length;
      if (!started || method === 'reset') {
        const base = started ? mv : CAPITAL; const per = base / topN;
        pos.clear();
        for (const r of target) pos.set(r.tkr, per / r.price);
        cash = base - per * target.length; started = true;
      } else {
        const tset = new Set(); for (const r of target) tset.add(r.tkr);
        let proceeds = 0;
        for (const t of [...pos.keys()]) { if (!tset.has(t)) { const p = markPrice(t, off); proceeds += p != null ? pos.get(t) * p : 0; pos.delete(t); } }
        const entries = target.filter(r => !pos.has(r.tkr));
        const openSlots = Math.max(1, topN - pos.size);
        let avail = proceeds + cash; const perSlot = avail / openSlots;
        for (const e of entries) { if (perSlot <= 1) break; pos.set(e.tkr, perSlot / e.price); avail -= perSlot; }
        cash = Math.max(0, avail);
      }
      if (mi > 0) { nRebRet++; if (mv / (lastRebVal || 1) - 1 > 0) wins++; }
      lastRebVal = mv > 0 ? mv : CAPITAL;
    }
    const finalV = equity[nM - 1];
    const yrs = yearsOf(SCHED.startDate, SCHED.endDate);
    const cagr = yrs > 0 ? (Math.pow(finalV / CAPITAL, 1 / yrs) - 1) * 100 : 0;
    let peak = -1, mdd = 0;
    for (let i = 0; i < nM; i++) { const v = equity[i]; if (v > peak) peak = v; else if (peak > 0) { const dd = (peak - v) / peak * 100; if (dd > mdd) mdd = dd; } }
    const rets = []; for (let i = 1; i < nM; i++) if (equity[i-1] > 0) rets.push(equity[i] / equity[i-1] - 1);
    const mean = rets.reduce((a, b) => a + b, 0) / (rets.length || 1);
    const vol = Math.sqrt(rets.reduce((a, b) => a + (b - mean) ** 2, 0) / (rets.length || 1) * 12) * 100;
    return { cagr, maxDD: mdd, finalV, winRate: nRebRet ? 100 * wins / nRebRet : 0, vol,
             avgPicks: sumPicks / nM };
  }
  MAIN.startDate = START;        MAIN.endDate = END;
  OOS1.startDate = '2023-03-31'; OOS1.endDate = END;
  OOS2.startDate = '2021-06-30'; OOS2.endDate = '2024-06-30';
  // extra report windows (all offs ⊂ MAIN cache)
  const WX = [
    ['w2020_21', '2020-03-31', '2021-09-30'],
    ['w2023_24', '2023-03-31', '2024-09-30'],
    ['w2026',    '2026-03-31', END],
  ].map(([k, s, e]) => { const sc = schedule(s, e); sc.startDate = s; sc.endDate = e; sc.key = k; return sc; });

  // ===== parity check vs the REAL simulate() =====
  const atomBy = (f, op, v) => { const i = ATOMS.findIndex(a => a.field === f && a.op === op && a.val === v); if (i < 0) throw new Error('no atom ' + f + op + v); return CATOMS[i]; };
  const PARITY = [
    { s:'mdd6', d:'high', a:[['d52','<=',10],['profitYoyPct','>',0]], n:5, m:'reset' },
    { s:'vol', d:'low', a:[['fiiChgPp','>',0]], n:5, m:'reset' },
    { s:'composite', d:'high', a:[['turnover','>=',100],['rangePos','>=',70],['profitTTM','>',0]], n:5, m:'reset' },
    { s:'d52', d:'low', a:[['dma200','>',0]], n:5, m:'reset' },
    { s:'profitStreak', d:'high', a:[['delivPct','>=',40],['ret6m','>',0]], n:5, m:'hold' },
    { s:'ret12m', d:'high', a:[['rsi','>=',55]], n:3, m:'reset' },
    { s:'beta', d:'low', a:[['diiChgPp','>=',0.5],['mdd6','<=',25]], n:5, m:'reset' },
    { s:'delivPct', d:'high', a:[['profitYoyPct','>',25]], n:5, m:'reset' },
    { s:'postDrift', d:'high', a:[['volSurge','>=',1.5]], n:5, m:'hold' },
    { s:'daysHigh', d:'low', a:[['upPct','>=',55],['vol','<',45]], n:5, m:'reset' },
    { s:'turnSurge', d:'high', a:[['stoch','>=',80]], n:8, m:'reset' },
    { s:'fiiChgPp', d:'high', a:[['composite','>',0],['dma50','>',0]], n:5, m:'reset' },
  ];
  // EVAL mode: score a hand-picked list of configs on all 3 windows, skip everything else.
  // EVAL_FILE=<json array of {sortBy,dir,topN?,method?,filters:[{field,op,val}]}> node _gridmega_run.js
  if (process.env.EVAL_FILE) {
    const list = JSON.parse(fs.readFileSync(process.env.EVAL_FILE, 'utf8'));
    const rows = list.map(c => {
      const atoms = c.filters.map(f => { const i = ATOMS.findIndex(a => a.field === f.field && a.op === f.op && a.val === f.val);
        if (i < 0) throw new Error('no atom ' + JSON.stringify(f)); return CATOMS[i]; });
      const fmtW = m => ({ cagr: +m.cagr.toFixed(2), maxDD: +m.maxDD.toFixed(1), winRate: +m.winRate.toFixed(1), avgPicks: +m.avgPicks.toFixed(2) });
      const main = simFast(c.sortBy, c.dir, atoms, c.topN || 5, c.method || 'reset', MAIN);
      const o1 = simFast(c.sortBy, c.dir, atoms, c.topN || 5, c.method || 'reset', OOS1);
      const o2 = simFast(c.sortBy, c.dir, atoms, c.topN || 5, c.method || 'reset', OOS2);
      const extra = {};
      for (const w of WX) { const m = simFast(c.sortBy, c.dir, atoms, c.topN || 5, c.method || 'reset', w);
        extra[w.key] = { totRet: +((m.finalV / CAPITAL - 1) * 100).toFixed(1), maxDD: +m.maxDD.toFixed(1) }; }
      return { ...c, main: fmtW(main), oos2023: fmtW(o1), oosMid: fmtW(o2), ...extra,
               robust: +Math.min(main.cagr, o1.cagr, o2.cagr).toFixed(2) };
    });
    fs.writeFileSync(path.join(ROOT, 'scripts', '_gridmega_eval.json'), JSON.stringify(rows, null, 1));
    console.error('EVAL done: ' + rows.length + ' configs'); process.exit(0);
  }
  if (!process.env.EVAL_FILE && !process.env.SELECT_FILE) {
    console.error('parity check vs simulate() — 12 configs…'); const t0 = Date.now();
    let bad = 0;
    for (const pc of PARITY) {
      const filters = pc.a.map(([f, op, v]) => ({ field: f, op, val: v }));
      const cfg = { ...BASECFG, sortBy: pc.s, dir: pc.d, topN: pc.n, method: pc.m, filters };
      const real = simulate(cfg);
      const fast = simFast(pc.s, pc.d, pc.a.map(x => atomBy(...x)), pc.n, pc.m, MAIN);
      const ok = Math.abs(real.cagr - fast.cagr) < 1e-6 && Math.abs(real.finalV - fast.finalV) < 0.01
              && Math.abs(real.maxDD - fast.maxDD) < 1e-6 && Math.abs(real.winRate - fast.winRate) < 1e-6;
      if (!ok) { bad++; console.error('PARITY FAIL ' + pc.d + '-' + pc.s + ' [' + pc.a.map(x=>x.join('')).join(' & ') + '] n' + pc.n + ' ' + pc.m +
        '\n  real cagr=' + real.cagr + ' finalV=' + real.finalV + ' maxDD=' + real.maxDD + ' win=' + real.winRate +
        '\n  fast cagr=' + fast.cagr + ' finalV=' + fast.finalV + ' maxDD=' + fast.maxDD + ' win=' + fast.winRate); }
    }
    if (bad) { console.error('PARITY FAILED for ' + bad + '/12 — ABORTING'); process.exit(2); }
    console.error('parity OK (12/12 exact) in ' + ((Date.now()-t0)/1000|0) + 's');
  }

  // ===== enumerate filter sets: all 1/2/3-atom combos with distinct fields =====
  const nA = CATOMS.length;
  const FSETS = [];
  for (let i = 0; i < nA; i++) FSETS.push([i]);
  for (let i = 0; i < nA; i++) for (let j = i + 1; j < nA; j++) { if (CATOMS[i].fi === CATOMS[j].fi) continue; FSETS.push([i, j]); }
  for (let i = 0; i < nA; i++) for (let j = i + 1; j < nA; j++) { if (CATOMS[i].fi === CATOMS[j].fi) continue;
    for (let k = j + 1; k < nA; k++) { if (CATOMS[k].fi === CATOMS[i].fi || CATOMS[k].fi === CATOMS[j].fi) continue; FSETS.push([i, j, k]); } }
  const TOTAL = FSETS.length * SORTFIELDS.length * 2;
  console.error('atoms=' + nA + ' fsets=' + FSETS.length + ' sorts=' + SORTFIELDS.length + ' × 2 dirs → ' + TOTAL.toLocaleString() + ' combos');
  const fdesc = fset => fset.map(x => { const a = CATOMS[x]; return a.field + a.op + a.val; }).join(' & ');

  // SELECT mode: re-score a set of grid ROW INDICES in THIS window and emit exact metrics.
  // The Phases Lab needs cagr/totRet/maxDD/winRate for the ~10k combos that made some window's
  // top list, in all 11 windows. Addressing them by row index (not by re-parsing the CSV's filter
  // text) means the descriptor comes from the same enumeration that produced the row — it cannot
  // drift. totRet is taken from finalV, never re-derived from a 2dp CAGR.
  // SELECT_FILE=<json array of row indices> node _gridmega_run.js <start> <end>
  if (process.env.SELECT_FILE) {
    const want = new Set(JSON.parse(fs.readFileSync(process.env.SELECT_FILE, 'utf8')));
    const rows = []; let idx = 0;
    for (const sfld of SORTFIELDS) for (const dir of DIRS) for (let fi = 0; fi < FSETS.length; fi++, idx++) {
      if (!want.has(idx)) continue;
      const r = simFast(sfld, dir, FSETS[fi].map(x => CATOMS[x]), TOPN, METHOD, MAIN);
      rows.push({ i: idx, s: sfld, d: dir, f: fdesc(FSETS[fi]), cagr: +r.cagr.toFixed(2),
                  tot: +((r.finalV / CAPITAL - 1) * 100).toFixed(1), dd: +r.maxDD.toFixed(1),
                  win: +r.winRate.toFixed(1), picks: +r.avgPicks.toFixed(2) });
    }
    const bA = nearestIdx(NIFTY500, START), bB = nearestIdx(NIFTY500, END);
    const out = { window: [START, END], years: +yearsOf(START, END).toFixed(2), total: TOTAL,
      bench: { totRet: (bA && bB) ? +((bB / bA - 1) * 100).toFixed(1) : null,
               cagr: (bA && bB) ? +benchCagrOf(START, END).toFixed(2) : null }, rows };
    const F = path.join(ROOT, 'scripts', '_gridmega_sel_' + TAG + '.json');
    fs.writeFileSync(F, JSON.stringify(out));
    console.error('SELECT done: ' + rows.length + '/' + want.size + ' rows → ' + path.basename(F));
    process.exit(0);
  }

  // quick benchmark for ETA
  {
    const t0 = Date.now(); let n = 0;
    outer: for (const sfld of ['mdd6', 'ret6m', 'profitTTM']) for (const dir of DIRS) for (let q = 0; q < 500; q++) {
      simFast(sfld, dir, FSETS[(q * 197) % FSETS.length].map(x => CATOMS[x]), 5, 'reset', MAIN);
      if (++n >= 3000) break outer;
    }
    const per = (Date.now() - t0) / n;
    console.error('benchmark: ' + per.toFixed(2) + ' ms/combo → est ' + Math.round(TOTAL * per / 60000) + ' min for main grid');
    if (process.env.PARITY_ONLY) { console.error('PARITY_ONLY set — exiting'); process.exit(0); }
  }

  // ===== main grid =====
  const t0 = Date.now();
  const TOPK = 1500, TOPR = 900;
  let topC = [], topR = [];   // {key fields..., cagr, maxDD, winRate, vol, avgPicks}
  let cutC = -Infinity, cutR = -Infinity;
  const csvBuf = []; const CSV_FILE = path.join(ROOT, 'scripts', '_gridmega_all_' + TAG + '.csv.gz');
  try { fs.unlinkSync(CSV_FILE); } catch (e) {}
  const flushCsv = () => { if (!csvBuf.length) return; fs.appendFileSync(CSV_FILE, zlib.gzipSync(csvBuf.join('\n') + '\n')); csvBuf.length = 0; };
  csvBuf.push('sortBy,dir,filters,cagr,maxDD,winRate,avgPicks');
  let done = 0, degenerate = 0;
  const CKPT = path.join(ROOT, 'scripts', '_gridmega_ckpt_' + TAG + '.json'); // per-window: runs go in parallel
  for (const sfld of SORTFIELDS) for (const dir of DIRS) {
    for (let fi = 0; fi < FSETS.length; fi++) {
      const fset = FSETS[fi];
      const r = simFast(sfld, dir, fset.map(x => CATOMS[x]), TOPN, METHOD, MAIN);
      done++;
      if (r.avgPicks < 2.5) degenerate++;   // mostly-empty basket — tracked but still recorded
      csvBuf.push(sfld + ',' + dir + ',' + fdesc(fset).replace(/,/g, ';') + ',' + r.cagr.toFixed(2) + ',' + r.maxDD.toFixed(1) + ',' + r.winRate.toFixed(1) + ',' + r.avgPicks.toFixed(2));
      if (csvBuf.length >= 200000) flushCsv();
      const rec = { sortBy: sfld, dir, fset, topN: TOPN, method: METHOD, cagr: r.cagr, maxDD: r.maxDD, winRate: r.winRate, vol: r.vol, avgPicks: r.avgPicks };
      if (r.cagr > cutC) { topC.push(rec); if (topC.length > TOPK * 2) { topC.sort((a, b) => b.cagr - a.cagr); topC.length = TOPK; cutC = topC[TOPK - 1].cagr; } }
      const ra = r.cagr / (r.maxDD || 1);
      if (ra > cutR && r.cagr > 15) { topR.push({ ...rec, ra }); if (topR.length > TOPR * 2) { topR.sort((a, b) => b.ra - a.ra); topR.length = TOPR; cutR = topR[TOPR - 1].ra; } }
      if (done % 100000 === 0) {
        const el = (Date.now() - t0) / 1000, rate = done / el;
        console.error(done.toLocaleString() + '/' + TOTAL.toLocaleString() + '  ' + (el/60).toFixed(1) + 'min  ' + rate.toFixed(0) + '/s  ETA ' + (((TOTAL - done) / rate) / 60).toFixed(0) + 'min');
        if (done % 500000 === 0) { topC.sort((a,b)=>b.cagr-a.cagr); if (topC.length>TOPK) topC.length=TOPK;
          fs.writeFileSync(CKPT, JSON.stringify({ done, total: TOTAL, top50: topC.slice(0, 50) })); }
      }
    }
  }
  flushCsv();
  topC.sort((a, b) => b.cagr - a.cagr); if (topC.length > TOPK) topC.length = TOPK;
  topR.sort((a, b) => b.ra - a.ra); if (topR.length > TOPR) topR.length = TOPR;
  console.error('main grid done: ' + done.toLocaleString() + ' combos in ' + ((Date.now()-t0)/60000).toFixed(1) + ' min (degenerate<2.5 picks: ' + degenerate.toLocaleString() + ')');
  console.error('best raw: ' + topC[0].dir + '-' + topC[0].sortBy + ' [' + fdesc(topC[0].fset) + '] cagr=' + topC[0].cagr.toFixed(2));
  if (process.env.MAIN_ONLY) {   // phase grids: CSV + top-2000 JSON only, skip refine/OOS/verify
    fs.writeFileSync(path.join(ROOT, 'scripts', '_gridmega_top_' + TAG + '.json'), JSON.stringify({
      window: [START, END], earnBasis: EARN_BASIS, top: topC.slice(0, 2000).map(r => ({ sortBy: r.sortBy, dir: r.dir, filters: fdesc(r.fset),
        cagr: +r.cagr.toFixed(2), maxDD: +r.maxDD.toFixed(1), winRate: +r.winRate.toFixed(1), avgPicks: +r.avgPicks.toFixed(2) })) }));
    console.error('MAIN_ONLY done → _gridmega_top_' + TAG + '.json');
    process.exit(0);
  }

  // ===== refinement: topN/method variants + a 4th filter on the leaders =====
  const keyOf = r => r.sortBy + '|' + r.dir + '|' + r.fset.join(',') + '|' + r.topN + '|' + r.method;
  const pool = new Map();
  const addPool = (r, tag) => { const k = keyOf(r); if (!pool.has(k)) pool.set(k, { ...r, tag }); };
  topC.slice(0, 150).forEach(r => addPool(r, 'grid'));
  topR.slice(0, 100).forEach(r => addPool(r, 'grid'));
  {
    const t1 = Date.now(); let nref = 0;
    const seeds = [...pool.values()];
    for (const s of seeds) {
      for (const n of [3, 5, 8, 10].filter(n => n !== TOPN)) { const r = simFast(s.sortBy, s.dir, s.fset.map(x => CATOMS[x]), n, METHOD, MAIN);
        addPool({ sortBy: s.sortBy, dir: s.dir, fset: s.fset, topN: n, method: METHOD, ...r }, 'topN'); nref++; }
      const alt = METHOD === 'hold' ? 'reset' : 'hold';
      const rh = simFast(s.sortBy, s.dir, s.fset.map(x => CATOMS[x]), TOPN, alt, MAIN);
      addPool({ sortBy: s.sortBy, dir: s.dir, fset: s.fset, topN: TOPN, method: alt, ...rh }, alt); nref++;
    }
    // 4th filter on the top slice only
    const ext = [...new Map([...topC.slice(0, 50), ...topR.slice(0, 30)].map(r => [keyOf(r), r])).values()];
    for (const s of ext) {
      const used = new Set(s.fset.map(x => CATOMS[x].fi));
      for (let ai = 0; ai < nA; ai++) { if (used.has(CATOMS[ai].fi)) continue;
        const fset = [...s.fset, ai].sort((a, b) => a - b);
        const r = simFast(s.sortBy, s.dir, fset.map(x => CATOMS[x]), TOPN, METHOD, MAIN);
        if (r.cagr > s.cagr) addPool({ sortBy: s.sortBy, dir: s.dir, fset, topN: TOPN, method: METHOD, ...r }, '4filter'); nref++; }
    }
    console.error('refinement: ' + nref + ' extra sims in ' + ((Date.now()-t1)/1000|0) + 's; pool=' + pool.size);
  }

  // ===== out-of-sample validation on every pool member =====
  {
    const t1 = Date.now();
    for (const r of pool.values()) {
      const atoms = r.fset.map(x => CATOMS[x]);
      const o1 = simFast(r.sortBy, r.dir, atoms, r.topN, r.method, OOS1);
      const o2 = simFast(r.sortBy, r.dir, atoms, r.topN, r.method, OOS2);
      r.oos1 = { cagr: +o1.cagr.toFixed(2), maxDD: +o1.maxDD.toFixed(1) };
      r.oos2 = { cagr: +o2.cagr.toFixed(2), maxDD: +o2.maxDD.toFixed(1) };
      r.robust = Math.min(r.cagr, o1.cagr, o2.cagr);
    }
    console.error('OOS done in ' + ((Date.now()-t1)/1000|0) + 's');
  }

  // ===== final verification of the headline strategies through the REAL simulate() =====
  const all = [...pool.values()];
  all.sort((a, b) => b.cagr - a.cagr);
  const byRobust = all.slice().sort((a, b) => b.robust - a.robust);
  const verifySet = [...new Map([...all.slice(0, 15), ...byRobust.slice(0, 15)].map(r => [keyOf(r), r])).values()];
  {
    const t1 = Date.now();
    for (const r of verifySet) {
      const cfg = { ...BASECFG, sortBy: r.sortBy, dir: r.dir, topN: r.topN, method: r.method,
                    filters: r.fset.map(x => ({ field: CATOMS[x].field, op: CATOMS[x].op, val: CATOMS[x].val })) };
      const real = simulate(cfg);
      r.verified = Math.abs(real.cagr - r.cagr) < 1e-6;
      r.realCagr = +real.cagr.toFixed(2);
      if (!r.verified) console.error('VERIFY MISMATCH ' + keyOf(r) + ' fast=' + r.cagr + ' real=' + real.cagr);
    }
    console.error('verified ' + verifySet.length + ' through simulate() in ' + ((Date.now()-t1)/1000|0) + 's — ' + verifySet.filter(r => r.verified).length + ' exact');
  }

  // ===== output =====
  const lbl = {}; FIELDS.forEach(f => lbl[f.v] = f.l);
  const fmt = r => ({
    strategy: r.dir + '-' + r.sortBy + ' (' + r.method + ', top' + r.topN + ')  filters: ' + fdesc(r.fset),
    factor: lbl[r.sortBy],
    cagr: +r.cagr.toFixed(2), maxDD: +r.maxDD.toFixed(1), winRate: +r.winRate.toFixed(1),
    avgPicks: +r.avgPicks.toFixed(2), oos2023: r.oos1, oosMid: r.oos2, robustCagr: +(+r.robust).toFixed(2),
    verified: r.verified || false, tag: r.tag,
    raw: { ...BASECFG, sortBy: r.sortBy, dir: r.dir, topN: r.topN, method: r.method,
           filters: r.fset.map(x => ({ field: CATOMS[x].field, op: CATOMS[x].op, val: CATOMS[x].val })),
           cagr: +r.cagr.toFixed(2), maxDD: +r.maxDD.toFixed(1), benchCagr: +benchCagrOf(START, END).toFixed(2) },
  });
  const out = {
    window: [START, END], universe: UNIVERSE, freq: 1, defaultTopN: TOPN, method: METHOD,
    bench: { main: +benchCagrOf(START, END).toFixed(2), oos2023: +benchCagrOf('2023-03-31', END).toFixed(2), oosMid: +benchCagrOf('2021-06-30', '2024-06-30').toFixed(2) },
    totalCombos: done, atoms: ATOMS.map(a => a.field + a.op + a.val), degenerate,
    elapsedMin: +((Date.now() - t0) / 60000).toFixed(1),
    topByCAGR: all.slice(0, 60).map(fmt),
    topByRobust: byRobust.slice(0, 40).map(fmt),
    topByRiskAdj: all.slice().sort((a, b) => (b.cagr / (b.maxDD || 1)) - (a.cagr / (a.maxDD || 1))).slice(0, 30).map(fmt),
  };
  fs.writeFileSync(path.join(ROOT, 'scripts', '_gridmega_result_' + START + VTAG + '.json'), JSON.stringify(out, null, 1));
  console.error('DONE → scripts/_gridmega_result_' + START + VTAG + '.json   best=' + out.topByCAGR[0].strategy + ' cagr=' + out.topByCAGR[0].cagr + '  bench=' + out.bench.main);
})();
