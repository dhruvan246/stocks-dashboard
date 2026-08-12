// postDrift COVERAGE, measured at the REBALANCE level (DATA_RUNBOOK §91).
//
// Annual "does this company have any dated quarter this year" OVERSTATES postDrift coverage.
// The engine needs, at EACH monthly rebalance, ALL of:
//   1. a quarterly row with npCon/npStd non-null AND its announce date non-null AND date <= rebalance
//   2. the YEAR-AGO same quarter's np non-null and != 0     <- profitMetrics() `if (yoy==null) continue`
//   3. a price bar at/before that announce date              <- priceAt(tkr, resultDateOff) > 0
// Miss any one and r.postDrift is null and the stock is dropped from the screen entirely
// (simulate() filters `fieldVal(r,sortBy) != null`).
//
// Build + run (appended to the engine, shares scope — same recipe as grid_search_mega.js):
//   cat scripts/gridmega_shim.js docs/backtest-engine.js scripts/postdrift_coverage.js > scripts/_pdcov_run.js
//   node --max-old-space-size=3072 scripts/_pdcov_run.js [start] [end]
// Reads LIVE data from scripts/_live/ ONLY — stage with `python3 scripts/gridmega_fetch_live.py` (§7.0/§7.1b).
// Writes scripts/_postdrift_coverage.json  { byMonth:[...], byYear:[...], gaps:[...] }
(function () {
  const fs = require('fs'), zlib = require('zlib'), path = require('path');
  const ROOT = path.resolve(__dirname, '..');
  const LIVE = p => path.join(ROOT, 'scripts', '_live', p);
  const GZf = f => JSON.parse(zlib.gunzipSync(fs.readFileSync(f)));
  const Jf = f => JSON.parse(fs.readFileSync(f, 'utf8'));

  // ===== data load (LIVE files only) — mirror of grid_search_mega.js =====
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
  console.error('loaded: stocks=' + SF.nTot + ' end=' + SF.end + ' fund=' + Object.keys(FUND).length);

  const START = process.argv[2] || '2002-03-31';
  const END   = process.argv[3] || SFD.end;
  const BASIS = process.env.BASIS || 'con';       // engine default earnBasis; con falls back to std

  // ---- rebalance schedule: EXACTLY simulate()'s (last trading day <= calendar month-end) ----
  const months = monthsBetween(START, END);
  const _tdset = new Set();
  for (const _r of ['RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','ITC','SBIN','LT']) {
    const _s = SERIES[_r]; if (_s && _s.d) for (const _o of _s.d) _tdset.add(_o); }
  const _td = [..._tdset].sort((a,b)=>a-b);
  const snapTD = o => { if (!_td.length) return o; const i = idxLE(_td, o); return i < 0 ? o : _td[i]; };

  // ZEROGUARD=1 → require the announce date to be TRUTHY (> 0), not merely non-null.
  // ann=0 is the documented "date unknown" SENTINEL (runbook §15, memory qe0-live-quarter-guard);
  // the engine's `arr[i][ai] != null` admits it, because `0 != null` is TRUE in JS. Measuring both
  // ways shows what the sentinel is costing postDrift.
  const ZEROGUARD = process.env.ZEROGUARD === '1';
  const NOYOY = process.env.NOYOY === '1';
  const annOK = v => ZEROGUARD ? (v != null && v > 0) : (v != null);

  // ---- per-row diagnosis: WHY is postDrift null? (mirrors profitMetrics + the postDrift line) ----
  // Returns one of: 'ok' | 'no-series' | 'no-dated-quarter' | 'no-yoy-base' | 'no-price-at-result'
  // plus, for the fillable classes, the quarter-end that would have been used.
  function diagnose(sym, tkr, dateInt) {
    const arr = fundFor(sym);
    if (!arr || !arr.length) return { why: 'no-series' };
    const tries = BASIS === 'std' ? [[1,2]] : [[3,4],[1,2]];
    let sawDated = false, lastFail = null, lastNeed = null;
    for (const [ni, ai] of tries) {
      let ci = -1;
      for (let i = arr.length - 1; i >= 0; i--) { if (arr[i][ni] != null && annOK(arr[i][ai]) && arr[i][ai] <= dateInt) { ci = i; break; } }
      if (ci < 0) { lastFail = lastFail || 'no-dated-quarter'; continue; }
      sawDated = true;
      const cur = arr[ci];
      const npAt = qe => { const q = arr.find(x => x[0] === qe); return (q && q[ni] != null) ? q[ni] : null; };
      const b = npAt(cur[0] - 10000);
      // NOYOY=1 → model postDrift decoupled from the YoY computation it never consumes.
      // postDrift = (price / priceAt(resultDate) - 1)*100; the year-ago base is irrelevant to it.
      // the EXACT cell whose absence blocks this row: the year-ago same quarter
      if (!NOYOY && (b == null || b === 0)) { lastFail = 'no-yoy-base'; lastNeed = cur[0] - 10000; continue; }
      if (cur[ai] === 0) return { why: 'ann-zero-sentinel', qe: cur[0], ann: 0 };
      const ds = '' + cur[ai];
      const ro = dayOff(ds.slice(0,4) + '-' + ds.slice(4,6) + '-' + ds.slice(6,8));
      const pr = priceAt(tkr, ro);
      if (!(pr != null && pr > 0)) return { why: 'no-price-at-result', qe: cur[0], ann: cur[ai] };
      return { why: 'ok', qe: cur[0], ann: cur[ai] };
    }
    // needQe = the EXACT missing cell. For no-yoy-base it is the year-ago quarter we could not read;
    // for no-dated-quarter it is the newest quarter that should have been announceable by dateInt.
    return { why: lastFail || (sawDated ? 'no-yoy-base' : 'no-dated-quarter'),
             needQe: lastNeed != null ? lastNeed : wantedQe(dateInt) };
  }
  // newest quarter-end that could plausibly be announced by dateInt (quarter end + ~45d filing window)
  function wantedQe(dateInt) {
    const y = Math.floor(dateInt/10000), m = Math.floor(dateInt/100)%100;
    const qEnds = [[y,3,31],[y,6,30],[y,9,30],[y,12,31],[y-1,3,31],[y-1,6,30],[y-1,9,30],[y-1,12,31]];
    let best = null;
    for (const [yy,mm,dd] of qEnds) { const qe = yy*10000+mm*100+dd;
      // announceable ~45 days after quarter end
      const am = mm + 1, ay = am > 12 ? yy+1 : yy, amm = am > 12 ? am-12 : am;
      const ann = ay*10000 + amm*100 + 15;
      if (ann <= dateInt && (best == null || qe > best)) best = qe; }
    return best;
  }

  const byMonth = [], gapRows = [];
  for (let mi = 0; mi < months.length; mi++) {
    const md = months[mi], off = snapTD(dayOff(md));
    if (off > SF.endOff) break;
    const dateInt = parseInt(isoOff(off).replace(/-/g,''), 10);
    const members = membersAsOf('Nifty 500', isoOff(off));
    const maxBarAge = off >= dayOff('2002-01-02') ? 14 : 28;
    let nMembersMapped = 0, nScreen = 0, nOk = 0;
    const why = { 'no-series':0, 'no-dated-quarter':0, 'no-yoy-base':0, 'no-price-at-result':0, 'ann-zero-sentinel':0 };
    for (const tkr in SERIES) {
      const m = META[tkr]; if (!m) continue;
      if (m.symbol.includes('DVR')) continue;
      if (members && !members.has(m.symbol)) continue;
      nMembersMapped++;
      // ---- the engine's own screenability gates (a row that never reaches the screen can't be a postDrift gap)
      const s = SERIES[tkr]; const li = (s && s.d && s.d.length) ? idxLE(s.d, off) : -1;
      if (li < 0 || off - s.d[li] > maxBarAge) continue;
      const price = priceAt(tkr, off), p0 = priceAt(tkr, off - 30);
      if (price == null || p0 == null || p0 <= 0) continue;
      const hl = hl52(tkr, off); if (!hl) continue;
      if (rsi14(tkr, off) == null) continue;   // simulate() drops rsi==null rows
      nScreen++;
      const d = diagnose(m.symbol, tkr, dateInt);
      if (d.why === 'ok') nOk++;
      else { why[d.why] = (why[d.why]||0) + 1;
             gapRows.push({ md, sym: m.symbol, why: d.why, needQe: d.needQe || null, qe: d.qe || null }); }
    }
    byMonth.push({ md, members: members ? members.size : null, mapped: nMembersMapped,
                   screen: nScreen, ok: nOk, pct: nScreen ? +(nOk/nScreen*100).toFixed(2) : null, why });
    if (mi % 24 === 0) console.error(md + '  screen=' + nScreen + ' ok=' + nOk + ' ' + (nScreen?(nOk/nScreen*100).toFixed(1):'-') + '%');
  }

  // ---- per-year roll-up (average of the monthly rates, and the pooled rate) ----
  const yrs = {};
  for (const r of byMonth) { const y = r.md.slice(0,4); (yrs[y] = yrs[y] || []).push(r); }
  const byYear = Object.keys(yrs).sort().map(y => {
    const rs = yrs[y], so = rs.reduce((a,b)=>a+b.screen,0), oo = rs.reduce((a,b)=>a+b.ok,0);
    const w = { 'no-series':0,'no-dated-quarter':0,'no-yoy-base':0,'no-price-at-result':0,'ann-zero-sentinel':0 };
    rs.forEach(r => { for (const k in w) w[k] += r.why[k]; });
    return { year: y, months: rs.length, screen: so, ok: oo, pct: so ? +(oo/so*100).toFixed(2) : null,
             worstMonth: rs.reduce((a,b)=> (a==null||b.pct<a.pct)?b:a, null).md, why: w };
  });

  const out = path.join(ROOT, 'scripts', '_postdrift_coverage' + (ZEROGUARD?'_zg':'') + (NOYOY?'_ny':'') + '.json');
  fs.writeFileSync(out, JSON.stringify({ measuredAt: process.env.STAMP || null, sfEnd: SF.end,
                                         basis: BASIS, start: START, end: END, byYear, byMonth,
                                         gaps: gapRows }, null, 0));
  console.error('\n year  months  screened      ok    pct    noSeries  noDatedQ  noYoYBase  noPrice  annZero');
  for (const r of byYear) console.error(
    ' ' + r.year + '   ' + String(r.months).padStart(2) + '   ' + String(r.screen).padStart(8) +
    String(r.ok).padStart(8) + String(r.pct).padStart(8) + '%' +
    String(r.why['no-series']).padStart(10) + String(r.why['no-dated-quarter']).padStart(10) +
    String(r.why['no-yoy-base']).padStart(11) + String(r.why['no-price-at-result']).padStart(9) +
    String(r.why['ann-zero-sentinel']).padStart(9));
  console.error('\nwrote ' + out + '  (' + gapRows.length + ' gap rows)');
})();
