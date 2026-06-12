'use strict';
/* ============================================================================
 * Shared survivorship-free backtest engine
 * Used by: stock-backtest.html (full backtester) and saved-strategies.html
 *          (strategy cards + Today's Picks deployment basket).
 *
 * DOM-FREE. Pages provide their own UI; this file owns the data + the math.
 * Data source is always survivorship-free (NSE bhavcopy incl. delisted names).
 * ========================================================================== */
const DAY = 86400;
let META = {}, SERIES = {}, IDXH = {}, FNOH = [], START_TS = 0, NIFTY = {};
let SF = null, TURN = {}, SF_END_OFF = Infinity;
const DATA_MODE = 'sf';                       // survivorship-free only
const TURN_OPTS = [['100', '≥₹1 Cr'], ['500', '≥₹5 Cr'], ['2000', '≥₹20 Cr'], ['10000', '≥₹100 Cr']]; // daily turnover (₹ lacs)
const FIELDS = [
  { v: 'changePercent', l: 'Change % (lookback)' },
  { v: 'rsi', l: 'RSI(14)' },
  { v: 'd52', l: 'Distance from 52w High % (high 100, price 95 → 5; near-high = ≤ 10)' },
  { v: 'd52_low_pct', l: 'Distance from 52w Low % (% above the low)' },
  { v: 'indRank', l: 'Industry Momentum Rank (1=hot…10=cold)' },
  { v: 'mcap', l: 'Market Cap (₹Cr)' },
  { v: 'hist_mcap', l: 'Historical Mcap (₹Cr, approx)' },
  // --- extended technical factors (close + turnover + Nifty derived) ---
  { v: 'ret3m', l: 'Return — 3 month %' },
  { v: 'ret6m', l: 'Return — 6 month %' },
  { v: 'ret12m', l: 'Return — 12 month %' },
  { v: 'rsNifty', l: 'Relative strength vs Nifty (6m) %' },
  { v: 'accel', l: 'Momentum acceleration %' },
  { v: 'dma50', l: 'Distance from 50-DMA %' },
  { v: 'dma200', l: 'Distance from 200-DMA %' },
  { v: 'rangePos', l: '52-week range position (0=low…100=high)' },
  { v: 'daysHigh', l: 'Days since 52-week high' },
  { v: 'vol', l: 'Volatility — annualised %' },
  { v: 'riskMom', l: 'Risk-adjusted momentum (3m ÷ vol)' },
  { v: 'beta', l: 'Beta vs Nifty' },
  { v: 'mdd6', l: 'Max drawdown — 6 month %' },
  { v: 'upPct', l: 'Up-day consistency % (3m)' },
  { v: 'turnover', l: 'Avg daily turnover (₹ lacs, 20d)' },
  { v: 'turnSurge', l: 'Turnover surge (5d ÷ 90d)' },
  { v: 'macd', l: 'MACD histogram (12,26,9)' },
  { v: 'stoch', l: 'Stochastic %K (14)' },
  { v: 'bollB', l: 'Bollinger %b (20,2)' },
];
const FIELD_LABEL = {}; FIELDS.forEach(f => FIELD_LABEL[f.v] = f.l);
const fmtINR = n => '₹' + Math.round(n).toLocaleString('en-IN');
const pct = n => (n >= 0 ? '+' : '') + n.toFixed(1) + '%';

/* ---- data loading (pass an optional onProgress(msg) callback) ---- */
async function gunzipJSON(url) {
  const buf = await (await fetch(url + '?t=' + Date.now())).arrayBuffer();
  const stream = new Blob([new Uint8Array(buf)]).stream().pipeThrough(new DecompressionStream('gzip'));
  return JSON.parse(await new Response(stream).text());
}
// stock_data.bin only supplies point-in-time index membership + time base + benchmark.
async function loadCore() {
  const D = await gunzipJSON('./stock_data.bin');
  IDXH = D.indicesHistory || {}; FNOH = D.fnoHistory || []; START_TS = D.startTs;
  try { NIFTY = (await (await fetch('./nifty.json?t=' + Date.now())).json()).px || {}; } catch (e) { NIFTY = {}; }
}
async function loadSF() {
  if (SF) return true;
  const D = await gunzipJSON('./sf_stock_data.bin');
  const ts = START_TS, ser = {}, meta = {}, turn = {};
  for (const sym in D.data) {
    const o = D.data[sym], n = o.d.length, d = new Array(n), p = new Array(n), t = new Array(n);
    for (let i = 0; i < n; i++) {
      const y = o.d[i];
      const off = Math.floor((Date.UTC(Math.floor(y / 10000), (Math.floor(y / 100) % 100) - 1, y % 100) / 1000 - ts) / DAY);
      d[i] = off; p[i] = Math.round(o.c[i] * 100); t[i] = o.t[i] || 0;
    }
    ser[sym] = { d, p }; turn[sym] = { d, t }; const sm = D.meta[sym] || {};
    meta[sym] = { symbol: sym, name: sm.name || sym, industry: sm.ind || 'Other', sector: sm.ind || 'Other', mcap: 0, latest: o.c[n - 1], alive: sm.alive };
  }
  const endOff = Math.floor((Date.parse((D.end || '2024-01-01') + 'T00:00:00Z') / 1000 - ts) / DAY);
  SF = { meta, series: ser, turn, startTs: ts, endOff, start: D.start, end: D.end,
         nDead: Object.values(meta).filter(m => !m.alive).length, nTot: Object.keys(meta).length };
  return true;
}
function activateSF() { SERIES = SF.series; META = SF.meta; TURN = SF.turn; SF_END_OFF = SF.endOff; START_TS = SF.startTs; }
async function loadEngineData(onProgress) {
  onProgress && onProgress('Loading market data…');
  await loadCore();
  onProgress && onProgress('Loading survivorship-free data (~17 MB)…');
  await loadSF(); activateSF();
  onProgress && onProgress('');
}

/* ---- price / factor helpers ---- */
function dayOff(dstr) { return Math.floor((Date.parse(dstr + 'T00:00:00Z') / 1000 - START_TS) / DAY); }
function isoOff(off) { return new Date((START_TS + off * DAY) * 1000).toISOString().slice(0, 10); }
function idxLE(arr, off) { let lo = 0, hi = arr.length - 1, ans = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (arr[m] <= off) { ans = m; lo = m + 1; } else hi = m - 1; } return ans; }
function priceAt(tkr, off) { const s = SERIES[tkr]; if (!s) return null; const i = idxLE(s.d, off); return i < 0 ? null : s.p[i] / 100; }
function turnoverAt(tkr, off) { const s = TURN[tkr]; if (!s) return 0; const i = idxLE(s.d, off); return i < 0 ? 0 : s.t[i]; }
// held position that stops trading >1 quarter before data end → marked to zero (loss realised)
function markPrice(tkr, off) { const s = SERIES[tkr]; if (!s) return null; const ld = s.d[s.d.length - 1]; if (off > ld && ld < SF_END_OFF - 90) return 0; return priceAt(tkr, off); }
function hl52(tkr, off) { const s = SERIES[tkr]; if (!s) return null; const lo = off - 365; let i = idxLE(s.d, off); if (i < 0) return null; let hi = -1e18, low = 1e18; for (let k = i; k >= 0 && s.d[k] >= lo; k--) { const p = s.p[k]; if (p > hi) hi = p; if (p < low) low = p; } return { hi: hi / 100, low: low / 100 }; }
// Wilder-smoothed RSI(14) — the industry-standard formula (matches Trendlyne/StockView "Day RSI")
function rsi14(tkr, off) {
  const s = SERIES[tkr]; if (!s) return null; const i = idxLE(s.d, off); if (i < 15) return null;
  const n = 14, start = Math.max(1, i - 100);
  let avgG = 0, avgL = 0, k = start, cnt = 0;
  for (; k <= i && cnt < n; k++, cnt++) { const ch = s.p[k] - s.p[k - 1]; if (ch > 0) avgG += ch; else avgL -= ch; }
  avgG /= n; avgL /= n;
  for (; k <= i; k++) { const ch = s.p[k] - s.p[k - 1]; avgG = (avgG * (n - 1) + (ch > 0 ? ch : 0)) / n; avgL = (avgL * (n - 1) + (ch < 0 ? -ch : 0)) / n; }
  if (avgG + avgL === 0) return 50;
  return 100 - 100 / (1 + avgG / (avgL || 1e-9));
}
function lastSnap(list, dstr) { let best = null; for (const s of list) { if (s.effectiveDate <= dstr && (!best || s.effectiveDate > best.effectiveDate)) best = s; } return best || (list.length ? list[0] : null); }
function membersAsOf(name, dstr) {
  if (name === '__FNO__') { const snap = lastSnap(FNOH, dstr); return snap ? new Set(snap.symbols) : null; }
  const snap = lastSnap(IDXH[name] || [], dstr); return snap ? new Set(snap.symbols) : null;
}
function maxOffset() { let mx = 0; for (const k in SERIES) { const d = SERIES[k].d; if (d && d.length) { const v = d[d.length - 1]; if (v > mx) mx = v; } } return mx; }
function monthsBetween(start, end) {
  const out = []; let y = +start.slice(0, 4), m = +start.slice(5, 7); const ey = +end.slice(0, 4), em = +end.slice(5, 7);
  while (y < ey || (y === ey && m <= em)) { const last = new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10); out.push(last); m++; if (m > 12) { m = 1; y++; } }
  if (out.length) out[out.length - 1] = end; return out;
}

/* ---- screening (the shared "screen → filter → rank" step) ---- */
/* ---- extended technical factors (close + turnover + Nifty; no fundamentals) ---- */
function retPctAt(tkr, off, days) { const p = priceAt(tkr, off), p0 = priceAt(tkr, off - days); return (p != null && p0 != null && p0 > 0) ? (p / p0 - 1) * 100 : null; }
function winCloses(tkr, off, days) { const s = SERIES[tkr]; if (!s) return null; const lo = off - days; let i = idxLE(s.d, off); if (i < 0) return null; const out = []; for (let k = i; k >= 0 && s.d[k] >= lo; k--) out.push(s.p[k] / 100); out.reverse(); return out; }
function smaAt(tkr, off, days) { const v = winCloses(tkr, off, days); if (!v || !v.length) return null; return v.reduce((a, b) => a + b, 0) / v.length; }
function retsOf(v) { const r = []; for (let i = 1; i < v.length; i++) if (v[i - 1] > 0) r.push(v[i] / v[i - 1] - 1); return r; }
function stdOf(a) { if (a.length < 2) return 0; const m = a.reduce((x, y) => x + y, 0) / a.length; return Math.sqrt(a.reduce((x, y) => x + (y - m) ** 2, 0) / (a.length - 1)); }
function niftyRetAt(off, days) { const a = nearestNifty(isoOff(off)), b = nearestNifty(isoOff(off - days)); return (a && b) ? (a / b - 1) * 100 : null; }
function turnAvgAt(tkr, off, days) { const s = TURN[tkr]; if (!s) return 0; const lo = off - days; let i = idxLE(s.d, off); if (i < 0) return 0; let sum = 0, n = 0; for (let k = i; k >= 0 && s.d[k] >= lo; k--) { sum += s.t[k]; n++; } return n ? sum / n : 0; }
function emaSeries(arr, p) { const k = 2 / (p + 1); let e = arr[0]; const out = [e]; for (let i = 1; i < arr.length; i++) { e = arr[i] * k + e * (1 - k); out.push(e); } return out; }
function computeTech(tkr, off, px) {
  const r1 = retPctAt(tkr, off, 30), r1p = retPctAt(tkr, off - 30, 30);
  const r3 = retPctAt(tkr, off, 91), r6 = retPctAt(tkr, off, 182), r12 = retPctAt(tkr, off, 365);
  const nr6 = niftyRetAt(off, 182), s50 = smaAt(tkr, off, 50), s200 = smaAt(tkr, off, 200), hl = hl52(tkr, off);
  const w90 = winCloses(tkr, off, 90), rets90 = w90 ? retsOf(w90) : [];
  const vol = rets90.length > 2 ? stdOf(rets90) * Math.sqrt(252) * 100 : null;
  let daysHigh = null; { const s = SERIES[tkr]; if (s) { const lo = off - 365; let i = idxLE(s.d, off), hi = -1, hidx = -1; for (let k = i; k >= 0 && s.d[k] >= lo; k--) if (s.p[k] > hi) { hi = s.p[k]; hidx = s.d[k]; } if (hidx >= 0) daysHigh = off - hidx; } }
  let mdd = null; { const v = winCloses(tkr, off, 182); if (v && v.length) { let pk = -1, d = 0; for (const p of v) { if (p > pk) pk = p; else if (pk > 0) { const x = (pk - p) / pk * 100; if (x > d) d = x; } } mdd = d; } }
  let beta = null; { const s = SERIES[tkr]; if (s) { const lo = off - 120; let i = idxLE(s.d, off); const ix = []; for (let k = i; k >= 0 && s.d[k] >= lo; k--) ix.push(k); ix.reverse(); const sr = [], nr = []; for (let j = 1; j < ix.length; j++) { const p0 = s.p[ix[j - 1]] / 100, p1 = s.p[ix[j]] / 100, n0 = nearestNifty(isoOff(s.d[ix[j - 1]])), n1 = nearestNifty(isoOff(s.d[ix[j]])); if (p0 > 0 && n0 && n1) { sr.push(p1 / p0 - 1); nr.push(n1 / n0 - 1); } } if (sr.length > 5) { const mn = nr.reduce((a, b) => a + b, 0) / nr.length, ms = sr.reduce((a, b) => a + b, 0) / sr.length; let cov = 0, vn = 0; for (let j = 0; j < sr.length; j++) { cov += (sr[j] - ms) * (nr[j] - mn); vn += (nr[j] - mn) ** 2; } beta = vn > 0 ? cov / vn : null; } } }
  let macd = null; { const v = winCloses(tkr, off, 320); if (v && v.length > 35) { const e12 = emaSeries(v, 12), e26 = emaSeries(v, 26), ml = v.map((_, i) => e12[i] - e26[i]), sig = emaSeries(ml, 9); macd = ml[ml.length - 1] - sig[sig.length - 1]; } }
  let stoch = null; { const v = winCloses(tkr, off, 21); if (v && v.length) { const hi = Math.max(...v), lo = Math.min(...v); stoch = hi > lo ? (px - lo) / (hi - lo) * 100 : 50; } }
  let bollB = null; { const v = winCloses(tkr, off, 28); if (v && v.length > 1) { const m = v.reduce((a, b) => a + b, 0) / v.length, sd = stdOf(v), up = m + 2 * sd, dn = m - 2 * sd; bollB = up > dn ? (px - dn) / (up - dn) * 100 : 50; } }
  const t5 = turnAvgAt(tkr, off, 7), t90 = turnAvgAt(tkr, off, 90);
  return {
    ret3m: r3, ret6m: r6, ret12m: r12,
    rsNifty: (r6 != null && nr6 != null) ? r6 - nr6 : null,
    accel: (r1 != null && r1p != null) ? r1 - r1p : null,
    dma50: (s50 && px) ? (px / s50 - 1) * 100 : null,
    dma200: (s200 && px) ? (px / s200 - 1) * 100 : null,
    rangePos: (hl && hl.hi > hl.low) ? (px - hl.low) / (hl.hi - hl.low) * 100 : null,
    daysHigh, vol, riskMom: (r3 != null && vol) ? r3 / vol : null, beta, mdd6: mdd,
    upPct: rets90.length ? rets90.filter(x => x > 0).length / rets90.length * 100 : null,
    turnover: turnAvgAt(tkr, off, 20), turnSurge: t90 > 0 ? t5 / t90 : null, macd, stoch, bollB,
  };
}
// extended factors are EXPENSIVE — only compute them when the strategy actually uses one
const EXT_FIELDS = new Set(['ret3m','ret6m','ret12m','rsNifty','accel','dma50','dma200','rangePos','daysHigh','vol','riskMom','beta','mdd6','upPct','turnover','turnSurge','macd','stoch','bollB']);
function needsTech(cfg) { return EXT_FIELDS.has(cfg.sortBy) || (cfg.filters || []).some(f => EXT_FIELDS.has(f.field)); }
function factorsAt(off, cfg) {
  const lookOff = off - Math.round(cfg.lookback * 30.44);
  const members = cfg.indexName ? membersAsOf(cfg.indexName, isoOff(off)) : null;
  const useTech = needsTech(cfg);
  const rows = [];
  for (const tkr in SERIES) {
    const m = META[tkr]; if (!m) continue;
    if (members && !members.has(m.symbol)) continue;
    const price = priceAt(tkr, off); const p0 = priceAt(tkr, lookOff);
    if (price == null || p0 == null || p0 <= 0) continue;
    if (turnoverAt(tkr, off) < cfg.mcapFloor) continue;   // point-in-time daily turnover floor
    const hl = hl52(tkr, off); if (!hl) continue;
    const r = { tkr, sym: m.symbol, name: m.name, ind: (m.industry || m.sector || 'Other'),
      price, chg: (price / p0 - 1) * 100, rsi: rsi14(tkr, off),
      // Trendlyne convention: POSITIVE distance below the 52w high (high 100, price 95 -> 5)
      d52: (hl.hi - price) / hl.hi * 100, d52low: (price - hl.low) / hl.low * 100,
      mcap: m.mcap, histMcap: 0 };
    if (useTech) Object.assign(r, computeTech(tkr, off, price));   // extended technical factors
    rows.push(r);
  }
  const byInd = {}; rows.forEach(r => { (byInd[r.ind] = byInd[r.ind] || []).push(r.chg); });
  const indAvg = Object.entries(byInd).map(([k, v]) => [k, v.reduce((a, b) => a + b, 0) / v.length]).sort((a, b) => b[1] - a[1]);
  const indRankMap = {}; indAvg.forEach(([k], i) => { indRankMap[k] = Math.min(10, 1 + Math.floor(i / Math.max(1, indAvg.length / 10))); });
  rows.forEach(r => { r.indRank = indRankMap[r.ind] || 10; });
  return rows;
}
function fieldVal(r, f) {
  switch (f) { case 'changePercent': return r.chg; case 'rsi': return r.rsi; case 'd52': return r.d52; case 'd52_low_pct': return r.d52low; case 'indRank': return r.indRank; case 'mcap': return r.mcap; case 'hist_mcap': return r.histMcap; }
  return (f in r && typeof r[f] === 'number') ? r[f] : null;   // extended tech factors stored under their own key
}
function passFilters(r, filters) {
  for (const f of (filters || [])) { const x = fieldVal(r, f.field); if (x == null) return false;
    if (!(f.op === '>' ? x > f.val : f.op === '>=' ? x >= f.val : f.op === '<' ? x < f.val : f.op === '<=' ? x <= f.val : x === f.val)) return false; }
  return true;
}
// ranked candidate list as of a date (top cfg.topN = picks, the rest = "also qualifying")
function screenAsOf(cfg, dateStr) {
  const off = dayOff(dateStr);
  let rows = factorsAt(off, cfg).filter(r => r.rsi != null && passFilters(r, cfg.filters));
  rows.sort((a, b) => { const x = fieldVal(a, cfg.sortBy), y = fieldVal(b, cfg.sortBy); return cfg.dir === 'high' ? y - x : x - y; });
  return rows;
}
// turn ranked picks into a whole-share buy basket for a given capital
function allocateBasket(picks, capital) {
  const per = capital / (picks.length || 1);
  let deployed = 0;
  const rows = picks.map(r => { const shares = Math.floor(per / r.price); const alloc = shares * r.price; deployed += alloc; return { ...r, shares, alloc }; });
  return { rows, deployed, cash: capital - deployed };
}
// buy top-N once at `start`, hold unchanged to `end` (equal-weight, delisting→0)
function computeHold(cfg, start, end, capital) {
  const picks = screenAsOf(cfg, start).slice(0, cfg.topN);
  const endOff = dayOff(end);
  const per = capital / (picks.length || 1);
  const rows = picks.map(r => {
    const units = per / r.price; const ep = markPrice(r.tkr, endOff);
    const endPrice = (ep == null ? r.price : ep); const endVal = units * endPrice;
    return { sym: r.sym, entryPrice: r.price, endPrice, retPct: (endPrice / r.price - 1) * 100, startVal: per, endVal };
  });
  const startVal = per * picks.length, endVal = rows.reduce((a, b) => a + b.endVal, 0);
  return { rows, startVal, endVal, retPct: startVal > 0 ? (endVal / startVal - 1) * 100 : 0,
           cash: capital - startVal };
}

/* ---- the backtester ---- */
function simulate(cfg) {
  const months = monthsBetween(cfg.start, cfg.end);
  const N = cfg.topN;
  let pos = {}, cash = 0, started = false; const equity = [], rebs = [], trades = []; let entryInfo = {}, lastRebVal = cfg.capital, monthsSinceReb = 1e9, latest = [], latestCash = 0;
  const mark = off => { let v = cash; for (const t in pos) { const p = markPrice(t, off); if (p != null) v += pos[t] * p; } return v; };
  const fLabel = { changePercent: 'Chg%', rsi: 'RSI', d52: '52wHi%', d52_low_pct: '52wLo%', indRank: 'IndRank', mcap: 'Mcap', hist_mcap: 'HMcap' }[cfg.sortBy] || FIELD_LABEL[cfg.sortBy] || cfg.sortBy;
  for (let mi = 0; mi < months.length; mi++) {
    const md = months[mi], off = dayOff(md);
    const mv = started ? mark(off) : cfg.capital;
    equity.push([md, mv]);
    monthsSinceReb++;
    const isReb = (mi === 0) || (monthsSinceReb >= cfg.freq);
    if (isReb) {
      let rows = factorsAt(off, cfg).filter(r => r.rsi != null && passFilters(r, cfg.filters));
      rows.sort((a, b) => { const x = fieldVal(a, cfg.sortBy), y = fieldVal(b, cfg.sortBy); return cfg.dir === 'high' ? y - x : x - y; });
      const target = rows.slice(0, N); const tmap = {}; target.forEach(r => tmap[r.tkr] = r); const tset = new Set(target.map(r => r.tkr));
      for (const t of Object.keys(pos)) { if (!tset.has(t)) { const e = entryInfo[t]; if (e) { const mp = markPrice(t, off); const xp = (mp == null ? e.price : mp);
        trades.push({ sym: META[t].symbol, entryDate: e.date, exitDate: md, entryPrice: e.price, exitPrice: xp, retPct: (xp / e.price - 1) * 100, factor: e.factor, rsi: e.rsi, held: false }); }
        delete entryInfo[t]; } }
      const wasEntry = new Set(target.filter(r => !(r.tkr in pos)).map(r => r.tkr));
      if (!started || cfg.method === 'reset') {
        const base = started ? mv : cfg.capital; const per = base / N; pos = {};
        target.forEach(r => { pos[r.tkr] = per / r.price; });
        cash = base - per * target.length; started = true;
      } else {
        const valOf = t => { const p = markPrice(t, off); return p != null ? pos[t] * p : 0; };
        const exits = Object.keys(pos).filter(t => !tset.has(t));
        let proceeds = 0; exits.forEach(t => { proceeds += valOf(t); delete pos[t]; });
        const entries = target.filter(r => !(r.tkr in pos));
        let avail = proceeds + cash; cash = 0;
        if (entries.length) {
          const avg = exits.length ? proceeds / exits.length : avail / entries.length;
          for (const e of entries) { const a = Math.min(avg, avail); if (a <= 1) break; pos[e.tkr] = a / e.price; avail -= a; }
          cash = Math.max(0, avail);
        } else { cash = avail; }
      }
      for (const r of target) { if (wasEntry.has(r.tkr)) { entryInfo[r.tkr] = { date: md, price: r.price, factor: fieldVal(r, cfg.sortBy), rsi: r.rsi, chg: r.chg }; } }
      const now = mark(off); latestCash = cash;
      const holds = Object.keys(pos).map(t => { const p = markPrice(t, off), v = p ? pos[t] * p : 0, r = tmap[t] || {};
        return { sym: META[t].symbol, ind: (META[t].industry || META[t].sector || 'Other'), wt: now ? v / now * 100 : 0, val: v,
                 isNew: wasEntry.has(t), factor: fieldVal(r, cfg.sortBy), rsi: r.rsi, chg: r.chg, d52: r.d52, mcap: r.mcap }; }).sort((a, b) => b.wt - a.wt);
      rebs.push({ date: md, val: now, ret: started ? (mv / (lastRebVal || 1) - 1) * 100 : 0, cash, cashWt: now ? cash / now * 100 : 0, nNew: wasEntry.size, holds });
      lastRebVal = mv > 0 ? mv : cfg.capital; monthsSinceReb = 0;
      latest = holds.slice(); latest._cashWt = now ? cash / now * 100 : 0; latest._cash = cash;
    }
  }
  { const lastOff = dayOff(cfg.end); for (const t in entryInfo) { const e = entryInfo[t]; const mp = markPrice(t, lastOff); const xp = (mp == null ? e.price : mp);
    trades.push({ sym: META[t].symbol, entryDate: e.date, exitDate: cfg.end, entryPrice: e.price, exitPrice: xp, retPct: (xp / e.price - 1) * 100, factor: e.factor, rsi: e.rsi, held: true }); } }
  const bench = []; const startN = nearestNifty(cfg.start);
  if (startN) for (const [d] of equity) { const nv = nearestNifty(d); bench.push([d, nv ? cfg.capital * nv / startN : null]); }
  const years = (Date.parse(cfg.end) - Date.parse(cfg.start)) / (365.25 * 864e5);
  const finalV = equity[equity.length - 1][1];
  const cagr = years > 0 ? (Math.pow(finalV / cfg.capital, 1 / years) - 1) * 100 : 0;
  const benchFinal = bench.length ? bench[bench.length - 1][1] : null;
  const benchCagr = (benchFinal && years > 0) ? (Math.pow(benchFinal / cfg.capital, 1 / years) - 1) * 100 : null;
  const rets = []; for (let i = 1; i < equity.length; i++) { if (equity[i - 1][1] > 0) rets.push(equity[i][1] / equity[i - 1][1] - 1); }
  const mean = rets.reduce((a, b) => a + b, 0) / (rets.length || 1);
  const vol = Math.sqrt(rets.reduce((a, b) => a + (b - mean) ** 2, 0) / (rets.length || 1) * 12) * 100;
  const periodRebs = rebs.slice(1); const wins = periodRebs.filter(r => r.ret > 0).length;
  trades.sort((a, b) => a.entryDate < b.entryDate ? 1 : -1);
  return { equity, bench, rebs, trades, latest, latestCash, cfg, years, finalV, cagr, benchCagr, vol, fLabel,
           maxDD: maxDrawdown(equity), winRate: periodRebs.length ? 100 * wins / periodRebs.length : 0 };
}
function nearestNifty(dstr) { if (NIFTY[dstr]) return NIFTY[dstr]; let d = new Date(dstr + 'T00:00:00Z'); for (let i = 0; i < 7; i++) { d.setUTCDate(d.getUTCDate() - 1); const k = d.toISOString().slice(0, 10); if (NIFTY[k]) return NIFTY[k]; } return null; }
function maxDrawdown(eq) { let peak = -1, mdd = 0; for (const [, v] of eq) { if (v > peak) peak = v; else if (peak > 0) { const dd = (peak - v) / peak * 100; if (dd > mdd) mdd = dd; } } return mdd; }

/* ---- config labels + localStorage ---- */
function strategyLabel(c) {
  const S = { changePercent: 'Momentum', rsi: 'RSI', d52: '52w-High', d52_low_pct: '52w-Low', indRank: 'Industry-rank', mcap: 'Mcap', hist_mcap: 'Hist-mcap' };
  const F = { 1: 'Monthly', 3: 'Quarterly', 6: 'Half-yearly', 12: 'Yearly' }[c.freq] || c.freq + 'mo';
  const uni = c.indexName ? String(c.indexName).replace('__FNO__', 'F&O') : c.mcapFloor ? '≥₹' + (+c.mcapFloor).toLocaleString('en-IN') + 'L turnover' : 'All stocks';
  const nf = (c.filters || []).length;
  return `${S[c.sortBy] || FIELD_LABEL[c.sortBy] || c.sortBy} ${c.dir === 'high' ? 'top' : 'bottom'}-${c.topN} · ${uni} · ${F}${nf ? ' · ' + nf + ' filter' + (nf > 1 ? 's' : '') : ''}`;
}
function universeLabel(c) { return c.indexName ? String(c.indexName).replace('__FNO__', 'F&O Stocks') : c.mcapFloor ? '≥₹' + (+c.mcapFloor).toLocaleString('en-IN') + 'L turnover' : 'All stocks'; }
function freqLabel(c) { return { 1: 'Monthly', 3: 'Quarterly', 6: 'Half-yearly', 12: 'Yearly' }[c.freq] || c.freq + 'mo'; }
function filterExpr(c) {
  const L = { changePercent: 'Change %', rsi: 'RSI', d52: '% from 52w High', d52_low_pct: '% from 52w Low', indRank: 'Industry rank', mcap: 'Mcap', hist_mcap: 'Hist mcap' };
  return (c.filters || []).map(f => `${L[f.field] || FIELD_LABEL[f.field] || f.field} ${f.op} ${f.val}`).join(' AND ');
}
function loadLS(k) { try { return JSON.parse(localStorage.getItem(k) || '[]'); } catch (e) { return []; } }
function saveLS(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }
