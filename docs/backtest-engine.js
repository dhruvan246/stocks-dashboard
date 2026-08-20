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
let META = {}, SERIES = {}, IDXH = {}, FNOH = [], START_TS = 0, NIFTY = {}, NIFTY500 = {}, CORE_META = {};
let SF = null, TURN = {}, SF_END_OFF = Infinity;
const DATA_MODE = 'sf';                       // survivorship-free only
const TURN_OPTS = [['100', '≥₹1 Cr'], ['500', '≥₹5 Cr'], ['2000', '≥₹20 Cr'], ['10000', '≥₹100 Cr']]; // daily turnover (₹ lacs)
// <optgroup> headings for the builder dropdowns — see stock-backtest.html fieldOptionsHtml().
// Same taxonomy the site glossary uses for the Backtest page. Groups render in order of first
// appearance below, members in the order listed; a heading carries the qualifier common to its
// members so the option labels stay short enough not to truncate.
// Must stay in sync with stock-backtest.html FIELDS (that page carries its own engine copy).
const G_MOM = 'Price & momentum';
const G_TRND = 'Trend & price levels';
const G_RISK = 'Risk';
const G_LIQ = 'Liquidity & participation';
const G_OSC = 'Oscillators';
const G_FUND = 'Fundamentals — point-in-time';
const OWN_GRP = 'Ownership — FII/DII (latest filed qtr)';
const G_SIZE = 'Size — ⚠ always 0, use Turnover';
const FIELDS = [
  { v: 'ret1m', g: G_MOM, l: 'Return — 1 month %' },
  { v: 'ret3m', g: G_MOM, l: 'Return — 3 month %' },
  { v: 'ret6m', g: G_MOM, l: 'Return — 6 month %' },
  { v: 'ret12m', g: G_MOM, l: 'Return — 12 month %' },
  { v: 'accel', g: G_MOM, l: 'Momentum acceleration %' },
  { v: 'riskMom', g: G_MOM, l: 'Risk-adjusted momentum (3m ÷ vol)' },
  { v: 'postDrift', g: G_MOM, l: 'Post-result drift % (return since last earnings date)' },
  { v: 'composite', g: G_MOM, l: 'Quality-Momentum composite (z: TTM growth + 12m return − volatility)' },

  { v: 'd52', g: G_TRND, l: 'Distance from 52w High % (high 100, price 95 → 5; near-high = ≤ 10)' },
  { v: 'd52_low_pct', g: G_TRND, l: 'Distance from 52w Low % (% above the low)' },
  { v: 'rangePos', g: G_TRND, l: '52-week range position (0=low…100=high)' },
  { v: 'daysHigh', g: G_TRND, l: 'Days since 52-week high' },
  { v: 'dma50', g: G_TRND, l: 'Distance from 50-DMA %' },
  { v: 'dma200', g: G_TRND, l: 'Distance from 200-DMA %' },
  { v: 'indRank', g: G_TRND, l: 'Industry Momentum Rank (1=hot…10=cold)' },

  { v: 'vol', g: G_RISK, l: 'Volatility — annualised %' },
  { v: 'beta', g: G_RISK, l: 'Beta vs Nifty' },
  { v: 'mdd6', g: G_RISK, l: 'Max drawdown — 6 month %' },
  { v: 'upPct', g: G_RISK, l: 'Up-day consistency % (3m)' },

  { v: 'turnover', g: G_LIQ, l: 'Avg daily turnover (₹ lacs, 20d)' },
  { v: 'turnSurge', g: G_LIQ, l: 'Turnover surge (5d ÷ 90d)' },
  { v: 'volSurge', g: G_LIQ, l: 'Volume surge — shares (5d ÷ 90d)' },
  { v: 'delivPct', g: G_LIQ, l: 'Delivery % (20d avg, 2002+ data)' },

  { v: 'rsi', g: G_OSC, l: 'RSI(14)' },
  { v: 'macd', g: G_OSC, l: 'MACD histogram (12,26,9)' },
  { v: 'stoch', g: G_OSC, l: 'Stochastic %K (14)' },
  { v: 'bollB', g: G_OSC, l: 'Bollinger %b (20,2)' },

  // point-in-time quarterly net profit (XBRL)
  { v: 'profitYoyPct', g: G_FUND, l: 'Net Profit Qtr Growth YoY %' },
  { v: 'profitBase', g: G_FUND, l: 'Net Profit Yr-ago Qtr ₹Cr (YoY base)' },
  { v: 'profitAccel', g: G_FUND, l: 'Profit-growth acceleration (this-Q YoY − last-Q YoY, pts)' },
  { v: 'profitTTM', g: G_FUND, l: 'Profit growth TTM % (last 4Q vs prior 4Q)' },
  { v: 'profitStreak', g: G_FUND, l: 'Profit-growth streak (consecutive +YoY quarters)' },

  // point-in-time FII/DII holding (quarterly SHP filings)
  { v: 'fiiPct', g: OWN_GRP, l: 'FII holding %' },
  { v: 'fiiChgPp', g: OWN_GRP, l: 'FII holding change QoQ (pp)' },
  { v: 'diiPct', g: OWN_GRP, l: 'DII holding %' },
  { v: 'diiChgPp', g: OWN_GRP, l: 'DII holding change QoQ (pp)' },

  // Dead in survivorship-free mode (the only mode) — kept so old saved strategies still resolve.
  { v: 'mcap', g: G_SIZE, l: 'Market Cap (₹Cr)' },
  { v: 'hist_mcap', g: G_SIZE, l: 'Historical Mcap (₹Cr)' },
];
const FIELD_LABEL = {}; FIELDS.forEach(f => FIELD_LABEL[f.v] = f.l);
const fmtINR = n => '₹' + Math.round(n).toLocaleString('en-IN');
const pct = n => (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
// link a stock symbol to its detail page (shared by the backtest + strategies pages)
function stockHref(sym) { return './stock.html?sym=' + encodeURIComponent(sym); }
function stockLink(sym, cls) { return '<a href="' + stockHref(sym) + '" class="' + (cls || '') + '" title="' + sym + ' — full details">' + sym + '</a>'; }

/* ---- data loading (pass an optional onProgress(msg) callback) ---- */
async function gunzipJSON(url) {
  const buf = await (await fetch(url)).arrayBuffer();
  const stream = new Blob([new Uint8Array(buf)]).stream().pipeThrough(new DecompressionStream('gzip'));
  return JSON.parse(await new Response(stream).text());
}
// Core supplies point-in-time index membership + the time base + dash metadata (CORE_META, keyed
// SYMBOL.NS — the only place market cap lives; SF's own meta carries none).
//
// It reads dash_slim.bin (2 MB), NOT stock_data.bin (17.5 MB). Both files are written by the same
// build_compressed.py run and their indicesHistory / fnoHistory / startTs are byte-identical; the
// 15.5 MB difference is stock_data.bin's full price series, which activateSF() overwrites moments
// later and no caller ever reads. Slim's meta is also the fresher one (regenerated more often), and
// it is only consumed by the stock page's fallback render — screening never reads mcap from here
// (rowsAt takes META[sym].mcap, which the SF path sets to 0).
async function loadCore() {
  const D = await gunzipJSON('./dash_slim.bin');   // browser-cached (ETag); no cache-buster → instant on repeat loads
  IDXH = D.indicesHistory || {}; FNOH = D.fnoHistory || []; START_TS = D.startTs;
  CORE_META = D.meta || {};
  try { NIFTY = (await (await fetch('./nifty.json')).json()).px || {}; } catch (e) { NIFTY = {}; }
  try { NIFTY500 = (await (await fetch('./nifty500.json')).json()).px || {}; } catch (e) { NIFTY500 = {}; }
}
// Data lives in a dedicated same-origin repo (dhruvan246.github.io/sf-data/), force-pushed daily
// (no bloat), served from Pages — same origin, no CORS. Layout since 2026-08-03 is BY DATE:
//   sf_recent_*.bin  bars from sf_meta.deepFrom (2019-01-01) — covers the default 2020-03-31
//                    window and every wave preset with a full 365d lookback, at ~1/3 the bytes
//   sf_deep_*.bin    everything earlier (2002-2018 daily + pre-2002 weekly), fetched by
//                    ensureDeepHistory() only when a run's window starts before ~2020
// A meta WITHOUT `deepFrom` = the legacy by-symbol sf_stock_data_*.bin layout — kept loadable so
// the pages can deploy ahead of the data flip.
const SF_BASE = 'https://dhruvan246.github.io/sf-data/';
const SF_DB = 'sfcache';
function _sfdb() { return new Promise((res, rej) => { const r = indexedDB.open(SF_DB, 1); r.onupgradeneeded = () => { if (!r.result.objectStoreNames.contains('bin')) r.result.createObjectStore('bin'); }; r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); }); }
async function _sfGet(k) { try { const db = await _sfdb(); return await new Promise(res => { const t = db.transaction('bin', 'readonly').objectStore('bin').get(k); t.onsuccess = () => res(t.result || null); t.onerror = () => res(null); }); } catch (e) { return null; } }
async function _sfPut(k, v) { try { const db = await _sfdb(); await new Promise(res => { const t = db.transaction('bin', 'readwrite').objectStore('bin').put(v, k); t.onsuccess = () => res(); t.onerror = () => res(); }); } catch (e) {} }
async function _sfClear() { try { const db = await _sfdb(); await new Promise(res => { const t = db.transaction('bin', 'readwrite').objectStore('bin').clear(); t.onsuccess = t.onerror = () => res(); }); } catch (e) {} }
// Fetch one .bin part (IndexedDB-cached by data version) and return its parsed JSON.
// `allowClear` — only the INITIAL page load may wipe the cache on a version change; a deep-history
// cache miss must never evict the recent parts cached moments earlier under the same version.
let _SF_CLEARED = false;
async function _sfPart(file, key, ver, allowClear) {
  let buf = ver ? await _sfGet(key + ':' + ver) : null;
  if (!buf) {
    if (allowClear && !_SF_CLEARED) { await _sfClear(); _SF_CLEARED = true; }   // evict older versions
    const resp = await fetch(SF_BASE + file + '?v=' + (ver || Date.now()), { cache: 'reload' });
    if (!resp.ok) throw new Error('HTTP ' + resp.status + ' ' + file);
    buf = await resp.arrayBuffer();
    if (ver) _sfPut(key + ':' + ver, buf);
  }
  return JSON.parse(await new Response(new Blob([new Uint8Array(buf)]).stream().pipeThrough(new DecompressionStream('gzip'))).text());
}
// Normalise one symbol's raw record into engine arrays: day offsets, integer-paise closes, exact
// intraday high/low when the data carries them, legacy per-mil offsets otherwise.
function _sfNorm(o, ts) {
  const n = o.d.length, d = new Array(n), p = new Array(n), t = new Array(n);
  const hasHL = o.h && o.l, h = hasHL ? new Array(n) : null, l = hasHL ? new Array(n) : null;
  for (let i = 0; i < n; i++) {
    const y = o.d[i];
    const off = Math.floor((Date.UTC(Math.floor(y / 10000), (Math.floor(y / 100) % 100) - 1, y % 100) / 1000 - ts) / DAY);
    d[i] = off; p[i] = Math.round(o.c[i] * 100); t[i] = o.t[i] || 0;
    if (hasHL) { h[i] = Math.round(o.h[i] * 100); l[i] = Math.round(o.l[i] * 100); }
  }
  const ser = { d, p };
  if (hasHL) { ser.h = h; ser.l = l; }                     // EXACT intraday high/low (x100, like p)
  else if (o.hb && o.lb) { ser.hb = o.hb; ser.lb = o.lb; } // old-format fallback (per-mil offsets)
  if (o.v)  ser.v  = o.v;    // traded volume (shares)
  if (o.dv) ser.dv = hasHL ? o.dv : o.dv.map(x => x / 10);   // delivery % (normalised to exact %)
  return { ser, t, lastClose: o.c[n - 1] };
}
function _sfMeta(sym, sm, lastClose) {
  return { symbol: sym, name: sm.name || sym, industry: sm.ind || 'Other', sector: sm.ind || 'Other', mcap: 0, latest: lastClose, alive: sm.alive, raw: sm.raw || null };
}
async function loadSF() {
  if (SF) return true;
  // Cache version = end + CONTENT rev. A heal/backfill republishes the parts WITHOUT advancing
  // `end` (the 2002-2019 delivery backfill did exactly that), and keying on `end` alone pinned
  // every returning visitor to the stale cached bytes until the next trading day. `rev` is absent
  // on an older sf_meta.json — then this degrades to the previous end-only key.
  let M = {}; try { M = (await (await fetch(SF_BASE + 'sf_meta.json?t=' + Date.now())).json()) || {}; } catch (e) {}
  const ver = M.end ? M.end + (M.rev ? ':' + M.rev : '') : '';
  const files = M.deepFrom
    ? Array.from({ length: M.recent || 1 }, (_, i) => ['sf_recent_' + (i + 1) + '.bin', 'sfr' + (i + 1)])
    : Array.from({ length: M.parts || 2 }, (_, i) => ['sf_stock_data_' + (i + 1) + '.bin', 'sfp' + (i + 1)]);   // legacy by-symbol layout
  const D = { data: {}, meta: {}, end: '', start: '' };
  for (const [file, key] of files) {
    const Dp = await _sfPart(file, key, ver, true);
    Object.assign(D.data, Dp.data); Object.assign(D.meta, Dp.meta);
    D.end = Dp.end || D.end; D.start = Dp.start || D.start;
  }
  const ts = START_TS, ser = {}, meta = {}, turn = {};
  for (const sym in D.data) {
    const r = _sfNorm(D.data[sym], ts);
    ser[sym] = r.ser; turn[sym] = { d: r.ser.d, t: r.t };
    meta[sym] = _sfMeta(sym, D.meta[sym] || {}, r.lastClose);
  }
  const endOff = Math.floor((Date.parse((D.end || '2024-01-01') + 'T00:00:00Z') / 1000 - ts) / DAY);
  // start/nTot/nDead describe the FULL dataset (from the meta) even before deep history loads, so
  // date pickers and universe stats never shrink to the recent slice.
  SF = { meta, series: ser, turn, startTs: ts, endOff, start: M.fullStart || D.start, end: D.end, ver,
         // where TRUE daily bars begin (2002-01-02); earlier bars are weekly, so oscillator-family
         // factors aren't meaningful there — the full-history window starts here, not at `start`.
         dailyFrom: M.dailyFrom || '',
         deepFrom: M.deepFrom || null, deepParts: M.deepFrom ? (M.deep || 0) : 0, deepLoaded: !(M.deepFrom && M.deep > 0),
         nDead: M.nTot != null ? M.nDead : Object.values(meta).filter(m => !m.alive).length,
         nTot: M.nTot != null ? M.nTot : Object.keys(meta).length };
  return true;
}
function activateSF() { SERIES = SF.series; META = SF.meta; TURN = SF.turn; SF_END_OFF = SF.endOff; START_TS = SF.startTs; }

/* ---- deep history (bars before SF.deepFrom), loaded on demand ------------------------------
 * Quick runs (default 2020-03-31 window, wave presets, live screens) never need it. A run whose
 * window starts before ~2020 awaits ensureHistoryFor(start) first; merge PREPENDS each symbol's
 * older bars, and symbols that died before deepFrom arrive here for the first time. Pre-deepFrom
 * bars never change day-to-day, so mixing a cached recent part with a fresher deep part is safe. */
function needsDeepFor(dstr) { return !!(SF && !SF.deepLoaded && dstr && dayOff(dstr) - 366 < dayOff(SF.deepFrom)); }
function _histGuard(dstr) { if (needsDeepFor(dstr)) throw new Error('pre-' + SF.deepFrom.slice(0, 4) + ' history not loaded — await ensureHistoryFor(start) first'); }
let _DEEP_LOADING = null;
async function ensureDeepHistory(onProgress) {
  if (!SF || SF.deepLoaded) return true;
  if (!_DEEP_LOADING) _DEEP_LOADING = _loadDeep(onProgress).catch(e => { _DEEP_LOADING = null; throw e; });
  return _DEEP_LOADING;
}
async function _loadDeep(onProgress) {
  for (let pi = 1; pi <= SF.deepParts; pi++) {
    onProgress && onProgress('Loading pre-' + SF.deepFrom.slice(0, 4) + ' history (' + pi + '/' + SF.deepParts + ')…');
    const Dp = await _sfPart('sf_deep_' + pi + '.bin', 'sfd' + pi, SF.ver, false);
    for (const sym in Dp.data) {
      const { ser: ds, t: dt, lastClose } = _sfNorm(Dp.data[sym], SF.startTs);
      const rs = SF.series[sym];
      if (!rs) {   // delisted before deepFrom — first sighting of this symbol
        SF.series[sym] = ds; SF.turn[sym] = { d: ds.d, t: dt };
        SF.meta[sym] = _sfMeta(sym, Dp.meta[sym] || {}, lastClose);
        continue;
      }
      rs.d = ds.d.concat(rs.d); rs.p = ds.p.concat(rs.p);
      // Optional per-bar arrays must stay index-aligned with d. Both slices come from one source
      // array so presence always matches; if it ever doesn't, drop the pair (falls back to closes)
      // rather than serve a misaligned window.
      for (const [a, b] of [['h', 'l'], ['hb', 'lb']]) {
        if (ds[a] && rs[a] && ds[b] && rs[b]) { rs[a] = ds[a].concat(rs[a]); rs[b] = ds[b].concat(rs[b]); }
        else if (ds[a] || rs[a] || ds[b] || rs[b]) { delete rs[a]; delete rs[b]; }
      }
      for (const k of ['v', 'dv']) {
        if (ds[k] && rs[k]) rs[k] = ds[k].concat(rs[k]);
        else if (rs[k]) delete rs[k];
      }
      const tu = SF.turn[sym]; tu.d = rs.d; tu.t = dt.concat(tu.t);
    }
  }
  SF.deepLoaded = true; activateSF();
  onProgress && onProgress('');
  return true;
}
async function ensureHistoryFor(dstr, onProgress) { if (needsDeepFor(dstr)) await ensureDeepHistory(onProgress); return true; }
async function loadEngineData(onProgress) {
  onProgress && onProgress('Loading market data…');
  await loadCore();
  onProgress && onProgress('Loading market history — downloads once a day, then cached…');
  await loadSF(); activateSF();
  await loadFund();   // point-in-time quarterly net profit (small file; enables profit factors)
  await loadShp();    // point-in-time FII/DII holdings (small file; enables shareholding factors)
  onProgress && onProgress('');
}

/* ---- SINGLE-STOCK FAST PATH (docs/stock.html) -------------------------------------------------
 * A screen needs the whole market; a company page needs one company. loadEngineData() above pulls
 * ~80 MB (dash_slim 2 MB + the recent sf-data part ~78 MB + fundamentals; deep pre-2019 history
 * only on demand) before it can draw anything, which is why opening a stock used to crawl.
 * scripts/build_stock_slices.py pre-cuts one
 * file per symbol — the same arrays this engine builds in memory, already normalised — so the page
 * loads ~16 KB and every helper below (hl52 / rsi14 / computeTech / retPctAt / priceAt) runs against
 * it unchanged. One shape, one set of maths, no second implementation to keep in sync.
 *
 * Deliberately NOT set here: SF (leaving it null keeps loadSF()'s guard intact, so a page can still
 * fall back to the full engine in the same visit) and IDXH/FNOH (index chips are precomputed into
 * the slice — reading membership was the ONLY reason the page ever fetched the 17 MB stock_data.bin).
 * ---------------------------------------------------------------------------------------------- */
// Production: sf-data is the SAME origin as the site (both dhruvan246.github.io), so this is a
// plain same-origin fetch, no CORS. Serving docs/ locally, point at a locally built ./stk/ instead.
const SLICE_BASE = (location.hostname === 'localhost' || location.hostname === '127.0.0.1' || location.protocol === 'file:')
  ? './stk/' : 'https://dhruvan246.github.io/sf-data/stk/';
// Mirrors slug() in build_stock_slices.py / build_stock_fin.py — 23 symbols carry & or + (M&M, L&T…)
function slugSym(s) { return String(s).replace(/[^A-Za-z0-9._-]/g, '_'); }
async function loadStockSlice(sym) {
  const r = await fetch(SLICE_BASE + slugSym(sym) + '.json');
  if (!r.ok) throw new Error('slice HTTP ' + r.status);
  const S = await r.json();
  if (!S || !Array.isArray(S.p) || !S.p.length || typeof S.d0 !== 'number') throw new Error('slice malformed');
  return S;
}
// Per-stock financials (docs/fin/<SLUG>.json — profit, revenue/margins, shareholding). Separate file
// because results land on their own cadence, hours after the daily price rebuild. Absent = no filings.
async function loadStockFin(sym) {
  try {
    const r = await fetch('./fin/' + slugSym(sym) + '.json');
    if (!r.ok) return null;
    const F = await r.json();
    return (F && typeof F === 'object') ? F : null;
  } catch (e) { return null; }
}
function installStockSlice(S) {
  const sym = S.sym, n = S.p.length;
  const d = new Array(n); d[0] = S.d0;
  const dd = S.dd || []; for (let i = 1; i < n; i++) d[i] = d[i - 1] + dd[i - 1];
  // h/l/v/dv/t cover only the last k bars (nothing on the page looks back past 52 weeks); pad the
  // head so every array stays index-aligned with d. Padding is never null — that would poison the
  // 52-week low — it is whatever means "no intraday range known": high = low = close, or a 0 offset.
  const pad = (a, fill) => { if (!a) return null; const out = new Array(n), off = n - a.length;
    for (let i = 0; i < off; i++) out[i] = fill(i);
    for (let i = 0; i < a.length; i++) out[off + i] = a[i];
    return out; };
  const ser = { d, p: S.p };
  // Same branches loadSF() applies to the whole-market payload, so both paths agree exactly.
  if (S.h && S.l) { ser.h = pad(S.h, i => S.p[i]); ser.l = pad(S.l, i => S.p[i]); }
  else if (S.hb && S.lb) { ser.hb = pad(S.hb, () => 0); ser.lb = pad(S.lb, () => 0); }
  if (S.v)  ser.v  = pad(S.v, () => 0);
  if (S.dv) ser.dv = pad(S.hl ? S.dv : S.dv.map(x => x / 10), () => 0);   // legacy dv is stored x10
  START_TS = S.ts;
  SERIES = {}; SERIES[sym] = ser;
  TURN   = {}; TURN[sym]   = { d, t: pad(S.t, () => 0) || new Array(n).fill(0) };
  META   = {}; META[sym]   = { symbol: sym, name: S.name || sym, industry: S.ind || '', sector: S.ind || '',
                               mcap: 0, latest: S.p[n - 1] / 100, alive: !!S.alive, raw: S.raw != null ? S.raw : null };
  const endOff = Math.floor((Date.parse((S.end || '') + 'T00:00:00Z') / 1000 - S.ts) / DAY);
  SF_END_OFF = isFinite(endOff) ? endOff : d[n - 1];
  CORE_META = {};
  if (S.mcap != null) CORE_META[sym + '.NS'] = { symbol: sym, mcap: S.mcap, latest: S.mcapAt != null ? S.mcapAt : null };
  return sym;
}
function installStockFin(sym, F) {
  FUND = {}; if (F && F.fund) FUND[sym] = F.fund;   // already alias-resolved at build time
}

/* ---- LIVE intraday overlay (optional — used by the 🎯 Live Picks screen) ----------------------
 * Splices ONE synthetic bar carrying today's live LTP onto the end of each stock's price series,
 * so EVERY price factor (52w distance, returns, RSI, stoch, Bollinger…) is computed by the same
 * engine code off the live price — no parallel copy of the maths to keep in sync.
 *
 * Why this exists: screens used to be anchored to SF.end (the last completed bhavcopy). On a Monday
 * in results season that hides both the weekend's filings and every intraday move, so a strategy
 * showed a stale basket. Screening at liveScreenDate() also advances the point-in-time EARNINGS
 * gate (profitAt/profitMetrics compare against the as-of date), which is what surfaces results
 * declared after the last price bar.
 *
 * Deliberate limits: only the LTP is known, so the synthetic bar's high == low == LTP — a 52w high
 * set intraday ABOVE the last print is not seen. Turnover (TURN) is never extended, because a
 * partial-day figure would drag the 20d average down. Volume/delivery carry the previous day
 * forward for the same reason. Fail-silent throughout: an unquoted symbol keeps its last close.
 */
const LIVE_WORKER = 'https://stocksworld-quotes.dhruvan2510.workers.dev';
const LIVE_MAX = 800;          // universe cap — beyond this the quote fan-out isn't worth the wait
const LIVE_BATCH = 30;         // worker caps ?symbols= at 30 per call
const LIVE_CONC = 6;           // batches in flight at once
let LIVE_BAR = { off: null, syms: [], quotes: {}, at: 0 };
function liveQuote(sym) { return LIVE_BAR.quotes[sym] || null; }
function liveInfo() { return { off: LIVE_BAR.off, n: LIVE_BAR.syms.length, at: LIVE_BAR.at }; }
// IST calendar date, independent of the viewer's timezone (toISOString renders UTC; +5:30 shifts it).
function istToday() { return new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10); }
// The date a LIVE screen runs for: today, but never before the last bar we actually hold.
function liveScreenDate() { const t = istToday(), e = (SF && SF.end) || t; return t > e ? t : e; }
// Remove the synthetic bars (idempotent — every re-run clears before it re-fetches).
function clearLiveOverlay() {
  if (LIVE_BAR.off == null) { LIVE_BAR = { off: null, syms: [], quotes: {}, at: 0 }; return; }
  for (const sym of LIVE_BAR.syms) {
    const s = SERIES[sym]; if (!s || !s.d || !s.d.length) continue;
    if (s.d[s.d.length - 1] !== LIVE_BAR.off) continue;      // not ours (data refreshed under us)
    for (const k of ['d', 'p', 'h', 'l', 'hb', 'lb', 'v', 'dv']) if (s[k] && s[k].length) s[k].pop();
  }
  LIVE_BAR = { off: null, syms: [], quotes: {}, at: 0 };
}
async function _liveFetch(chunk, quotes) {
  const r = await fetch(LIVE_WORKER + '/?symbols=' + encodeURIComponent(chunk.join(',')), { cache: 'no-store' });
  const d = await r.json();
  let got = 0;
  for (const k in ((d && d.data) || {})) { const q = d.data[k]; if (q && q.ltp != null) { quotes[k] = q; got++; } }
  return got;
}
// Quote the strategy's universe and splice the live bars in. Returns a summary for the UI.
// Accepts ONE cfg or an ARRAY of them (all-picks.html overlays every saved strategy at once) —
// the quoted set is the union of their universes, so one fan-out serves the whole page.
async function applyLiveOverlay(cfg, onProgress) {
  clearLiveOverlay();
  const date = liveScreenDate(), off = dayOff(date);
  const cfgs = Array.isArray(cfg) ? cfg : [cfg];
  const set = new Set();
  let unbounded = false;
  for (const c of cfgs) {
    const m = (c && c.indexName) ? membersAsOf(c.indexName, date) : null;
    if (!m) { unbounded = true; continue; }
    for (const s of m) if (SERIES[s]) set.add(s);
  }
  const syms = [...set];
  // Unbounded universes (All stocks / turnover floors) would need thousands of quotes — skip the
  // price overlay there, but still screen at today's date so fresh EARNINGS are picked up.
  if (!syms.length || syms.length > LIVE_MAX) return { date, n: 0, quoted: 0, tried: syms.length, skipped: true, unbounded };
  const quotes = {};
  const batches = []; for (let i = 0; i < syms.length; i += LIVE_BATCH) batches.push(syms.slice(i, i + LIVE_BATCH));
  let done = 0;
  for (let i = 0; i < batches.length; i += LIVE_CONC) {
    await Promise.all(batches.slice(i, i + LIVE_CONC).map(b =>
      _liveFetch(b, quotes).catch(() => 0).then(() => {
        done += b.length;
        onProgress && onProgress(`Fetching live prices… ${Math.min(done, syms.length)}/${syms.length}`);
      })));
  }
  // Yahoo drops symbols intermittently (a first pass typically misses ~15%) — re-ask for the gaps in
  // small chunks. Without this, a stock silently keeps Friday's price and can drop out of the basket.
  for (let round = 0; round < 2; round++) {
    const missing = syms.filter(s => !quotes[s]);
    if (!missing.length) break;
    onProgress && onProgress(`Fetching live prices… retrying ${missing.length}`);
    const rb = []; for (let i = 0; i < missing.length; i += 10) rb.push(missing.slice(i, i + 10));
    for (let i = 0; i < rb.length; i += LIVE_CONC) {
      await Promise.all(rb.slice(i, i + LIVE_CONC).map(b => _liveFetch(b, quotes).catch(() => 0)));
    }
  }
  const spliced = [];
  for (const sym in quotes) {
    const px = quotes[sym].ltp; if (!(px > 0)) continue;
    const s = SERIES[sym]; if (!s || !s.d || !s.d.length) continue;
    const last = s.d.length - 1;
    if (s.d[last] >= off) continue;                    // series already covers today — nothing to add
    const cp = Math.round(px * 100);
    s.d.push(off); s.p.push(cp);
    if (s.h) s.h.push(cp);                             // LTP only: no intraday range is available
    if (s.l) s.l.push(cp);
    if (s.hb) s.hb.push(0);
    if (s.lb) s.lb.push(0);
    if (s.v) s.v.push(s.v[last]);                      // carry forward — a part-day figure skews surges
    if (s.dv) s.dv.push(s.dv[last]);
    spliced.push(sym);
  }
  LIVE_BAR = { off, syms: spliced, quotes, at: Date.now() };
  return { date, n: spliced.length, quoted: Object.keys(quotes).length, tried: syms.length, skipped: false, unbounded };
}

/* ---- price / factor helpers ---- */
function dayOff(dstr) { return Math.floor((Date.parse(dstr + 'T00:00:00Z') / 1000 - START_TS) / DAY); }
function isoOff(off) { return new Date((START_TS + off * DAY) * 1000).toISOString().slice(0, 10); }
function idxLE(arr, off) { let lo = 0, hi = arr.length - 1, ans = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (arr[m] <= off) { ans = m; lo = m + 1; } else hi = m - 1; } return ans; }
function priceAt(tkr, off) { const s = SERIES[tkr]; if (!s) return null; const i = idxLE(s.d, off); return i < 0 ? null : s.p[i] / 100; }
function turnoverAt(tkr, off) { const s = TURN[tkr]; if (!s) return 0; const i = idxLE(s.d, off); return i < 0 ? 0 : s.t[i]; }
// held position that stops trading >1 quarter before data end → marked to zero (loss realised)
// Mark a HOLDING's value. A series that ended (delisting/merger/scheme) is carried at its LAST
// TRADED CLOSE, so the position exits at that price on the next rebalance — the same convention
// build_shp_backtest.py has always used ("delisted exits at last close", runbook §22/§86d). It was
// `return 0` ("delisting→0") until 2026-08-11: honest for bankruptcies but a phantom -100% for
// merger absorptions (EICHER→EICHERMOT holders got shares, not zero). Loss up to the last print is
// still fully counted; entries into already-dead series are blocked by factorsAt's freshness gate.
function markPrice(tkr, off) { const s = SERIES[tkr]; if (!s) return null; return priceAt(tkr, off); }
// 52-week high/low over [off-365, off] using TRUE intraday highs/lows when the data
// carries them (hb/lb = per-mil offsets from close); falls back to closes otherwise.
function hl52(tkr, off) {
  const s = SERIES[tkr]; if (!s) return null; const lo = off - 365; let i = idxLE(s.d, off); if (i < 0) return null;
  let hi = -1e18, low = 1e18;
  for (let k = i; k >= 0 && s.d[k] >= lo; k--) {
    const ph = s.h ? s.h[k] : (s.hb ? s.p[k] * (1000 + s.hb[k]) / 1000 : s.p[k]);   // exact high (x100)
    const pl = s.l ? s.l[k] : (s.lb ? s.p[k] * (1000 - s.lb[k]) / 1000 : s.p[k]);   // exact low (x100)
    if (ph > hi) hi = ph; if (pl < low) low = pl;
  }
  return { hi: hi / 100, low: low / 100 };
}
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
// NO floor to list[0]. Before an index's FIRST snapshot its membership is UNKNOWN, and handing
// back the earliest roster backtests the past with the companies that had already won by then —
// a Nifty 50 run at 2005-06-30 was screening the 2015-09-28 roster (+10.2 years of look-ahead
// AND survivorship). Only Nifty 500 escaped, being the one index with snapshots back to 2002.
// Measured 2026-08-12: Nifty Bank @2010 -> 2017 roster (+6.8y), Nifty IT @2010 -> 2016 (+6.2y),
// F&O @pre-2015 -> the 2015-01-30 roster. simulate() now clamps the start date instead.
// (Sync: stock-backtest.html)
function lastSnap(list, dstr) { let best = null; for (const s of list) { if (s.effectiveDate <= dstr && (!best || s.effectiveDate > best.effectiveDate)) best = s; } return best; }
function snapList(name) { return (name === '__FNO__') ? (FNOH || []) : (IDXH[name] || []); }
// Earliest date this universe's membership is KNOWN (null = no history at all for it).
function membershipStart(name) { let m = null; for (const s of snapList(name)) { if (!m || s.effectiveDate < m) m = s.effectiveDate; } return m; }
function membersAsOf(name, dstr) {
  const list = snapList(name);
  if (!list.length) return null;                     // unknown universe -> caller's "no index filter" path
  const snap = lastSnap(list, dstr);
  // Before the first snapshot membership is UNKNOWN — an EMPTY set (screen matches nothing, loudly)
  // is the honest answer. Returning null here would silently mean "every stock on the exchange".
  return snap ? new Set(snap.symbols) : new Set();
}
function maxOffset() { let mx = 0; for (const k in SERIES) { const d = SERIES[k].d; if (d && d.length) { const v = d[d.length - 1]; if (v > mx) mx = v; } } return mx; }
function monthsBetween(start, end) {
  const out = []; let y = +start.slice(0, 4), m = +start.slice(5, 7); const ey = +end.slice(0, 4), em = +end.slice(5, 7);
  while (y < ey || (y === ey && m <= em)) { const last = new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10); out.push(last); m++; if (m > 12) { m = 1; y++; } }
  if (out.length) out[out.length - 1] = end; return out;
}

/* ---- screening (the shared "screen → filter → rank" step) ---- */
/* ---- extended technical factors (close + turnover + Nifty; no fundamentals) ---- */
// SHORT HISTORY IS ALLOWED, exactly as hl52() allows it. The two were inconsistent: hl52 walks
// whatever bars exist inside [off-365, off] — so a stock listed 60 days ago gets a 60-day high and
// a real d52 — while this demanded a bar at exactly off-days and returned null otherwise. Every one
// of the 950 empty ret12m cells on the Nifty-500 matrix was a recent listing (the OLDEST was 363
// days at its missing month, against the 365 required); ret6m the same, oldest 180 against 182.
// Now the base falls back to the FIRST bar in the window, i.e. the listing bar, so a young stock
// reports its return since listing.
// ⚠️ THE CONSEQUENCE, and it is deliberate — user's explicit call 2026-08-12, no minimum span:
// the returned number is then measured over LESS than `days`. MANKIND four months after listing
// contributes a 4-month return to a `ret12m` sort, ranked head-to-head against genuine 12-month
// returns. That is the same compromise hl52 already makes when it calls a 60-day high a "52w high",
// and it is why the 52w band was never flagged as a coverage hole while these were.
function retPctAt(tkr, off, days) {
  const p = priceAt(tkr, off); if (p == null) return null;
  let p0 = priceAt(tkr, off - days);
  if (p0 == null) {                       // listed inside the window — use its first bar
    const s = SERIES[tkr];
    if (!s || !s.d.length || s.d[0] > off) return null;
    p0 = s.p[0] / 100;
  }
  return p0 > 0 ? (p / p0 - 1) * 100 : null;
}
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
  // true share-volume surge + delivery % (from the full bhavcopy fields; null when data absent)
  let volSurge = null, delivPct = null;
  { const s = SERIES[tkr];
    if (s && s.v) { const avg = (days) => { const lo = off - days; let i = idxLE(s.d, off), sum = 0, n = 0; for (let k = i; k >= 0 && s.d[k] >= lo; k--) { sum += s.v[k]; n++; } return n ? sum / n : 0; };
      const v5 = avg(7), v90 = avg(90); volSurge = v90 > 0 ? v5 / v90 : null; }
    if (s && s.dv) { const lo = off - 28; let i = idxLE(s.d, off), sum = 0, n = 0; for (let k = i; k >= 0 && s.d[k] >= lo; k--) { if (s.dv[k] > 0) { sum += s.dv[k]; n++; } } delivPct = n ? sum / n : null; } }
  return {
    ret1m: r1, ret3m: r3, ret6m: r6, ret12m: r12,
    rsNifty: (r6 != null && nr6 != null) ? r6 - nr6 : null,
    accel: (r1 != null && r1p != null) ? r1 - r1p : null,
    dma50: (s50 && px) ? (px / s50 - 1) * 100 : null,
    dma200: (s200 && px) ? (px / s200 - 1) * 100 : null,
    rangePos: (hl && hl.hi > hl.low) ? (px - hl.low) / (hl.hi - hl.low) * 100 : null,
    daysHigh, vol, riskMom: (r3 != null && vol) ? r3 / vol : null, beta, mdd6: mdd,
    upPct: rets90.length ? rets90.filter(x => x > 0).length / rets90.length * 100 : null,
    turnover: turnAvgAt(tkr, off, 20), turnSurge: t90 > 0 ? t5 / t90 : null, volSurge, delivPct, macd, stoch, bollB,
  };
}
// extended factors are EXPENSIVE — only compute them when the strategy actually uses one
const EXT_FIELDS = new Set(['ret1m','ret3m','ret6m','ret12m','rsNifty','accel','dma50','dma200','rangePos','daysHigh','vol','riskMom','beta','mdd6','upPct','turnover','turnSurge','volSurge','delivPct','macd','stoch','bollB','composite']);
function needsTech(cfg) { return EXT_FIELDS.has(cfg.sortBy) || (cfg.filters || []).some(f => EXT_FIELDS.has(f.field)); }

// ---- Fundamentals: point-in-time quarterly net profit (StockView's profitYoyPct/profitBase) ----
// FUND = { SYM: [ [qEndYYYYMMDD, npStd_cr, annStd, npCon_cr, annCon], ... sorted by qEnd ] }
let FUND = {};
// Ticker-rename aliases: pre-rename (point-in-time) symbol -> current symbol that holds the quarterly
// profit data. F&O membership + delisted price series use the name that traded THEN, but fundamentals
// are keyed by the CURRENT name; without this, every renamed stock (TATAMOTORS→TMPV, PVR→PVRINOX,
// CADILAHC→ZYDUSLIFE, …) returns null profit for its old-name era and is silently dropped from any
// profit-based screen. Covers F&O + all index universes. Source: scripts/_rename_map.json. (2026-06-23)
const FUND_ALIAS = {"3IINFOTECH":"3IINFOLTD","4THDIM":"GVPTECH","8KMILES":"SECURKLOUD","A2ZMES":"A2ZINFRA","AARVEEDEN":"VGL","ABANLLOYD":"ABAN","ABANLOYD":"ABAN","ABBALPO":"GVPIL","ABMINTLTD":"ABMINTLLTD","ABSHEKINDS":"TRIDENT","ABSIND":"STYRENIX","ACKRUTI":"HUBTOWN","ACRYSIL":"CARYSIL","ADANIEXPO":"ADANIENT","ADANIGAS":"ATGL","ADANITRANS":"ADANIENSOL","ADHUNIKIND":"INCREDIBLE","ADI":"PRIVISCL","ADLABS":"IMAGICAA","ADLABSFILM":"RELMEDIA","ADORWELD":"ADOR","ADVANIORLI":"ADOR","AEGISCHEM":"AEGISLOG","AFL":"INSPIRISYS","AGCNET":"BBOX","AGRE":"FMNL","AHL":"AFSL","AIL":"GVPIL","AKRUTI":"HUBTOWN","AKZOINDIA":"JSWDULUX","ALLSEC":"ALLDIGI","ALMBICHEM":"ALEMBICLTD","ALOKTEXT":"ALOKINDS","ALSTOMT&D":"GVT&D","AMARAJABAT":"ARE&M","AMDMET":"AMDIND","AMIORG":"ACUTAAS","AMTREXAPPL":"BOSCH-HCIL","ANAMALFIN":"SHIVATEX","ANANDAMRUB":"BALAXI","ANDHRACEMT":"ACL","ANDPAPER":"ANDHRAPAP","ANGELBRKG":"ANGELONE","ANSALINFRA":"ANSALAPI","ANSALPROP":"ANSALAPI","APCOTEXLAT":"APCOTEXIND","APIL":"GVPIL","APOLLOSIND":"BIRLAMONEY","APPAPER":"ANDHRAPAP","AREVA":"GVT&D","AREVAT&D":"GVT&D","ARHAMFISCL":"ADROITINFO","ARISINFRA":"ARIS","ARL":"ARVINDREM","ARROWCOAT":"ARROWGREEN","ARROWGROUP":"DELTACORP","ARVINDMILL":"ARVIND","ARVINFRA":"ARVSMART","ASHCO":"ASHCONIUL","ASIANHOTEL":"ASIANHOTNR","ASTRAIDL":"ASTRAZEN","ATFL":"SUNDROP","ATLANTA":"ATLANTAA","ATULPROD":"ATUL","AVANTI":"AVANTIFEED","AVAYAGCL":"BBOX","AVENTIS":"SANOFI","AXIS-IT&T":"AXISCADES","BAFNAPHARM":"BAFNAPH","BAGYAMETAL":"BHAGYANGR","BAJAJAUTO":"BAJAJHLDNG","BAJAJCORP":"BAJAJCON","BAJAJTEMPO":"FORCEMOT","BAJAUTOFIN":"BAJFINANCE","BALASPONGE":"JAIBALAJI","BANCOPROD":"BANCOINDIA","BARATBIJLE":"BBL","BARBEQUE":"UFBL","BARTRONICS":"ASMS","BELCERAMIC":"BELLCERATL","BFLSOFTWAR":"MPHASIS","BHAGWATIHO":"TGBHOTELS","BHAGYNAGAR":"BHAGYANGR","BHANSALENG":"BEPL","BHARTI":"BHARTIARTL","BHUSANSTL":"TATASTLBSL","BILT":"BALLARPUR","BINANIIND":"BILVYAPAR","BINDALAGRO":"OSWALGREEN","BIRLA3M":"3MINDIA","BIRLAERIC":"BIRLACABLE","BIRLAJUTE":"BIRLACORPN","BOC":"LINDEINDIA","BOROSIL":"BORORENEW","BSES":"RELINFRA","BURGERKING":"RBA","CADILAHC":"ZYDUSLIFE","CAMBRIDGE":"XCHANGING","CAMLIN":"KOKUYOCMLN","CARBEVFLOW":"GRAPHITE","CAREERP":"CPCAP","CASTROL":"CASTROLIND","CEBBCO":"JWL","CENTURYTEX":"ABREL","CESCVENT":"RPSGVENT","CGIGARSH":"IGARASHI","CHEMPLAST":"CHEMPLASTS","CHLORIDIND":"EXIDEIND","CHOLADBS":"CHOLAFIN","CHOLAINV":"CHOLAFIN","CILNOVA":"CNOVAPETRO","CIMCOBIRLA":"CIMMCO","CINEMAX":"CINELINE","CINEPRO":"CINELINE","CLNINDIA":"SUDARCOLOR","COATESIND":"DICIND","COLGATE":"COLPAL","COLORCHEM":"SUDARCOLOR","COLORCHIPS":"ADROITINFO","COMPUAGE":"COMPINFO","COREBIO":"AUSOMENT","COREEMBLG":"AUSOMENT","COREPROTEC":"COREEDUTEC","COROMNFERT":"COROMANDEL","COSMOFILMS":"COSMOFIRST","CPIL":"CPCAP","CREATIVE":"CNL","CROMPGREAV":"CGPOWER","CURATECH":"CURAA","D-LINK":"SMARTLINK","DAAWAT":"LTFOODS","DALMIACEM":"DALMIASUG","DATATECH":"DATAMATICS","DCB":"DCBBANK","DCMSRMCONS":"DCMSHRIRAM","DECCANCEM":"DECCANCE","DEWANHOUS":"PIRAMALFIN","DHANVARSHA":"TRU","DHARAMSI":"DMCC","DHFL":"PIRAMALFIN","DIAPOWER":"DIACABS","DIGJAMLTD":"DIGJAMLMTD","DOLAT":"DOLATALGO","DOLPHINOFF":"DOLPHIN","DPL":"DVL","DPTL":"DVL","DUNCANSIND":"DUNCANSLTD","DYNACONS":"DSSL","DYNASYS":"DSSL","DYNATECH":"DUCON","EBIXFOREX":"DELPHIFX","ELECONENGG":"ELECON","EMAMIINFRA":"EMAMIREAL","EMERCK":"PGHL","ENKEI":"ALICON","EONELECT":"EON","ESL":"ELECTROSL","ESSARSHIP":"ESSARPORTS","ESSELPACK":"EPL","ETERNEVRST":"EVERESTIND","EXCEL":"LANDSMILL","EXCELINFO":"LANDSMILL","FAGBEARING":"SCHAEFFLER","FAGPRECISN":"SCHAEFFLER","FAIRCHEM":"PRIVISCL","FCEL":"FCONSUMER","FINANTECH":"63MOONS","FLEX":"UFLEX","FORTUNEFIN":"THEINVEST","FOURSOFT":"PALREDTEC","FRL":"FEL","FRLDVR":"FELDVR","FUJITSICIM":"ZENSARTECH","FUTUREVENT":"FCONSUMER","GAL":"SHAH","GANESHHOUC":"GANESHHOU","GANHSGFIN":"GANESHHOU","GARWALLROP":"GARFIBRES","GATI":"ACLGATI","GBN":"TV18BRDCST","GECALSTHOM":"GVT&D","GENUSOVERE":"GENUSPOWER","GEOJIT":"GEOJITFSL","GEOJITBNPP":"GEOJITFSL","GEPIL":"GVPIL","GESCOCORP":"MAHLIFE","GESHIPPING":"GESHIP","GET&D":"GVT&D","GISOLUTION":"TPHQ","GKW":"GKWLIMITED","GLOBALTELE":"GTL","GLS":"ALIVUS","GMRINFRA":"GMRAIRPORT","GOETZEIND":"FMGOETZE","GOLDINFRA":"OLECTRA","GOLDTELE":"OLECTRA","GOODLASNER":"KANSAINER","GPELECT":"DELTAMAGNT","GRAMOPHONE":"SAREGAMA","GREAVES":"GREAVESCOT","GREENFIRE":"EQUIPPP","GSSAMERICA":"GSS","GTCIND":"GOLDENTOBC","GUJAMBCEM":"AMBUJACEM","GUJAPOLEQP":"GUJAPOLLO","GUJAPOLIND":"GUJAPOLLO","GUJFLUORO":"GFLLIMITED","GUJGASLTD":"GUJENERGY","GUJSIDHCEM":"GSCLCEMENT","GULFCORP":"GOCLCORP","GULFOILCOR":"GOCLCORP","GWALCHEM":"GEECEE","HBLPOWER":"HBLENGINE","HBSTOCK":"HBSL","HCIL":"HSCL","HCL-HP":"HCL-INSYS","HEROHONDA":"HEROMOTOCO","HEUBACHIND":"SUDARCOLOR","HEXAWARE":"HEXT","HGSL":"HGS","HIL":"BIRLANU","HIMACHLFUT":"HFCL","HIMADCHEM":"HSCL","HINDALC0":"HINDALCO","HINDCONS":"HCC","HINDLEVER":"HINDUNILVR","HINDMOTOR":"HINDMOTORS","HINDSANIT":"AGI","HINDUJAFIN":"NDLVENTURE","HINDUJATMT":"NDLVENTURE","HINDUJAVEN":"NDLVENTURE","HITACHIHOM":"BOSCH-HCIL","HITECHPLAS":"HITECHCORP","HOECHST":"SANOFI","HOPFL":"PGIL","HOTELEELA":"HLVLTD","HOVS":"HGM","HSIL":"AGI","HTMT":"NDLVENTURE","HTMTGLOBAL":"HGS","HUGHESTELE":"TTML","HYDRBADIND":"BIRLANU","HYDROS&S":"KINGFA","I-FLEX":"OFSS","IBIPL":"RTNINDIA","IBN18":"TV18BRDCST","IBPOW":"RTNPOWER","IBREALEST":"EMBDL","IBULHSGFIN":"SAMMAANCAP","IBULISL":"IBULLSLTD","IBWSL":"IBULLSLTD","ICI":"JSWDULUX","IDFCBANK":"IDFCFIRSTB","IDLCHEM":"GOCLCORP","IGPETRO":"IGPL","IIFLSEC":"IIFLCAPS","IIFLWAM":"360ONE","IILTD":"INSECTICID","ILFSTRANS":"IL&FSTRANS","INDBANKMER":"INDBANK","INDIAINFO":"IIFL","INDOCOUNT":"ICIL","INDOSOLAR":"WAAREEINDO","INDRAYON":"ABIRLANUVO","INDSHAVING":"GILLETTE","INEABS":"STYRENIX","INEOSSTYRO":"STYRENIX","INFIBEAM":"CCAVENUE","INFOSYSTCH":"INFY","INFOTECENT":"CYIENT","INFRATEL":"INDUSTOWER","INTEGRA":"ESSENTIA","INVESTCORP":"TATAINVEST","IOL":"LINDEINDIA","IPAPPM":"ANDHRAPAP","ISPATIND":"JSWISPAT","IT&T":"AXISCADES","ITCAGRO":"SUNDROP","ITDCEM":"CEMPRO","JBMAUTOCOM":"JBMA","JBMT":"ASAL","JCHAC":"BOSCH-HCIL","JDORGO":"JDORGOCHEM","JINDALSWHL":"JSWHL","JKINDUSTRY":"JKTYRE","JPHYDRO":"JPPOWER","JSTAINLESS":"JSL","JSTRIPS":"NSIL","JSWSTL":"JSWSTEEL","JTLINFRA":"JTLIND","JUBILANT":"JUBLPHARMA","KALECONSUL":"ACCELYA","KALPATPOWR":"KPIL","KALSTEELS":"KSL","KALYANISTL":"KSL","KARDA":"DHARAN","KAVVERITEL":"KAVDEFENCE","KBCGLOBAL":"DHARAN","KBL":"KIRLOSBROS","KESARENTER":"KESARENT","KEYCORPSER":"KEYFINSERV","KIL":"KAMDHENU","KIRELECTRI":"KECL","KIRIDYES":"KIRIINDUS","KIRLOSKCUM":"CUMMINSIND","KIRLOSOIL":"KIRLOSIND","KOTAKMAH":"KOTAKBANK","KPIGLOBAL":"KPIGREEN","KPIT":"BSOFT","KRITIIND":"KRITI","KSBPUMPS":"KSB","L&T":"LT","L&TFH":"LTF","LAKME":"TRENT","LAKSHMIFIN":"LFIC","LANABS":"STYRENIX","LAXMIMACH":"LMW","LEADEDSYS":"TRIGYN","LGBROS":"LGBBROSLTD","LINCPEN":"LINC","LOGIXMICRO":"IZMO","LSIL":"LLOYDSENGG","LTI":"LTM","LTIM":"LTM","LYCOS":"BCG","LYPSAGEMS":"AURUS","MACMILLAN":"MPSLTD","MADRASCEM":"RAMCOCEM","MADRASREFN":"CHENNPETRO","MAGMA":"POONAWALLA","MAHAVIRSPG":"VTL","MAHINDCIE":"CIEINDIA","MAHINDFORG":"CIEINDIA","MAJESCO":"AURUM","MANALIPET":"MANALIPETC","MANALU":"MAANALU","MANALUMN":"MANINDS","MANDHANA":"GBGLOBAL","MARICOIND":"MARICO","MAX":"MFSL","MAXWELL":"VIPCLOTHNG","MAYTASINFR":"IL&FSENGG","MBSWITCH":"UEL","MCDOWELL-N":"UNITDSPR","MEDIAVIDEO":"NOESISIND","MEGASOFT":"SIGMAADV","MERCK":"PGHL","MFL":"EPIGRAL","MIC":"MICEL","MICO":"BOSCHLTD","MICROSEC":"HEALTHX","MID-DAY":"NEXTMEDIA","MINDAIND":"UNOMINDA","MIRCELECTR":"ONIDA","MIRZATAN":"MIRZAINT","MITTALLAMI":"GREENPLY","MLL":"MERCATOR","MMFORGINGS":"MMFL","MMFSL":"CGCL","MODILUFT":"SPICEJET","MODISNME":"MODISONLTD","MONNETISPA":"JSWISPL","MORAREALTY":"PENINLAND","MORARJESPG":"PENINLAND","MORARJETEX":"MORARJEE","MOREPENHTL":"BLUECOAST","MOTHERSUMI":"MOTHERSON","MPGLYCEM":"ANIKINDS","MPHASISBFL":"MPHASIS","MRO-TEK":"UMIYA-MRO","MSKPROJ":"WELENT","MUDRA":"ELAND","MUNDRAPORT":"ADANIPORTS","MYSORECEM":"HEIDELBERG","NAGAAGRI":"NACLIND","NAGARCONST":"NCC","NAGPURENG":"JAYNECOIND","NAGREEKA":"NAGREEKEXP","NAGREKAEXP":"NAGREEKEXP","NAHAREXP":"NAHARPOLY","NAHAREXPOT":"NAHARPOLY","NAHARINVST":"NAHARPOLY","NAHARSPG":"NAHARSPING","NANDAN":"NDL","NANDANEXIM":"NDL","NAVBARFERO":"NAVA","NAVNETPUBL":"NAVNETEDUL","NBVENTURES":"NAVA","NEOCURE":"COUNCODOS","NEOCURTHER":"COUNCODOS","NEYVELILIG":"NLCINDIA","NGCT":"SPCENET","NICCOCORP":"NICCO","NICOLASPIR":"PEL","NIITTECH":"COFORGE","NILKAMPLST":"NILKAMAL","NIRVIKARA":"BALKRISHNA","NISSAN":"NCOPPER","NORTHGATE":"EQUIPPP","NOVAPETRO":"GSLNOVA","NTL":"NEUEON","NUMERICPW":"SWELECTES","NXTDIGITAL":"NDLVENTURE","OBERHOTEL":"EIHAHOTELS","OBEROIREAL":"OBEROIRLTY","OMMETALS":"OMINFRAL","ORCHIDCHEM":"ORCHPHARMA","ORCHIDPHAR":"ORCHPHARMA","ORIENTABRA":"ORIENTCER","ORIENTCERA":"ORIENTBELL","ORIENTREF":"RHIM","ORTINLAB":"ORTINGLOBE","ORTINLABSS":"ORTINGLOBE","OTL":"ORIENTALTL","P&G":"PGHH","PALRED":"PALREDTEC","PALREDTECH":"PALREDTEC","PANTALOONR":"FEL","PAPERPROD":"HUHTAMAKI","PDSMFL":"PDSL","PDUMJEAGRO":"3PLAND","PDUMJEIND":"3PLAND","PDUMJEPULP":"AMJLAND","PEACOCKIND":"PILITA","PEERLESHIP":"SEAMECLTD","PERIATEA":"PKTEA","PERLENGINF":"SAMBHAAV","PFRL":"ABFRL","PHILIPCARB":"PCBL","PILANIINV":"PILANIINVS","PILIND":"PILITA","PIPAVAVDOC":"SWANDEF","PIPAVAVYD":"SWANDEF","PIRHEALTH":"PEL","PITTILAM":"PITTIENG","PONDYOXIDE":"POCL","PRAKASHCON":"SETUINFRA","PRECOTMILL":"PRECOT","PREMAUTO":"PREMIER","PRETAILDVR":"FELDVR","PRISMCEM":"PRSMJOHNSN","PRIYADCEM":"RAIN","PROINDIA":"EQUIPPP","PROSEED":"EQUIPPP","PROVOGUE":"PROVOGE","PROZONE":"PROZONER","PROZONECSC":"PROZONER","PROZONINTU":"PROZONER","PVR":"PVRINOX","RAINCOM":"RAIN","RAJASSPG":"RSWM","RAJOIL":"ROML","RAJRAYON":"RAJRILTD","RAMADHOTEL":"ADVANIHOTR","RANEMADRAS":"RANEHOLDIN","RCVL":"RCOM","RDEL":"SWANDEF","REIAGRO":"REIAGROLTD","REL":"RELINFRA","RESURGERE":"RMMIL","REVATHI":"SEMAC","REVATHIEQU":"RVTH","RJL":"RGL","RNAM":"NAM-INDIA","RNAVAL":"SWANDEF","ROMAN":"TARMAT","RTNINFRA":"RTNINDIA","RUCHISOYA":"PATANJALI","SABTN":"AQYLON","SABTNL":"AQYLON","SAGARCEM":"SAGCEM","SAH":"AERONEU","SAHPETRO":"GULFPETRO","SALONACOT":"SALONA","SASTASUNDR":"HEALTHX","SATINDLTD":"AEROENTER","SATNAMOVER":"KOHINOOR","SAVITACHEM":"SOTL","SAWPIPES":"JINDALSAW","SCANDENT":"XCHANGING","SCAPDVR":"GATECHDVR","SEINV":"PAISALO","SEINVEST":"PAISALO","SEJALGLASS":"SEJALLTD","SELAN":"ANTELOPUS","SELMCL":"SELMC","SENTINEL":"STEL","SEPOWER":"SAMPANN","SEQUENT":"VIYASH","SESAGOA":"VEDL","SEZAL":"SEJALLTD","SEZALGLASS":"SEJALLTD","SGLTL":"SETL","SHAKTIGAS":"HAVISHA","SHALMPAINT":"SHALPAINTS","SHARRESLTD":"CREST","SHARYANRES":"CREST","SHIL":"HINDWAREAP","SHIVTEX":"SHIVATEX","SHREE":"AJMERA","SHREYAS":"TRANSWORLD","SHREYASHIP":"TRANSWORLD","SHRIRAMEPC":"SEPC","SHRMHONDA":"HONDAPOWER","SICAL":"SICALLOG","SIGNET":"SIGIND","SIGNETIND":"SIGIND","SIMPLEXCON":"SIMPLEXINF","SITICABLE":"SITINET","SIYARSILK":"SIYSIL","SKFBEARING":"SKFINDIA","SKSMICRO":"BHARATFIN","SMLISUZU":"SMLMAH","SMOBILITY":"DIGISPICE","SMSLIFE":"HALEOSLABS","SOFTPRO":"CURAA","SOFTSOLINT":"PVP","SOLAREX":"SOLARINDS","SOLECTCENT":"CENTUM","SONAMCLOCK":"SONAMLTD","SONASTEER":"JTEKTINDIA","SORILHOLD":"IBULLSLTD","SPENTEX":"CLCIND","SPENTXIND":"CLCIND","SPHEREGSL":"ADROITINFO","SPICEMOBI":"DIGISPICE","SPICEMOBIL":"DIGISPICE","SPLLTD":"SOMANYCERA","SPYL":"SHEKHAWATI","SQSBFSI":"EXPLEOSOL","SREINTFIN":"SREINFRA","SRIADIKARI":"AQYLON","SRICHAKTYR":"TVSSRICHAK","SRTRANSFIN":"SHRIRAMFIN","SSLT":"VEDL","STAMPEDE":"GATECH","STROPTICAL":"STLTECH","STRTECH":"STLTECH","STYABS":"STYRENIX","SUBEX":"SUBEXLTD","SUBHASPROJ":"SPMLINFRA","SUJANATOW":"NEUEON","SUNCLAYLTD":"TVSHLTD","SUNCLAYTON":"TVSHLTD","SUNDARMHLD":"TSFINV","SUNDRMCLAY":"TVSHLTD","SUNFLAGIRN":"SUNFLAG","SUPPETRO":"SPLPETRO","SURANATELE":"SURANAT&P","SURANAVEL":"SURANASOL","SUTLEJINDS":"SILINV","SUVENPHAR":"COHANCE","SWANENERGY":"SWANCORP","SWARAJMAZD":"SMLMAH","TANTIACONS":"TICL","TASCPHARMA":"MARKSANS","TATAGLOBAL":"TATACONSUM","TATAHONEY":"HONAUT","TATAMOTORS":"TMPV","TATASPONGE":"TATASTLLP","TATATEA":"TATACONSUM","TATATELECM":"BBOX","TATATELSER":"TTML","TATATIMKEN":"TIMKEN","TCLCONS":"TICL","TELCO":"TMPV","TEXMACOLTD":"TEXINFRA","THEBYKE":"BYKE","THINKSOFT":"EXPLEOSOL","TIDEWATER":"VEEDOL","TIFIN":"CHOLAHLDNG","TIMESGTY":"TEAMGTY","TIPSINDLTD":"TIPSMUSIC","TISCO":"TATASTEEL","TITANOCOMP":"DENORA","TMRVL":"HEADSUP","TRAFALHSE":"CEMPRO","TRIL":"TARIL","TTKHEALTH":"TTKHLTCARE","TUBEINVEST":"CHOLAHLDNG","TV18":"TV-18","TVS-SUZUKI":"TVSMOTOR","TWL":"TITAGARH","UCALFUEL":"UCAL","UJAAS":"UEL","UNIPHOS":"UPL","URAVI":"URAVIDEF","USHABEL":"USHAMART","UTIBANK":"AXISBANK","UTTAMVALUE":"UVSL","VAIBHAVGEM":"VAIBHAVGBL","VAKRANSOFT":"VAKRANGEE","VAMORGANIC":"JUBLPHARMA","VATECH":"WABAG","VICEROY":"VHLTD","VIDHIDYE":"VIDHIING","VIKASGLOB":"VIKASECO","VIKASMCORP":"VIKASLIFE","VINYLCHEM":"VINYLINDIA","VIPUL":"VIPULLTD","VISASTEEL":"VISACHROME","VISHALRET":"V2RETAIL","VSGML":"VHL","VSNL":"TATACOMM","WABCO-TVS":"ZFCVINDIA","WABCOINDIA":"ZFCVINDIA","WEIZFOREX":"DELPHIFX","WELGUJ":"WELCORP","WELPROJ":"WELENT","WELSPUNIND":"WELSPUNLIV","WELSYNTEX":"AYMSYNTEX","WENDTINDIA":"WENDT","WESTHATCH":"VENKEYS","WFL":"WEL","WIDIA":"KENNAMET","WINSOMYARN":"WINSOME","WORTH":"WORTHPERI","WSIND":"WSI","WWIL":"SITINET","YAARI":"IBULLSLTD","YAARII":"IBULLSLTD","ZEENEWS":"ZEEMEDIA","ZEETELE":"ZEEL","ZENITHBIR":"ZENITHSTL","ZOMATO":"ETERNAL","ZUARIAGRO":"ZUARIIND","ZUARIGLOB":"ZUARIIND"};
function fundFor(sym) { return FUND[sym] || (FUND_ALIAS[sym] ? FUND[FUND_ALIAS[sym]] : null) || null; }
const FUND_FIELDS = new Set(['profitYoyPct', 'profitBase', 'profitAccel', 'profitTTM', 'profitStreak', 'postDrift', 'composite']);
function needsFund(cfg) { return FUND_FIELDS.has(cfg.sortBy) || (cfg.filters || []).some(f => FUND_FIELDS.has(f.field)); }
function dateIntOff(off) { return parseInt(isoOff(off).replace(/-/g, ''), 10); }
function profitAt(sym, dateInt, basis) {
  const arr = fundFor(sym); if (!arr || !arr.length) return null;
  // 'conOnly' reads the consolidated slots with NO standalone fallback. It is a MEASUREMENT
  // basis for the coverage matrix (scripts/build_coverage_matrix.js) — no UI offers it and no
  // saved strategy can carry it, so 'con' (blend) and 'std' behaviour is untouched.
  const tries = basis === 'std' ? [[1, 2]] : basis === 'conOnly' ? [[3, 4]] : [[3, 4], [1, 2]];   // con falls back to std
  for (const [npIdx, annIdx] of tries) {
    let cur = null;
    // ⚠️ ann must be TRUTHY, not merely non-null: 0 is the "announce date UNKNOWN" sentinel (runbook
    // §15/§91) and `0 != null` is TRUE in JS, so `!= null` admitted it as "announced at time zero" —
    // making a quarter years in the FUTURE visible at every past rebalance. (Sync: stock-backtest.html)
    for (let i = arr.length - 1; i >= 0; i--) { const q = arr[i]; if (q[npIdx] != null && q[annIdx] > 0 && q[annIdx] <= dateInt) { cur = q; break; } }
    if (!cur) continue;
    const baseEnd = cur[0] - 10000; let base = null;
    for (const q of arr) { if (q[0] === baseEnd && q[npIdx] != null) { base = q; break; } }
    if (!base) continue;
    const b = base[npIdx], c = cur[npIdx];
    // YoY% for EVERY stock with a non-zero base — tiny (₹0.01cr) and negative/loss bases included.
    // Divide by |base| so loss→profit reads positive. Null only when base is exactly 0 (÷0).
    // NOTE: tiny bases give extreme % (₹0.01cr→+18000%); add a current-profit filter to tame it.
    return { cur: c, base: b, yoy: b !== 0 ? (c - b) / Math.abs(b) * 100 : null };
  }
  return null;
}
// Richer point-in-time earnings metrics from the same quarterly net-profit series (no extra data):
// acceleration (YoY trend), TTM growth (4Q vs prior 4Q), consecutive +YoY streak, last result date.
// Mirrors stock-backtest.html's profitMetrics so saved strategies rank identically across pages.
function profitMetrics(sym, dateInt, basis) {
  const arr = fundFor(sym); if (!arr || !arr.length) return null;
  // ⚠️ THE BASIS FALLBACK USED TO BE DECIDED BY THE ENTRY TEST ALONE, and that cost 1,144 of the
  // 1,744 empty profitTTM cells on the Nifty-500 coverage matrix (2020-01..2026-06). `tries`
  // declares con->std, but the loop committed to a basis the moment its CURRENT quarter was
  // announced and its YoY resolved, then returned whatever that basis happened to produce. ttm
  // needs EIGHT quarters and accel needs two: one hole anywhere in the consolidated window
  // returned null while the standalone window sat complete in the same array. APOLLOHOSP at
  // 2020-01-31 is the worked example — con is missing only Mar-2018, std holds all eight.
  // What this does NOT do is switch basis wholesale. The consolidated result is kept for
  // everything it could answer (yoy, base, streak, resultDate) and standalone fills ONLY the
  // specific fields con left null. (Sync: stock-backtest.html profitMetrics.)
  // ★ MEASURED COST, recorded so nobody has to rediscover it: over the 36,891 stock-months where
  // BOTH bases resolve, std and con TTM growth agree in SIGN only 86.9% of the time (median gap
  // 6.9 growth points, p90 125.5). So ~1 in 8 substituted cells will pass a `profitTTM > 0`
  // filter that consolidated would have failed. On the user's own 12 affected tracked strategies
  // this moved CAGR by -10.4pp to +1.4pp over 2020-01..2026-06; 18 unaffected strategies moved by
  // exactly zero. Adopted 2026-08-12 on the user's explicit call, after that measurement.
  let _pmPending = null;
  // 'conOnly' = consolidated slots, no std fill-in (coverage measurement only — see profitAt).
  // With a single [[3,4]] try the partial-pending path still behaves: a con pass that answered
  // yoy but not ttm/accel stashes _pmPending, the loop ends, and the partial con result is
  // returned — exactly "pure con": the missing fields stay null instead of borrowing std.
  const tries = basis === 'std' ? [[1, 2]] : basis === 'conOnly' ? [[3, 4]] : [[3, 4], [1, 2]];
  for (const [ni, ai] of tries) {
    // ann > 0, not != null — see profitAt above (0 = date-unknown sentinel, runbook §15/§91).
    let ci = -1; for (let i = arr.length - 1; i >= 0; i--) { if (arr[i][ni] != null && arr[i][ai] > 0 && arr[i][ai] <= dateInt) { ci = i; break; } }
    if (ci < 0) continue;
    const npAt = qe => { const q = arr.find(x => x[0] === qe); return (q && q[ni] != null) ? q[ni] : null; };
    const yoyOf = q => { const c = q[ni], b = npAt(q[0] - 10000); return (c != null && b != null && b !== 0) ? (c - b) / Math.abs(b) * 100 : null; };
    const cur = arr[ci], yoy = yoyOf(cur);
    if (yoy == null) continue;
    const base = npAt(cur[0] - 10000);
    let accel = null; if (ci - 1 >= 0) { const py = yoyOf(arr[ci - 1]); if (py != null) accel = yoy - py; }
    let ttm = null; { let ok = true, last4 = [], prev4 = [];
      for (let k = 0; k < 4; k++) { const q = arr[ci - k]; const v = q ? q[ni] : null; if (v == null) { ok = false; break; } last4.push(v); }
      // last4 is 4 ARRAY-adjacent rows, not 4 CALENDAR-adjacent quarters — a quarter our data never
      // captured is silently stepped over, so the "4Q vs prior 4Q" window can quietly widen past 12
      // months (external cross-validation, 2026-08-20, 24 trades: e.g. TCS 2011-01 summed
      // 2008-09-30..2010-12-31, not a true trailing year). Quarter-ends are quantized to Mar/Jun/Sep/Dec,
      // so 4 TRUE consecutive quarters are always exactly 9 months apart oldest-to-newest; reject wider.
      if (ok) { const monthIdx = d => Math.floor(d / 10000) * 12 + Math.floor(d / 100) % 100;
        if (monthIdx(arr[ci][0]) - monthIdx(arr[ci - 3][0]) > 10) ok = false; }
      if (ok) for (let k = 0; k < 4; k++) { const v = npAt(arr[ci - k][0] - 10000); if (v == null) { ok = false; break; } prev4.push(v); }
      if (ok) { const s1 = last4.reduce((a, b) => a + b, 0), s0 = prev4.reduce((a, b) => a + b, 0); ttm = s0 !== 0 ? (s1 - s0) / Math.abs(s0) * 100 : null; } }
    let streak = 0; for (let i = ci; i >= 0; i--) { const y = yoyOf(arr[i]); if (y != null && y > 0) streak++; else break; }
    // con answered, but not completely -> stash it and let std try. `ni === 3` gates this to the
    // consolidated pass, so a std-only request can never fall through to anything.
    if ((ttm == null || accel == null) && ni === 3) { _pmPending = { yoy, base, accel, ttm, streak, resultDate: cur[ai] }; continue; }
    if (_pmPending) { const p = _pmPending; _pmPending = null;
      // con wins every field it filled; std supplies only the ones con left null
      return { yoy: p.yoy, base: p.base, streak: p.streak, resultDate: p.resultDate,
               accel: p.accel != null ? p.accel : accel,
               ttm: p.ttm != null ? p.ttm : ttm }; }
    return { yoy, base, accel, ttm, streak, resultDate: cur[ai] };
  }
  return _pmPending;   // con answered partially and std could not be reached — keep the con result
}
// postDrift = (price / priceAt(lastResultDate) - 1) * 100 — it consumes ONLY the announce date,
// never the YoY. profitMetrics() bails on `yoy == null`, which threw resultDate away with it, so a
// stock whose year-ago quarter we simply don't hold was dropped from a postDrift screen for a
// reason the factor does not depend on (12,801 screen-rows, runbook §91g). Resolved separately here;
// profitYoyPct/accel/TTM/streak and their basis-fallback keep the old behaviour exactly.
// Takes the LATEST real announce date across the bases in play, not the first basis that has one:
// the earnings EVENT is basis-agnostic (one filing), and the two bases print different dates on
// 3.5% of rows / 7.6% of symbols (measured 2026-08-12) where con-first would return a stale date.
// ann > 0, not != null — 0 is the date-unknown sentinel (§15/§91c). (Sync: stock-backtest.html)
function lastResultDate(sym, dateInt, basis) {
  const arr = fundFor(sym); if (!arr || !arr.length) return null;
  // 'conOnly': consolidated announce dates only (coverage measurement only — see profitAt)
  const tries = basis === 'std' ? [[1, 2]] : basis === 'conOnly' ? [[3, 4]] : [[3, 4], [1, 2]];
  let best = null;
  for (const [ni, ai] of tries) {
    for (let i = arr.length - 1; i >= 0; i--) {
      const q = arr[i];
      if (q[ni] != null && q[ai] > 0 && q[ai] <= dateInt) { if (best == null || q[ai] > best) best = q[ai]; break; }
    }
  }
  return best;
}
async function loadFund() {
  if (Object.keys(FUND).length) return;
  try { FUND = await (await fetch('./sf_fundamentals.json')).json(); } catch (e) { console.warn('no fundamentals data', e); FUND = {}; }
}

// ---- Shareholding pattern: point-in-time FII/DII holding % (quarterly SHP filings) ----
// SHPD = { SYM: [ [qEndYYYYMMDD, fii%, dii%, subYYYYMMDD], ... sorted by qEnd ] } — sub is the
// filing's ACTUAL submission date; a quarter is only visible once filed (no look-ahead).
// Keep in sync with stock-backtest.html (self-contained copy). Data: docs/shp_engine.json (runbook §22d).
let SHPD = {};
const SHP_FIELDS = new Set(['fiiPct', 'fiiChgPp', 'diiPct', 'diiChgPp']);
function needsShp(cfg) { return SHP_FIELDS.has(cfg.sortBy) || (cfg.filters || []).some(f => SHP_FIELDS.has(f.field)); }
async function loadShp() {
  if (Object.keys(SHPD).length) return;
  try { SHPD = await (await fetch('./shp_engine.json')).json(); } catch (e) { console.warn('no shareholding data', e); SHPD = {}; return; }
  // merge renamed tickers' eras under the CURRENT key (filings were made under the old name then)
  for (const old in FUND_ALIAS) {
    if (!SHPD[old]) continue;
    const nw = FUND_ALIAS[old], by = {};
    (SHPD[nw] || []).concat(SHPD[old]).forEach(q => { if (!by[q[0]] || q[3] > by[q[0]][3]) by[q[0]] = q; });
    SHPD[nw] = Object.values(by).sort((a, b) => a[0] - b[0]);
  }
}
function prevQeInt(qe) { let y = Math.floor(qe / 10000), m = Math.floor(qe / 100) % 100 - 3; if (m <= 0) { y--; m += 12; } return y * 10000 + m * 100 + { 3: 31, 6: 30, 9: 30, 12: 31 }[m]; }
// A row dated anything other than a March/June/September/December quarter end is an EVENT-driven
// SHP (capital change / SAST) merged in by §22k — same shape, but no calendar-previous quarter.
function isQuarterEnd(d) { return ({ 3: 31, 6: 30, 9: 30, 12: 31 }[Math.floor(d / 100) % 100] || 0) === d % 100; }
function shpAt(sym, dateInt) {
  const arr = SHPD[sym] || (FUND_ALIAS[sym] ? SHPD[FUND_ALIAS[sym]] : null); if (!arr || !arr.length) return null;
  let ci = -1; for (let i = arr.length - 1; i >= 0; i--) { if (arr[i][3] <= dateInt) { ci = i; break; } }
  if (ci < 0) return null;
  const cur = arr[ci];
  // QoQ change vs the CALENDAR-previous quarter only (gaps break it), and NEVER across the
  // Sep-2022 SEBI format change (DR blocks were reclassified into FII/DII — not a stake change).
  let dfii = null, ddii = null;
  if (cur[0] !== 20220930) {
    if (isQuarterEnd(cur[0])) {
      const pq = prevQeInt(cur[0]);
      for (let i = ci - 1; i >= 0; i--) { const q = arr[i]; if (q[0] === pq) { if (q[3] <= dateInt) { dfii = +(cur[1] - q[1]).toFixed(2); ddii = +(cur[2] - q[2]).toFixed(2); } break; } if (q[0] < pq) break; }
    } else {
      // EVENT row (mid-quarter as-on date, §22k): it has no calendar-previous quarter, so the
      // QoQ walk above would leave the deltas null — and a null factor is FILTERED OUT of the
      // screen, silently dropping the stock from every diiChgPp/fiiChgPp strategy in exactly the
      // month its stake actually moved. Measure the change against the latest ALREADY-VISIBLE
      // reading instead, which is what "change since we last knew" means for an event filing.
      for (let i = ci - 1; i >= 0; i--) { const q = arr[i]; if (q[3] <= dateInt) { dfii = +(cur[1] - q[1]).toFixed(2); ddii = +(cur[2] - q[2]).toFixed(2); break; } }
    }
  }
  return { fii: cur[1], dii: cur[2], dfii, ddii };
}

function factorsAt(off, cfg) {
  const lookOff = off - 30;   // fixed 1-month window for r.chg (industry-momentum rank + legacy changePercent strategies)
  const members = cfg.indexName ? membersAsOf(cfg.indexName, isoOff(off)) : null;
  const useTech = needsTech(cfg);
  const useFund = needsFund(cfg); const fundDate = useFund ? dateIntOff(off) : 0; const basis = cfg.earnBasis || 'con';
  const useShp = needsShp(cfg); const shpDate = useShp ? dateIntOff(off) : 0;
  // Entry-freshness gate: a symbol with no bar in the last 14 days (28 in the pre-2002 weekly-bar
  // era) is not tradeable at this screen date. priceAt() carries the last bar forward forever, so
  // without this a screen "buys" a series that already died — a renamed/delisted symbol's stale
  // print (MUNJALAUTO entered 2006-06-30 at a close 45 days old, then marked to 0 = phantom -100%).
  // Suspended stocks are excluded only while suspended (couldn't be bought anyway) and re-admitted
  // once bars resume. Daily-era gaps >14d are 0.044% of all bar-transitions (measured 2026-08-11).
  const maxBarAge = off >= dayOff('2002-01-02') ? 14 : 28;   // (Sync: stock-backtest.html)
  const rows = [];
  for (const tkr in SERIES) {
    const m = META[tkr]; if (!m) continue;
    if (m.symbol.includes('DVR')) continue;   // skip differential-voting-rights shares (TATAMTRDVR, JISLDVREQS, …) — a secondary security of a company already in the universe via its ordinary share; avoids double-counting. StockView dedups the same way (members_on drops the DVR, keeps TMPV). The DVR stays in indicesHistory (it WAS a real constituent) but is excluded from screening. (Sync: stock-backtest.html)
    if (members && !members.has(m.symbol)) continue;
    const s = SERIES[tkr]; const li = (s && s.d && s.d.length) ? idxLE(s.d, off) : -1;
    if (li < 0 || off - s.d[li] > maxBarAge) continue;
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
    if (useFund) { const pf = profitMetrics(m.symbol, fundDate, basis);
      r.profitYoyPct = pf ? pf.yoy : null; r.profitBase = pf ? pf.base : null;
      r.profitAccel = pf ? pf.accel : null; r.profitTTM = pf ? pf.ttm : null; r.profitStreak = pf ? pf.streak : null;
      // postDrift resolves its own result date — NOT pf.resultDate, which is null whenever the
      // year-ago quarter is missing even though the drift itself never uses it (§91g).
      const rd = lastResultDate(m.symbol, fundDate, basis);
      if (rd) { const ds = '' + rd, ro = dayOff(ds.slice(0, 4) + '-' + ds.slice(4, 6) + '-' + ds.slice(6, 8)); const pr = priceAt(tkr, ro); r.postDrift = (pr != null && pr > 0) ? (price / pr - 1) * 100 : null; } else r.postDrift = null; }
    if (useShp) { const sh = shpAt(m.symbol, shpDate);
      r.fiiPct = sh ? sh.fii : null; r.fiiChgPp = sh ? sh.dfii : null;
      r.diiPct = sh ? sh.dii : null; r.diiChgPp = sh ? sh.ddii : null; }
    rows.push(r);
  }
  const byInd = {}; rows.forEach(r => { (byInd[r.ind] = byInd[r.ind] || []).push(r.chg); });
  const indAvg = Object.entries(byInd).map(([k, v]) => [k, v.reduce((a, b) => a + b, 0) / v.length]).sort((a, b) => b[1] - a[1]);
  const indRankMap = {}; indAvg.forEach(([k], i) => { indRankMap[k] = Math.min(10, 1 + Math.floor(i / Math.max(1, indAvg.length / 10))); });
  rows.forEach(r => { r.indRank = indRankMap[r.ind] || 10; });
  // Quality-Momentum composite: cross-sectional z-scores of TTM profit growth (+), 12m return (+), volatility (−).
  if (cfg.sortBy === 'composite' || (cfg.filters || []).some(f => f.field === 'composite')) {
    const zAssign = (key, sign) => { const v = rows.map(r => r[key]).filter(x => typeof x === 'number' && isFinite(x));
      if (v.length < 5) { rows.forEach(r => r['_z_' + key] = null); return; }
      const mean = v.reduce((a, b) => a + b, 0) / v.length, sd = Math.sqrt(v.reduce((a, b) => a + (b - mean) ** 2, 0) / v.length) || 1;
      rows.forEach(r => { const x = r[key]; r['_z_' + key] = (typeof x === 'number' && isFinite(x)) ? sign * (x - mean) / sd : null; }); };
    zAssign('profitTTM', 1); zAssign('ret12m', 1); zAssign('vol', -1);
    rows.forEach(r => { const a = r['_z_profitTTM'], b = r['_z_ret12m'], c = r['_z_vol']; r.composite = (a != null && b != null && c != null) ? (a + b + c) : null; });
  }
  return rows;
}
function fieldVal(r, f) {
  switch (f) { case 'changePercent': return r.chg; case 'rsi': return r.rsi; case 'd52': return r.d52; case 'd52_low_pct': return r.d52low; case 'indRank': return r.indRank; case 'mcap': return r.mcap; case 'hist_mcap': return r.histMcap; case 'profitYoyPct': return r.profitYoyPct; case 'profitBase': return r.profitBase; }
  return (f in r && typeof r[f] === 'number') ? r[f] : null;   // extended tech factors stored under their own key
}
function passFilters(r, filters) {
  for (const f of (filters || [])) { let x = fieldVal(r, f.field); if (x == null) return false;
    if (typeof x === 'number') x = Math.round(x * 1e6) / 1e6;   // kill sub-1e-6 float noise so exact-boundary ties (e.g. 10.0000000002 -> 10) pass <=/>= filters (keep in sync with stock-backtest.html)
    if (!(f.op === '>' ? x > f.val : f.op === '>=' ? x >= f.val : f.op === '<' ? x < f.val : f.op === '<=' ? x <= f.val : x === f.val)) return false; }
  return true;
}
// ranked candidate list as of a date (top cfg.topN = picks, the rest = "also qualifying")
function screenAsOf(cfg, dateStr) {
  _histGuard(dateStr);   // a screen at a pre-2020 date needs the deep parts loaded first
  const off = dayOff(dateStr);
  let rows = factorsAt(off, cfg).filter(r => r.rsi != null && passFilters(r, cfg.filters));
  rows = rows.filter(r => fieldVal(r, cfg.sortBy) != null);   // can't rank on a missing factor (e.g. no earnings yet)
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
// buy top-N once at `start`, hold unchanged to `end` (equal-weight; a delisted holding stays valued at its last traded close)
function computeHold(cfg, start, end, capital) {
  _histGuard(start);
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
  // Membership clamp: a universe cannot be screened before its first snapshot (see lastSnap).
  // NEVER mutate cfg — bt-identity.js fingerprints it, so changing cfg.start here would fork a
  // saved strategy's identity and duplicate it. Use a local effective start instead.
  const _msStart = cfg.indexName ? membershipStart(cfg.indexName) : null;
  const _start = (_msStart && cfg.start < _msStart) ? _msStart : cfg.start;
  const membershipNote = (_start !== cfg.start)
    ? (cfg.indexName === '__FNO__' ? 'F&O' : cfg.indexName) + ' membership is only known from ' +
      _msStart + ' — backtest starts there, not ' + cfg.start + '.'
    : null;
  if (membershipNote) console.warn('[backtest]', membershipNote);
  _histGuard(_start);   // LOUD failure beats silently backtesting years with no bars loaded
  const months = monthsBetween(_start, cfg.end);
  // Rebalance on the LAST TRADING DAY <= the calendar month-end (a month-end can fall on a weekend/holiday).
  // Standard/NSE 52w hi/lo = trailing 365d from the last TRADING day; anchoring the window on the raw calendar
  // date while pricing off the last trading day can drop a genuine 52w low just outside the calendar window.
  // Snap so price AND the 52w window share one trading-day anchor. (Keep in sync with stock-backtest.html.)
  const _tdset = new Set();
  for (const _r of ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'ITC', 'SBIN', 'LT']) { const _s = SERIES[_r]; if (_s && _s.d) for (const _o of _s.d) _tdset.add(_o); }
  const _td = [..._tdset].sort((a, b) => a - b);
  const snapTD = o => { if (!_td.length) return o; const i = idxLE(_td, o); return i < 0 ? o : _td[i]; };
  const N = cfg.topN;
  let pos = {}, cash = 0, started = false; const equity = [], rebs = [], trades = []; let entryInfo = {}, lastRebVal = cfg.capital, monthsSinceReb = 1e9, latest = [], latestCash = 0;
  const mark = off => { let v = cash; for (const t in pos) { const p = markPrice(t, off); if (p != null) v += pos[t] * p; } return v; };
  const SHORT = { changePercent: 'Chg%', rsi: 'RSI', d52: '52wHi%', d52_low_pct: '52wLo%', indRank: 'IndRank', mcap: 'Mcap', hist_mcap: 'HMcap', profitYoyPct: 'NP-YoY%', profitBase: 'NP-base' };
  const fLabel = SHORT[cfg.sortBy] || FIELD_LABEL[cfg.sortBy] || cfg.sortBy;
  // columns to surface in the rebalance log = the sort field + every active filter field (deduped, in order)
  const dispFields = [...new Set([cfg.sortBy, ...(cfg.filters || []).map(f => f.field)])];
  const dispCols = dispFields.map(f => ({ field: f, label: SHORT[f] || FIELD_LABEL[f] || f }));
  for (let mi = 0; mi < months.length; mi++) {
    const md = months[mi], off = snapTD(dayOff(md));
    const mv = started ? mark(off) : cfg.capital;
    equity.push([md, mv]);
    monthsSinceReb++;
    const isReb = (mi === 0) || (monthsSinceReb >= cfg.freq);
    if (isReb) {
      let rows = factorsAt(off, cfg).filter(r => r.rsi != null && passFilters(r, cfg.filters));
      rows = rows.filter(r => fieldVal(r, cfg.sortBy) != null);   // can't rank a stock on a factor it's missing (keep in sync with stock-backtest.html)
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
        // each EMPTY SLOT (N − winners kept) gets an equal share of ALL available cash; entries fill
        // slots, unfilled slots keep their cash share. per-slot (not per-exit) — fixes over-concentration.
        const openSlots = Math.max(1, N - Object.keys(pos).length);
        let avail = proceeds + cash; const perSlot = avail / openSlots;
        for (const e of entries) { if (perSlot <= 1) break; pos[e.tkr] = perSlot / e.price; avail -= perSlot; }
        cash = Math.max(0, avail);
      }
      for (const r of target) { if (wasEntry.has(r.tkr)) { entryInfo[r.tkr] = { date: md, price: r.price, factor: fieldVal(r, cfg.sortBy), rsi: r.rsi, chg: r.chg }; } }
      const now = mark(off); latestCash = cash;
      const holds = Object.keys(pos).map(t => { const p = markPrice(t, off), v = p ? pos[t] * p : 0, r = tmap[t] || {};
        return { sym: META[t].symbol, ind: (META[t].industry || META[t].sector || 'Other'), wt: now ? v / now * 100 : 0, val: v,
                 isNew: wasEntry.has(t), factor: fieldVal(r, cfg.sortBy), rsi: r.rsi, chg: r.chg, d52: r.d52, mcap: r.mcap,
                 fv: Object.fromEntries(dispFields.map(f => [f, fieldVal(r, f)])) }; }).sort((a, b) => b.wt - a.wt);
      rebs.push({ date: md, val: now, ret: started ? (mv / (lastRebVal || 1) - 1) * 100 : 0, cash, cashWt: now ? cash / now * 100 : 0, nNew: wasEntry.size, holds });
      lastRebVal = mv > 0 ? mv : cfg.capital; monthsSinceReb = 0;
      latest = holds.slice(); latest._cashWt = now ? cash / now * 100 : 0; latest._cash = cash;
    }
  }
  { const lastOff = dayOff(cfg.end); for (const t in entryInfo) { const e = entryInfo[t]; const mp = markPrice(t, lastOff); const xp = (mp == null ? e.price : mp);
    trades.push({ sym: META[t].symbol, entryDate: e.date, exitDate: cfg.end, entryPrice: e.price, exitPrice: xp, retPct: (xp / e.price - 1) * 100, factor: e.factor, rsi: e.rsi, held: true }); } }
  // benchmark (Nifty 50) + Nifty 500 (headline benchmark — the backtest universe); keep in sync with stock-backtest.html
  const bench = []; const startN = nearestNifty(_start);
  if (startN) for (const [d] of equity) { const nv = nearestNifty(d); bench.push([d, nv ? cfg.capital * nv / startN : null]); }
  const bench500 = []; const startN5 = nearestIdx(NIFTY500, _start);
  if (startN5) for (const [d] of equity) { const nv = nearestIdx(NIFTY500, d); bench500.push([d, nv ? cfg.capital * nv / startN5 : null]); }
  const years = (Date.parse(cfg.end) - Date.parse(_start)) / (365.25 * 864e5);   // CAGR over the period actually simulated
  const finalV = equity[equity.length - 1][1];
  const cagr = years > 0 ? (Math.pow(finalV / cfg.capital, 1 / years) - 1) * 100 : 0;
  const benchSeries = (bench500.length ? bench500 : bench);
  const benchFinal = benchSeries.length ? benchSeries[benchSeries.length - 1][1] : null;
  const benchCagr = (benchFinal && years > 0) ? (Math.pow(benchFinal / cfg.capital, 1 / years) - 1) * 100 : null;
  const rets = []; for (let i = 1; i < equity.length; i++) { if (equity[i - 1][1] > 0) rets.push(equity[i][1] / equity[i - 1][1] - 1); }
  const mean = rets.reduce((a, b) => a + b, 0) / (rets.length || 1);
  const vol = Math.sqrt(rets.reduce((a, b) => a + (b - mean) ** 2, 0) / (rets.length || 1) * 12) * 100;
  const periodRebs = rebs.slice(1); const wins = periodRebs.filter(r => r.ret > 0).length;
  trades.sort((a, b) => a.entryDate < b.entryDate ? 1 : -1);
  return { equity, bench, bench500, rebs, trades, latest, latestCash, cfg, years, finalV, cagr, benchCagr, vol, fLabel, dispCols,
           effStart: _start, membershipNote,
           maxDD: maxDrawdown(equity), winRate: periodRebs.length ? 100 * wins / periodRebs.length : 0 };
}
function nearestIdx(map, dstr) { if (map[dstr]) return map[dstr]; let d = new Date(dstr + 'T00:00:00Z'); for (let i = 0; i < 7; i++) { d.setUTCDate(d.getUTCDate() - 1); const k = d.toISOString().slice(0, 10); if (map[k]) return map[k]; } return null; }
function nearestNifty(dstr) { return nearestIdx(NIFTY, dstr); }
function maxDrawdown(eq) { let peak = -1, mdd = 0; for (const [, v] of eq) { if (v > peak) peak = v; else if (peak > 0) { const dd = (peak - v) / peak * 100; if (dd > mdd) mdd = dd; } } return mdd; }

/* ---- config labels + localStorage ---- */
// Short field labels for the filter fingerprint in strategyLabel() below — distinct from the
// longer FIELD_LABEL used in dropdowns/tables. Keep in sync with FIELDS (each page's own copy).
const SHORT_FIELD = { changePercent: 'Chg%', rsi: 'RSI', d52: '52wHi%', d52_low_pct: '52wLo%', indRank: 'IndRank', mcap: 'Mcap', hist_mcap: 'HMcap',
  profitYoyPct: 'NP-YoY%', profitBase: 'NP-base', fiiPct: 'FII%', fiiChgPp: 'FII Δpp', diiPct: 'DII%', diiChgPp: 'DII Δpp',
  ret1m: 'Ret-1m%', ret3m: 'Ret-3m%', ret6m: 'Ret-6m%', ret12m: 'Ret-12m%', accel: 'MomAccel%', dma50: '50DMA%', dma200: '200DMA%',
  rangePos: 'RangePos', daysHigh: 'DaysSinceHi', vol: 'Vol%', riskMom: 'RiskMom', beta: 'Beta', mdd6: 'MDD-6m%', upPct: 'UpDay%',
  turnover: 'Turnover', turnSurge: 'TurnSurge', volSurge: 'VolSurge', delivPct: 'Deliv%', macd: 'MACD', stoch: 'Stoch%K', bollB: 'BollB',
  profitAccel: 'ProfitAccel', profitTTM: 'ProfitTTM%', profitStreak: 'ProfitStreak', postDrift: 'PostDrift%', composite: 'QM-Composite' };
// Filters are what make two strategies with the same sort/topN/universe/frequency actually
// different — a bare "N filters" count collides whenever the count matches too, so the label
// spells out each filter (short field + op + val) instead of just counting them.
function filterFingerprint(filters) {
  return (filters || []).map(f => `${SHORT_FIELD[f.field] || FIELD_LABEL[f.field] || f.field} ${f.op} ${f.val}`).join(', ');
}
function strategyLabel(c) {
  const S = { changePercent: 'Momentum', rsi: 'RSI', d52: '52w-High', d52_low_pct: '52w-Low', indRank: 'Industry-rank', mcap: 'Mcap', hist_mcap: 'Hist-mcap', profitYoyPct: 'Profit-growth', profitBase: 'Profit-base' };
  const F = { 1: 'Monthly', 3: 'Quarterly', 6: 'Half-yearly', 12: 'Yearly' }[c.freq] || c.freq + 'mo';
  const uni = c.indexName ? String(c.indexName).replace('__FNO__', 'F&O') : c.mcapFloor ? '≥₹' + (+c.mcapFloor).toLocaleString('en-IN') + 'L turnover' : 'All stocks';
  const fp = filterFingerprint(c.filters);
  return `${S[c.sortBy] || FIELD_LABEL[c.sortBy] || c.sortBy} ${c.dir === 'high' ? 'top' : 'bottom'}-${c.topN} · ${uni} · ${F}${fp ? ' · ' + fp : ''}`;
}
function universeLabel(c) { return c.indexName ? String(c.indexName).replace('__FNO__', 'F&O Stocks') : c.mcapFloor ? '≥₹' + (+c.mcapFloor).toLocaleString('en-IN') + 'L turnover' : 'All stocks'; }
function freqLabel(c) { return { 1: 'Monthly', 3: 'Quarterly', 6: 'Half-yearly', 12: 'Yearly' }[c.freq] || c.freq + 'mo'; }
function filterExpr(c) {
  const L = { changePercent: 'Change %', rsi: 'RSI', d52: '% from 52w High', d52_low_pct: '% from 52w Low', indRank: 'Industry rank', mcap: 'Mcap', hist_mcap: 'Hist mcap' };
  return (c.filters || []).map(f => `${L[f.field] || FIELD_LABEL[f.field] || f.field} ${f.op} ${f.val}`).join(' AND ');
}
function loadLS(k) { try { return JSON.parse(localStorage.getItem(k) || '[]'); } catch (e) { return []; } }
function saveLS(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }
