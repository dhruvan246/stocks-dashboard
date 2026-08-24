/* =============================================================================
 * STOCKSWORLD — live data proxy (Cloudflare Worker)
 *
 * WHY THIS EXISTS
 *   The dashboard is a static GitHub Pages site, so it can't fetch live data
 *   itself: quote/announcement endpoints block cross-origin browser calls
 *   (CORS). This tiny Worker runs on Cloudflare's free tier, fetches
 *   server-side, and returns the data to the page with CORS enabled.
 *
 * ROUTES
 *   GET ?symbols=RELIANCE,TCS      -> live quotes (Yahoo NSE feed, ~15 min delayed)
 *        {"asOf":<ms>,"source":"yahoo-nse","data":{"RELIANCE":{"ltp":..,"prevClose":..},...}}
 *        Index symbols work too (no .NS appended): ?symbols=^NSEI,^CRSLDX,^NSEBANK,^INDIAVIX
 *   GET ?chart=^NSEI               -> verbatim Yahoo intraday chart JSON (1m/1d) for ONE
 *        symbol — used by the home-page ticker (price + prevClose + sparkline series).
 *        Whitelisted passthrough (only the Yahoo chart endpoint), cached 30 s per symbol.
 *   GET ?quotes=^GSPC,GC=F,BTC-USD -> live quotes for VERBATIM Yahoo symbols (no .NS
 *        appended — supports futures GC=F, FX EURUSD=X, crypto BTC-USD, any index).
 *        Same response shape as ?symbols=. Used by the Global Markets page.
 *        Cached 30 s per symbol set. Cap 40 symbols per call.
 *   GET ?announcements=1           -> today+yesterday NSE corporate announcements
 *        {"asOf":<ms>,"source":"nse","rows":[[symbol,company,"YYYY-MM-DD HH:MM:SS",
 *          subject,caption,file],...]}   (same row shape as docs/announcements.json;
 *        `file` has the nsearchives /corporate/ prefix stripped)
 *        Cached in the Worker for 90 s — many visitors still mean ~1 NSE call/min.
 *   GET ?nse=volume-gainers        -> whitelisted NSE live-analysis passthrough with
 *        cookie warmup, cached 60 s per key. Keys: volume-gainers (live volume
 *        spurts, used by the Volume Shockers page), gainers / loosers (live top
 *        movers incl. the allSec whole-market bucket, used by the Top Movers page),
 *        fiidii (day's provisional FII/DII cash numbers, ~6pm — FII/DII page),
 *        large-deals (today's bulk/block deals snapshot, evening — Deals page).
 *        Response = NSE's own JSON + {asOf}; array responses ride under .data
 *        (volume-gainers data capped at 60 rows).
 *   GET ?ipo=CMLL                  -> live subscription for ONE open IPO (NSE
 *        ipo-active-category): {asOf,symbol,updateTime,total,rows} where total =
 *        overall subscription multiple. Used by the IPOs page. Cached 60 s.
 *   GET ?gift=1                    -> live GIFT NIFTY (Nifty futures at NSE IX,
 *        Yahoo doesn't carry it): {asOf,price,prevClose,change,pchg,expiry,ts,
 *        series} from www.nseix.com — most-traded NIFTY futures contract, plus
 *        a ~40-point downsample of the exchange's intraday tick graph for the
 *        home-page sparkline. Quote cached 30 s; the tick graph is ~1.2 MB so
 *        it's refetched at most every 5 min. Used by the home-page ticker.
 *
 * DEPLOY:  see scripts/LIVE_FEED_SETUP.md  (paste this whole file over the old one)
 * ========================================================================== */

const CORS = {
  'Access-Control-Allow-Origin': '*',          // tighten to your Pages URL if you like
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': '*',
};

const NSE_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
const PDF_PREFIX = 'https://nsearchives.nseindia.com/corporate/';
const MON = { jan:1, feb:2, mar:3, apr:4, may:5, jun:6, jul:7, aug:8, sep:9, oct:10, nov:11, dec:12 };
let ANN_CACHE = { ts: 0, body: null };         // per-isolate cache, 90 s
const CHART_CACHE = new Map();                 // sym -> { ts, text }, 30 s
const QUOTE_CACHE = new Map();                 // symbol-set -> { ts, text }, 30 s
const NSE_CACHE = new Map();                   // nse-key -> { ts, text }, 60 s
let GIFT_CACHE = { ts: 0, text: null };        // ?gift=1 quote envelope, 30 s
let GIFT_GRAPH = { ts: 0, series: null };      // downsampled intraday graph, 5 min

// Whitelisted NSE live-analysis endpoints (key -> [path, referer]). Only these
// can be proxied — never a caller-supplied path.
const NSE_LIVE = {
  'volume-gainers': ['/api/live-analysis-volume-gainers',
                     'https://www.nseindia.com/market-data/volume-gainers-spurts'],
  'gainers':        ['/api/live-analysis-variations?index=gainers',
                     'https://www.nseindia.com/market-data/top-gainers-losers'],
  'loosers':        ['/api/live-analysis-variations?index=loosers',
                     'https://www.nseindia.com/market-data/top-gainers-losers'],
  'fiidii':         ['/api/fiidiiTradeReact',
                     'https://www.nseindia.com/reports/fii-dii'],
  'large-deals':    ['/api/snapshot-capital-market-largedeal',
                     'https://www.nseindia.com/market-data/large-deals'],
};

export default {
  async fetch(request) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });

    const url = new URL(request.url);
    if (url.searchParams.get('announcements')) return announcements();
    const chart = url.searchParams.get('chart');
    if (chart) return chartPassthrough(chart);
    const quotes = url.searchParams.get('quotes');
    if (quotes) return yahooQuotes(quotes);
    const nse = url.searchParams.get('nse');
    if (nse) return nseLive(nse);
    const ipo = url.searchParams.get('ipo');
    if (ipo) return ipoSubscription(ipo);
    if (url.searchParams.get('gift')) return giftNifty();
    const filings = url.searchParams.get('filings');
    if (filings) return filingsPassthrough(url, filings);
    const pdf = url.searchParams.get('pdf');
    if (pdf) return pdfPassthrough(pdf);

    const symbols = (url.searchParams.get('symbols') || '')
      .split(',').map(s => s.trim().toUpperCase()).filter(Boolean).slice(0, 30); // cap per call

    if (!symbols.length) return json({ error: 'pass ?symbols=RELIANCE,TCS or ?announcements=1' }, 400);

    const data = {};
    await Promise.all(symbols.map(async sym => {
      try {
        const ysym = sym.startsWith('^') ? sym : sym + '.NS';   // indices (^NSEI…) have no .NS suffix
        const r = await fetch(
          `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ysym)}?interval=1d&range=1d`,
          { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' } }
        );
        if (!r.ok) return;
        const d = await r.json();
        const m = d && d.chart && d.chart.result && d.chart.result[0] && d.chart.result[0].meta;
        if (m && m.regularMarketPrice != null) {
          data[sym] = {
            ltp: m.regularMarketPrice,
            prevClose: m.chartPreviousClose != null ? m.chartPreviousClose : (m.previousClose != null ? m.previousClose : null),
          };
        }
      } catch (e) { /* skip this symbol */ }
    }));

    return json({ asOf: Date.now(), source: 'yahoo-nse', data });
  },
};

/* ------- live quotes for VERBATIM Yahoo symbols (Global Markets page) ------ */

async function yahooQuotes(csv) {
  const syms = [...new Set(String(csv).split(',').map(s => s.trim().toUpperCase())
    .filter(s => /^[\^]?[A-Z0-9.\-&=]{1,20}$/.test(s)))].slice(0, 40);
  if (!syms.length) return json({ error: 'pass ?quotes=^GSPC,GC=F,BTC-USD' }, 400);

  const key = syms.join(',');
  const now = Date.now();
  const hit = QUOTE_CACHE.get(key);
  if (hit && now - hit.ts < 30_000) return new Response(hit.text, { headers: { ...CORS, 'content-type': 'application/json' } });

  const data = {};
  await Promise.all(syms.map(async sym => {
    try {
      const r = await fetch(
        `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=1d`,
        { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' } }
      );
      if (!r.ok) return;
      const d = await r.json();
      const m = d && d.chart && d.chart.result && d.chart.result[0] && d.chart.result[0].meta;
      if (m && m.regularMarketPrice != null) {
        data[sym] = {
          ltp: m.regularMarketPrice,
          prevClose: m.chartPreviousClose != null ? m.chartPreviousClose : (m.previousClose != null ? m.previousClose : null),
        };
      }
    } catch (e) { /* skip this symbol */ }
  }));

  const text = JSON.stringify({ asOf: now, source: 'yahoo', data });
  QUOTE_CACHE.set(key, { ts: now, text });
  if (QUOTE_CACHE.size > 20) QUOTE_CACHE.delete(QUOTE_CACHE.keys().next().value);
  return new Response(text, { headers: { ...CORS, 'content-type': 'application/json' } });
}

/* ------- verbatim Yahoo intraday chart for ONE symbol (home-page ticker) --- */

async function chartPassthrough(sym) {
  sym = String(sym).trim().toUpperCase();
  if (!/^[\^]?[A-Z0-9.\-&]{1,20}$/.test(sym)) return json({ error: 'bad symbol' }, 400);
  const now = Date.now();
  const hit = CHART_CACHE.get(sym);
  if (hit && now - hit.ts < 30_000) return new Response(hit.text, { headers: { ...CORS, 'content-type': 'application/json' } });
  const r = await fetch(
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1m&range=1d`,
    { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' } }
  );
  if (!r.ok) return json({ error: 'yahoo HTTP ' + r.status }, 502);
  const text = await r.text();
  CHART_CACHE.set(sym, { ts: now, text });
  if (CHART_CACHE.size > 50) CHART_CACHE.delete(CHART_CACHE.keys().next().value);
  return new Response(text, { headers: { ...CORS, 'content-type': 'application/json' } });
}

/* ---- NSE cookie warmup: visit the homepage like a browser, keep its cookies --- */

async function nseCookie() {
  const home = await fetch('https://www.nseindia.com/', {
    headers: { 'User-Agent': NSE_UA, 'Accept': 'text/html,application/xhtml+xml' },
    redirect: 'follow',
  });
  return cookieHeader(home);
}

/* ---------------- whitelisted NSE live-analysis passthrough ---------------- */

async function nseLive(key) {
  const cfg = NSE_LIVE[key];
  if (!cfg) return json({ error: 'unknown ?nse= key — one of: ' + Object.keys(NSE_LIVE).join(', ') }, 400);
  const now = Date.now();
  const hit = NSE_CACHE.get(key);
  if (hit && now - hit.ts < 60_000) return new Response(hit.text, { headers: { ...CORS, 'content-type': 'application/json' } });
  try {
    const cookie = await nseCookie();
    const r = await fetch('https://www.nseindia.com' + cfg[0], {
      headers: {
        'User-Agent': NSE_UA,
        'Accept': 'application/json, text/plain, */*',
        'Referer': cfg[1],
        ...(cookie ? { 'Cookie': cookie } : {}),
      },
    });
    if (!r.ok) return json({ error: 'NSE HTTP ' + r.status }, 502);
    const j = await r.json();
    if (j && Array.isArray(j.data) && j.data.length > 60) j.data = j.data.slice(0, 60); // volume-gainers: cap payload
    // array responses (fiidii) ride under .data so the envelope stays an object
    const body = Array.isArray(j) ? { asOf: now, source: 'nse', data: j } : { asOf: now, source: 'nse', ...j };
    const text = JSON.stringify(body);
    NSE_CACHE.set(key, { ts: now, text });
    return new Response(text, { headers: { ...CORS, 'content-type': 'application/json' } });
  } catch (e) {
    return json({ error: String((e && e.message) || e) }, 502);
  }
}

/* ------- whitelisted NSE integrated-filings passthrough (CI numbers fetch) -------
 * 2026-07-20: NSE's Akamai serves its bot-challenge page to the integrated-filing-results
 * API for datacenter IPs (GitHub runners AND curl_cffi Chrome-TLS) — while Cloudflare's
 * edge still passes. update_fundamentals.py falls back to this route when blocked.
 * STRICT: fixed path, validated params only — never a caller-supplied URL. */

async function filingsPassthrough(url, idx) {
  if (!/^(equities|sme)$/.test(idx)) return json({ error: 'filings must be equities|sme' }, 400);
  const from = url.searchParams.get('from') || '', to = url.searchParams.get('to') || '';
  if (!/^\d{2}-\d{2}-\d{4}$/.test(from) || !/^\d{2}-\d{2}-\d{4}$/.test(to))
    return json({ error: 'from/to must be DD-MM-YYYY' }, 400);
  const page = Math.min(Math.max(parseInt(url.searchParams.get('page') || '1', 10) || 1, 1), 200);
  const size = Math.min(Math.max(parseInt(url.searchParams.get('size') || '200', 10) || 200, 1), 200);
  try {
    const cookie = await nseCookie();
    const r = await fetch(
      `https://www.nseindia.com/api/integrated-filing-results?index=${idx}&period=Quarterly` +
      `&from_date=${from}&to_date=${to}&page=${page}&size=${size}`,
      { headers: {
          'User-Agent': NSE_UA,
          'Accept': 'application/json, text/plain, */*',
          'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-financial-results',
          ...(cookie ? { 'Cookie': cookie } : {}),
        } }
    );
    if (!r.ok) return json({ error: 'NSE HTTP ' + r.status }, 502);
    const text = await r.text();
    if (/^\s*</.test(text)) return json({ error: 'NSE served non-JSON (challenge page)' }, 502);
    return new Response(text, { headers: { ...CORS, 'content-type': 'application/json' } });
  } catch (e) {
    return json({ error: String((e && e.message) || e) }, 502);
  }
}

/* ------- NSE archive PDF relay (vision routine's blocked-fetch fallback) -------
 * 2026-07-21: nsearchives.nseindia.com hard-403s every scripted transport we have
 * (GitHub runners, local python, even curl_cffi Chrome-TLS from a residential IP)
 * while Cloudflare's edge still passes NSE — so filing PDFs for NSE-only names
 * with no BSE copy (GFSTEELS etc.) were unfetchable and sat "numbers being
 * parsed" until XBRL. bse_vision_prep._nse_pdf_with_retry falls back to this
 * route. STRICT: bare archive filenames only (no slashes, no URLs) — the route
 * can only reach nsearchives.nseindia.com/corporate/, never act as an open proxy. */

async function pdfPassthrough(file) {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.\-]{0,150}\.pdf$/i.test(file) || file.includes('..'))
    return json({ error: 'pdf must be a bare nsearchives corporate filename' }, 400);
  try {
    const cookie = await nseCookie();
    const r = await fetch('https://nsearchives.nseindia.com/corporate/' + file, {
      headers: {
        'User-Agent': NSE_UA,
        'Accept': 'application/pdf,*/*',
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-announcements',
        ...(cookie ? { 'Cookie': cookie } : {}),
      },
    });
    if (!r.ok) return json({ error: 'NSE HTTP ' + r.status }, 502);
    const buf = await r.arrayBuffer();
    if (buf.byteLength < 8 || new Uint8Array(buf, 0, 1)[0] === 0x3c)   // '<' = challenge/error page
      return json({ error: 'NSE served non-PDF (challenge page)' }, 502);
    return new Response(buf, { headers: { ...CORS, 'content-type': 'application/pdf',
                                          'cache-control': 'public, max-age=86400' } });
  } catch (e) {
    return json({ error: String((e && e.message) || e) }, 502);
  }
}

/* ---------------- live IPO subscription for ONE open issue ----------------- */

async function ipoSubscription(sym) {
  sym = String(sym).trim().toUpperCase();
  if (!/^[A-Z0-9\-&]{1,20}$/.test(sym)) return json({ error: 'bad symbol' }, 400);
  const key = 'ipo:' + sym;
  const now = Date.now();
  const hit = NSE_CACHE.get(key);
  if (hit && now - hit.ts < 60_000) return new Response(hit.text, { headers: { ...CORS, 'content-type': 'application/json' } });
  try {
    const cookie = await nseCookie();
    const r = await fetch('https://www.nseindia.com/api/ipo-active-category?symbol=' + encodeURIComponent(sym), {
      headers: {
        'User-Agent': NSE_UA,
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nseindia.com/market-data/all-upcoming-issues-ipo',
        ...(cookie ? { 'Cookie': cookie } : {}),
      },
    });
    if (!r.ok) return json({ error: 'NSE HTTP ' + r.status }, 502);
    const j = await r.json();
    // dataList row 0 is a header row; the Total row has srNo == null
    const rows = ((j && j.dataList) || []).filter(x => x && x.category && x.srNo !== 'Sr.No.');
    const total = rows.find(x => x.srNo == null || /^total$/i.test(x.category));
    const text = JSON.stringify({
      asOf: now, source: 'nse', symbol: sym,
      updateTime: (j && j.updateTime) || null,
      total: total ? parseFloat(total.noOfTotalMeant) : null,
      rows: rows.map(x => [x.category, x.noOfShareOffered, x.noOfSharesBid, x.noOfTotalMeant]),
    });
    NSE_CACHE.set(key, { ts: now, text });
    return new Response(text, { headers: { ...CORS, 'content-type': 'application/json' } });
  } catch (e) {
    return json({ error: String((e && e.message) || e) }, 502);
  }
}

/* ---------------- live GIFT NIFTY from NSE IX (home-page ticker) -----------
 * Yahoo has no GIFT Nifty symbol (verified 2026-08-25: search + every candidate
 * chart symbol 404), and www.nseix.com sends no CORS headers — so the page
 * can't fetch it itself. Quote = the most-traded NIFTY futures contract on the
 * exchange's own derivatives-watch API (rollover then picks the new front month
 * automatically). CLOSE on that row is the previous close — LASTPRICE-CLOSE
 * reproduces the row's own CHANGE field exactly (checked live). */

const NSEIX_HDRS = {
  'User-Agent': NSE_UA,
  'Accept': 'application/json, text/plain, */*',
  'Referer': 'https://www.nseix.com/',
};

async function giftNifty() {
  const now = Date.now();
  if (GIFT_CACHE.text && now - GIFT_CACHE.ts < 30_000)
    return new Response(GIFT_CACHE.text, { headers: { ...CORS, 'content-type': 'application/json' } });
  try {
    const r = await fetch('https://www.nseix.com/api/derivatives-watch?inst_type1=IDX&type=live',
                          { headers: NSEIX_HDRS });
    if (!r.ok) return json({ error: 'NSE IX HTTP ' + r.status }, 502);
    const j = await r.json();
    const futs = ((j && j.data) || []).filter(x =>
      x && x.SYMBOL === 'NIFTY' && x.INSTRUMENTTYPE === 'FUTIDX' && x.LASTPRICE != null);
    if (!futs.length) return json({ error: 'no NIFTY futures row in NSE IX response' }, 502);
    futs.sort((a, b) => (b.CONTRACTSTRADED || 0) - (a.CONTRACTSTRADED || 0));
    const f = futs[0];

    // sparkline: the exchange's intraday tick graph is ~1.2 MB / ~50k points —
    // refetch at most every 5 min and keep an even 40-point downsample
    if (!GIFT_GRAPH.series || now - GIFT_GRAPH.ts > 300_000) {
      try {
        const g = await (await fetch('https://www.nseix.com/api/deep-intraday-graph',
                                     { headers: NSEIX_HDRS })).json();
        const pts = [];
        for (const s of (g && g.data) || [])
          for (const p of (s && s.data) || [])
            if (p && p[1] != null) pts.push(p[1]);
        if (pts.length > 1) {
          const N = 40, out = [];
          for (let i = 0; i < N; i++) out.push(pts[Math.round(i * (pts.length - 1) / (N - 1))]);
          GIFT_GRAPH = { ts: now, series: out };
        }
      } catch (e) { /* sparkline is optional — quote still goes out */ }
    }

    const text = JSON.stringify({
      asOf: now, source: 'nseix',
      price: f.LASTPRICE,
      prevClose: f.CLOSE != null ? f.CLOSE : null,
      change: f.CHANGE != null ? f.CHANGE : null,
      pchg: f.PERCHANGE != null ? f.PERCHANGE : null,
      expiry: f.EXPIRYDATE || null,
      ts: f.TIMESTMP || null,
      series: GIFT_GRAPH.series || [],
    });
    GIFT_CACHE = { ts: now, text };
    return new Response(text, { headers: { ...CORS, 'content-type': 'application/json' } });
  } catch (e) {
    return json({ error: String((e && e.message) || e) }, 502);
  }
}

/* ---------------- NSE corporate announcements (today + yesterday IST) ------ */

async function announcements() {
  const now = Date.now();
  if (ANN_CACHE.body && now - ANN_CACHE.ts < 90_000) return json(ANN_CACHE.body);
  try {
    // 1) visit the NSE homepage like a browser to collect session cookies
    const cookie = await nseCookie();

    // 2) query today + yesterday (IST clock) so late-evening filings are covered
    const ist = new Date(now + 330 * 60000);
    const from = ddmmyyyy(new Date(ist.getTime() - 86400000));
    const to = ddmmyyyy(ist);
    const r = await fetch(
      `https://www.nseindia.com/api/corporate-announcements?index=equities&from_date=${from}&to_date=${to}`,
      { headers: {
          'User-Agent': NSE_UA,
          'Accept': 'application/json, text/plain, */*',
          'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-announcements',
          ...(cookie ? { 'Cookie': cookie } : {}),
        } }
    );
    if (!r.ok) return json({ error: 'NSE HTTP ' + r.status, rows: [] }, 502);
    const j = await r.json();

    const rows = [];
    for (const rec of (Array.isArray(j) ? j : [])) {
      const sym = String(rec.symbol || '').trim().toUpperCase();
      const dt = parseDt(rec);
      if (!sym || !dt) continue;
      let cap = String(rec.attchmntText || '').replace(/\s+/g, ' ').trim();
      if (cap.length > 500) cap = cap.slice(0, 499).replace(/\s+$/, '') + '…';
      let f = String(rec.attchmntFile || '').trim();
      if (f.startsWith(PDF_PREFIX)) f = f.slice(PDF_PREFIX.length);
      rows.push([
        sym,
        String(rec.sm_name || '').replace(/\s+/g, ' ').trim(),
        dt,
        String(rec.desc || '').replace(/\s+/g, ' ').trim() || 'Others',
        cap,
        f,
      ]);
    }
    rows.sort((a, b) => (a[2] < b[2] ? 1 : a[2] > b[2] ? -1 : 0));

    const body = { asOf: now, source: 'nse', rows };
    ANN_CACHE = { ts: now, body };
    return json(body);
  } catch (e) {
    return json({ error: String((e && e.message) || e), rows: [] }, 502);
  }
}

function cookieHeader(res) {
  let list = [];
  if (typeof res.headers.getSetCookie === 'function') list = res.headers.getSetCookie();
  else {
    const raw = res.headers.get('set-cookie');
    // split a combined Set-Cookie header only on commas that start a new cookie
    if (raw) list = raw.split(/,(?=\s*[A-Za-z0-9_\-^]+=)/);
  }
  return list.map(c => c.split(';')[0].trim()).filter(Boolean).join('; ');
}

function ddmmyyyy(d) {
  // d is already shifted to IST; read the UTC fields to get the IST clock
  const p = n => String(n).padStart(2, '0');
  return p(d.getUTCDate()) + '-' + p(d.getUTCMonth() + 1) + '-' + d.getUTCFullYear();
}

function parseDt(rec) {
  // sort_date "2026-07-12 15:35:30" or an_dt "12-Jul-2026 15:35:30" -> "YYYY-MM-DD HH:MM:SS"
  let m = /^(\d{4}-\d{2}-\d{2})[ T]?(\d{2}:\d{2}(:\d{2})?)?/.exec(String(rec.sort_date || ''));
  if (m) return m[1] + ' ' + ((m[2] || '00:00') + ':00').slice(0, 8);
  m = /^(\d{2})-([A-Za-z]{3})-(\d{4})\s*(\d{2}:\d{2}(:\d{2})?)?/.exec(String(rec.an_dt || ''));
  if (!m) return null;
  const mo = MON[m[2].toLowerCase()];
  if (!mo) return null;
  return m[3] + '-' + String(mo).padStart(2, '0') + '-' + m[1] + ' ' + ((m[4] || '00:00') + ':00').slice(0, 8);
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { ...CORS, 'content-type': 'application/json' } });
}
