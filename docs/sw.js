/* STOCKSWORLD service worker — makes the installed app launch fast & work offline.
 * Strategy:
 *   - HTML pages: network-first (online users always get the latest), cache fallback offline.
 *   - css/js/png: stale-while-revalidate (instant, refreshes in the background).
 *   - NEVER cache the big price blobs (*.bin) or live/data JSON, and ignore cross-origin
 *     requests (the sf-data repo, Supabase, the live-quote Worker) — those stay network-only
 *     so data is always fresh and the cache never bloats.
 * Bump CACHE when the shell asset list changes. */
const CACHE = 'sw-shell-v269';  // v269: stock.html — cash-flow columns of 9, 15 or 18 month years labelled with their length (cf_m) (v268: phone control bars — theme.css/theme.js .sw-strip + .sw-filters (search + Filters button, one-line chip strips), first on Quarterly Results; cards show price · mcap · PE (v267: Zerodha live prices on terminal (kite-live.js) (v266: Order book tab — Zerodha live Today column (v265: Breakout watch — Zerodha BE/SM symbols (v264: Breakout watch — per-stock Zerodha retry + missing-price reasons (v263: Breakout watch — Zerodha live status line (v262: Breakout watch — live Today column (Kite real-time, Yahoo fallback) (v261: Breakout watch — BSE-only stocks added (v260: Daily Ideas — Mcap column in Breakout watch, Confirmed jumps, Order wins (v259: Daily Ideas — Breakout watch tab (rose 3x+, fell 50%+, back near or through the old peak) (v258: Backtest engine e24 — CEAT -> CEATLTD alias (§179t) (v257: Quarterly Results — Just Declared follows the quarter chip (cards for other quarters hidden, counted on an 'Other quarters' line) (v256: Quarterly Results — Just Declared: wider result-caption filter on the live relay + cards for filings whose caption names no quarter (numbers announced that day) (v255: Commodities — new price blocks: China chemical quotes (Guidechem, ~80 incl. H-acid), CAI cotton spot, Spices Board, MOIL manganese letters (v254: Daily Ideas — Order wins adding up table live (10 letter-checked flags, return since the flag filing) (v253: Daily Ideas — Confirmed order-book jumps table live (23 companies checked against their filings, return since the jump filing + since an earlier faded jump, faded tag) (v252: Daily Ideas — order-book history published for all 11 order-book cards (order_book_history.json, 189 company-stated figures) (v251: Daily Ideas Order book tab — Confirmed jumps table (returns since the jump filing) + Order wins adding up (hidden until their data is published) (v250: Daily Ideas — Order book section on each order-book card (every stated order book by date, chart, year-on-year change, book ÷ sales; from order_book_history.json), above Revenue guidance (v249: Daily Ideas — Order book jumps table one line per company (short Confirmed/Corrected tag + filing links; full notes and quotes under 'More'), company column pinned (v248: Daily Ideas — Order book jumps tab (stated order books read from the companies' own filings, latest vs a year earlier, checked rows marked) + Order book lane (v247: Nifty Microcap 250 added — home card, index-chart ?ix=microcap, movers, quarterly results, backtest universes (v246: Daily Ideas redesigned — search box, filter chips, sort, one row per stock, company page with Overview / Updates / Filings / Full research / Sources tabs, sortable scorecard (v245: Daily Ideas — revenue-guidance line on every card, 'Updates since the call' timeline (good news green / red flags red), market cap at the latest close (v244: shareholding.html — quarter picker + 'Filed on' date filter; v243: stock.html — no sideways page scroll at 761–1300px (main pinned to the body); Insights + shareholding open on the newest column with the label column pinned)
const SHELL = [
  './', './index.html', './nse-bse-dashboard.html', './stock-backtest.html', './saved-strategies.html',
  './backtest-history.html', './strategy-backtest.html', './options-backtest.html', './fo-engine.js', './all-picks.html', './strategy-mixer.html', './mutual-funds.html', './fii-dii.html',
  './backtest.html', './sectors.html', './market-mood.html', './bank-credit.html', './shareholding.html',
  './stock.html', './announcements.html', './quarterly-results.html', './status.html', './data-checks.html',
  './discovery.html', './deals.html', './insider.html', './delivery.html', './volume.html', './ipos.html', './actions.html', './watchlist.html', './live-tracking.html', './insurer-inbox.html', './analytics.html',
  './results-coverage.html', './fill-coverage.html', './coverage.html', './monthly-returns.html', './macro.html',
  './indices.html', './global.html', './movers.html', './index-chart.html', './capex.html', './ideas.html', './commodities.html',
  './theme.css', './theme.js', './glossary.js', './bt-names.js', './backtest-engine.js', './live-perf.js', './bt-identity.js', './strategy-hover-card.js', './bt-sync.js', './sw-sync.js', './sw-watchlist.js',
  './manifest.webmanifest',
  './icon-192.png', './icon-512.png', './icon-512-maskable.png', './apple-touch-icon.png'
];

self.addEventListener('install', function (e) {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then(function (c) { return c.addAll(SHELL).catch(function () {}); }));
});

self.addEventListener('activate', function (e) {
  e.waitUntil(
    caches.keys().then(function (keys) {
      return Promise.all(keys.filter(function (k) { return k !== CACHE; }).map(function (k) { return caches.delete(k); }));
    }).then(function () { return self.clients.claim(); })
  );
});

self.addEventListener('fetch', function (e) {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;                       // leave cross-origin (data repo / Supabase / Worker) alone
  if (url.pathname.endsWith('.bin') || url.pathname.endsWith('.bin.gz') || url.pathname.endsWith('.json')) return;  // never cache big blobs / live data

  // Network-first for everything cacheable (HTML / CSS / JS / PNG): always fresh when online,
  // so fixes land immediately; fall back to cache only when offline.
  e.respondWith(
    fetch(req).then(function (r) {
      if (r && r.status === 200) { const cp = r.clone(); caches.open(CACHE).then(function (c) { c.put(req, cp); }); }
      return r;
    }).catch(function () {
      return caches.match(req).then(function (m) {
        if (m) return m;
        const wantsHTML = req.mode === 'navigate' || (req.headers.get('accept') || '').indexOf('text/html') >= 0;
        return wantsHTML ? caches.match('./index.html') : undefined;
      });
    })
  );
});
