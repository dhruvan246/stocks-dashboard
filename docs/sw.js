/* STOCKSWORLD service worker — makes the installed app launch fast & work offline.
 * Strategy:
 *   - HTML pages: network-first (online users always get the latest), cache fallback offline.
 *   - css/js/png: stale-while-revalidate (instant, refreshes in the background).
 *   - NEVER cache the big price blobs (*.bin) or live/data JSON, and ignore cross-origin
 *     requests (the sf-data repo, Supabase, the live-quote Worker) — those stay network-only
 *     so data is always fresh and the cache never bloats.
 * Bump CACHE when the shell asset list changes. */
const CACHE = 'sw-shell-v131';  // v131: strategy names site-wide in the user's compact convention — sort phrase · filter phrases (basis method); bt-names.js strategyEnglish rewritten, defaults (Nifty 500 · monthly) silent, non-defaults ride the bracket (v130: All Picks membership dropdown lists every SAVED MIX from the Strategy Mixer ('__MIX__<label>' on the shared bt_uni_filter key, counts per mix, guarded on Saved Strategies; a mix deleted elsewhere falls back to All) (v129: engine e13 — §120 fallback 30d→28d: a clockless synthetic visibility date must be STRICTLY before every screen; census of all 56 stamp-month month-ends 2003-16 shows the earliest screen is day 29 (incl. the Sat-29-Apr-2006 special session), so day 28 never collides while +30 could land ON a day-30 screen and flap a full month on a day-29 screen (v128: engine e12 — §120 SHP +30d fallback: rows still un-dated after §105 recovery (≈pre-2014) become PIT-visible at quarter-end+30d (measured floor: ≤30d covers 99.5%/89.8% of 2014/2015 dated filings vs 91%/74% at the legal 21d), both twins; early-era DII/FII backtests invest on a disclosed late-biased approximation instead of holding cash (v127: Mixer "Saved mixes" — name the current selection (💾 Save mix as…) and reload it by chip; mix_presets_v1 joins the synced SETTINGS keys (theme.js), seeded with "⭐ 8 favourites" (v126: sw-sync stale-write guards — pushSettings never overwrites a remote key newer than this tab's stamp (per-key ts merge vs a fetch-first read on visibilitychange-hidden, mirror-guarded on pagehide); kvGet's SETTINGS dirty-replay merges mirror↔remote by ts instead of clobbering (a stale owner tab regressed ⭐favourites 8→6 at 18:25 IST 30-Aug) (v125: engine e11 — end-date rebalance guard in BOTH engine twins (simulate() never opens a basket on cfg.end; zero-holding phantom picks, StockView R5 F-03) (v124: engine e10 — SW-1 un-dated pre-Jun-2016 SHP rows (sub=99999999 sentinel, PIT-invisible; alias-merge prefers dated rows, both twins) + CSV export metadata preamble: maxDD, sharpe_rf0, final_period, accel/RSI definitions (quantmac r5 SW-3/4/5) (v123: Mixer boot AWAITS the settings pull (time-capped) so the synced mix/favourites preselect on a fresh device; key list hoisted to window.SW_SETTINGS_KEYS (theme.js); pushSettings now MERGES unknown remote keys instead of replacing the doc — an old cached tab's push was deleting newly-added synced keys (v122: ⭐ favourites go cross-device — All Picks membership dropdown gains '⭐ Favourite strategies' (shared bt_uni_filter guarded on Saved Strategies), and the Strategy Mixer's selection (mix_state_v1) joins the synced SETTINGS keys in theme.js (v121: All Picks "Monthly returns" card + matrix modal (every saved strategy month-by-month since 31 Mar 2020; docs/monthly_returns.json baked nightly by monthly-returns.yml) (v120: save serial (DDMMYY-NN) shared across pages — saveSerials() in bt-identity.js, .sw-sn in theme.css; shown on Saved Strategies, All Picks (cards+board), the strategy page & the Mixer (picker+tray); Mixer now loads bt-identity.js (v119: engine e9 — dead-con guard in profitAt/profitMetrics, both twins (BANDHANBNK stale-con class, quantmac v2) (v118: a strategy page can never open BLANK — boot() guarded, one-shot self-heal on a stale-asset ReferenceError, loadLS always returns an array (v117: strategy hover card on Backtest History too (v116: strategy hover card (years + market cycles) shared by Saved Strategies AND All Picks via new strategy-hover-card.js (v115: Saved Strategies 📈 Today's topper (live leaderboard) + wave returns on the name-hover card; live-perf.js shared with All Picks (v114: Command Deck home page — command bar + live bento (sectors/breadth/flows/spotlight/season/IPO) (v113: GIFT NIFTY live card on the home-page ticker (v112: Options Backtest page (options-backtest.html + fo-engine.js, NSE F&O EOD store) (v111: engine e8 — foldFundAliases folds old-key fundamentals under merged keys (runbook §105) (v110: strategy names shown in plain English via strategyEnglish() (new shared bt-names.js), terse code-name on hover; basisSuffix/nameWithBasis moved there out of the engine (v109: strategy names carry the earnings basis (· Consolidated / · Standalone) via nameWithBasis() when a rule reads earnings (v108: shpAt comment reframed - SHP-date recovery campaign in flight (v107: shpAt convention comment in both engine twins (runbook §105) (v106: saved-strategies KPI strip + richer empty state; stock-backtest strategy templates row + 3-step guide (v105: strategy-backtest chips/record/picker; v104: stock.html landing; v103: UI-audit polish) (v100: TTM contiguity check strengthened to pairwise gaps — a duplicate qe row (APOLLOTYRE/CARBORUNIV) slipped past the endpoint-only check (ENGINE_VER e6->e7) (v99: profitMetrics TTM requires 4 calendar-consecutive quarters, both engine twins (ENGINE_VER e5->e6) ...) ))))))
const SHELL = [
  './', './index.html', './nse-bse-dashboard.html', './stock-backtest.html', './saved-strategies.html',
  './backtest-history.html', './strategy-backtest.html', './options-backtest.html', './fo-engine.js', './all-picks.html', './strategy-mixer.html', './mutual-funds.html', './fii-dii.html',
  './backtest.html', './sectors.html', './market-mood.html', './bank-credit.html', './shareholding.html',
  './stock.html', './announcements.html', './quarterly-results.html', './status.html',
  './discovery.html', './deals.html', './insider.html', './delivery.html', './volume.html', './ipos.html', './actions.html', './watchlist.html', './live-tracking.html', './insurer-inbox.html', './analytics.html',
  './results-coverage.html', './fill-coverage.html', './coverage.html', './monthly-returns.html', './macro.html',
  './indices.html', './global.html', './movers.html', './index-chart.html',
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
