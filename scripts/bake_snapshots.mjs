// Headless "bake" of backtest result snapshots.
//
// WHY: stock-backtest.html caches each strategy's full result in Supabase (bt_snapshots), keyed by
// snapKey() = strategy identity + window + method + ENGINE_VER + data-date. After the daily data
// refresh the data-date rolls forward, so every key changes and yesterday's snapshots go stale. The
// page self-heals (a first view recomputes + re-caches), but that first view is slow. This script
// pre-warms every snapshot by driving the page's own ?bake=all mode in a real browser — so the
// snapshots are always fresh AND every user gets the instant path.
//
// It runs the page's actual simulate()/bakeAll(), so baked results are byte-identical to what the 👁
// renders. Writes go through the public write token already embedded in bt-sync.js (same as a user
// clicking "Save"), so no secrets are needed here.
//
// Run: node scripts/bake_snapshots.mjs      (needs `playwright` + a chromium install)

import { chromium } from 'playwright';

const BASE   = 'https://dhruvan246.github.io/stocks-dashboard';
const SFDATA = 'https://dhruvan246.github.io/sf-data';
const BAKE_URL = `${BASE}/stock-backtest.html?bake=all`;

const PAGES_WAIT_MS = 8 * 60 * 1000;   // max wait for both GitHub Pages deploys to publish the new data date
const BAKE_WAIT_MS  = 25 * 60 * 1000;  // overall budget for the bake across all reload batches
const PER_ITER_MS   = 8 * 60 * 1000;   // per-batch wait (one browser session before it OOMs/finishes)
const MAX_ITERS     = 12;              // reload the page this many times at most (each resets browser memory)

async function fetchEnd(url) {
  try {
    const r = await fetch(url, { cache: 'no-store' });
    if (!r.ok) return '';
    return (await r.json()).end || '';
  } catch { return ''; }
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

// 1) The page computes the snapshot data-version from the main-repo marker (./sf_meta.json) while the
//    bake computes it from the sf-data dataset it loads. Both must agree for the baked keys to match
//    what real visitors look up — so wait until both Pages deploys have published the same `end`.
async function waitForPages() {
  const deadline = Date.now() + PAGES_WAIT_MS;
  while (Date.now() < deadline) {
    const [mainEnd, dataEnd] = await Promise.all([
      fetchEnd(`${BASE}/sf_meta.json?t=${Date.now()}`),
      fetchEnd(`${SFDATA}/sf_meta.json?t=${Date.now()}`),
    ]);
    console.log(`[pages] main marker end=${mainEnd || '?'}  sf-data end=${dataEnd || '?'}`);
    if (mainEnd && dataEnd && mainEnd === dataEnd) return mainEnd;
    await sleep(20000);
  }
  console.warn('[pages] markers did not converge within the wait window — baking anyway (self-heals next run)');
  return '';
}

// One bake batch in a FRESH browser. The page loads the full ~110 MB dataset and computes many
// backtests in a single session — memory climbs until Chromium is OOM-killed (~18 strategies in on
// the CI runner), which is why one-shot baking never finishes. But the page skips snapshots already
// in Supabase (snapGet) and persists each result immediately (snapSet), so a fresh browser per batch
// resets memory and every reload skips what's done and bakes a few more — progressive completion.
async function bakeOnce(iter, budgetMs) {
  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--js-flags=--max-old-space-size=4096'],
  });
  let status = '(no status)', done = false, crashed = false;
  try {
    const page = await browser.newPage();
    page.on('console', m => console.log('[page]', m.text()));
    page.on('pageerror', e => console.log('[page-error]', e.message));
    await page.goto(BAKE_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
    try {
      await page.waitForFunction(() => {
        const o = document.getElementById('out');
        return o && (/✅ Done/.test(o.textContent) || /Bake error/.test(o.textContent));
      }, { timeout: budgetMs, polling: 5000 });
    } catch { /* per-batch timeout OR the page context was destroyed by an OOM crash */ }
    try {
      status = await page.evaluate(() => {
        const o = document.getElementById('out');
        return o ? o.textContent.replace(/\s+/g, ' ').trim() : '(no #out element)';
      });
      done = /✅ Done/.test(status);
    } catch { status = '(browser crashed mid-bake)'; crashed = true; }
  } catch (e) {
    status = `(browser launch/nav failed: ${e && e.message || e})`; crashed = true;
  } finally {
    try { await browser.close(); } catch {}
  }
  return { status, done, crashed };
}

async function main() {
  const dataEnd = await waitForPages();
  console.log(`[bake] starting bake against data end=${dataEnd || '(unknown)'}`);

  const deadline = Date.now() + BAKE_WAIT_MS;
  let done = false, prevStatus = '';
  for (let iter = 1; iter <= MAX_ITERS && Date.now() < deadline; iter++) {
    const budget = Math.max(30000, Math.min(PER_ITER_MS, deadline - Date.now()));
    const { status, done: d, crashed } = await bakeOnce(iter, budget);
    console.log(`[bake] batch ${iter}: ${status}`);
    if (d) { done = true; break; }
    if (/Bake error/.test(status)) { console.error('[bake] page reported a bake error'); process.exit(1); }
    // Stall guard: identical readable status two batches running = stuck on the same job (not just OOM
    // progressing). Crashes read no status, so they never trip this and keep looping (they DO progress).
    if (!crashed && status === prevStatus) { console.warn('[bake] no progress across two batches — stopping'); break; }
    if (!crashed) prevStatus = status;
  }

  if (done) { console.log('[bake] done — all snapshots pre-warmed'); return; }
  // Non-critical: the page self-heals each snapshot on a first view, so a partial bake (budget
  // exhausted before every snapshot warmed) is NOT a failure — exit 0 so it doesn't email on every
  // run. Genuine problems ("Bake error" above, or a fatal throw below) still exit 1 and stay visible.
  console.warn('[bake] partial bake — some snapshots not pre-warmed within the retry budget (non-critical — page self-heals on first view)');
  return;
}

main().catch(e => { console.error('[bake] fatal:', e && e.message || e); process.exit(1); });
