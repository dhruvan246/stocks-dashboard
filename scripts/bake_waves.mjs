// Headless "bake" of the Best-return WAVE snapshot.
//
// WHY: saved-strategies.html's Best-return tab shows every strategy's return over four market-cycle
// windows (Wave 1/2/3 + Entire cycle). Computing that for all strategies in the browser is slow, so we
// precompute it once and store ONE snapshot in Supabase (bt_snapshots id 'waves'). The page then loads
// it instantly (snapGet). After the daily data refresh the "…–date" waves move, so this re-bakes.
//
// It drives the page's own ?bakewaves=1 mode in a real browser, so the baked numbers are byte-identical
// to what the page computes live. Writes go through the public write token embedded in bt-sync.js (same
// as a user clicking "Save"), so no secrets are needed here.
//
// Run: node scripts/bake_waves.mjs      (needs `playwright` + a chromium install)

import { chromium } from 'playwright';

const BASE   = 'https://dhruvan246.github.io/stocks-dashboard';
const SFDATA = 'https://dhruvan246.github.io/sf-data';
// One SESSION per run: every batch below reuses it so they resume each other's progress. A new run
// (the next daily bake) gets a new session → the page recomputes every wave fresh against that day's data.
const SESSION  = process.env.GITHUB_RUN_ID || String(Date.now());
const BAKE_URL = `${BASE}/saved-strategies.html?bakewaves=1&session=${SESSION}`;

const PAGES_WAIT_MS = 8 * 60 * 1000;    // max wait for both GitHub Pages deploys to publish the new data date
const BAKE_WAIT_MS  = 40 * 60 * 1000;   // overall budget across all batches (the 6-yr cycle wave is ~40s/strategy)
const PER_ITER_MS   = 9 * 60 * 1000;    // per-batch wait (one fresh browser before it's killed/finishes)
const MAX_ITERS     = 14;               // reload in a fresh browser this many times at most (each resumes)

const sleep = ms => new Promise(r => setTimeout(r, ms));
async function fetchEnd(url) {
  try { const r = await fetch(url, { cache: 'no-store' }); if (!r.ok) return ''; return (await r.json()).end || ''; }
  catch { return ''; }
}

// The page computes the data-version from ./sf_meta.json while the engine loads it from the sf-data
// dataset — wait until both Pages deploys have published the same `end` so the bake matches visitors.
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
  console.warn('[pages] markers did not converge — baking anyway');
  return '';
}

// One batch in a FRESH browser (resets memory). The page resumes — it skips waves already saved for the
// current data date and bakes the next one, saving after each — so batches are additive.
async function bakeOnce(budgetMs) {
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
        const o = document.getElementById('bakeOut');
        return o && (/✅ Done/.test(o.textContent) || /Bake error/.test(o.textContent));
      }, { timeout: budgetMs, polling: 5000 });
    } catch { /* per-batch timeout OR context destroyed by a crash */ }
    try {
      status = await page.evaluate(() => {
        const o = document.getElementById('bakeOut');
        return o ? o.textContent.replace(/\s+/g, ' ').trim() : '(no #bakeOut element)';
      });
      done = /✅ Done/.test(status);
    } catch { status = '(browser crashed mid-bake)'; crashed = true; }
  } catch (e) {
    status = `(launch/nav failed: ${e && e.message || e})`; crashed = true;
  } finally {
    try { await browser.close(); } catch {}
  }
  return { status, done, crashed };
}

async function main() {
  const dataEnd = await waitForPages();
  console.log(`[bake-waves] starting against data end=${dataEnd || '(unknown)'}`);
  const deadline = Date.now() + BAKE_WAIT_MS;
  let done = false, prevStatus = '';
  for (let iter = 1; iter <= MAX_ITERS && Date.now() < deadline; iter++) {
    const budget = Math.max(30000, Math.min(PER_ITER_MS, deadline - Date.now()));
    const { status, done: d, crashed } = await bakeOnce(budget);
    console.log(`[bake-waves] batch ${iter}: ${status}`);
    if (d) { done = true; break; }
    if (/Bake error/.test(status)) { console.error('[bake-waves] page reported a bake error'); process.exit(1); }
    // Stall guard: identical readable status two batches running = stuck on the same wave (not progressing).
    // Crashes read no status and keep looping (they DO progress via the incremental per-wave save).
    if (!crashed && status === prevStatus) { console.warn('[bake-waves] no progress across two batches — stopping'); break; }
    if (!crashed) prevStatus = status;
  }
  if (done) { console.log('[bake-waves] done — wave snapshot saved'); return; }
  // Non-critical: the page falls back to the full-history CAGR ranking if the snapshot is missing/stale.
  console.warn('[bake-waves] partial/incomplete within budget (non-critical — page falls back / uses partial snapshot)');
}

main().catch(e => { console.error('[bake-waves] fatal:', e && e.message || e); process.exit(1); });
