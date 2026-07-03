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
const BAKE_WAIT_MS  = 25 * 60 * 1000;  // max wait for the bake itself to finish

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

async function main() {
  const dataEnd = await waitForPages();
  console.log(`[bake] starting bake against data end=${dataEnd || '(unknown)'}`);

  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage();
  page.on('console', m => console.log('[page]', m.text()));
  page.on('pageerror', e => console.log('[page-error]', e.message));

  await page.goto(BAKE_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });

  // The page writes progress into #out and ends with either "✅ Done" or "Bake error".
  let finished = true;
  try {
    await page.waitForFunction(() => {
      const o = document.getElementById('out');
      return o && (/✅ Done/.test(o.textContent) || /Bake error/.test(o.textContent));
    }, { timeout: BAKE_WAIT_MS, polling: 5000 });
  } catch {
    finished = false;
  }

  const status = await page.evaluate(() => {
    const o = document.getElementById('out');
    return o ? o.textContent.replace(/\s+/g, ' ').trim() : '(no #out element)';
  });
  await browser.close();

  console.log(`[bake] final status: ${status}`);
  if (!finished) { console.error('[bake] bake did not finish within the time limit'); process.exit(1); }
  if (/Bake error/.test(status)) { console.error('[bake] page reported a bake error'); process.exit(1); }
  console.log('[bake] done — all snapshots pre-warmed');
}

main().catch(e => { console.error('[bake] fatal:', e && e.message || e); process.exit(1); });
