// Daily strategy picks-logger + forward-tracking baker.
//
// Drives saved-strategies.html?logpicks=1 (bake_snapshots.mjs pattern) which:
//   1. logs every unique saved strategy's Today's Picks (with entry prices) into
//      Supabase sw_picks_log — the append-only paper-trade memory, and
//   2. rebuilds the full forward-tracking NAV series from that log (engine prices),
//      exposing it as window.__SW_TRACK.
// This script then writes __SW_TRACK to docs/live_tracking.json, which
// live-tracking.html renders instantly. Run AFTER the daily data refresh has
// published (the caller workflow chains off the snapshot bake, which itself waits
// for the GitHub Pages deploys to converge).
//
// Run: node scripts/log_picks.mjs      (needs `playwright` + chromium)

import { chromium } from 'playwright';
import { writeFileSync } from 'fs';

const BASE = 'https://dhruvan246.github.io/stocks-dashboard';
const URL = `${BASE}/saved-strategies.html?logpicks=1`;
const WAIT_MS = 12 * 60 * 1000;

async function main() {
  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--js-flags=--max-old-space-size=4096'],
  });
  try {
    const page = await browser.newPage();
    page.on('console', m => console.log('[page]', m.text()));
    page.on('pageerror', e => console.log('[page-error]', e.message));
    await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await page.waitForFunction(() => {
      const o = document.getElementById('logpicksOut');
      return o && (/✅ LogPicks done/.test(o.textContent) || /LogPicks error/.test(o.textContent));
    }, { timeout: WAIT_MS, polling: 5000 });
    const status = await page.evaluate(() => document.getElementById('logpicksOut').textContent);
    console.log('[logpicks]', status);
    const track = await page.evaluate(() => window.__SW_TRACK || null);
    if (track && Array.isArray(track.strategies)) {
      writeFileSync('docs/live_tracking.json', JSON.stringify(track));
      console.log(`[logpicks] wrote docs/live_tracking.json — ${track.strategies.length} strategies, data day ${track.updated}`);
    } else {
      console.warn('[logpicks] no tracking payload produced');
    }
    if (/LogPicks error/.test(status)) process.exit(1);
  } finally {
    try { await browser.close(); } catch {}
  }
}

main().catch(e => { console.error('[logpicks] fatal:', e && e.message || e); process.exit(1); });
