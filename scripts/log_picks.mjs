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
// Whole-page attempts. This job used to be single-shot: one hiccup anywhere and the day's picks were
// never logged AND the run emailed. Unlike the two bakes, a lost run here is a real data gap — nothing
// self-heals it, because the log is keyed by DATA DAY and the next run writes the next day.
// ★ RETRYING IS SAFE, and that had to be checked before adding it: the page calls
// swSync.picksSet(day, sid, …), documented as "upsert … one row per data-day per strategy", so a
// second attempt on the same data day OVERWRITES the same rows. It cannot double-log a basket.
const ATTEMPTS = +process.env.LOGPICKS_ATTEMPTS || 3;

async function attempt(n) {
  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--js-flags=--max-old-space-size=4096'],
  });
  try {
    // ⚠️ BLOCK SERVICE WORKERS — theme.js reloads the page on `controllerchange`, which fires the first
    // time a browser meets a new sw.js. Mid-run that restarts everything. bake_waves.mjs has carried
    // this note (and this fix) for weeks; this driver never got it, and its 2026-08-02 failure log shows
    // the signature — the Tailwind banner printed TWICE, i.e. the page loaded twice.
    const page = await (await browser.newContext({ serviceWorkers: 'block' })).newPage();
    page.on('console', m => console.log('[page]', m.text()));
    page.on('pageerror', e => console.log('[page-error]', e.message));
    // Name a failed resource: Chromium's console line carries the status but not the url.
    page.on('requestfailed', r => console.log('[page-net] FAILED', r.url(), '—', (r.failure() || {}).errorText || '?'));
    page.on('response', r => { if (r.status() >= 400) console.log('[page-net]', r.status(), r.url()); });
    await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
    // ⚠️ OPTIONS GO IN THE THIRD ARGUMENT — waitForFunction(pageFunction, arg?, options?). Passing
    // `{timeout, polling}` second made it the page function's `arg` and left options undefined, so this
    // wait ran on Playwright's 30 s DEFAULT, not WAIT_MS. That is precisely how run 30750190078 died:
    // "page.waitForFunction: Timeout 30000ms exceeded" at 30 s, while the page was healthy and had
    // already logged 32 of 34 strategies. WAIT_MS has been 12 min in the source since day one.
    await page.waitForFunction(() => {
      const o = document.getElementById('logpicksOut');
      return o && (/✅ LogPicks done/.test(o.textContent) || /LogPicks error/.test(o.textContent));
    }, null, { timeout: WAIT_MS, polling: 5000 });
    const status = await page.evaluate(() => document.getElementById('logpicksOut').textContent);
    console.log(`[logpicks] attempt ${n}:`, status);
    const track = await page.evaluate(() => window.__SW_TRACK || null);
    // Only write the payload when the run actually succeeded — a partial log would otherwise overwrite
    // docs/live_tracking.json with a series built from a half-written day.
    const ok = !/LogPicks error/.test(status);
    if (ok && track && Array.isArray(track.strategies)) {
      writeFileSync('docs/live_tracking.json', JSON.stringify(track));
      console.log(`[logpicks] wrote docs/live_tracking.json — ${track.strategies.length} strategies, data day ${track.updated}`);
    } else if (ok) {
      console.warn('[logpicks] no tracking payload produced');
    }
    return ok;
  } finally {
    try { await browser.close(); } catch {}
  }
}

async function main() {
  for (let n = 1; n <= ATTEMPTS; n++) {
    try {
      if (await attempt(n)) return;
      console.warn(`[logpicks] attempt ${n}/${ATTEMPTS} reported an error`);
    } catch (e) {
      console.warn(`[logpicks] attempt ${n}/${ATTEMPTS} threw: ${e && e.message || e}`);
    }
    if (n === ATTEMPTS) {
      console.error(`[logpicks] all ${ATTEMPTS} attempts failed — the day's picks were NOT logged`);
      process.exit(1);
    }
  }
}

main().catch(e => { console.error('[logpicks] fatal:', e && e.message || e); process.exit(1); });
