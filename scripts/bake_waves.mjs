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
//
// ENV (used by the sibling FULL-HISTORY job, bake-full-history.yml — same script, longer budget):
//   BAKE_WAVES     comma list of windows to bake → page's ?waves= (default: the daily four)
//   BAKE_WAIT_MIN  overall budget in minutes (default 70)
//   BAKE_MAX_ITERS batch cap (default 60)
//   BAKE_DATA_RETRIES consecutive "Bake error: market data" batches to tolerate (default 3)

import { chromium } from 'playwright';

const BASE   = 'https://dhruvan246.github.io/stocks-dashboard';
const SFDATA = 'https://dhruvan246.github.io/sf-data';
// One SESSION per run: every batch below reuses it so they resume each other's progress. A new run
// (the next daily bake) gets a new session → the page recomputes every wave fresh against that day's data.
const SESSION  = process.env.GITHUB_RUN_ID || String(Date.now());
const WAVES    = (process.env.BAKE_WAVES || '').trim();
const BAKE_URL = `${BASE}/saved-strategies.html?bakewaves=1&session=${SESSION}`
               + (WAVES ? `&waves=${encodeURIComponent(WAVES)}` : '');

const PAGES_WAIT_MS = 8 * 60 * 1000;    // max wait for both GitHub Pages deploys to publish the new data date
// Overall budget across all batches. Cost ≈ strategies × total window-years × ~6s: the four waves span
// 1.5 + 1.5 + 0.35 + 6.3 ≈ 9.7 years, so ~47 strategies ≈ 48 min of pure compute, plus a fresh browser
// per batch re-downloading the engine + market data. At 40 min the `cycle` wave — baked LAST and by far
// the longest window — never finished: it was left as `pending` and the tab kept serving older numbers.
// (Stock counts became separate rows on 2026-08-03, taking 35 strategies to 47, so this got tighter.)
// The full-history window is ~24 years on its own, which is why it runs as a separate job with
// BAKE_WAIT_MIN/BAKE_MAX_ITERS raised rather than being squeezed in here.
const BAKE_WAIT_MS  = (+process.env.BAKE_WAIT_MIN || 70) * 60 * 1000;   // must stay under the job's timeout-minutes, minus ~5 min of setup
const PER_ITER_MS   = 15 * 60 * 1000;   // per-batch ceiling — NOT what actually ends a batch, see MAX_ITERS
// ⚠️ HISTORICAL NOTE, NOW CORRECTED (2026-08-17): this used to read "a batch never lasts anywhere near
// PER_ITER_MS: the page's renderer dies ~30s in (it holds ~100 MB of market data)". That diagnosis was
// wrong. Batches ended at ~31s because the per-batch timeout was passed in waitForFunction's `arg`
// position and never applied, so Playwright's 30s default governed every wait (see bakeOnce). The
// renderer was alive throughout — it answered page.evaluate with a readable status every single batch.
// With the argument fixed, a batch now runs until the page finishes, OOMs, or the budget expires.
// Each batch still makes progress — the page saves after every strategy and the next one resumes — at
// roughly 4-5 strategies/batch on the 6.3-yr `cycle` wave and 10-30 on the shorter ones. At 14 the run hit
// the cap in 8m47s having left `cycle` at 27/45 (run 30765373241), which is why that tab kept serving stale
// numbers. ~21 batches covers all four waves at 45 strategies; 60 leaves room to grow. The real stop is the
// deadline above (60 x ~35s is ~35 min, well inside it) plus the stall guard in main().
const MAX_ITERS     = +process.env.BAKE_MAX_ITERS || 60;   // reload in a fresh browser this many times at most (each resumes)
// ⚠️ CONSECUTIVE market-data load failures to ride out before failing the run. loadEngineData() pulls
// ~80 MB across several files (dash_slim + the sf-data parts + fundamentals + shareholding), so ONE
// 503 anywhere in it makes the page's ensureEngine() return false and print "Bake error: market data".
// That is a transient far more often than a defect: on 2026-08-17 run 31998364764 died on a single 503
// at batch 6, and the next scheduled run 28 min later baked cleanly — as did all seven after it — so
// the only lasting effect was a failure email plus auto-rerun's 5-attempt ladder on top of it.
// Retrying costs nothing to write: the page nulls ENGINE_LOADING when the load throws, and every batch
// is a fresh browser, so going round the loop again IS the retry. A PERSISTENT failure still fails the
// run — the dataset genuinely being unreachable is worth the alarm.
const DATA_RETRIES  = process.env.BAKE_DATA_RETRIES != null ? +process.env.BAKE_DATA_RETRIES : 3;

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
    // ⚠️ BLOCK SERVICE WORKERS. theme.js reloads the page on `controllerchange`, which fires the first
    // time a browser meets a new sw.js version — mid-bake, destroying the run's progress. Every batch
    // is a fresh browser, so EVERY batch met it: uniform ~45s batches that end without a crash and
    // re-report the same strategy index. That's what capped the 2026-08-03 full-history bake at 5/32
    // (the Tailwind banner logged twice per batch = the page loading twice). The bake needs no SW.
    const page = await (await browser.newContext({ serviceWorkers: 'block' })).newPage();
    page.on('console', m => console.log('[page]', m.text()));
    page.on('pageerror', e => console.log('[page-error]', e.message));
    // ⚠️ NAME THE RESOURCE. Chromium's own console line is "Failed to load resource: the server
    // responded with a status of 503 ()" — with NO url — so the 2026-08-17 failure could not be
    // traced to a file at all, only to "something the engine fetches". These two listeners are what
    // make the next one diagnose itself; both are silent unless something actually fails.
    page.on('requestfailed', r => console.log('[page-net] FAILED', r.url(), '—', (r.failure() || {}).errorText || '?'));
    page.on('response', r => { if (r.status() >= 400) console.log('[page-net]', r.status(), r.url()); });
    await page.goto(BAKE_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
    try {
      // ⚠️ OPTIONS GO IN THE THIRD ARGUMENT. Playwright declares
      //   waitForFunction(pageFunction, arg?, options?)
      // so passing `{timeout, polling}` second hands it to the page function as `arg` and leaves
      // options UNDEFINED — every wait then ran on Playwright's 30 s DEFAULT, silently ignoring the
      // budget computed above. Measured on run 32013243427 (2026-08-17): 13 batch intervals, mean
      // 31.1 s (min 25, max 35) against a configured 900 s. The note below used to blame an
      // OOM-killed renderer for that cadence; it was wrong — every batch returned a READABLE status
      // ("Baking w1 15/40"), and a dead renderer cannot answer page.evaluate at all. Same bug hit
      // log_picks.mjs, which died with "Timeout 30000ms exceeded" while asking for 12 minutes.
      await page.waitForFunction(() => {
        const o = document.getElementById('bakeOut');
        return o && (/✅ Done/.test(o.textContent) || /Bake error/.test(o.textContent));
      }, null, { timeout: budgetMs, polling: 5000 });
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
  let done = false, curWave = null, best = -1, flat = 0, dataFails = 0;
  // Progress = the (wave, strategy index) in "Baking <wave> N/M (i/j waves)". Compare the NUMBER, not
  // the whole string: the page paints the last COMPLETED index and then blocks on the next backtest,
  // so a batch that died part-way through strategy N+1 still reads "N/M" despite having saved real
  // work. String equality read that as stuck and stopped runs that were progressing fine. The index
  // restarts at each new wave, so it is only comparable WITHIN one wave.
  const parse = s => { const m = /Baking\s+(\S+)\s+(\d+)\s*\/\s*(\d+)/.exec(s || ''); return m ? { wave: m[1], n: +m[2] } : null; };
  for (let iter = 1; iter <= MAX_ITERS && Date.now() < deadline; iter++) {
    const budget = Math.max(30000, Math.min(PER_ITER_MS, deadline - Date.now()));
    const { status, done: d, crashed } = await bakeOnce(budget);
    console.log(`[bake-waves] batch ${iter}: ${status}`);
    if (d) { done = true; break; }
    // ⚠️ This was the ONLY outcome in this loop that did not get another go: a crash `continue`s, an
    // unreadable status `continue`s, a flat index gets four chances, and running out of budget below
    // is explicitly "non-critical" and exits 0. A market-data 503 — the most transient failure the
    // job has — was the one thing that killed the run outright and mailed about it. Count them
    // CONSECUTIVELY (a batch that gets past the engine load clears the counter, see below), so a
    // second 503 an hour later in the same run is not treated as the same incident.
    if (/Bake error/.test(status)) {
      if (/market data/i.test(status) && ++dataFails <= DATA_RETRIES) {
        console.warn(`[bake-waves] market-data load failed (${dataFails}/${DATA_RETRIES}) — retrying in a fresh browser`);
        continue;
      }
      console.error('[bake-waves] page reported a bake error'
        + (dataFails ? ` — market data failed ${dataFails} batches in a row, past the ${DATA_RETRIES} allowed` : ''));
      process.exit(1);
    }
    dataFails = 0;                                            // got past the engine load → not the transient
    const p = parse(status);
    if (crashed || !p) continue;                              // unreadable status; crashes DO progress
    if (p.wave !== curWave) { curWave = p.wave; best = p.n; flat = 0; continue; }   // moved to the next wave
    if (p.n > best) { best = p.n; flat = 0; continue; }        // advanced within this wave
    // Genuinely flat: same-or-lower index, same wave. One batch can legitimately fail to finish a
    // single long backtest (the full-history window is ~24 years), so allow a few before giving up.
    if (++flat >= 4) { console.warn(`[bake-waves] no progress across ${flat} batches (stuck at ${curWave} ${best}) — stopping`); break; }
  }
  if (done) { console.log('[bake-waves] done — wave snapshot saved'); return; }
  // Non-critical: the page falls back to the full-history CAGR ranking if the snapshot is missing/stale.
  console.warn('[bake-waves] partial/incomplete within budget (non-critical — page falls back / uses partial snapshot)');
}

main().catch(e => { console.error('[bake-waves] fatal:', e && e.message || e); process.exit(1); });
