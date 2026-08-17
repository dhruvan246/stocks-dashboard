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
// Consecutive transient data failures to ride out before failing the run — see the TRANSIENT matcher in
// main(). A persistent failure still exits 1; the point is not to email over one 503.
const DATA_RETRIES  = process.env.BAKE_DATA_RETRIES != null ? +process.env.BAKE_DATA_RETRIES : 3;

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
    // ⚠️ BLOCK SERVICE WORKERS — theme.js reloads the page on `controllerchange`, restarting the batch.
    // bake_waves.mjs documented and fixed this; this driver never got it, and the cost is visible in
    // run 29350543209: the Tailwind banner printed TWICE per batch (the page loading twice) while
    // progress crawled 28 -> 42 -> 46 -> 49 -> 53 of 63 with "0 baked" over and over, never finishing.
    const page = await (await browser.newContext({ serviceWorkers: 'block' })).newPage();
    page.on('console', m => console.log('[page]', m.text()));
    page.on('pageerror', e => console.log('[page-error]', e.message));
    // Name a failed resource: Chromium's console line carries the status but not the url.
    page.on('requestfailed', r => console.log('[page-net] FAILED', r.url(), '—', (r.failure() || {}).errorText || '?'));
    page.on('response', r => { if (r.status() >= 400) console.log('[page-net]', r.status(), r.url()); });
    await page.goto(BAKE_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
    try {
      // ⚠️ OPTIONS GO IN THE THIRD ARGUMENT — waitForFunction(pageFunction, arg?, options?). Passed
      // second, `{timeout, polling}` became the page function's `arg` and options stayed undefined, so
      // every wait ran on Playwright's 30 s DEFAULT instead of budgetMs. Measured in the sibling bake
      // (run 32013243427): 13 intervals, mean 31.1 s against a configured 900 s, with a readable status
      // every batch — so the old "the renderer dies ~30s in" theory was wrong; a dead renderer cannot
      // answer page.evaluate. Fixing this is what lets a batch use the budget it was given.
      await page.waitForFunction(() => {
        const o = document.getElementById('out');
        return o && (/✅ Done/.test(o.textContent) || /Bake error/.test(o.textContent));
      }, null, { timeout: budgetMs, polling: 5000 });
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
  let done = false, best = -1, flat = 0, dataFails = 0;
  // Progress = the N in the page's "Baking snapshots — N/63". Compare the NUMBER, not the whole string:
  // the old guard was `status === prevStatus`, and bake_waves.mjs already learned the hard way that
  // string equality reads a batch which died part-way through job N+1 (so it still prints N) as stuck,
  // and stops runs that were progressing fine. The tail of the status carries the current job NAME, so
  // two batches working on different jobs at the same index also differ as strings while being flat.
  const parse = s => { const m = /(\d+)\s*\/\s*(\d+)/.exec(s || ''); return m ? +m[1] : null; };
  // Transient shapes the page's OUTER catch can surface. Everything thrown inside the per-job loop is
  // already caught there and counted as `fail`, so a "Bake error" here means the whole bake could not
  // start — in practice a data load: ensureLiveData throws 'market data unavailable', and the fetch
  // helper throws 'HTTP <status> <url>'. Anything else stays immediately fatal.
  const TRANSIENT = /market data|HTTP (5\d\d|429|408)\b|Failed to fetch|NetworkError|load failed/i;
  for (let iter = 1; iter <= MAX_ITERS && Date.now() < deadline; iter++) {
    const budget = Math.max(30000, Math.min(PER_ITER_MS, deadline - Date.now()));
    const { status, done: d, crashed } = await bakeOnce(iter, budget);
    console.log(`[bake] batch ${iter}: ${status}`);
    if (d) { done = true; break; }
    // A transient data failure is retried, not fatal — the page persists each snapshot (snapSet) and
    // skips what is already cached, so the next fresh browser resumes. Counted CONSECUTIVELY: any batch
    // that gets past the load clears it. This mirrors the fix bake_waves.mjs needed on 2026-08-17.
    if (/Bake error/.test(status)) {
      if (TRANSIENT.test(status) && ++dataFails <= DATA_RETRIES) {
        console.warn(`[bake] transient data failure (${dataFails}/${DATA_RETRIES}) — retrying in a fresh browser`);
        continue;
      }
      console.error('[bake] page reported a bake error'
        + (dataFails ? ` — data load failed ${dataFails} batches in a row, past the ${DATA_RETRIES} allowed` : ''));
      process.exit(1);
    }
    dataFails = 0;
    const p = parse(status);
    if (crashed || p == null) continue;                       // unreadable status; crashes DO progress
    if (p > best) { best = p; flat = 0; continue; }           // advanced
    // Genuinely flat: same-or-lower index. One batch can legitimately fail to finish a single long
    // backtest (some saved windows span ~24 years), so allow a few before giving up.
    if (++flat >= 4) { console.warn(`[bake] no progress across ${flat} batches (stuck at ${best}) — stopping`); break; }
  }

  if (done) { console.log('[bake] done — all snapshots pre-warmed'); return; }
  // Non-critical: the page self-heals each snapshot on a first view, so a partial bake (budget
  // exhausted before every snapshot warmed) is NOT a failure — exit 0 so it doesn't email on every
  // run. Genuine problems ("Bake error" above, or a fatal throw below) still exit 1 and stay visible.
  console.warn('[bake] partial bake — some snapshots not pre-warmed within the retry budget (non-critical — page self-heals on first view)');
  return;
}

main().catch(e => { console.error('[bake] fatal:', e && e.message || e); process.exit(1); });
