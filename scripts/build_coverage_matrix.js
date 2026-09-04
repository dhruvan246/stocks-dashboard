'use strict';
/* ============================================================================
 * Bake docs/coverage/*.json — the payload behind docs/coverage.html (private).
 *
 * WHAT IT MEASURES
 *   For every month-end and every universe: how many of that date's members
 *   carry a usable value for each screening parameter. "Usable" is decided by
 *   the LIVE ENGINE, not by a re-implementation: this script loads
 *   docs/backtest-engine.js verbatim into a Node `vm` context, injects the
 *   same globals loadCore()/loadSF()/loadFund()/loadShp() would have set, and
 *   asks factorsAt() + fieldVal() — the exact accessors a backtest screens on.
 *   So a cell here is "what a strategy could actually see at that date".
 *
 *   Two families come from files the engine does not read (revenue/margins from
 *   sf_revop, sector classification): those are counted here directly and the
 *   rule is documented per-parameter in the payload (`rule`), never guessed at
 *   render time.
 *
 * ENGINE-IN-VM GOTCHA (learned the hard way, keep it):
 *   the engine declares its globals with `let`, which in Node's vm does NOT
 *   attach to the contextified sandbox as a settable property. Inject with an
 *   in-context assignment (`SERIES = …` inside runInContext) — never
 *   `sandbox.SERIES = …`. All runInContext calls against one context share a
 *   single global lexical scope, so this works.
 *
 * DATA FRESHNESS:
 *   docs/sf_stock_data.bin in this repo is a FROZEN snapshot (the real ~193 MB
 *   file is past GitHub's 100 MB cap and is never committed). Default is
 *   therefore --bin auto → download the LIVE release asset. Pass an explicit
 *   --bin path in CI, where a freshly appended bin already exists on disk.
 *
 *   node --max-old-space-size=12288 scripts/build_coverage_matrix.js \
 *        [--bin auto|<path>] [--out docs/coverage] [--from 2002-01] [--dates N]
 * ========================================================================== */
const fs = require('fs');
const path = require('path');
const os = require('os');
const vm = require('vm');
const zlib = require('zlib');
const { execFileSync } = require('child_process');

const ROOT = path.dirname(__dirname);
const DOCS = path.join(ROOT, 'docs');
const RELEASE_BIN = 'https://github.com/dhruvan246/stocks-dashboard/releases/download/data/sf_stock_data.bin';

/* ---- args ---- */
const ARGV = process.argv.slice(2);
function arg(name, dflt) { const i = ARGV.indexOf('--' + name); return i >= 0 ? ARGV[i + 1] : dflt; }
const OUT_DIR = path.resolve(ROOT, arg('out', 'docs/coverage'));
const BIN_ARG = arg('bin', 'auto');
const FROM = arg('from', '2002-01');          // true daily bars start 2002-01-02; earlier is weekly
const MAX_DATES = +arg('dates', 0) || 0;      // 0 = all (a small N is the dev/smoke setting)
/* --explain <slug> [--explain-from YYYY-MM-DD] [--explain-to YYYY-MM-DD] [--explain-out <path>]
 * Names the symbols behind every sub-100 cell for ONE universe, emitted from the same scan that
 * writes the payload — so the queue file and the page can never disagree. Analysis only: it does
 * not change a single counted cell. Off unless asked for. */
const EXPLAIN = arg('explain', '') || null;
const EXPLAIN_FROM = arg('explain-from', '0000-00-00');
const EXPLAIN_TO = arg('explain-to', '9999-12-31');
const EXPLAIN_PATH = arg('explain-out', 'scripts/n500_cov_explain.json');
const EXPLAIN_OUT = {};
/* --facts <path>: dump per-symbol first-bar / oldest-filing / first-SHP boundaries from the engine's
 * own loaded state, for adjudicating pre-history classes. Analysis only, like --explain. */
const FACTS_PATH = arg('facts', '') || null;

const log = (...a) => console.log('[coverage]', ...a);

/* ============================================================================
 * PARAMETERS — every screening input the site holds, grouped into the families
 * the page shows as columns. `k` is the engine field name (fieldVal key) unless
 * `src` says otherwise. `rule` is what the page prints when you ask "what does
 * this cell count?" — it is the honest definition, not a label.
 * ========================================================================== */
const FAMILIES = [
  { id: 'price', label: 'Price', note: 'A tradeable bar at the date. Every other column is gated on this one.', params: [
    { k: 'price', rule: 'Close ≤ the rebalance date, with a bar in the last 14 days (28 pre-2002) — the engine\'s entry-freshness gate.' },
    { k: 'chg', rule: '1-month price change % (needs a close 30 days earlier).' },
  ] },
  { id: 'band52', label: '52W band', note: 'Trailing-365d high/low, from true intraday H/L where the bhavcopy carries them.', params: [
    { k: 'd52', rule: 'Distance below the 52w high %.' },
    { k: 'd52_low_pct', rule: 'Distance above the 52w low %.' },
    { k: 'rangePos', rule: '52w range position — null when high == low.' },
    { k: 'daysHigh', rule: 'Days since the 52w high.' },
  ] },
  { id: 'momentum', label: 'Momentum', note: 'Trailing returns — each needs a close that far back.', params: [
    { k: 'ret1m', rule: 'Return over 30 days.' },
    { k: 'ret3m', rule: 'Return over 91 days.' },
    { k: 'ret6m', rule: 'Return over 182 days.' },
    { k: 'ret12m', rule: 'Return over 365 days.' },
    { k: 'accel', rule: 'This month\'s return minus last month\'s (needs 60 days).' },
    { k: 'riskMom', rule: '3m return ÷ volatility (needs both).' },
    { k: 'rsNifty', rule: '6m return minus Nifty\'s 6m return — needs nifty.json to reach back that far.' },
  ] },
  { id: 'trend', label: 'Trend', note: 'Moving averages and the industry ranking built from them.', params: [
    { k: 'dma50', rule: 'Distance from the 50-session SMA % (needs 50 bars).' },
    { k: 'dma200', rule: 'Distance from the 200-session SMA % (needs 200 bars).' },
    { k: 'indRank', rule: 'Industry momentum rank 1-10 — assigned to every screened row, so this tracks Price.' },
  ] },
  { id: 'risk', label: 'Risk', note: 'Volatility, beta and drawdown over 90-180 day windows.', params: [
    { k: 'vol', rule: 'Annualised volatility — needs >2 returns in the trailing 90 days.' },
    { k: 'beta', rule: 'Beta vs Nifty over 120 days — needs >5 paired stock/Nifty returns, so it is 0 before nifty.json starts.' },
    { k: 'mdd6', rule: 'Max drawdown over 182 days.' },
    { k: 'upPct', rule: 'Share of up-days over 90 days.' },
  ] },
  { id: 'osc', label: 'Oscillators', note: 'Need enough consecutive daily bars — the pre-2002 weekly era cannot supply them.', params: [
    { k: 'rsi', rule: 'Wilder RSI(14) — needs 15 bars before the date.' },
    { k: 'macd', rule: 'MACD histogram — needs >35 closes in the trailing 320 days.' },
    { k: 'stoch', rule: 'Stochastic %K(14) — needs closes in the trailing 21 days.' },
    { k: 'bollB', rule: 'Bollinger %b — needs >1 close in the trailing 28 days.' },
  ] },
  { id: 'liquidity', label: 'Liquidity', note: 'Turnover and share-volume, from the bhavcopy.', params: [
    { k: 'turnover', zeroIsNull: true, rule: '20-day average daily turnover. turnAvgAt() returns 0 — never null — when nothing is recorded, so a plain non-null test would read 100% forever; this column counts > 0 only.' },
    { k: 'turnSurge', rule: '5d ÷ 90d turnover — null when the 90d average is 0.' },
    { k: 'volSurge', rule: '5d ÷ 90d share volume — null when the series carries no volume array at all.' },
  ] },
  { id: 'delivery', label: 'Delivery', note: 'Delivered-quantity %, backfilled to 2002.', params: [
    { k: 'delivPct', rule: '20-day average delivery % — null when no bar in the window carries a delivery figure.' },
  ] },
  { id: 'pat', label: 'PAT (point-in-time)', note: 'Quarterly net profit, visible only from its filing date — no look-ahead.', params: [
    { k: 'profitYoyPct', rule: 'Net-profit YoY % for the latest quarter FILED on or before the date, vs the same quarter a year earlier. Null when the year-ago base is exactly 0.' },
    { k: 'profitBase', rule: 'The year-ago quarter\'s net profit itself.' },
    { k: 'profitAccel', rule: 'This quarter\'s YoY minus last quarter\'s YoY.' },
    { k: 'profitStreak', rule: 'Consecutive +YoY quarters — 0 is a real answer, so every row with a filed quarter counts.', zeroOk: true },
  ] },
  { id: 'patttm', label: 'PAT TTM', note: 'Needs 8 consecutive quarters, so it lags the plain YoY by ~2 years.', params: [
    { k: 'profitTTM', rule: 'Last 4 filed quarters vs the 4 before them — needs all 8 present.' },
  ] },
  { id: 'drift', label: 'Post-result drift', note: 'Price move since the last earnings date.', params: [
    { k: 'postDrift', rule: 'Return from the close on the last filing date to the date — needs both a filed quarter and a close on that filing day.' },
  ] },
  { id: 'revenue', label: 'Revenue & margins', src: 'revop', note: 'From sf_revop, made point-in-time with sf_fundamentals\' filing date for the same quarter.', params: [
    { k: 'rev', src: 'revop', rule: 'Revenue (consolidated, else standalone) for the latest quarter filed on or before the date.' },
  ] },
  /* Reporting basis — deliberately its OWN family. The `rev` column above is "consolidated ELSE
   * standalone", so a company that never files consolidated still reads 100% there; that fallback
   * is right for a backtest but it HIDES how thin the consolidated basis is before FY2020. These
   * four decompose it. Kept OUT of the Revenue family because a family cell is its WEAKEST
   * parameter, so folding a ~60%-covered column in would have silently redefined an existing one. */
  { id: 'basis', label: 'Reporting basis', src: 'basis', note: 'Consolidated vs standalone counted SEPARATELY — the rev column merges them with a con-else-std fallback, so it cannot show how thin the consolidated basis is. Each basis is visible from its OWN announce date. NOT-APPLICABLE IS WIRED HERE (changed 2026-08-18). An empty cell means "we hold no figure on that basis at that date"; an N/A cell means the filing record shows there was no such figure to hold - most often a company that filed no consolidated statement at all before quarterly consolidation became compulsory in FY2020. Adjudicated per name against the exchange record plus a second reader, with the evidence in scripts/coverage_na_ledger.json; a name without evidence stays a visible gap. Note that quarterly consolidated results became compulsory only from FY2020 (runbook §51a), so much of the pre-2020 emptiness is a filing that never existed rather than a gap to fill.', params: [
    { k: 'revCon', src: 'basis', rule: 'Consolidated revenue (sf_revop slot 1) for the latest quarter whose CONSOLIDATED result was announced on or before the date (sf_fundamentals idx4 > 0). No N/A is applied — empty means we hold nothing on this basis.' },
    { k: 'revStd', src: 'basis', rule: 'Standalone revenue (sf_revop slot 0) for the latest quarter whose STANDALONE result was announced on or before the date (sf_fundamentals idx2 > 0).' },
    { k: 'patCon', src: 'basis', rule: 'Consolidated net profit (sf_fundamentals idx3, visible from its own announce date idx4). No N/A applied. Net profit has no column of its own anywhere else on this page — the PAT families show only DERIVED measures (YoY, TTM, streak, drift).' },
    { k: 'patStd', src: 'basis', rule: 'Standalone net profit (sf_fundamentals idx1, visible from its own announce date idx2).' },
  ] },
  /* PAT under the backtest's STANDALONE switch. The PAT families above are measured with the
   * engine's DEFAULT basis — 'con', which falls back to standalone FIELD-BY-FIELD — but the
   * dashboard's "Earnings basis" selector is part of a strategy's identity, and a Standalone
   * strategy sees a genuinely different series: profitMetrics('std') reads only idx1/idx2, no
   * fill-in. Measured 2026-08-12 (backtest-engine.js): where both bases resolve, std and con TTM
   * growth agree in SIGN only 86.9% of the time. These columns are factorsAt() run a SECOND time
   * with earnBasis:'std' — through the engine, never re-derived — so each cell is exactly what a
   * standalone-basis strategy could screen on at that date. RAW view like the basis family above:
   * no N/A wired (the blended family's N/A rules were adjudicated per-name against the BLEND and
   * are not assumed to transfer to the std series). */
  /* PAT on the consolidated slots with NO standalone fallback. The dashboard's "Consolidated"
   * setting is the BLEND (con, then std field-by-field — that is what the default PAT families
   * above measure), so pure-con is a series no strategy can screen on; the user asked for it
   * anyway (2026-08-16) as the diagnostic: how much of the blended coverage is secretly leaning
   * on standalone quarters. Measured through the engine like everything else — factorsAt() with
   * earnBasis:'conOnly', an additive measurement-only basis wired into BOTH engine twins
   * (backtest-engine.js + stock-backtest.html, same ternary in profitAt/profitMetrics/
   * lastResultDate; no UI offers it, ENGINE_VER deliberately not bumped because no saved
   * strategy can carry it). RAW view, no N/A, same as the std family below. */
  { id: 'patcon', label: 'PAT con basis', src: 'engcon', note: 'The PAT / PAT TTM / drift / composite columns above are the engine\'s DEFAULT basis, which is a BLEND: consolidated first, standalone filling any hole field-by-field. These columns are PURE consolidated — no fallback — so blend minus con is exactly how much the default screens lean on standalone quarters. No strategy can run on this series (the dashboard offers only the blend and pure-std); it exists as the honest measure of the consolidated basis itself. NOT-APPLICABLE IS WIRED HERE (changed 2026-08-18): quarterly consolidated filing became compulsory only from FY2020 (§51a), and a company that filed no consolidated statement has no cell to fill — so those dates are adjudicated per name, against the exchange filing record and a second reader, and excluded from the denominator rather than counted as gaps. Evidence per name lives in scripts/coverage_na_ledger.json; a name without evidence stays a visible gap on purpose. The same pass retracted 3,583 fabricated consolidated cells that were exact copies of standalone (see scripts/con_copy_retractions.json), which is why this family reads emptier than it did before — that emptiness is the truth the blend was hiding.', params: [
    { k: 'profitYoyCon', src: 'engcon', eng: 'profitYoyPct', rule: 'Consolidated-only net-profit YoY % — latest quarter whose CONSOLIDATED result was announced on or before the date (idx4 > 0, §91c sentinel rule), vs the same quarter a year earlier on the consolidated slot. Null when the year-ago base is exactly 0. Never borrows a standalone value.' },
    { k: 'profitBaseCon', src: 'engcon', eng: 'profitBase', rule: 'The year-ago quarter\'s consolidated net profit. Lockstep with the con-only YoY by construction (profitMetrics answers both or neither).' },
    { k: 'profitAccelCon', src: 'engcon', eng: 'profitAccel', rule: 'This-quarter con-only YoY minus last-quarter con-only YoY — reaches 5 quarters back on the consolidated slot alone.' },
    { k: 'profitTTMCon', src: 'engcon', eng: 'profitTTM', rule: 'Last 4 consolidated quarters vs the 4 before them — all 8 must be consolidated. The gap between this and the blended profitTTM is the std-substitution the default basis performs silently (std and con TTM growth agree in sign only 86.9% where both exist).' },
    { k: 'profitStreakCon', src: 'engcon', eng: 'profitStreak', rule: 'Consecutive positive con-only YoY quarters — 0 is a real answer, so every row where the con series resolves counts.' },
    { k: 'postDriftCon', src: 'engcon', eng: 'postDrift', rule: 'Return since the last CONSOLIDATED announce date (idx4 only) — differs from the blended postDrift where the bases print different dates (3.5% of rows, measured 2026-08-12).' },
    { k: 'compositeCon', src: 'engcon', eng: 'composite', rule: 'z(con-only profitTTM) + z(ret12m) − z(vol), cross-sectional over the same rows — null whenever the con-only TTM is.' },
  ] },
  { id: 'patstd', label: 'PAT std basis', src: 'engstd', note: 'The PAT / PAT TTM / drift / composite columns above are measured under the engine\'s DEFAULT Earnings basis — consolidated, falling back to standalone field-by-field. The backtest dashboard\'s "Earnings basis" switch is part of a strategy\'s identity, and a Standalone strategy sees a different series: no consolidated values, no fill-in (std and con TTM growth agree in sign only 86.9% of the time where both exist). These columns are the same factorsAt() engine pass run again with earnBasis = standalone. NOT-APPLICABLE IS WIRED HERE (changed 2026-08-18), though sparingly: the standalone series is dense, and the only excluded dates are ones where the company had not filed long enough for the parameter to reach - a recent listing whose store is complete from its first filing but which has fewer quarters than the lookback needs. Per-name evidence in scripts/coverage_na_ledger.json.', params: [
    { k: 'profitYoyStd', src: 'engstd', eng: 'profitYoyPct', rule: 'Standalone net-profit YoY % — latest quarter whose STANDALONE result was announced on or before the date (idx2 > 0, §91c sentinel rule), vs the same quarter a year earlier on the standalone slot. Null when the year-ago base is exactly 0. No consolidated fallback anywhere in this family.' },
    { k: 'profitBaseStd', src: 'engstd', eng: 'profitBase', rule: 'The year-ago quarter\'s standalone net profit. Moves in lockstep with the std YoY by construction — profitMetrics(\'std\') answers both or neither — but profitBase is separately screenable on the dashboard, so it keeps its own column.' },
    { k: 'profitAccelStd', src: 'engstd', eng: 'profitAccel', rule: 'This-quarter standalone YoY minus last-quarter standalone YoY — reaches 5 quarters back on the std slot alone.' },
    { k: 'profitTTMStd', src: 'engstd', eng: 'profitTTM', rule: 'Last 4 standalone quarters vs the 4 before them — needs all 8 on the std slot. The blended profitTTM can substitute a standalone value into any hole; this one cannot substitute anything, so it is the honest availability for a Standalone-basis strategy.' },
    { k: 'profitStreakStd', src: 'engstd', eng: 'profitStreak', rule: 'Consecutive positive standalone-YoY quarters — 0 is a real answer, so every row where the std series resolves counts.' },
    { k: 'postDriftStd', src: 'engstd', eng: 'postDrift', rule: 'Return since the last STANDALONE announce date — under the std switch lastResultDate() reads only idx2, and the two bases print different dates on 3.5% of rows (measured 2026-08-12), so this can differ from the blended postDrift.' },
    { k: 'compositeStd', src: 'engstd', eng: 'composite', rule: 'z(std profitTTM) + z(ret12m) − z(vol), cross-sectional over the same rows — null whenever the std TTM is. What a composite screen ranks on with the Standalone switch set.' },
  ] },
  { id: 'fii', label: 'FII', note: 'Quarterly shareholding filings, visible from their submission date.', params: [
    { k: 'fiiPct', rule: 'FII holding % from the latest SHP filing submitted on or before the date.' },
    { k: 'fiiChgPp', rule: 'QoQ change in pp — needs the calendar-previous quarter too, and is deliberately never computed across the Sep-2022 SEBI reclassification (DR blocks moved into FII/DII — a paperwork change, not a stake change). Rows whose visible filing IS 2022-09-30 are N/A, not missing: that is a refusal, not a gap. Companies whose visible filing is a mid-quarter event row dated after 30-Sep keep a real delta and stay in the denominator.' },
  ] },
  { id: 'dii', label: 'DII', note: 'Same filings as FII.', params: [
    { k: 'diiPct', rule: 'DII holding % from that same filing.' },
    { k: 'diiChgPp', rule: 'QoQ change in pp — same previous-quarter, Sep-2022 refusal and N/A rules as FII.' },
  ] },
  { id: 'industry', label: 'Industry', src: 'meta', note: 'Classification is a CURRENT attribute, not point-in-time — it answers "is this stock classified today", asked of that date\'s members.', params: [
    { k: 'industry', src: 'meta', rule: 'The dataset\'s industry for the symbol is known (not "Unknown"/"Other").' },
  ] },
  { id: 'composite', label: 'QM composite', note: 'Derived ranking — needs TTM growth, 12m return and volatility together.', params: [
    { k: 'composite', rule: 'Cross-sectional z(profitTTM) + z(ret12m) − z(vol); null when any input is missing.' },
  ] },
];
// flat parameter list, in column order
const PARAMS = [];
FAMILIES.forEach(f => f.params.forEach(p => PARAMS.push({ ...p, fam: f.id })));
const NP = PARAMS.length;

/* Parameters with NO point-in-time history at all — stated on the page rather than
 * rendered as a column of zeroes (mcap is verified min=max=0 in survivorship-free mode). */
const NO_HISTORY = [
  { k: 'mcap', why: 'Always 0 in survivorship-free mode — the NSE bhavcopy carries no market cap. Use Turnover for size.' },
  { k: 'hist_mcap', why: 'Same: always 0. Never filter or sort on it.' },
  { k: 'promoter / public holding', why: 'shareholding.json holds only the last 8 quarters; the engine-facing shp_engine.json carries FII/DII only, so there is no long history to count.' },
];

/* ============================================================================
 * UNIVERSES
 * ========================================================================== */
const LIQUID_FLOOR = 100;   // ₹ lacs of 20d average daily turnover = ₹1 crore/day

function readJSON(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function readGz(p) { return JSON.parse(zlib.gunzipSync(fs.readFileSync(p))); }

const MIN_BIN_BYTES = 5e7;   // the live asset is ~193 MB; anything this small is an error page

/* Why a downloaded file can exist and still be junk: `curl -s` WITHOUT `--fail` exits 0 on an
 * HTTP error and writes the error BODY to -o. gunzip then dies with "incorrect header check",
 * naming neither the download nor the status — exactly how the 2026-08-12 nightly failed, while
 * the release asset itself was intact and served a valid gzip minutes either side. Returns a
 * human-readable complaint, or null when the file is a plausible gzip. update_sf_data.py fetches
 * the same asset and has always retried and aborted loudly; this builder was the one caller
 * that did not. */
function gzipComplaint(p) {
  if (!fs.existsSync(p)) return 'no file was written';
  const size = fs.statSync(p).size;
  if (size < MIN_BIN_BYTES) {
    const head = fs.readFileSync(p).subarray(0, 300).toString('utf8').replace(/\s+/g, ' ').trim();
    return `only ${size} bytes (expected >${(MIN_BIN_BYTES / 1e6) | 0}MB) — body starts: ${head || '(empty)'}`;
  }
  const fd = fs.openSync(p, 'r'), magic = Buffer.alloc(2);
  fs.readSync(fd, magic, 0, 2, 0); fs.closeSync(fd);
  if (magic[0] !== 0x1f || magic[1] !== 0x8b) return `${size} bytes but not gzip (magic 0x${magic.toString('hex')})`;
  return null;
}

const sleepSync = ms => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

/* Download the live asset, retrying transient failures. -f makes curl exit non-zero on >=400 (so
 * execFileSync throws a message that names the status); -S keeps that message visible under -s.
 * A 200 carrying a non-gzip body still slips past curl, so every attempt is validated before it
 * is accepted, and a rejected file is DELETED — never left behind for the next run's cache check
 * to mistake for a good bin. */
function downloadBin(dest) {
  const TRIES = 4;
  for (let attempt = 1; attempt <= TRIES; attempt++) {
    let complaint;
    try {
      execFileSync('curl', ['-fsSL', '--connect-timeout', '20', '--max-time', '900',
                            '-o', dest, RELEASE_BIN], { stdio: 'inherit' });
      complaint = gzipComplaint(dest);
      if (!complaint) return;
    } catch (e) {
      complaint = `curl failed: ${String(e.message).split('\n')[0]}`;
    }
    log(`download attempt ${attempt}/${TRIES} rejected — ${complaint}`);
    try { fs.unlinkSync(dest); } catch { /* nothing to clean up */ }
    if (attempt < TRIES) sleepSync(attempt * 15000);   // 15s → 30s → 45s
  }
  throw new Error(`could not download a valid ${RELEASE_BIN} after ${TRIES} attempts`);
}

function resolveBin() {
  if (BIN_ARG !== 'auto') {
    const p = path.resolve(ROOT, BIN_ARG);
    log('using bin', p);
    return readGz(p);
  }
  const cache = path.join(os.tmpdir(), 'sf_stock_data_live.bin');
  const stale = gzipComplaint(cache);
  if (stale) {
    if (fs.existsSync(cache)) log('discarding unusable cached bin —', stale);
    log('downloading LIVE release asset (the in-repo bin is a frozen snapshot)…');
    downloadBin(cache);
  } else {
    log('reusing cached live bin', cache, (fs.statSync(cache).size / 1e6).toFixed(0) + 'MB');
  }
  return readGz(cache);
}

/* ---- month-end date list, snapped to the last trading day (same rule as simulate()) ---- */
function monthEnds(firstISO, lastISO) {
  const out = [];
  let y = +firstISO.slice(0, 4), m = +firstISO.slice(5, 7);
  const ey = +lastISO.slice(0, 4), em = +lastISO.slice(5, 7);
  while (y < ey || (y === ey && m <= em)) {
    out.push(new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10));
    m++; if (m > 12) { m = 1; y++; }
  }
  return out;
}

/* ============================================================================
 * MAIN
 * ========================================================================== */
function main() {
  const t0 = Date.now();

  log('reading dash_slim.bin…');
  const D = readGz(path.join(DOCS, 'dash_slim.bin'));
  const IDXH = D.indicesHistory || {}, FNOH = D.fnoHistory || [], START_TS = D.startTs;

  log('reading sf bin…');
  const RAW = resolveBin();
  log('  bin end =', RAW.end, '· symbols =', Object.keys(RAW.data).length);

  const NIFTY = (readJSON(path.join(DOCS, 'nifty.json')).px) || {};
  const REVOP = readJSON(path.join(DOCS, 'sf_revop.json'));
  const FUNDJ = readJSON(path.join(DOCS, 'sf_fundamentals.json'));
  /* Adjudicated NOT-APPLICABLE verdicts (scripts/coverage_na_ledger.json). Deliberately NOT a list
   * of names in this file: an N/A is a claim about a specific filer, and the claim's evidence has
   * to live beside it where a later session can audit or overturn it. Absent file = no verdicts and
   * the page is unchanged, so the ledger can never fail open into hiding a real gap. */
  const NA_LEDGER = (() => {
    const p = path.join(ROOT, 'scripts', 'coverage_na_ledger.json');
    if (!fs.existsSync(p)) { log('na ledger: absent — no N/A verdicts applied'); return {}; }
    const j = JSON.parse(fs.readFileSync(p, 'utf8'));
    const out = {};
    for (const k in j) if (!k.startsWith('_')) out[k] = j[k];
    log('na ledger: ' + (Object.keys(out).map(k => `${k}=${Object.keys(out[k]).length}`).join(' ') || 'empty'));
    return out;
  })();
  // param -> sym -> entry, honouring optional from/to bounds on a verdict
  const naLedgerHit = (param, sym, iso) => {
    const e = NA_LEDGER[param] && NA_LEDGER[param][sym];
    if (!e) return false;
    if (e.from && iso < e.from) return false;
    if (e.to && iso > e.to) return false;
    return true;
  };

  /* ---- context: the live engine, verbatim ---- */
  const ctx = vm.createContext({
    console, Date, Math, JSON, Object, Array, Number, String, Set, Map, isFinite, parseInt, parseFloat,
    location: { hostname: 'build', protocol: 'file:' },
    localStorage: { getItem: () => null, setItem: () => {} },
    // loadFund()/loadShp() run verbatim against local files — no second copy of the
    // FUND_ALIAS merge to keep in sync.
    fetch: async (url) => {
      const f = path.join(DOCS, String(url).replace(/^\.\//, ''));
      return { ok: fs.existsSync(f), json: async () => readJSON(f) };
    },
  });
  vm.runInContext(fs.readFileSync(path.join(DOCS, 'backtest-engine.js'), 'utf8'), ctx,
    { filename: 'backtest-engine.js' });

  // hand the raw payload over WITHOUT copying it through the sandbox boundary twice
  ctx.__RAW = RAW; ctx.__D = { idxh: IDXH, fnoh: FNOH, startTs: START_TS, nifty: NIFTY, end: RAW.end };

  log('normalising series through the engine\'s own _sfNorm…');
  vm.runInContext(`
    START_TS = __D.startTs; IDXH = __D.idxh; FNOH = __D.fnoh; NIFTY = __D.nifty;
    (function () {
      const ser = {}, meta = {}, turn = {};
      for (const sym in __RAW.data) {
        const r = _sfNorm(__RAW.data[sym], __D.startTs);
        ser[sym] = r.ser; turn[sym] = { d: r.ser.d, t: r.t };
        meta[sym] = _sfMeta(sym, __RAW.meta[sym] || {}, r.lastClose);
        delete __RAW.data[sym];                       // free as we go — the live bin is ~193 MB gz
      }
      SERIES = ser; META = meta; TURN = turn;
      SF_END_OFF = Math.floor((Date.parse(__D.end + 'T00:00:00Z') / 1000 - __D.startTs) / 86400);
    })();
  `, ctx);
  ctx.__RAW = null;
  if (global.gc) global.gc();

  vm.runInContext('__loaded = (async () => { await loadFund(); await loadShp(); return [Object.keys(FUND).length, Object.keys(SHPD).length]; })();', ctx);

  return ctx.__loaded.then(([nf, ns]) => {
    log(`engine ready · ${Object.keys(vm.runInContext('META', ctx)).length} symbols · FUND ${nf} · SHPD ${ns}`);
    run(ctx, { IDXH, FNOH, START_TS, REVOP, FUNDJ, end: RAW.end, t0, naLedgerHit });
  });
}

function run(ctx, C) {
  const { IDXH, FNOH, START_TS, REVOP, FUNDJ, end, naLedgerHit } = C;

  /* ---- revenue visibility index: QE -> filing date, from sf_fundamentals ------------------
   * sf_revop carries no filing date of its own. sf_fundamentals does (idx2 std / idx4 con) for
   * the SAME quarter, so a revenue cell becomes visible on the day that quarter's result was
   * announced. Quarters with no fundamentals row have no known filing date and are NOT counted
   * (an unknown date is not a licence to assume "always visible").
   *
   * ⚠️ `> 0`, NOT `!= null` (runbook §91c). ann = 0 is the documented "announce date UNKNOWN"
   * sentinel, and `0 != null` is TRUE in JS — that exact test put a 23-year look-ahead into four
   * production selection loops. A falsy sentinel needs a truthy test. Here it would also shadow
   * the good basis: min(0, 20240515) = 0, dropping a quarter whose other basis IS dated. */
  const REV_INDEX = {};                    // SYM -> [[annInt, qeStr], …] ascending by annInt
  for (const sym in REVOP) {
    const fRows = FUNDJ[sym] || [];
    const annOf = {};                       // qeInt -> earliest known announce date
    for (const q of fRows) {
      const a = [q[2], q[4]].filter(x => x > 0);
      if (a.length) annOf[q[0]] = Math.min(...a);
    }
    const out = [];
    for (const qe in REVOP[sym]) {
      const ann = annOf[+qe];
      if (ann) out.push([ann, qe]);
    }
    if (out.length) { out.sort((a, b) => a[0] - b[0]); REV_INDEX[sym] = out; }
  }
  // sf_fundamentals row by quarter-end, for the PAT value lookup (basis family)
  const FUND_BY_QE = {};
  for (const sym in FUNDJ) {
    const m = {};
    for (const q of FUNDJ[sym] || []) m[String(q[0])] = q;
    FUND_BY_QE[sym] = m;
  }
  // BASIS_IDX is built AFTER FIRSTBAR below — it has to gate on the first traded bar (§99).

  /* ---- NO N/A ON THE BASIS FAMILY, DELIBERATELY (user, 2026-08-16: "dont wire NA, add empty
   * values. NA part i'll check later") ------------------------------------------------------
   * An empty consolidated cell here means EXACTLY "we hold no consolidated figure at this date",
   * with nothing excluded from the denominator. That is the raw view the user asked to inspect
   * before any not-applicable policy is decided.
   *   Context for whoever wires it later — quarterly consolidated results only became compulsory
   * from FY2020 (runbook §51a), so much of the pre-2020 emptiness is a filing that never existed
   * rather than a backfill miss, and `scripts/no_con_filing.json` already holds per-company
   * verdicts. ⚠️ That ledger CANNOT be applied as written: measured 2026-08-16 against
   * sf_fundamentals, 344 of its 760 `never_filed_con` names hold a dated con PAT and 133 of the
   * 200 `started_filing_con` names hold one BEFORE their declared start. The split that resolves
   * it is DIVERGENCE, not presence — for 326 of those 344, con == std to the paisa on every
   * quarter (the con-slot-holds-a-copy defect, which is what the ledger's own build test keyed
   * on), but 18 never-filed names genuinely diverge, and 130 of the 133 pre-start values diverge,
   * typically the four FY2019 quarters from 20180630 — a full year before the mandate. So any
   * future N/A pass needs a guard that refuses a verdict wherever a divergent consolidated figure
   * of ours already covers that quarter (3,884 verdicts were refused when this was trialled). */

  const FUND_ALIAS = vm.runInContext('FUND_ALIAS', ctx);
  const revFor = sym => REV_INDEX[sym] || (FUND_ALIAS[sym] ? REV_INDEX[FUND_ALIAS[sym]] : null) || null;
  const revopFor = sym => REVOP[sym] || (FUND_ALIAS[sym] ? REVOP[FUND_ALIAS[sym]] : null) || null;
  const basisFor = sym => BASIS_IDX[sym] || (FUND_ALIAS[sym] ? BASIS_IDX[FUND_ALIAS[sym]] : null) || null;
  const fundQeFor = sym => FUND_BY_QE[sym] || (FUND_ALIAS[sym] ? FUND_BY_QE[FUND_ALIAS[sym]] : null) || null;
  const lastVisible = (arr, dateInt) => {           // latest [ann, qe] with ann <= dateInt
    if (!arr) return null;
    for (let i = arr.length - 1; i >= 0; i--) if (arr[i][0] <= dateInt) return arr[i];
    return null;
  };

  /* First REAL filing date per symbol: min announce date that is not before the symbol's first
   * traded bar (§99 — scheme/prospectus carry-ins are stamped pre-listing qe+45d defaults and are
   * not filings). Lets the revenue family mark "nothing about this entity was public yet" as N/A,
   * the same rule the profit family applies in-vm. NSLNISP @2023-04-28 is the measured case. */
  const FIRSTBAR = vm.runInContext(`(function(){ const o = {};
    for (const t in SERIES) { const m = META[t]; if (!m) continue; const s = SERIES[t];
      if (s && s.d && s.d.length) o[m.symbol] = +isoOff(s.d[0]).replace(/-/g, ''); }
    return o; })()`, ctx);
  const FIRSTREAL = {};
  for (const sym in FUNDJ) {
    const fb = FIRSTBAR[sym];
    let fr = 0;
    for (const q of FUNDJ[sym]) {
      for (const a of [q[2], q[4]]) {
        if (a > 0 && (fb == null || a >= fb) && (!fr || a < fr)) fr = a;
      }
    }
    if (fr) FIRSTREAL[sym] = fr;
  }
  const firstRealAnn = sym => FIRSTREAL[sym] || (FUND_ALIAS[sym] ? FIRSTREAL[FUND_ALIAS[sym]] : null) || null;

  /* ---- PER-BASIS index: each basis visible from ITS OWN announce date -----------------------
   * The `rev` column resolves ONE quarter with min(annStd, annCon) and takes con-else-std from it.
   * That answers "could a backtest see a revenue number", which is right for that column but merges
   * the two bases. The `basis` family asks a different question — "was the CONSOLIDATED number
   * itself available" — so a consolidated cell is gated on the consolidated filing's own date
   * (idx4), never on a standalone one that can precede it by weeks.
   *   Two gates on every announce date, and both are load-bearing:
   *   `a > 0`  — §91c, ann = 0 is the UNKNOWN sentinel and `0 != null` is true in JS.
   *   `a >= FIRSTBAR[sym]` — §99, the SAME rule annOk applies in-vm. Pre-listing qe+45d stamps
   *     from prospectus/scheme carry-ins are not filings, and counting one as "visible" would
   *     make this column report data a strategy could not have seen. 5,247 such stamps still
   *     exist repo-wide, so an ungated index would have re-imported the look-ahead class the
   *     profit family just removed.
   *   BASIS_IDX[sym] = { con: [[ann, qeStr], …], std: [[ann, qeStr], …] }, each ascending. */
  const BASIS_IDX = {};
  let basisAnnDropped = 0;
  for (const sym in FUNDJ) {
    const fb = FIRSTBAR[sym];
    const con = [], std = [];
    for (const q of FUNDJ[sym] || []) {
      if (q[4] > 0) { if (fb == null || q[4] >= fb) con.push([q[4], String(q[0])]); else basisAnnDropped++; }
      if (q[2] > 0) { if (fb == null || q[2] >= fb) std.push([q[2], String(q[0])]); else basisAnnDropped++; }
    }
    if (con.length || std.length) {
      con.sort((a, b) => a[0] - b[0]); std.sort((a, b) => a[0] - b[0]);
      BASIS_IDX[sym] = { con, std };
    }
  }
  log(`basis family: ${basisAnnDropped} pre-listing announce stamps ignored (§99 look-ahead gate)`);

  /* ---- dates ---- */
  const dayOff = d => vm.runInContext(`dayOff(${JSON.stringify(d)})`, ctx);
  let dates = monthEnds(FROM + '-01', end.slice(0, 7));
  // trading-day snap: the engine's own convention (simulate()), so a month-end on a
  // weekend/holiday is priced off the same bar a real rebalance would use.
  const tdset = vm.runInContext(`(function(){const s=new Set();
    for (const r of ['RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','ITC','SBIN','LT']) { const x=SERIES[r]; if (x&&x.d) for (const o of x.d) s.add(o); }
    return [...s].sort((a,b)=>a-b); })()`, ctx);
  const idxLE = (arr, v) => { let lo = 0, hi = arr.length - 1, a = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (arr[m] <= v) { a = m; lo = m + 1; } else hi = m - 1; } return a; };
  const endOff = dayOff(end);
  dates = dates.map(d => {
    const o0 = dayOff(d), i = idxLE(tdset, Math.min(o0, endOff));
    return i < 0 ? null : { iso: vm.runInContext(`isoOff(${tdset[i]})`, ctx), off: tdset[i], label: d };
  }).filter(Boolean);
  // a month whose snap lands in an earlier month has no trading day yet — drop it
  dates = dates.filter(d => d.iso.slice(0, 7) === d.label.slice(0, 7));
  if (MAX_DATES) dates = dates.slice(-MAX_DATES);
  log(`scanning ${dates.length} month-ends · ${dates[0].iso} → ${dates[dates.length - 1].iso}`);

  /* ---- universes ---- */
  const firstSnap = list => (list && list.length) ? list.map(s => s.effectiveDate).sort()[0] : null;
  const UNIVERSES = [
    { slug: 'all', name: 'All listed', kind: 'all', note: 'Every symbol in the survivorship-free dataset that is tradeable at the date (delisted names included, DVR shares excluded). The denominator is that count — there is no external roll to check against.' },
    { slug: 'liquid', name: 'Liquid (≥₹1cr/day)', kind: 'liquid', note: 'All listed, filtered to ≥₹1 crore of 20-day average daily turnover at the date.' },
    { slug: 'fno', name: 'F&O stocks', kind: 'index', index: '__FNO__', from: firstSnap(FNOH), note: 'NSE derivatives eligibility as of the date.' },
  ];
  Object.keys(IDXH).forEach(n => UNIVERSES.push({
    slug: n.toLowerCase().replace(/[^a-z0-9]+/g, '-'), name: n, kind: 'index', index: n,
    from: firstSnap(IDXH[n]), note: 'Point-in-time index membership from indicesHistory.',
  }));

  /* Membership sets per universe per date — via the engine's own membersAsOf, so the roll is
   * resolved exactly as a backtest resolves it.
   *
   * ⚠️ ONE DELIBERATE DEPARTURE. lastSnap() ends `best || list[0]`: asked for a date BEFORE the
   * first snapshot it hands back the EARLIEST roll rather than nothing. For a backtest that is a
   * reasonable fallback; for a coverage chart it is fabricated membership — only Nifty 500's roll
   * reaches 2002, so every other index would show 13-19 years of "members" it never had (Nifty
   * Healthcare's roll starts 2021-03-31). Dates before a universe's first snapshot are recorded
   * as -1 = no roll, and the page prints them as "–". */
  const memberSets = {};
  for (const u of UNIVERSES) {
    if (u.kind !== 'index') continue;
    memberSets[u.slug] = dates.map(d => {
      if (u.from && d.iso < u.from) return null;
      const arr = vm.runInContext(`(function(){const s = membersAsOf(${JSON.stringify(u.index)}, ${JSON.stringify(d.iso)}); return s ? [...s] : null;})()`, ctx);
      return arr ? new Set(arr) : null;
    });
  }

  /* ---- the scan ------------------------------------------------------------------------
   * One factorsAt() call per date over the WHOLE market (indexName null), then every universe
   * is a mask over those rows. Costs one expensive pass instead of 30. */
  const zero = () => new Int32Array(NP);
  const counts = {};   // slug -> [date][param] = covered count
  const members = {};  // slug -> [date] = universe size at that date
  const naCounts = {};   // slug -> [date][param] = NOT-APPLICABLE count (excluded from the denominator)
  for (const u of UNIVERSES) { counts[u.slug] = dates.map(zero); naCounts[u.slug] = dates.map(zero); members[u.slug] = new Int32Array(dates.length); }

  const CFG = {
    indexName: null, mcapFloor: 0, earnBasis: 'con', sortBy: 'composite',
    // factorsAt never APPLIES filters (screenAsOf does) — listing one field per family here is
    // simply how needsTech/needsFund/needsShp are switched on for every family at once.
    filters: [{ field: 'fiiPct' }, { field: 'profitTTM' }, { field: 'ret12m' }],
  };
  ctx.__CFG = CFG;
  // Second engine pass for the PAT-std family: the same CFG with the dashboard's Standalone
  // switch flipped. fiiPct is dropped — SHP is basis-independent and already measured by the
  // main pass, so skipping shpAt() here is pure saving. sortBy stays 'composite' so this pass
  // computes compositeStd's cross-sectional z-scores exactly as a std-basis screen would.
  const CFG_STD = {
    indexName: null, mcapFloor: 0, earnBasis: 'std', sortBy: 'composite',
    filters: [{ field: 'profitTTM' }, { field: 'ret12m' }],
  };
  ctx.__CFG_STD = CFG_STD;
  // Third engine pass for the PAT-con family: 'conOnly' is the measurement-only basis added to
  // both engine twins — consolidated slots, no standalone fallback. Same filter/sortBy shape as
  // the std pass and for the same reasons.
  const CFG_CON = {
    indexName: null, mcapFloor: 0, earnBasis: 'conOnly', sortBy: 'composite',
    filters: [{ field: 'profitTTM' }, { field: 'ret12m' }],
  };
  ctx.__CFG_CON = CFG_CON;
  // engine field name behind each PAT-std / PAT-con column, in family order
  const STD_ENG_KEYS = PARAMS.filter(p => p.src === 'engstd').map(p => p.eng);
  const CON_ENG_KEYS = PARAMS.filter(p => p.src === 'engcon').map(p => p.eng);

  const revopIdx = { rev: [1, 0], op: [3, 2], ebit: [8, 7] };   // [con, std] slots in sf_revop
  let lastLog = 0;
  for (let di = 0; di < dates.length; di++) {
    const d = dates[di];
    ctx.__OFF = d.off; ctx.__DATEINT = +d.iso.replace(/-/g, '');
    // pull only what we need across the boundary: symbol + one flag per engine parameter
    const rows = vm.runInContext(`(function(){
      const rows = factorsAt(__OFF, __CFG), keys = ${JSON.stringify(PARAMS.filter(p => !p.src).map(p => p.k))};
      const zeroIsNull = ${JSON.stringify(PARAMS.filter(p => !p.src).map(p => !!p.zeroIsNull))};
      // PAT-std: the SAME engine run again with the dashboard's Standalone basis switch on.
      // The row set is identical by construction (basis changes only r.profit* values, never the
      // price/freshness gates that admit a row) — so a length mismatch means the join below
      // would silently mislabel stocks; fail the bake loudly instead.
      const stdRows = factorsAt(__OFF, __CFG_STD), stdKeys = ${JSON.stringify(STD_ENG_KEYS)};
      if (stdRows.length !== rows.length) throw new Error('std-basis pass row-set mismatch at off=' + __OFF + ': ' + stdRows.length + ' vs ' + rows.length);
      const stdByTkr = new Map(); for (const s of stdRows) stdByTkr.set(s.tkr, s);
      // PAT-con: the same engine once more with the 'conOnly' measurement basis — pure
      // consolidated, no standalone fallback. Same row-set identity, same loud guard.
      const conRows = factorsAt(__OFF, __CFG_CON), conKeys = ${JSON.stringify(CON_ENG_KEYS)};
      if (conRows.length !== rows.length) throw new Error('con-basis pass row-set mismatch at off=' + __OFF + ': ' + conRows.length + ' vs ' + rows.length);
      const conByTkr = new Map(); for (const s of conRows) conByTkr.set(s.tkr, s);
      const out = new Array(rows.length);
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i], flags = new Array(keys.length);
        for (let j = 0; j < keys.length; j++) {
          const v = fieldVal(r, keys[j]);
          const ok = (v != null && typeof v === 'number' && isFinite(v)) && !(zeroIsNull[j] && v === 0);
          flags[j] = ok ? 1 : 0;
        }
        // ---- NOT-APPLICABLE, as distinct from MISSING -------------------------------------
        // A cell the stock COULD NOT have is not a coverage gap, and counting it as one makes the
        // page ask for something that cannot exist. postDrift is the worked case: it is the return
        // from the close on the last filing date, so it needs a PRICE on that date. A newly listed
        // company carries pre-listing quarters (restated accounts from its prospectus or, for
        // NSLNISP, the NMDC demerger scheme) whose announce dates precede its first bar — the
        // engine then correctly asks for a close on a day the stock did not trade and correctly
        // returns null. NSLNISP at 2023-04-28 resolves to 2021-05-30, nine months before it listed.
        // Marked N/A, excluded from the denominator, and reported separately. Nothing is invented.
        const na = new Array(keys.length).fill(0);
        // fiiPct / diiPct: a company's FIRST-EVER shareholding filing lands AFTER the quarter it
        // covers — SEBI Reg 31 allows 21 days — so at a month-end that IS (or snaps to) that first
        // quarter-end, no shareholding exists for it anywhere yet. CAMS' first filing covers
        // 2021-03-31 and was submitted 2021-04-15; VALIANTORG 6 days after; HLEGLAS 12; KIRLFER 21;
        // TBOTEK 17. Counting those as gaps asks us to fill a number that would be a look-ahead.
        // Gated on the EARLIEST filing this symbol has: once anything has been filed, a later hole
        // is a real gap again. ⚠️ This runs AFTER the bulk-stamp date corrections landed in
        // scripts/shp_cell_fix.json — before them AFFLE's earliest filing read 2020-07-15 and this
        // rule would have marked 3 genuine defects N/A, hiding them behind their own symptom.
        const shp = (typeof SHPD !== 'undefined' && SHPD) ? (SHPD[r.sym] || (typeof FUND_ALIAS !== 'undefined' && FUND_ALIAS[r.sym] ? SHPD[FUND_ALIAS[r.sym]] : null)) : null;
        if (shp && shp.length) {
          let firstSub = 0;
          for (const q of shp) if (q[3] > 0 && (!firstSub || q[3] < firstSub)) firstSub = q[3];
          if (firstSub > __DATEINT) {
            ['fiiPct', 'diiPct', 'fiiChgPp', 'diiChgPp'].forEach(function (k) {
              const j = keys.indexOf(k);
              if (j >= 0 && !flags[j]) na[j] = 1;
            });
          }
          // fiiChgPp / diiChgPp: the engine NEVER computes a QoQ change across the Sep-2022 SEBI
          // reclassification — depository-receipt blocks were moved into the FII/DII buckets, so
          // the delta would report a paperwork change as a stake change (backtest-engine.js:689,
          // the cur[0] !== 20220930 guard). Where the visible filing IS 2022-09-30 the change is not
          // missing data, it is a refusal, and the page must not ask anyone to fill it. Gated on
          // the row the engine would actually read, so the handful of companies whose visible
          // filing is a mid-quarter EVENT row dated after 30-Sep (§22k) keep a real, computable
          // delta and stay in the denominator: Nifty 500 goes 4/500, 8/500, 14/500 -> 4/4, 8/8,
          // 14/14 across Oct/Nov/Dec-2022 rather than reading ~0%.
          let cur = null;
          for (let i = shp.length - 1; i >= 0; i--) { if (shp[i][3] > 0 && shp[i][3] <= __DATEINT) { cur = shp[i]; break; } }
          if (cur && cur[0] === 20220930) {
            ['fiiChgPp', 'diiChgPp'].forEach(function (k) {
              const j = keys.indexOf(k);
              if (j >= 0 && !flags[j]) na[j] = 1;
            });
          }
          // LATE-FILED PRIOR is the same refusal, found by the SHP session (2026-08-16) while
          // validating this page's 174-cell residue: in 62 of them the calendar-previous
          // quarter's row EXISTS but was SUBMITTED after this screen date (HINDALCO's Sep-2019
          // SHP filed 2020-09-21, eight months after its Dec-2019 filing) — shpAt correctly
          // refuses it, because a live trader could not compute a delta from a document that did
          // not exist yet. Filling would be a look-ahead; the metric is N/A at these dates and
          // computes on its own once the late submission date passes. Spot-verified against
          // shp_engine before wiring (peer count: 62 of 62, no exceptions).
          if (cur && cur[0] !== 20220930 && isQuarterEnd(cur[0])) {
            const pq = prevQeInt(cur[0]);
            let prior = null;
            for (let i = 0; i < shp.length; i++) if (shp[i][0] === pq) { prior = shp[i]; break; }
            if (prior && prior[3] > __DATEINT) {
              ['fiiChgPp', 'diiChgPp'].forEach(function (k) {
                const j = keys.indexOf(k);
                if (j >= 0 && !flags[j]) na[j] = 1;
              });
            } else if (!prior) {
              // PRE-LISTING PRIOR — the same refusal one step earlier, and the case the rule above
              // cannot see. There the prior row EXISTS and was filed late; here it does not exist
              // at all, because the company was not listed yet and never filed a shareholding
              // pattern for that quarter. BSE correctly returns nothing: there is no document to
              // fetch and the delta cannot be computed by anyone. This is the 2021-2025 IPO cohort
              // (PAYTM, NYKAA, LODHA, SWIGGY, HYUNDAI, MEESHO, GROWW …) plus every later listing.
              //
              // TWO gates, and the SECOND one is load-bearing. "Our earliest shp row is later than
              // the prior quarter" is a claim about OUR OWN DATA, and a hole in our history looks
              // exactly like a company that had not listed — the circular inference §57a forbids.
              // So the TAPE arbitrates independently: the prior quarter must also fall before the
              // symbol's first traded bar. Measured 2026-08-16 against the authoritative --explain
              // (§92, same vm scan that writes the payload): 79 of the 87 residue member-dates
              // match the shp shape, and 78 of those are pre-listing on the tape. The one that is
              // NOT is HLEGLAS — first traded 2021-02-22, prior quarter 2021-09-30, seven months
              // after it listed — a genuine missing Sep-2021 filing. Without the tape gate this
              // rule would have marked that real defect N/A and hidden it behind its own symptom.
              // No tape row at all means no second reader, so the verdict is withheld, not assumed.
              // The symbol's OWN earliest row. loadShp() (backtest-engine.js:678-683) merges a
              // renamed/merged predecessor's whole era under the CURRENT key — filings really were
              // made under the old name — so SHPD['PIRAMALFIN'] carries DHFL's 37 rows back to
              // 2011-12-31 and a naive min() answers a question about a DIFFERENT COMPANY. That is
              // why the first cut of the IPO-anchor reader below never fired for the one case it
              // was written for. Subtract the predecessor's quarters, so "earliest" means earliest
              // for THIS entity; with no alias the set is empty and this is a plain min().
              const predQ = {};
              if (typeof FUND_ALIAS !== 'undefined') {
                for (const old in FUND_ALIAS) {
                  if (FUND_ALIAS[old] === r.sym && SHPD[old]) {
                    for (const z of SHPD[old]) predQ[z[0]] = 1;
                  }
                }
              }
              const ps = String(pq);
              const pqOff = dayOff(ps.slice(0, 4) + '-' + ps.slice(4, 6) + '-' + ps.slice(6, 8));
              const serP = (SERIES[r.tkr] && SERIES[r.tkr].d && SERIES[r.tkr].d.length) ? SERIES[r.tkr].d[0] : null;
              // …and a PRE-LISTING row pollutes "earliest" the same way an alias does. VALIANTORG
              // holds an shp row dated 2019-05-04 while it first traded 2020-10-05 — a pre-IPO/SAST
              // disclosure — so a naive min() returned 2019 and the rule refused to fire even though
              // the tape says the company was not listed at the Sep-2020 prior quarter and no
              // shareholding pattern could exist for it. Same §99 gate the profit family applies to
              // announce dates: a row dated before the first traded bar is not this listed entity's
              // filing history. Both exclusions are about the SAME question — is this row ours, now?
              const ownRow = (q) => {
                if (!(q > 0) || predQ[q]) return false;
                if (serP == null) return true;
                const qs = String(q);
                return dayOff(qs.slice(0, 4) + '-' + qs.slice(4, 6) + '-' + qs.slice(6, 8)) >= serP;
              };
              let firstQe = 0;
              for (const q of shp) if (ownRow(q[0]) && (!firstQe || q[0] < firstQe)) firstQe = q[0];
              // Second reader, alias-aware: FUND_ALIAS bridges a predecessor's tape into a merged
              // entity's ticker (DHFL/DEWANHOUS → PIRAMALFIN), so for those the series start is the
              // OLD company's 1990s bar and the tape can never confirm a fresh listing. The SHP
              // feed carries its own listing record for exactly this case: an IPO-anchor EVENT row
              // (§22k) — a non-quarter-end row dated at listing and submitted within days of that
              // date (PIRAMALFIN: 20251107, submitted 20251111 — a real filed document, not an
              // inference from absence). Either reader confirming pre-listing suffices; both
              // silent → verdict withheld, the cell stays visible.
              // CALENDAR arithmetic, deliberately NOT dayOff. dayOff maps a date to a TRADING
              // SESSION offset; an IPO-anchor date or its submission date can be a day the session
              // set does not contain, and the comparison then yields no verdict silently — which is
              // exactly how the first cut of this rule shipped as dead code, marking nothing.
              // Plain date maths asks the question actually being asked: was this filed within days
              // of the listing event.
              const ordOf = function (n) {
                return Date.UTC(Math.floor(n / 10000), (Math.floor(n / 100) % 100) - 1, n % 100) / 86400000;
              };
              let ipoAnchorOk = false;
              if (firstQe && !isQuarterEnd(firstQe) && firstQe > pq) {
                for (const q of shp) {
                  if (q[0] === firstQe && q[3] > 0) {
                    const lag = ordOf(q[3]) - ordOf(firstQe);
                    if (lag >= 0 && lag <= 15) ipoAnchorOk = true;
                    break;
                  }
                }
              }
              if (firstQe && firstQe > pq && ((serP != null && pqOff < serP) || ipoAnchorOk)) {
                ['fiiChgPp', 'diiChgPp'].forEach(function (k) {
                  const j = keys.indexOf(k);
                  if (j >= 0 && !flags[j]) na[j] = 1;
                });
              }
            }
          } else if (cur && !isQuarterEnd(cur[0])) {
            // LATE-FILED CURRENT — the same look-ahead refusal as LATE-FILED PRIOR, one row over.
            // When the visible row is a mid-quarter EVENT row, the quarter-end row that SHOULD be
            // current can exist in shp yet have been submitted only after this screen date —
            // TBOTEK at 2024-09-30: visible row is the 2024-05-15 IPO anchor because its Jun-2024
            // quarterly SHP was submitted 2024-12-31, six months late, and even the Sep-2024 one
            // arrived 2024-10-17. No document a live trader could read carried a quarterly pair,
            // so the delta was uncomputable on that date; it computes on its own once the late
            // submission date passes. Gated on the row EXISTING with a real submission date that
            // is provably later — an absent quarter-end row proves nothing and stays visible.
            let nextQe = null;
            for (const q of shp) {
              if (isQuarterEnd(q[0]) && q[0] > cur[0] && (!nextQe || q[0] < nextQe[0])) nextQe = q;
            }
            if (nextQe && nextQe[3] > __DATEINT) {
              ['fiiChgPp', 'diiChgPp'].forEach(function (k) {
                const j = keys.indexOf(k);
                if (j >= 0 && !flags[j]) na[j] = 1;
              });
            }
          }
        }
        // profitYoyPct / profitBase / profitStreak: the YoY needs the SAME QUARTER A YEAR EARLIER.
        // For a company that listed or demerged inside the window, that quarter is older than any
        // row the company has — KPITTECH at 2020-01-31 needs 2018-12, its own oldest row is 2019-03
        // and it first traded 2019-04-22, because KPIT Technologies did not exist as this entity
        // before the demerger. Asking for its Dec-2018 profit is asking what a company earned
        // before it was that company. Same shape for SBICARD (listed 2020-03), ROSSARI (2020-07),
        // ROUTE (2020-01), MAZDOCK and UTIAMC (2020-10).
        // Gated on OUR OWN OLDEST ROW for the symbol, so a hole in the MIDDLE of a series stays a
        // real gap — CELLO needs 2022-12 while holding rows from 2022-06, and is deliberately NOT
        // marked N/A here.
        {
          const arr = (typeof fundFor === 'function') ? fundFor(r.sym) : null;
          if (arr && arr.length) {
            // An announce date is REAL only if it does not predate the symbol's first traded bar —
            // §99: scheme/prospectus carry-ins get stamped qe+45d defaults from before the entity
            // was listed, and treating those as filings creates both phantom "current quarters"
            // (NSLNISP's five 2020-era rows, anns 20200530.., listed 2023-02) and phantom history
            // (CELLO's Jun-2022 row, ann exactly qe+45d, 449 days pre-listing).
            const ser0 = (SERIES[r.tkr] && SERIES[r.tkr].d && SERIES[r.tkr].d.length) ? SERIES[r.tkr].d[0] : null;
            const annOk = function (a) {
              if (!(a > 0)) return false;
              if (ser0 == null) return true;
              const s = String(a);
              return dayOff(s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8)) >= ser0;
            };
            let ci = -1, ni = 3;
            for (let i = arr.length - 1; i >= 0; i--) { if (arr[i][3] != null && annOk(arr[i][4]) && arr[i][4] <= __DATEINT) { ci = i; ni = 3; break; } }
            if (ci < 0) for (let i = arr.length - 1; i >= 0; i--) { if (arr[i][1] != null && annOk(arr[i][2]) && arr[i][2] <= __DATEINT) { ci = i; ni = 1; break; } }
            if (ci < 0) {
              // Rows exist but the first REAL filing (ann >= first bar) is still in the future —
              // nothing about this entity was public at this date, so the whole profit family is
              // N/A, exactly like the first-SHP-filing rule above. NSLNISP at 2023-04-28 is the
              // measured case: first real filing 2023-05-23, three weeks after the month-end;
              // screener's series starts Jun-2023 and its plant only began production Aug-2023.
              ['profitYoyPct', 'profitBase', 'profitStreak', 'profitAccel', 'profitTTM', 'composite'].forEach(function (k) {
                const j = keys.indexOf(k);
                if (j >= 0 && !flags[j]) na[j] = 1;
              });
            }
            if (ci >= 0) {
              // "Our own oldest row" must mean OUR OWN FILED HISTORY. A newly-listed company
              // carries pre-listing quarters in from its prospectus/scheme, stamped with the
              // qe+45d DEFAULT rather than a real filing date (§99, §52) — that is not evidence
              // the company was reporting then, and treating it as the start of history turns an
              // unobtainable quarter into a "real gap". CELLO is the measured case: one Jun-2022
              // row with ann 2022-08-14 == qe+45d exactly, 449 days BEFORE its first traded bar
              // (2023-11-06). Its own Dec-2023 and Mar-2024 filings carry no year-ago quarter
              // column at all, and screener's series starts Jun-2023 — the quarter was never
              // published by anyone, yet that single row made it look fillable.
              // A row counts toward the oldest-row test only when it carries a real announce date
              // that is not before the symbol's first traded bar. Same first-bar test postDrift uses.
              const ser0 = (SERIES[r.tkr] && SERIES[r.tkr].d && SERIES[r.tkr].d.length) ? SERIES[r.tkr].d[0] : null;
              let oldest = 99999999;
              for (const q of arr) {
                const anns = [q[2], q[4]].filter(function (x) { return x > 0; });
                if (!anns.length) continue;                  // undated: not evidence of filing
                if (ser0 != null) {
                  const a = Math.min.apply(null, anns);
                  const aIso = String(a).slice(0, 4) + '-' + String(a).slice(4, 6) + '-' + String(a).slice(6, 8);
                  if (dayOff(aIso) < ser0) continue;         // pre-listing carry-in (§99)
                }
                if (q[0] < oldest) oldest = q[0];
              }
              if (oldest === 99999999) for (const q of arr) if (q[0] < oldest) oldest = q[0];  // no dated row survives -> fall back, never widen the gap
              // How far back each parameter REACHES, in quarters, from the latest visible one.
              // Same test for all of them, and the same one this rule always applied to the YoY
              // trio: is the quarter it reaches for older than any row this symbol has? Then it
              // predates the company's existence as this entity and cannot be filled. If it falls
              // INSIDE the symbol's own span the quarter should be on file, so a null there stays a
              // visible gap — CELLO needs 2022-12 while holding rows from 2022-06 and is
              // deliberately NOT marked N/A. Generalised beyond the YoY trio 2026-08-16: profitTTM
              // reaches 7 quarters back and profitAccel 5, and both were counting a demerged or
              // freshly-listed company's own pre-existence as a coverage gap — 440 and 61
              // member-dates in Nifty 500 alone. composite is gated on profitTTM because it is null
              // whenever profitTTM is (z(profitTTM) + z(ret12m) − z(vol)).
              const REACH = { profitYoyPct: 4, profitBase: 4, profitStreak: 4, profitAccel: 5, profitTTM: 7, composite: 7 };
              Object.keys(REACH).forEach(function (k) {
                const j = keys.indexOf(k);
                if (j < 0 || flags[j]) return;
                let need = arr[ci][0];
                for (let s = 0; s < REACH[k]; s++) need = prevQeInt(need);
                if (need < oldest) na[j] = 1;
              });
              // ZERO BASE is a refusal, not a gap: YoY against a base of exactly 0 is
              // arithmetically undefined, and the parameter's own rule says so ("Null when the
              // year-ago base is exactly 0"). Only a MEASURED zero counts — the base row must
              // carry a real (annOk) announce date, because a 0 without one is the unknown
              // sentinel (§91c) and stays a visible gap. NSLNISP is the measured case: its
              // pre-production quarters filed PAT of literally 0.00 (screener shows the same),
              // so every YoY against them is undefined forever — 18 month-ends no fill can close.
              {
                const rowAt = function (qe) { for (let i = 0; i < arr.length; i++) if (arr[i][0] === qe) return arr[i]; return null; };
                const patOf = function (row) { return row == null ? null : (ni === 3 ? row[3] : row[1]); };
                const annOf = function (row) { return row == null ? 0 : (ni === 3 ? row[4] : row[2]); };
                const zeroBase = function (qe) { const b = rowAt(qe); return b != null && patOf(b) === 0 && annOk(annOf(b)); };
                const cur = arr[ci][0];
                const b4 = prevQeInt(prevQeInt(prevQeInt(prevQeInt(cur))));
                ['profitYoyPct', 'profitBase', 'profitStreak'].forEach(function (k) {
                  const j = keys.indexOf(k);
                  if (j >= 0 && !flags[j] && !na[j] && zeroBase(b4)) na[j] = 1;
                });
                const jA = keys.indexOf('profitAccel');
                if (jA >= 0 && !flags[jA] && !na[jA] && (zeroBase(b4) || zeroBase(prevQeInt(b4)))) na[jA] = 1;
                // TTM: prior-4 window summing to exactly 0 (all rows present, dated, measured)
                var pr = [], q = b4, allz = true;
                for (var s2 = 0; s2 < 4; s2++) { var rw = rowAt(q); if (rw == null || !annOk(annOf(rw)) || patOf(rw) == null) { allz = false; break; } if (patOf(rw) !== 0) allz = false; pr.push(rw); q = prevQeInt(q); }
                if (allz && pr.length === 4) {
                  ['profitTTM', 'composite'].forEach(function (k) {
                    const j = keys.indexOf(k);
                    if (j >= 0 && !flags[j] && !na[j]) na[j] = 1;
                  });
                }
              }
            }
          }
        }
        const jPD = keys.indexOf('postDrift');
        if (jPD >= 0 && !flags[jPD]) {
          const lrd = lastResultDate(r.sym, __DATEINT, __CFG.earnBasis);
          const ser = SERIES[r.tkr];
          if (lrd > 0 && ser && ser.d && ser.d.length && dayOff((String(lrd).slice(0,4)+'-'+String(lrd).slice(4,6)+'-'+String(lrd).slice(6,8))) < ser.d[0]) na[jPD] = 1;
        }
        // PAT-std flags, from the std-basis pass's row for this same stock. Same non-null test
        // as the engine columns; no zeroIsNull (a streak of 0 is a real answer, §91).
        const sr = stdByTkr.get(r.tkr);
        const sflags = new Array(stdKeys.length);
        for (let j = 0; j < stdKeys.length; j++) {
          const v = sr ? fieldVal(sr, stdKeys[j]) : null;
          sflags[j] = (v != null && typeof v === 'number' && isFinite(v)) ? 1 : 0;
        }
        const cr = conByTkr.get(r.tkr);
        const cflags = new Array(conKeys.length);
        for (let j = 0; j < conKeys.length; j++) {
          const v = cr ? fieldVal(cr, conKeys[j]) : null;
          cflags[j] = (v != null && typeof v === 'number' && isFinite(v)) ? 1 : 0;
        }
        out[i] = [r.sym, flags, r.turnover || 0, (r.ind && r.ind !== 'Other' && r.ind !== 'Unknown') ? 1 : 0, na, sflags, cflags];
      }
      return out;
    })()`, ctx);

    const dateInt = +d.iso.replace(/-/g, '');
    // engine-parameter column positions, in PARAMS order
    const engineCols = []; PARAMS.forEach((p, i) => { if (!p.src) engineCols.push(i); });
    const iIndustry = PARAMS.findIndex(p => p.k === 'industry');
    const iTurnover = PARAMS.findIndex(p => p.k === 'turnover');
    const revCols = ['rev'].map(k => PARAMS.findIndex(p => p.k === k && p.src === 'revop'));
    const BASIS_KEYS = ['revCon', 'revStd', 'patCon', 'patStd'];
    const basisCols = BASIS_KEYS.map(k => PARAMS.findIndex(p => p.k === k && p.src === 'basis'));
    // PAT-std / PAT-con column positions, in the same family order the flag arrays are built in
    const stdCols = PARAMS.map((p, i) => (p.src === 'engstd' ? i : -1)).filter(i => i >= 0);
    const conCols = PARAMS.map((p, i) => (p.src === 'engcon' ? i : -1)).filter(i => i >= 0);
    const STD_KEYS = stdCols.map(i => PARAMS[i].k);
    const CON_KEYS = conCols.map(i => PARAMS[i].k);

    // per-row flags for the non-engine families, computed once per row per date
    const perRow = rows.map(([sym, flags, turnover, indKnown, na, es, ec]) => {
      const rv = [0];
      const ridx = revFor(sym), rmap = revopFor(sym);
      if (ridx && rmap) {
        let qe = null;
        for (let i = ridx.length - 1; i >= 0; i--) if (ridx[i][0] <= dateInt) { qe = ridx[i][1]; break; }
        const cell = qe ? rmap[qe] : null;
        if (cell) ['rev'].forEach((k, j) => {
          const [ci, si] = revopIdx[k];
          rv[j] = (cell[ci] != null || cell[si] != null) ? 1 : 0;
        });
      }
      // ---- NOT-APPLICABLE for the revenue family, from the adjudicated ledger ----------------
      // A banking-format filer has no EBIT line to report: its P&L runs Interest Earned → Interest
      // Expended → Operating Profit BEFORE provisions, and interest is already deducted by then, so
      // "earnings before interest" is not a quantity that exists for it. Counting that as a gap
      // asked the page to fill a number no filing contains — 2,819 of the 3,402 missing ebit
      // member-dates in Nifty 500 were this. Only symbols with per-name evidence in the ledger are
      // marked; a name whose evidence is absent stays a visible gap on purpose.
      const rvna = [0];
      const fra = firstRealAnn(sym);
      ['rev'].forEach((k, j) => {
        if (!rv[j] && naLedgerHit(k, sym, d.iso)) rvna[j] = 1;
        // first REAL filing still in the future -> nothing was public for this entity yet (§99)
        else if (!rv[j] && fra && fra > dateInt) rvna[j] = 1;
      });

      /* ---- reporting basis: revCon, revStd, patCon, patStd ---------------------------------
       * Each basis resolves its OWN visible quarter from its OWN announce date, then reads that
       * basis's slot. No N/A is applied (see the note above): an empty cell means we hold no
       * figure on that basis at that date, full stop. */
      const bs = [0, 0, 0, 0];
      const bidx = basisFor(sym), fqe = fundQeFor(sym);
      const vCon = bidx ? lastVisible(bidx.con, dateInt) : null;
      const vStd = bidx ? lastVisible(bidx.std, dateInt) : null;
      if (rmap && vCon) { const c = rmap[vCon[1]]; if (c && c[1] != null) bs[0] = 1; }
      if (rmap && vStd) { const c = rmap[vStd[1]]; if (c && c[0] != null) bs[1] = 1; }
      if (fqe && vCon) { const q = fqe[vCon[1]]; if (q && q[3] != null) bs[2] = 1; }
      if (fqe && vStd) { const q = fqe[vStd[1]]; if (q && q[1] != null) bs[3] = 1; }
      /* ---- NOTHING WAS PUBLIC YET (§99) — the ONE N/A the basis + raw PAT families carry -------
       * Both families were built deliberately with no not-applicable logic: an empty cell means
       * "we hold nothing on this basis", full stop. That stays true, with one exception approved
       * by the user 2026-08-16: a company whose FIRST REAL FILING is still in the future had
       * published nothing at all on this date, so there is no figure for anyone to have held.
       * NSLNISP is the measured case — NMDC Steel listed 2023-02-20 and filed its first result
       * 2023-05-23, so at the 2023-04-28 month-end it had been listed nine weeks with nothing
       * filed. Verified against the exchange rather than inferred: NSE serves ZERO filings for it
       * in that window on either the quarterly or the annual stream, so the cell cannot be filled
       * by anyone. Its pre-2023 rows are demerger-scheme carry-ins stamped before the tape starts
       * (§99), and screener shows the entity at 0.0 revenue until FY2024 — an empty shell, not a
       * gap in our data. `firstRealAnn` already encodes exactly this test for the revenue family,
       * so this reuses it instead of adding a second rule or a name list. */
      const nothingPublicYet = !!(fra && fra > dateInt);
      const bsna = [0, 0, 0, 0];
      const esna = es.map(() => 0);
      const ecna = ec.map(() => 0);
      if (nothingPublicYet) {
        for (let j = 0; j < 4; j++) if (!bs[j]) bsna[j] = 1;
        for (let j = 0; j < es.length; j++) if (!es[j]) esna[j] = 1;
        for (let j = 0; j < ec.length; j++) if (!ec[j]) ecna[j] = 1;
      }
      // Adjudicated per-name verdicts from coverage_na_ledger.json also apply to these families —
      // the same naLedgerHit the revenue family consults, honouring from/to bounds. First user-
      // approved case 2026-08-16: IOB patCon/postDriftCon before its first-ever consolidated
      // filing (19-May-2022, five-source evidence in the ledger). Name-scoped and reversible;
      // no category rule.
      for (let j = 0; j < 4; j++) if (!bs[j] && !bsna[j] && naLedgerHit(BASIS_KEYS[j], sym, d.iso)) bsna[j] = 1;
      for (let j = 0; j < es.length; j++) if (!es[j] && !esna[j] && naLedgerHit(STD_KEYS[j], sym, d.iso)) esna[j] = 1;
      for (let j = 0; j < ec.length; j++) if (!ec[j] && !ecna[j] && naLedgerHit(CON_KEYS[j], sym, d.iso)) ecna[j] = 1;
      return { sym, flags, turnover, indKnown, rv, rvna, na, bs, bsna, es, esna, ec, ecna };
    });

    for (const u of UNIVERSES) {
      const cnt = counts[u.slug][di], nac = naCounts[u.slug][di];
      const set = u.kind === 'index' ? memberSets[u.slug][di] : null;
      if (u.kind === 'index' && !set) { members[u.slug][di] = -1; continue; }   // -1 = no roll yet
      let n = 0;
      // --explain: name the symbols behind every sub-100 cell, through THIS vm run (§92 — the
      // counts and the names must come from one measurement, or the queue can drift from the page).
      const ex = (EXPLAIN && u.slug === EXPLAIN && d.iso >= EXPLAIN_FROM && d.iso <= EXPLAIN_TO) ? (EXPLAIN_OUT[d.iso] = {}) : null;
      const seen = ex ? new Set() : null;
      for (const r of perRow) {
        if (set && !set.has(r.sym)) continue;
        if (u.kind === 'liquid' && !(r.turnover >= LIQUID_FLOOR)) continue;
        n++;
        if (seen) seen.add(r.sym);
        for (let j = 0; j < engineCols.length; j++) {
          if (r.flags[j]) cnt[engineCols[j]]++;
          else if (r.na && r.na[j]) { nac[engineCols[j]]++; if (ex) (ex['na:' + PARAMS[engineCols[j]].k] ||= []).push(r.sym); }   // inapplicable, not missing
          else if (ex) (ex[PARAMS[engineCols[j]].k] ||= []).push(r.sym);   // missing, not N/A
        }
        if (r.indKnown) cnt[iIndustry]++; else if (ex) (ex.industry ||= []).push(r.sym);
        for (let j = 0; j < revCols.length; j++) {
          if (r.rv[j]) cnt[revCols[j]]++;
          else if (r.rvna && r.rvna[j]) { nac[revCols[j]]++; if (ex) (ex['na:' + PARAMS[revCols[j]].k] ||= []).push(r.sym); }   // inapplicable, not missing
          else if (ex) (ex[PARAMS[revCols[j]].k] ||= []).push(r.sym);
        }
        for (let j = 0; j < 4; j++) {
          if (r.bs[j]) cnt[basisCols[j]]++;
          else if (r.bsna && r.bsna[j]) { nac[basisCols[j]]++; if (ex) (ex['na:' + PARAMS[basisCols[j]].k] ||= []).push(r.sym); }   // nothing was public yet (§99)
          else if (ex) (ex[PARAMS[basisCols[j]].k] ||= []).push(r.sym);   // otherwise no N/A by design
        }
        for (let j = 0; j < stdCols.length; j++) {
          if (r.es[j]) cnt[stdCols[j]]++;
          else if (r.esna && r.esna[j]) { nac[stdCols[j]]++; if (ex) (ex['na:' + PARAMS[stdCols[j]].k] ||= []).push(r.sym); }     // nothing was public yet (§99)
          else if (ex) (ex[PARAMS[stdCols[j]].k] ||= []).push(r.sym);   // otherwise RAW — no N/A
        }
        for (let j = 0; j < conCols.length; j++) {
          if (r.ec[j]) cnt[conCols[j]]++;
          else if (r.ecna && r.ecna[j]) { nac[conCols[j]]++; if (ex) (ex['na:' + PARAMS[conCols[j]].k] ||= []).push(r.sym); }     // nothing was public yet (§99)
          else if (ex) (ex[PARAMS[conCols[j]].k] ||= []).push(r.sym);   // otherwise RAW — no N/A
        }
      }
      // roll members that never reached factorsAt carry NO parameter at all — they are the Price
      // column's gap. Recorded separately so the queue can never silently omit them.
      if (ex && set) {
        for (const s of set) {
          if (s.includes('DVR') || /^DUMMY/.test(s) || seen.has(s)) continue;
          (ex.__norow ||= []).push(s);
        }
      }
      // an INDEX universe's denominator is the roll itself, not what survived the price gate —
      // that difference is exactly what the Price column is there to show.
      // ⚠️ …but NSE seeds the roll with `DUMMY*` placeholder tickers around corporate actions, and
      // those are not securities: they have no series in the dataset at all, so they can never be
      // covered by ANY parameter. Left in the denominator they became a floor that no amount of
      // data work could lift — 7 of the 78 Nifty-500 month-ends 2020-2026 read 500/504 = 99.21%
      // across THIRTY parameters including `price` itself, and the page said "the data is missing"
      // when the truth was "the member is not a stock". 11 such tickers across 6 indices, e.g.
      // DUMMYVEDL1-4 (2026-05/06), DUMMYABFRL/RAYMN/SIEMS (2025-06/07), DUMMYDBRLT, DUMMYHDLVR.
      members[u.slug][di] = set
        ? [...set].filter(s => !s.includes('DVR') && !/^DUMMY/.test(s)).length : n;
    }

    if (Date.now() - lastLog > 5000 || di === dates.length - 1) {
      lastLog = Date.now();
      log(`  ${di + 1}/${dates.length} ${d.iso} · ${rows.length} rows · ${((Date.now() - C.t0) / 1000).toFixed(0)}s`);
    }
  }

  /* --facts: per-symbol point-in-time boundaries, read from the ENGINE's own loaded state, for
   * adjudicating whether a missing cell could ever have existed (campaign class C3 PRE-HISTORY).
   * firstBar is the only one of these the repo cannot answer locally — it lives in the 193 MB bin —
   * so it has to come from here. Emitted once per symbol, not per date. */
  if (FACTS_PATH) {
    const syms = vm.runInContext(`(function(){
      const out = {};
      for (const tkr in SERIES) {
        const m = META[tkr]; if (!m) continue;
        const s = SERIES[tkr];
        if (!s || !s.d || !s.d.length) continue;
        out[m.symbol] = { firstBar: isoOff(s.d[0]), lastBar: isoOff(s.d[s.d.length - 1]), nBars: s.d.length };
      }
      return out;
    })()`, ctx);
    const fundOldest = vm.runInContext(`(function(){
      const out = {};
      for (const sym in FUND) {
        const a = FUND[sym]; if (!a || !a.length) continue;
        let lo = 99999999, hi = 0;
        for (const q of a) { if (q[0] < lo) lo = q[0]; if (q[0] > hi) hi = q[0]; }
        out[sym] = { oldestQe: lo, newestQe: hi, nRows: a.length };
      }
      return out;
    })()`, ctx);
    const shpFirst = vm.runInContext(`(function(){
      const out = {};
      for (const sym in SHPD) {
        const a = SHPD[sym]; if (!a || !a.length) continue;
        let f = 0; for (const q of a) if (q[3] > 0 && (!f || q[3] < f)) f = q[3];
        out[sym] = { firstSub: f, nRows: a.length, oldestQe: a[0] ? a[0][0] : null };
      }
      return out;
    })()`, ctx);
    const p = path.resolve(ROOT, FACTS_PATH);
    fs.writeFileSync(p, JSON.stringify({ generated: new Date().toISOString(), dataEnd: C.end, series: syms, fund: fundOldest, shp: shpFirst }));
    log(`--facts: wrote ${p} · ${Object.keys(syms).length} series · ${Object.keys(fundOldest).length} fund · ${Object.keys(shpFirst).length} shp`);
  }

  if (EXPLAIN) {
    const p = path.resolve(ROOT, EXPLAIN_PATH);
    fs.writeFileSync(p, JSON.stringify({
      universe: EXPLAIN, from: EXPLAIN_FROM, to: EXPLAIN_TO, dataEnd: C.end,
      generated: new Date().toISOString(), byDate: EXPLAIN_OUT,
    }));
    const nCells = Object.values(EXPLAIN_OUT).reduce((a, o) => a + Object.values(o).reduce((b, l) => b + l.length, 0), 0);
    log(`--explain: wrote ${p} · ${Object.keys(EXPLAIN_OUT).length} dates · ${nCells} named missing cells`);
  }

  writeOut(UNIVERSES, dates, counts, members, C, naCounts);
}

/* ============================================================================
 * FLAGS + OUTPUT
 * ========================================================================== */
// A date is flagged when its coverage sits below half its own neighbourhood median (that median
// being a real population, >200), or — when the universe has a known roll — under 90% of that
// date's members while the neighbours clear it. Smoke detector, not a gate.
function medianOf(a) {
  const v = a.filter(x => x >= 0).slice().sort((x, y) => x - y);
  if (!v.length) return 0;
  const h = v.length >> 1;
  return v.length % 2 ? v[h] : (v[h - 1] + v[h]) / 2;
}
const NEIGH = 6;   // ±6 month-ends

const ROLL_FLOOR = 0.9;     // an index date should carry data for ≥90% of its own members
const ROLL_MARGIN = 0.03;   // …and must miss the neighbours by ≥3pp before it counts as a hole

// `hasRoll` — only an index universe has a membership list to check a 90% floor against. For
// "All listed"/"Liquid" the denominator IS the screened count, so that test would just re-flag
// structural facts (a new listing has no 12-month return) every single month.
//
// Both checks require the LEFT neighbourhood to be a real population. Without that, the first
// month of every ramp reads as a crater: FII/DII coverage starting at 0 in 2002 is a floor, not
// a hole, and a page that shouts about it teaches you to ignore it.
// ⚠️ Judged on COVERAGE RATIOS against the effective denominator (members − N/A), never on raw
// counts. A count alone cannot tell a hole from a refusal: at Oct–Dec-2022 the FII/DII count
// collapses to 4/8/14 because the engine declines to compute a QoQ change across the Sep-2022 SEBI
// reclassification, and a raw-count detector called that a crater — three amber flags on Nifty 500
// (and on eight other universes) pointing at deliberate behaviour. On ratios those dates read
// 4/4, 8/8, 14/14 = 100% and correctly raise nothing. `den` also collapses to 0 where a parameter
// applies to nobody (Nifty Bank's ebit), which is skipped outright — there is no coverage to judge.
function computeFlags(series, denArr, memberArr, hasRoll) {
  const flags = [];
  for (let i = 0; i < series.length; i++) {
    const v = series[i]; if (v < 0) continue;
    // ---- N/A guard, ahead of the count-based tests -------------------------------------------
    // The tests below read RAW COUNTS, and a raw count cannot tell a hole from a refusal. At
    // Oct–Dec-2022 the FII/DII count collapses to 4/8/14 because the engine declines to compute a
    // QoQ change across the Sep-2022 SEBI reclassification — deliberate behaviour that lit three
    // amber flags on Nifty 500 and fifteen more across other universes. Judge coverage of the
    // population the parameter APPLIES to first: if that is healthy, the drop is explained and
    // there is nothing to smell. (4/4, 8/8, 14/14 = 100%.) Deliberately narrow — a date whose
    // applicable coverage is genuinely poor still falls through to the original tests unchanged.
    const den = denArr[i];
    if (den <= 0) continue;                          // applies to nobody — nothing to measure
    const lo = Math.max(0, i - NEIGH), hi = Math.min(series.length - 1, i + NEIGH);
    {
      // Compare against the NEIGHBOURS' applicable-coverage, not a flat floor. A flat floor would
      // also swallow the live-edge `roll` flags, which are the ones worth keeping: a results season
      // still landing genuinely reads worse than its neighbours and should smell. Only a date that
      // keeps pace with its neighbourhood once N/A is excluded is let through silently.
      const nbR = [];
      for (let k = lo; k <= hi; k++) if (k !== i && denArr[k] > 0 && series[k] >= 0) nbR.push(series[k] / denArr[k]);
      const nmedR = medianOf(nbR);
      if (nmedR > 0 && v / den >= nmedR - ROLL_MARGIN) continue;
    }
    const left = [], neigh = [];
    for (let k = lo; k <= hi; k++) { if (k === i || series[k] < 0) continue; neigh.push(series[k]); if (k < i) left.push(series[k]); }
    const med = medianOf(neigh), lmed = medianOf(left);
    if (lmed <= 200) continue;                       // nothing established before this date
    if (med > 200 && v < med / 2) { flags.push({ i, why: 'half', med: Math.round(med) }); continue; }
    if (!hasRoll) continue;
    const m = memberArr[i];
    if (m > 200 && v < ROLL_FLOOR * m) {
      const nb = []; for (let k = lo; k <= hi; k++) if (k !== i && memberArr[k] > 0 && series[k] >= 0) nb.push(series[k] / memberArr[k]);
      const nmed = medianOf(nb);
      if (nmed >= ROLL_FLOOR && v / m < nmed - ROLL_MARGIN) flags.push({ i, why: 'roll', med: Math.round(m * nmed) });
    }
  }
  return flags;
}

function writeOut(UNIVERSES, dates, counts, members, C, naCounts) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const stamp = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' IST';

  const famMeta = FAMILIES.map(f => ({
    id: f.id, label: f.label, note: f.note,
    params: f.params.map(p => ({ k: p.k, rule: p.rule })),
  }));

  const index = {
    updated: stamp, dataEnd: C.end, from: dates[0].label, to: dates[dates.length - 1].label,
    nDates: dates.length, nParams: NP, liquidFloorCr: LIQUID_FLOOR / 100,
    families: famMeta, noHistory: NO_HISTORY,
    universes: [],
  };

  for (const u of UNIVERSES) {
    const cnt = counts[u.slug], mem = members[u.slug], nac = naCounts[u.slug];
    const naTotals = PARAMS.map((_, pi) => dates.reduce((a, _d, di) => a + (mem[di] > 0 ? nac[di][pi] : 0), 0));
    // A family cell is the WEAKEST parameter in that family. Derived, not shipped: the page
    // recomputes it from `params` with the same one-line min. Shipping it too would be ~25% more
    // bytes AND a second source for one number — the exact split that lets two views disagree.
    const famSeries = FAMILIES.map(f => {
      const idxs = f.params.map(p => PARAMS.findIndex(q => q.k === p.k && q.fam === f.id));
      return dates.map((_, di) => {
        if (mem[di] < 0) return -1;
        let lo = Infinity;
        idxs.forEach(pi => { const v = cnt[di][pi]; if (v < lo) lo = v; });
        return lo === Infinity ? 0 : lo;
      });
    });
    // The family's effective denominator: members minus the N/A of whichever parameter is the
    // weakest here — the same pairing the page renders, so a flag is judged on the number the
    // reader actually sees.
    const famDen = FAMILIES.map(f => {
      const idxs = f.params.map(p => PARAMS.findIndex(q => q.k === p.k && q.fam === f.id));
      return dates.map((_, di) => {
        if (mem[di] < 0) return -1;
        let lo = Infinity, na = 0;
        idxs.forEach(pi => { const v = cnt[di][pi]; if (v < lo) { lo = v; na = nac[di][pi] || 0; } });
        return Math.max(0, mem[di] - na);
      });
    });
    const flagsByFam = {};
    let nFlags = 0;
    FAMILIES.forEach((f, fi) => {
      const fl = computeFlags(famSeries[fi], famDen[fi], Array.from(mem), u.kind === 'index');
      if (fl.length) { flagsByFam[f.id] = fl; nFlags += fl.length; }
    });

    const payload = {
      slug: u.slug, name: u.name, kind: u.kind, note: u.note, rollFrom: u.from || null,
      updated: stamp, dataEnd: C.end,
      dates: dates.map(d => d.iso), months: dates.map(d => d.label),
      members: Array.from(mem),
      params: PARAMS.map((p, pi) => dates.map((_, di) => (mem[di] < 0 ? -1 : cnt[di][pi]))),
      // NOT-APPLICABLE per param per date — members for whom the parameter CANNOT exist, as
      // opposed to members for whom it is merely absent. The page subtracts these from the
      // denominator and shows them separately, so a column reads 100% when every member that
      // COULD have a value has one. Emitted only for the params that ever use it, to keep the
      // payload from doubling for 42 columns of zeros.
      na: PARAMS.map((p, pi) => (naTotals[pi] ? dates.map((_, di) => (mem[di] < 0 ? -1 : nac[di][pi])) : 0)),
      paramKeys: PARAMS.map(p => p.k), paramFam: PARAMS.map(p => p.fam),
      flags: flagsByFam,
    };
    fs.writeFileSync(path.join(OUT_DIR, u.slug + '.json'), JSON.stringify(payload));
    const firstLive = Array.from(mem).findIndex(x => x > 0);
    index.universes.push({
      slug: u.slug, name: u.name, kind: u.kind, rollFrom: u.from || null,
      firstDate: firstLive >= 0 ? dates[firstLive].label : null,
      lastMembers: mem[mem.length - 1], nFlags,
    });
    log(`  wrote ${u.slug}.json · ${mem[mem.length - 1]} members now · ${nFlags} flags`);
  }

  fs.writeFileSync(path.join(OUT_DIR, 'index.json'), JSON.stringify(index));
  log(`done in ${((Date.now() - C.t0) / 1000 / 60).toFixed(1)} min → ${OUT_DIR}`);
}

main();
