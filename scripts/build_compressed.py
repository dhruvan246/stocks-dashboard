#!/usr/bin/env python3
"""Build a single-file HTML dashboard with gzip+base64 embedded data, decompressed
   client-side via the browser's native DecompressionStream API."""
import json, gzip, base64
from pathlib import Path
from datetime import datetime

ROOT     = Path(__file__).resolve().parent.parent
SRC      = ROOT / "scripts" / "stock_data.json"
OUT_HTML = ROOT / "docs" / "nse-bse-dashboard.html"

payload = json.loads(SRC.read_text())
start_ts = payload["startTs"]
gen_ts   = payload["generatedAt"]
gen_date = datetime.fromtimestamp(gen_ts).strftime("%d %b %Y %H:%M")
start_date = datetime.fromtimestamp(start_ts).strftime("%d %b %Y")

DAY = 86400
compact_series = {}
for tkr, pairs in payload["series"].items():
    ds, ps = [], []
    for ts, close in pairs:
        ds.append(int((ts - start_ts) // DAY))
        ps.append(int(round(close * 100)))
    compact_series[tkr] = {"d": ds, "p": ps}

compact = {
    "startTs": start_ts,
    "endTs":   payload["endTs"],
    "generatedAt": gen_ts,
    "meta":    payload["meta"],
    "series":  compact_series,
}

raw_json = json.dumps(compact, separators=(",", ":")).encode("utf-8")
print(f"Raw JSON: {len(raw_json)/1024/1024:.2f} MB  stocks={len(compact_series)}")

gz = gzip.compress(raw_json, compresslevel=9)
print(f"Gzipped: {len(gz)/1024/1024:.2f} MB  (ratio {len(raw_json)/len(gz):.1f}x)")

b64 = base64.b64encode(gz).decode("ascii")
print(f"Base64: {len(b64)/1024/1024:.2f} MB")

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Dhruvan's stocks data</title>
<script src="https://cdn.tailwindcss.com"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  :root { --gain:#16a34a; --gain-bg:#dcfce7; --loss:#dc2626; --loss-bg:#fee2e2; }
  body { font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; }
  table thead th { position:sticky; top:0; background:#f8fafc; z-index:10; }
  .gain{color:var(--gain);} .loss{color:var(--loss);}
  .chip-gain{background:var(--gain-bg);color:var(--gain);}
  .chip-loss{background:var(--loss-bg);color:var(--loss);}
  .scrollbar::-webkit-scrollbar{width:8px;height:8px;}
  .scrollbar::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:4px;}
  .scrollbar::-webkit-scrollbar-track{background:#f1f5f9;}
  #loadingOverlay{position:fixed;inset:0;background:#f8fafc;display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:50;transition:opacity .3s;}
  .spinner{width:44px;height:44px;border:4px solid #e2e8f0;border-top-color:#2563eb;border-radius:50%;animation:spin 1s linear infinite;}
  @keyframes spin{to{transform:rotate(360deg);}}
</style>
</head>
<body class="bg-slate-50 text-slate-800">

<div id="loadingOverlay">
  <div class="spinner"></div>
  <div class="mt-5 text-sm font-semibold text-slate-700">Loading 4,000+ NSE/BSE stocks&hellip;</div>
  <div class="mt-1 text-xs text-slate-500" id="loadingStatus">Decompressing embedded price history&hellip;</div>
</div>

<div class="min-h-screen">
  <header class="bg-white border-b border-slate-200 sticky top-0 z-20 shadow-sm">
    <div class="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <div class="w-9 h-9 rounded-lg bg-gradient-to-br from-blue-600 to-indigo-700 text-white flex items-center justify-center font-bold text-sm shadow">DS</div>
        <div>
          <h1 class="text-lg font-bold tracking-tight">Dhruvan's stocks data</h1>
        </div>
      </div>
      <div class="text-xs text-slate-500" id="lastUpdated">Data: __START_DATE__ &rarr; __GEN_DATE__ IST</div>
    </div>
  </header>

  <section class="max-w-7xl mx-auto px-6 py-6">
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-sm font-semibold text-slate-700 uppercase tracking-wide">Filters</h2>
        <span class="text-xs text-slate-500" id="universeCount">Loading universe&hellip;</span>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-7 gap-3">
        <div class="md:col-span-1">
          <label class="block text-xs font-medium text-slate-600 mb-1">From Date</label>
          <input type="date" id="fromDate" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white" />
        </div>
        <div class="md:col-span-1">
          <label class="block text-xs font-medium text-slate-600 mb-1">To Date</label>
          <input type="date" id="toDate" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white" />
        </div>
        <div class="md:col-span-1 relative">
          <label class="block text-xs font-medium text-slate-600 mb-1">Market Cap (&#8377; Cr)</label>
          <button type="button" id="mcapTrigger"
                  class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm bg-white text-left flex justify-between items-center hover:bg-slate-50 focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
            <span id="mcapLabel" class="truncate">All market caps</span>
            <svg class="w-4 h-4 text-slate-400 ml-1 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
          </button>
          <div id="mcapPanel" class="hidden absolute z-30 mt-1 w-64 bg-white border border-slate-200 rounded-lg shadow-lg p-2">
            <div class="flex justify-between text-[11px] text-slate-500 px-2 py-1 border-b border-slate-100 mb-1">
              <button type="button" id="mcapSelectAll" class="hover:text-blue-600 font-medium">Select all</button>
              <button type="button" id="mcapClear"     class="hover:text-blue-600 font-medium">Clear</button>
            </div>
            <label class="flex items-center gap-2 px-2 py-1 hover:bg-slate-50 rounded cursor-pointer text-sm"><input type="checkbox" class="mcap-cb rounded border-slate-300" data-bucket="below100"/>100 and below</label>
            <label class="flex items-center gap-2 px-2 py-1 hover:bg-slate-50 rounded cursor-pointer text-sm"><input type="checkbox" class="mcap-cb rounded border-slate-300" data-bucket="100to500"/>100 &ndash; 500</label>
            <label class="flex items-center gap-2 px-2 py-1 hover:bg-slate-50 rounded cursor-pointer text-sm"><input type="checkbox" class="mcap-cb rounded border-slate-300" data-bucket="500to1000"/>500 &ndash; 1,000</label>
            <label class="flex items-center gap-2 px-2 py-1 hover:bg-slate-50 rounded cursor-pointer text-sm"><input type="checkbox" class="mcap-cb rounded border-slate-300" data-bucket="1000to5000"/>1,000 &ndash; 5,000</label>
            <label class="flex items-center gap-2 px-2 py-1 hover:bg-slate-50 rounded cursor-pointer text-sm"><input type="checkbox" class="mcap-cb rounded border-slate-300" data-bucket="5000to20000"/>5,000 &ndash; 20,000</label>
            <label class="flex items-center gap-2 px-2 py-1 hover:bg-slate-50 rounded cursor-pointer text-sm"><input type="checkbox" class="mcap-cb rounded border-slate-300" data-bucket="above20000"/>20,000 and above</label>
          </div>
        </div>
        <div class="md:col-span-1">
          <label class="block text-xs font-medium text-slate-600 mb-1">Industry</label>
          <select id="sectorFilter" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white">
            <option value="all">All sectors</option>
          </select>
        </div>
        <div class="md:col-span-1">
          <label class="block text-xs font-medium text-slate-600 mb-1">Sort By</label>
          <select id="sortBy" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white">
            <option value="changeDesc">% Change &darr;</option>
            <option value="changeAsc">% Change &uarr;</option>
            <option value="mcapDesc">Market Cap &darr;</option>
            <option value="mcapAsc">Market Cap &uarr;</option>
            <option value="nameAsc">Name A&ndash;Z</option>
          </select>
        </div>
        <div class="md:col-span-1 flex items-end">
          <button id="loadBtn" class="w-full bg-blue-600 hover:bg-blue-700 active:bg-blue-800 text-white rounded-lg px-4 py-2 text-sm font-semibold shadow-sm transition">Load Data</button>
        </div>
      </div>
      <div class="flex flex-wrap gap-2 mt-4">
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="7">Last 7 days</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="30">Last 30 days</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="90">Last 90 days</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="365">1 year</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="1095">3 years</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="1825">5 years</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="3650">10 years</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-days="7300">20 years</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-since-1996="1">Since 1996</button>
        <button class="preset-btn text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-full px-3 py-1 font-medium" data-ytd="1">YTD</button>
      </div>
    </div>
  </section>

  <section class="max-w-7xl mx-auto px-6 pb-6">
    <div class="grid grid-cols-2 md:grid-cols-5 gap-3" id="statsGrid"></div>
  </section>

  <section class="max-w-7xl mx-auto px-6 pb-10">
    <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-slate-200 flex flex-wrap gap-3 justify-between items-center">
        <div>
          <h3 class="text-sm font-semibold text-slate-700 uppercase tracking-wide">Results</h3>
          <p class="text-xs text-slate-500 mt-0.5" id="resultCount">Pick dates + market cap, then click <span class="font-semibold">Load Data</span>.</p>
        </div>
        <div class="flex gap-2 items-center">
          <input type="text" id="searchBox" placeholder="Search symbol or company&hellip;" class="border border-slate-300 rounded-lg px-3 py-1.5 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 w-64" />
          <button id="exportBtn" class="text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg px-3 py-1.5 font-medium">Export CSV</button>
        </div>
      </div>
      <div class="max-h-[640px] overflow-auto scrollbar">
        <table class="w-full text-sm">
          <thead class="bg-slate-50 text-slate-600 text-xs uppercase">
            <tr>
              <th class="px-4 py-3 text-left font-semibold">#</th>
              <th class="px-4 py-3 text-left font-semibold">Symbol</th>
              <th class="px-4 py-3 text-left font-semibold">Company</th>
              <th class="px-4 py-3 text-left font-semibold">Industry</th>
              <th class="px-4 py-3 text-right font-semibold">Market Cap<br><span class="normal-case text-slate-400 text-[10px] font-normal">(&#8377; Cr)</span></th>
              <th class="px-4 py-3 text-right font-semibold">From Price<br><span class="normal-case text-slate-400 text-[10px] font-normal">(&#8377;)</span></th>
              <th class="px-4 py-3 text-right font-semibold">To Price<br><span class="normal-case text-slate-400 text-[10px] font-normal">(&#8377;)</span></th>
              <th class="px-4 py-3 text-right font-semibold">Change %</th>
            </tr>
          </thead>
          <tbody id="resultsBody" class="divide-y divide-slate-100">
            <tr><td colspan="8" class="text-center text-slate-400 py-16 text-sm">Loading stock data&hellip;</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <footer class="max-w-7xl mx-auto px-6 py-6 text-xs text-slate-400 text-center">
    Price data from Yahoo Finance &middot; Market caps from BSE &middot; Weekly closes 1996&ndash;2019, daily 2020&ndash;today &middot; NSE suffix .NS / BSE suffix .BO
  </footer>
</div>

<script id="compressedData" type="text/base64">__B64_PAYLOAD__</script>
<script>
let META = {}, SERIES = {}, UNIVERSE = [], START_TS = 0;
const DAY = 86400;

async function loadAndInit() {
  const statusEl = document.getElementById('loadingStatus');
  try {
    const t0 = performance.now();
    const b64 = document.getElementById('compressedData').textContent.trim();
    statusEl.textContent = 'Decoding ' + (b64.length / 1024 / 1024).toFixed(1) + ' MB...';
    // give the browser a frame to paint the spinner
    await new Promise(r => requestAnimationFrame(() => setTimeout(r, 16)));

    // Decode base64 -> Uint8Array
    const binStr = atob(b64);
    const bytes = new Uint8Array(binStr.length);
    for (let i = 0; i < binStr.length; i++) bytes[i] = binStr.charCodeAt(i);

    statusEl.textContent = 'Decompressing ' + (bytes.length / 1024 / 1024).toFixed(1) + ' MB gzip...';
    await new Promise(r => requestAnimationFrame(() => setTimeout(r, 16)));

    // DecompressionStream (Chrome/Edge/Safari 16+/Firefox 113+)
    const ds = new DecompressionStream('gzip');
    const stream = new Blob([bytes]).stream().pipeThrough(ds);
    const text = await new Response(stream).text();

    statusEl.textContent = 'Parsing ' + (text.length / 1024 / 1024).toFixed(1) + ' MB JSON...';
    await new Promise(r => requestAnimationFrame(() => setTimeout(r, 16)));

    const D = JSON.parse(text);
    META = D.meta; SERIES = D.series; START_TS = D.startTs;
    UNIVERSE = Object.keys(META);
    document.getElementById('compressedData').remove();

    // Populate the dropdown using BSE's IndustryNew (granular: Pharma, Metals,
    // Chemicals, etc.) with a fallback to the broad sector or 'Uncategorized'.
    // The selected key is stored on each option's data-key for filtering.
    const indCounts = {};
    for (const t of UNIVERSE) {
      const m = META[t];
      const ind = (m.industry && m.industry.trim()) || m.sector || 'Uncategorized';
      indCounts[ind] = (indCounts[ind] || 0) + 1;
    }
    const sectorSel = document.getElementById('sectorFilter');
    // Sort: largest industry first so common ones surface
    Object.entries(indCounts)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .forEach(([ind, n]) => {
        const opt = document.createElement('option');
        opt.value = ind;
        opt.textContent = ind + '  (' + n.toLocaleString('en-IN') + ')';
        sectorSel.appendChild(opt);
      });

    const dt = ((performance.now() - t0) / 1000).toFixed(1);
    statusEl.textContent = 'Ready in ' + dt + 's';
    const priced = Object.keys(SERIES).length;
    const unpriced = UNIVERSE.length - priced;
    document.getElementById('universeCount').textContent =
      UNIVERSE.length.toLocaleString('en-IN') + ' NSE & BSE listings' +
      ' (' + priced.toLocaleString('en-IN') + ' with prices, ' + unpriced.toLocaleString('en-IN') + ' metadata-only)';

    const today = new Date();
    const mar2020 = new Date(2020, 2, 1);
    document.getElementById('toDate').value   = today.toISOString().split('T')[0];
    document.getElementById('fromDate').value = mar2020.toISOString().split('T')[0];

    const ov = document.getElementById('loadingOverlay');
    ov.style.opacity = '0';
    setTimeout(() => ov.remove(), 300);

    loadData();
  } catch (err) {
    statusEl.innerHTML = 'Error: ' + (err && err.message ? err.message : err) +
      '<br><span class="text-[11px]">Your browser may not support DecompressionStream. Use Chrome 80+ / Edge / Safari 16+ / Firefox 113+.</span>';
    console.error(err);
  }
}

// Test a single mcap value against a single bucket key
function inMcapBucket(mcap, bucket) {
  if (bucket === 'below100')     return mcap > 0 && mcap <= 100;
  if (bucket === '100to500')     return mcap > 100    && mcap <= 500;
  if (bucket === '500to1000')    return mcap > 500    && mcap <= 1000;
  if (bucket === '1000to5000')   return mcap > 1000   && mcap <= 5000;
  if (bucket === '5000to20000')  return mcap > 5000   && mcap <= 20000;
  if (bucket === 'above20000')   return mcap > 20000;
  return false;
}
// True if mcap matches ANY bucket in the set; empty set means no filter (all)
function inAnyMcapBucket(mcap, bucketSet) {
  if (!bucketSet || bucketSet.size === 0) return true;
  for (const b of bucketSet) if (inMcapBucket(mcap, b)) return true;
  return false;
}

function firstOnOrAfter(arr, v) {
  let lo = 0, hi = arr.length - 1, ans = -1;
  while (lo <= hi) { const mid = (lo + hi) >> 1; if (arr[mid] >= v) { ans = mid; hi = mid - 1; } else lo = mid + 1; }
  return ans;
}
function lastOnOrBefore(arr, v) {
  let lo = 0, hi = arr.length - 1, ans = -1;
  while (lo <= hi) { const mid = (lo + hi) >> 1; if (arr[mid] <= v) { ans = mid; lo = mid + 1; } else hi = mid - 1; }
  return ans;
}

let lastResults = [];

function loadData() {
  const fromDate     = document.getElementById('fromDate').value;
  const toDate       = document.getElementById('toDate').value;
  const mcapBuckets  = getMcapBucketSet();   // Set of selected buckets (empty = all)
  const sectorFilter = document.getElementById('sectorFilter').value;
  if (!fromDate || !toDate)                 return alert('Please select both From Date and To Date');
  if (new Date(fromDate) >= new Date(toDate)) return alert('From Date must be earlier than To Date');

  const fromTs = Math.floor(new Date(fromDate + 'T00:00:00').getTime() / 1000);
  const toTs   = Math.floor(new Date(toDate   + 'T23:59:59').getTime() / 1000);
  const fromDayOffset = Math.floor((fromTs - START_TS) / DAY);
  const toDayOffset   = Math.floor((toTs   - START_TS) / DAY);

  const results = [];
  for (const ticker of UNIVERSE) {
    const m = META[ticker];
    if (!inAnyMcapBucket(m.mcap, mcapBuckets)) continue;
    const indKey = (m.industry && m.industry.trim()) || m.sector || 'Uncategorized';
    if (sectorFilter !== 'all' && indKey !== sectorFilter) continue;
    const ser = SERIES[ticker];
    // Base row — same shape regardless of whether we have prices.
    const row = {
      symbol: m.symbol, name: m.name,
      sector: indKey,                       // shown in the table chip
      sectorBroad: m.sector || '',           // kept for tooltip / CSV
      mcap: m.mcap,
      fromPrice: null, toPrice: null, changePercent: null,
      fromDate: null,  toDate: null,  noData: true,
    };
    if (ser) {
      const iFrom = firstOnOrAfter(ser.d, fromDayOffset);
      const iTo   = lastOnOrBefore(ser.d, toDayOffset);
      if (iFrom !== -1 && iTo !== -1 && iTo > iFrom) {
        const fromPrice = ser.p[iFrom] / 100;
        const toPrice   = ser.p[iTo]   / 100;
        if (fromPrice && toPrice) {
          row.fromPrice = fromPrice;
          row.toPrice   = toPrice;
          row.changePercent = ((toPrice - fromPrice) / fromPrice) * 100;
          row.fromDate = new Date((START_TS + ser.d[iFrom] * DAY) * 1000).toISOString().slice(0, 10);
          row.toDate   = new Date((START_TS + ser.d[iTo]   * DAY) * 1000).toISOString().slice(0, 10);
          row.noData = false;
        }
      }
    }
    results.push(row);
  }
  lastResults = results;
  renderResults(results);
  updateStats(results);
}

function renderResults(results) {
  const sortBy = document.getElementById('sortBy').value;
  const q = document.getElementById('searchBox').value.toLowerCase().trim();
  let f = results;
  if (q) f = f.filter(r => r.symbol.toLowerCase().includes(q) || r.name.toLowerCase().includes(q) || (r.sector || '').toLowerCase().includes(q));
  f = f.slice();
  // Push noData rows to the bottom for change-based sorts so price-bearing rows lead.
  const cmpChange = (a, b, dir) => {
    if (a.noData && b.noData) return 0;
    if (a.noData) return 1;
    if (b.noData) return -1;
    return dir * (a.changePercent - b.changePercent);
  };
  if      (sortBy === 'changeDesc') f.sort((a, b) => cmpChange(a, b, -1));
  else if (sortBy === 'changeAsc')  f.sort((a, b) => cmpChange(a, b,  1));
  else if (sortBy === 'mcapDesc')   f.sort((a, b) => b.mcap - a.mcap);
  else if (sortBy === 'mcapAsc')    f.sort((a, b) => a.mcap - b.mcap);
  else if (sortBy === 'nameAsc')    f.sort((a, b) => a.name.localeCompare(b.name));

  const tbody = document.getElementById('resultsBody');
  const MAX_ROWS = 500;
  const truncated = f.length > MAX_ROWS;
  const view = f.slice(0, MAX_ROWS);

  if (view.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="text-center text-slate-400 py-16 text-sm">No matching stocks. Adjust filters or search.</td></tr>';
  } else {
    const out = [];
    const DASH = '<span class="text-slate-400">\u2014</span>';
    for (let i = 0; i < view.length; i++) {
      const r = view[i];
      const mcap = r.mcap > 0 ? r.mcap.toLocaleString('en-IN', {maximumFractionDigits: 0}) : '\u2014';
      let fromCell, toCell, chgCell;
      if (r.noData) {
        fromCell = toCell = chgCell = DASH;
      } else {
        const cls = r.changePercent >= 0 ? 'chip-gain' : 'chip-loss';
        const arr = r.changePercent >= 0 ? '&#9650;' : '&#9660;';
        const sgn = r.changePercent >= 0 ? '+' : '';
        fromCell = '&#8377;' + r.fromPrice.toFixed(2);
        toCell   = '&#8377;' + r.toPrice.toFixed(2);
        chgCell  = '<span class="inline-flex items-center gap-1 ' + cls + ' rounded-md px-2 py-0.5 font-semibold text-xs tabular-nums">' + arr + ' ' + sgn + r.changePercent.toFixed(2) + '%</span>';
      }
      // Screener.in URL: NSE symbols use the symbol itself; BSE-only stocks use
      // the numeric scrip code (Screener accepts both formats).
      const screenerKey = encodeURIComponent(r.symbol);
      const screenerUrl = 'https://www.screener.in/company/' + screenerKey + '/';
      const linkAttrs = 'href="' + screenerUrl + '" target="_blank" rel="noopener" title="Open ' + r.symbol + ' on Screener.in"';
      out.push(
        '<tr class="hover:bg-slate-50 transition' + (r.noData ? ' bg-slate-50/40' : '') + '">' +
        '<td class="px-4 py-3 text-slate-400 text-xs">' + (i + 1) + '</td>' +
        '<td class="px-4 py-3"><a ' + linkAttrs + ' class="font-semibold text-slate-800 hover:text-blue-600 hover:underline">' + r.symbol + '</a></td>' +
        '<td class="px-4 py-3"><a ' + linkAttrs + ' class="text-slate-700 hover:text-blue-600 hover:underline">' + r.name + '</a></td>' +
        '<td class="px-4 py-3"><span class="text-xs bg-slate-100 text-slate-600 rounded-md px-2 py-0.5">' + r.sector + '</span></td>' +
        '<td class="px-4 py-3 text-right text-slate-700 tabular-nums">' + mcap + '</td>' +
        '<td class="px-4 py-3 text-right text-slate-600 tabular-nums">' + fromCell + '</td>' +
        '<td class="px-4 py-3 text-right text-slate-800 font-medium tabular-nums">' + toCell + '</td>' +
        '<td class="px-4 py-3 text-right">' + chgCell + '</td>' +
        '</tr>'
      );
    }
    tbody.innerHTML = out.join('');
  }
  const noDataCount = f.filter(r => r.noData).length;
  const noDataNote  = noDataCount ? ' &middot; <span class="text-slate-400">' + noDataCount.toLocaleString('en-IN') + ' without price data</span>' : '';
  document.getElementById('resultCount').innerHTML =
    '<span class="font-semibold text-slate-700">' + f.length.toLocaleString('en-IN') + '</span> stocks' + noDataNote +
    (truncated ? ' (showing top ' + MAX_ROWS + ' \u2014 use sort/search/filters to narrow)' : '');
}

function updateStats(results) {
  if (!results.length) { document.getElementById('statsGrid').innerHTML = ''; return; }
  const priced  = results.filter(r => !r.noData);
  const gainers = priced.filter(r => r.changePercent > 0).length;
  const losers  = priced.filter(r => r.changePercent < 0).length;
  const avg     = priced.length ? priced.reduce((s, r) => s + r.changePercent, 0) / priced.length : 0;
  const top     = priced.length ? priced.reduce((m, r) => r.changePercent > m.changePercent ? r : m) : null;
  const card = (label, value, cls = '') =>
    '<div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">' +
    '<div class="text-[11px] text-slate-500 uppercase font-semibold tracking-wide">' + label + '</div>' +
    '<div class="text-xl font-bold mt-1 ' + cls + '">' + value + '</div></div>';
  const topCard = top ?
    '<div class="bg-white rounded-xl shadow-sm border border-slate-200 p-4">' +
    '<div class="text-[11px] text-slate-500 uppercase font-semibold tracking-wide">Top Mover</div>' +
    '<div class="text-sm font-bold mt-1 text-slate-800 truncate">' + top.symbol + '</div>' +
    '<div class="text-xs gain mt-0.5">+' + top.changePercent.toFixed(2) + '%</div></div>'
    : card('Top Mover', '\u2014');
  document.getElementById('statsGrid').innerHTML =
    card('Total Stocks', results.length.toLocaleString('en-IN')) +
    card('Gainers', gainers.toLocaleString('en-IN'), 'gain') +
    card('Losers',  losers.toLocaleString('en-IN'),  'loss') +
    card('Avg Change', priced.length ? ((avg >= 0 ? '+' : '') + avg.toFixed(2) + '%') : '\u2014',
         priced.length ? (avg >= 0 ? 'gain' : 'loss') : '') +
    topCard;
}

function exportCSV() {
  if (!lastResults.length) return alert('No data to export. Load data first.');
  const rows = [['Symbol','Company','Sector','Market Cap (Cr)','From Date','From Price','To Date','To Price','Change %']];
  lastResults.forEach(r => rows.push([
    r.symbol, r.name, r.sector, r.mcap,
    r.fromDate || '', r.fromPrice != null ? r.fromPrice.toFixed(2) : '',
    r.toDate   || '', r.toPrice   != null ? r.toPrice.toFixed(2)   : '',
    r.changePercent != null ? r.changePercent.toFixed(2) : '',
  ]));
  const csv  = rows.map(row => row.map(c => '"' + String(c).replace(/"/g, '""') + '"').join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url;
  a.download = 'dhruvan-stocks-' + document.getElementById('fromDate').value + '-to-' + document.getElementById('toDate').value + '.csv';
  a.click();
  URL.revokeObjectURL(url);
}

document.getElementById('loadBtn').addEventListener('click', loadData);
document.getElementById('sortBy').addEventListener('change', () => lastResults.length && renderResults(lastResults));
document.getElementById('searchBox').addEventListener('input', () => lastResults.length && renderResults(lastResults));
document.getElementById('exportBtn').addEventListener('click', exportCSV);

// --- Market cap multi-select ---
const MCAP_LABELS = {
  'below100':    '\u2264 100 Cr',
  '100to500':    '100\u2013500 Cr',
  '500to1000':   '500\u20131k Cr',
  '1000to5000':  '1k\u20135k Cr',
  '5000to20000': '5k\u201320k Cr',
  'above20000':  '\u2265 20k Cr',
};
function getMcapBucketSet() {
  const cbs = document.querySelectorAll('#mcapPanel .mcap-cb:checked');
  return new Set(Array.from(cbs).map(c => c.dataset.bucket));
}
function updateMcapLabel() {
  const cbs = document.querySelectorAll('#mcapPanel .mcap-cb:checked');
  const lab = document.getElementById('mcapLabel');
  if (cbs.length === 0)        lab.textContent = 'All market caps';
  else if (cbs.length === 1)   lab.textContent = MCAP_LABELS[cbs[0].dataset.bucket];
  else if (cbs.length <= 3)    lab.textContent = Array.from(cbs).map(c => MCAP_LABELS[c.dataset.bucket]).join(', ');
  else                          lab.textContent = cbs.length + ' selected';
}
document.getElementById('mcapTrigger').addEventListener('click', e => {
  e.stopPropagation();
  document.getElementById('mcapPanel').classList.toggle('hidden');
});
document.querySelectorAll('#mcapPanel .mcap-cb').forEach(cb =>
  cb.addEventListener('change', () => { updateMcapLabel(); if (lastResults.length || META) loadData(); }));
document.getElementById('mcapSelectAll').addEventListener('click', () => {
  document.querySelectorAll('#mcapPanel .mcap-cb').forEach(cb => cb.checked = true);
  updateMcapLabel();
  if (Object.keys(META).length) loadData();
});
document.getElementById('mcapClear').addEventListener('click', () => {
  document.querySelectorAll('#mcapPanel .mcap-cb').forEach(cb => cb.checked = false);
  updateMcapLabel();
  if (Object.keys(META).length) loadData();
});
// click outside closes the panel
document.addEventListener('click', e => {
  const panel = document.getElementById('mcapPanel');
  const trigger = document.getElementById('mcapTrigger');
  if (!panel.contains(e.target) && !trigger.contains(e.target)) panel.classList.add('hidden');
});
document.querySelectorAll('.preset-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const today = new Date();
    let from;
    if (btn.dataset.ytd)             from = new Date(today.getFullYear(), 0, 1);
    else if (btn.dataset.since1996)  from = new Date(1996, 0, 1);
    else { const days = parseInt(btn.dataset.days, 10); from = new Date(); from.setDate(today.getDate() - days); }
    document.getElementById('toDate').value   = today.toISOString().split('T')[0];
    document.getElementById('fromDate').value = from.toISOString().split('T')[0];
  });
});

loadAndInit();
</script>
</body>
</html>
"""

out = HTML.replace("__B64_PAYLOAD__", b64).replace("__START_DATE__", start_date).replace("__GEN_DATE__", gen_date)
OUT_HTML.write_text(out, encoding="utf-8")
print(f"Wrote {OUT_HTML} ({OUT_HTML.stat().st_size/1024/1024:.2f} MB)")
