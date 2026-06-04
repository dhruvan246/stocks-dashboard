#!/usr/bin/env python3
"""Build the Mutual Funds dashboard — single-file HTML with all schemes embedded."""
import json, gzip, base64
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
SRC  = ROOT / "scripts" / "mutual_funds.json"
OUT  = ROOT / "docs" / "mutual-funds.html"

mf = json.loads(SRC.read_text())
print(f"Building MF dashboard for {len(mf)} schemes...")

raw = json.dumps(mf, separators=(",", ":")).encode()
gz  = gzip.compress(raw, compresslevel=9)
b64 = base64.b64encode(gz).decode()
print(f"  Raw {len(raw)/1024:.1f} KB → gzip {len(gz)/1024:.1f} KB → b64 {len(b64)/1024:.1f} KB")

gen = datetime.now().strftime("%d %b %Y %H:%M")

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Dhruvan's mutual funds</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  table { font-feature-settings: "tnum" 1; }
  table thead th { position:sticky; top:0; background:#f8fafc; z-index:10; }
  .badge { display:inline-block; padding:1px 6px; border-radius:9999px; font-size:10px; font-weight:600; }
  .b-equity { background:#dbeafe; color:#1e40af; }
  .b-debt   { background:#fef3c7; color:#92400e; }
  .b-hybrid { background:#fae8ff; color:#86198f; }
  .b-passive{ background:#dcfce7; color:#166534; }
  .b-fof    { background:#e0e7ff; color:#3730a3; }
  .b-other  { background:#f1f5f9; color:#475569; }
  #loadingOverlay{position:fixed;inset:0;background:#f8fafc;display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:50;}
  .pos { color:#15803d; font-weight:600; }
  .neg { color:#b91c1c; font-weight:600; }
</style>
</head>
<body class="bg-slate-50 min-h-screen text-slate-800">

<div id="loadingOverlay">
  <div class="w-10 h-10 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mb-3"></div>
  <p class="text-sm text-slate-600" id="statusText">Loading mutual funds…</p>
</div>

<header class="bg-white shadow-sm border-b border-slate-200 sticky top-0 z-30">
  <div class="max-w-screen-2xl mx-auto px-4 py-3 flex items-center justify-between">
    <h1 class="text-xl md:text-2xl font-bold text-slate-900">Dhruvan's mutual funds</h1>
    <div class="text-xs text-slate-500">Snapshot &middot; __GEN__</div>
    <nav class="text-sm">
      <a href="./nse-bse-dashboard.html" class="text-blue-600 hover:underline">&larr; Stocks dashboard</a>
    </nav>
  </div>
</header>

<main class="max-w-screen-2xl mx-auto px-4 py-6">
  <!-- Filter bar -->
  <div class="bg-white p-4 rounded-xl shadow-sm border border-slate-200 mb-4">
    <div class="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
      <div>
        <label class="block text-xs font-medium text-slate-600 mb-1">Search by name / AMC</label>
        <input type="search" id="searchBox" placeholder="e.g. Parag Parikh, Small Cap, HDFC…"
               class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"/>
      </div>
      <div>
        <label class="block text-xs font-medium text-slate-600 mb-1">Category</label>
        <select id="catFilter" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 bg-white">
          <option value="all">All categories</option>
        </select>
      </div>
      <div>
        <label class="block text-xs font-medium text-slate-600 mb-1">Min. years since inception</label>
        <select id="yrFilter" class="w-full border border-slate-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 bg-white">
          <option value="0">Any</option>
          <option value="1">1 year+</option>
          <option value="3">3 years+</option>
          <option value="5" selected>5 years+</option>
          <option value="10">10 years+</option>
        </select>
      </div>
      <div class="text-right">
        <button id="resetBtn" class="text-sm text-slate-600 hover:text-blue-600">Reset filters</button>
      </div>
    </div>
  </div>

  <!-- Stats -->
  <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4" id="statsBar">
    <div class="bg-white p-3 rounded-xl shadow-sm border border-slate-200">
      <div class="text-[11px] text-slate-500 uppercase">Showing</div>
      <div class="text-lg font-bold" id="statShown">—</div>
    </div>
    <div class="bg-white p-3 rounded-xl shadow-sm border border-slate-200">
      <div class="text-[11px] text-slate-500 uppercase">Categories</div>
      <div class="text-lg font-bold" id="statCats">—</div>
    </div>
    <div class="bg-white p-3 rounded-xl shadow-sm border border-slate-200">
      <div class="text-[11px] text-slate-500 uppercase">Avg CAGR (shown)</div>
      <div class="text-lg font-bold" id="statCagr">—</div>
    </div>
    <div class="bg-white p-3 rounded-xl shadow-sm border border-slate-200">
      <div class="text-[11px] text-slate-500 uppercase">Best returner (shown)</div>
      <div class="text-sm font-bold truncate" id="statBest" title="">—</div>
    </div>
  </div>

  <!-- Results -->
  <div class="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
    <div class="overflow-x-auto max-h-[calc(100vh-280px)] overflow-y-auto">
      <table class="w-full text-xs">
        <thead class="text-[10px] uppercase text-slate-600 border-b border-slate-200">
          <tr>
            <th class="px-2 py-2 text-left font-semibold w-10">#</th>
            <th class="px-3 py-2 text-left font-semibold cursor-pointer hover:bg-slate-100 select-none min-w-[260px]" data-sort="short">Mutual fund <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none" data-sort="aum">AUM<br><span class="normal-case text-slate-400 text-[9px] font-normal">(soon)</span></th>
            <th class="px-2 py-2 text-left font-semibold cursor-pointer hover:bg-slate-100 select-none" data-sort="cat">Category <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none" data-sort="years">Yrs <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none border-l border-slate-200 bg-slate-50" data-sort="r1d">1D <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-slate-50" data-sort="r1w">1W <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-slate-50" data-sort="r1m">1M <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-slate-50" data-sort="r3m">3M <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-slate-50" data-sort="r6m">6M <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-slate-50" data-sort="r1y">1Y <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none border-l border-slate-200 bg-blue-50" data-sort="r3y">3Y* <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-blue-50" data-sort="r5y">5Y* <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none bg-blue-50" data-sort="r10y">10Y* <span class="sort-ind text-slate-300">&#8597;</span></th>
            <th class="px-2 py-2 text-right font-semibold cursor-pointer hover:bg-slate-100 select-none border-l border-slate-200 bg-blue-100" data-sort="cagrPct"><b>Since incep*</b> <span class="sort-ind text-blue-600">&#9660;</span></th>
          </tr>
          <tr class="text-[9px] normal-case text-slate-400">
            <th class="px-2 py-1"></th>
            <th class="px-3 py-1 text-left">name &middot; AMC</th>
            <th class="px-2 py-1 text-right">₹ Cr</th>
            <th class="px-2 py-1"></th>
            <th class="px-2 py-1 text-right">since launch</th>
            <th colspan="6" class="px-2 py-1 text-center bg-slate-50 border-l border-slate-200">absolute % returns</th>
            <th colspan="4" class="px-2 py-1 text-center bg-blue-50 border-l border-slate-200">annualized (CAGR) %</th>
          </tr>
        </thead>
        <tbody id="resultsBody"></tbody>
      </table>
    </div>
  </div>
  <p class="text-[11px] text-slate-500 mt-3">
    * Columns marked with asterisk are annualized (CAGR). 1D–1Y are absolute returns.
    Data sources: AMFI NAVAll daily file (scheme master + current NAV) and mfapi.in (full NAV history).
    Only Direct-Growth variants shown (no Regular plans, no IDCW/dividend options).
    AUM is a placeholder for now — AMFI's scheme-wise AUM is published as a monthly XLSX and will be ingested separately.
    Returns shown as "—" mean the fund hasn't existed for that lookback period (e.g. no 10Y return for a 6-year-old fund).
    Side-pocket / Segregated schemes show -100% returns by design.
  </p>
</main>

<script id="compressedData" type="application/octet-stream">__B64__</script>
<script>
'use strict';
let ALL = [];
let SHOWN = [];
let SORT = { key: 'cagrPct', dir: -1 };

async function loadData() {
  const statusEl = document.getElementById('statusText');
  statusEl.textContent = 'Decoding…';
  const b64 = document.getElementById('compressedData').textContent.replace(/\s+/g,'');
  const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
  const text = await new Response(stream).text();
  ALL = JSON.parse(text);
  document.getElementById('compressedData').remove();

  // Populate category dropdown
  const cats = {};
  for (const r of ALL) cats[r.cat] = (cats[r.cat] || 0) + 1;
  const sel = document.getElementById('catFilter');
  // Sort categories: equity first, then hybrid, debt, passive, others
  const order = (c) => {
    if (c.startsWith('Equity')) return 1;
    if (c.startsWith('Hybrid')) return 2;
    if (c.startsWith('Passive')) return 3;
    if (c.startsWith('Debt'))   return 4;
    if (c.startsWith('FoF'))    return 5;
    if (c.startsWith('Solution')) return 6;
    return 7;
  };
  Object.keys(cats).sort((a,b) => order(a)-order(b) || a.localeCompare(b)).forEach(c => {
    const opt = document.createElement('option');
    opt.value = c;
    opt.textContent = c + '  (' + cats[c] + ')';
    sel.appendChild(opt);
  });

  document.getElementById('loadingOverlay').remove();
  render();
}

function badgeClass(cat) {
  if (cat.startsWith('Equity'))  return 'b-equity';
  if (cat.startsWith('Debt'))    return 'b-debt';
  if (cat.startsWith('Hybrid'))  return 'b-hybrid';
  if (cat.startsWith('Passive')) return 'b-passive';
  if (cat.startsWith('FoF'))     return 'b-fof';
  return 'b-other';
}

function fmtINR(n) {
  if (n == null || isNaN(n)) return '—';
  return n.toLocaleString('en-IN', { maximumFractionDigits: 0 });
}

function applyFilters() {
  const q     = document.getElementById('searchBox').value.trim().toLowerCase();
  const cat   = document.getElementById('catFilter').value;
  const minYr = parseFloat(document.getElementById('yrFilter').value) || 0;
  let s = ALL.filter(r => {
    if (cat !== 'all' && r.cat !== cat) return false;
    if (r.years < minYr) return false;
    if (q) {
      const hay = (r.short + ' ' + (r.amc || '') + ' ' + r.name).toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
  // Apply sort — push nulls to the bottom regardless of direction
  const dir = SORT.dir;
  s.sort((a, b) => {
    const va = a[SORT.key], vb = b[SORT.key];
    const aN = (va === null || va === undefined);
    const bN = (vb === null || vb === undefined);
    if (aN && bN) return 0;
    if (aN) return 1;
    if (bN) return -1;
    if (typeof va === 'string') return dir === -1 ? vb.localeCompare(va) : va.localeCompare(vb);
    return (vb - va) * (dir === -1 ? 1 : -1);
  });
  SHOWN = s;
}

function fmtRet(v) {
  if (v == null) return '<span class="text-slate-300">—</span>';
  const cls = v >= 0 ? 'pos' : 'neg';
  const sign = v >= 0 ? '+' : '';
  return '<span class="' + cls + '">' + sign + v.toFixed(1) + '</span>';
}

function render() {
  applyFilters();
  const tbody = document.getElementById('resultsBody');
  tbody.innerHTML = '';
  const frag = document.createDocumentFragment();
  // Limit rendered rows to first 1500 for speed
  const slice = SHOWN.slice(0, 1500);
  slice.forEach((r, i) => {
    const tr = document.createElement('tr');
    tr.className = i % 2 ? 'bg-slate-50' : '';
    const cagr = r.cagrPct;
    const staleBadge = r.stale ? ' <span class="badge b-other" title="Latest NAV is ' + (r.staleDays || '?') + ' days old — short-period returns suppressed">stale</span>' : '';
    tr.innerHTML =
      '<td class="px-2 py-2 text-slate-500 text-[10px]">' + (i + 1) + '</td>' +
      '<td class="px-3 py-2"><div class="font-medium text-slate-800 text-xs">' + r.short.replace(/[<>]/g, '') + staleBadge + '</div>' +
        (r.amc ? '<div class="text-[10px] text-slate-500">' + r.amc.replace(/[<>]/g, '') + '</div>' : '') + '</td>' +
      '<td class="px-2 py-2 text-right text-slate-300 italic text-[10px]">—</td>' +
      '<td class="px-2 py-2"><span class="badge ' + badgeClass(r.cat) + '">' + r.cat + '</span></td>' +
      '<td class="px-2 py-2 text-right text-slate-600">' + r.years.toFixed(1) + '</td>' +
      '<td class="px-2 py-2 text-right border-l border-slate-200">' + fmtRet(r.r1d) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r1w) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r1m) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r3m) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r6m) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r1y) + '</td>' +
      '<td class="px-2 py-2 text-right border-l border-slate-200">' + fmtRet(r.r3y) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r5y) + '</td>' +
      '<td class="px-2 py-2 text-right">' + fmtRet(r.r10y) + '</td>' +
      '<td class="px-2 py-2 text-right border-l border-slate-200 text-sm ' + (cagr >= 0 ? 'pos' : 'neg') + '">' + (cagr >= 0 ? '+' : '') + cagr.toFixed(2) + '</td>';
    frag.appendChild(tr);
  });
  tbody.appendChild(frag);

  // Stats
  document.getElementById('statShown').textContent = SHOWN.length.toLocaleString() + (SHOWN.length > 1500 ? '  (first 1,500 rendered)' : '');
  const cs = new Set(SHOWN.map(r => r.cat));
  document.getElementById('statCats').textContent = cs.size;
  if (SHOWN.length) {
    const avg = SHOWN.reduce((s, r) => s + (r.cagrPct || 0), 0) / SHOWN.length;
    document.getElementById('statCagr').textContent = (avg >= 0 ? '+' : '') + avg.toFixed(2) + '%';
    const top = SHOWN[0];
    const lbl = top.short + '  (' + top.cagrPct.toFixed(1) + '% CAGR)';
    const e = document.getElementById('statBest');
    e.textContent = lbl;
    e.setAttribute('title', lbl);
  } else {
    document.getElementById('statCagr').textContent = '—';
    document.getElementById('statBest').textContent = '—';
  }
}

// Wire events
document.addEventListener('DOMContentLoaded', () => {
  loadData().then(() => {
    document.getElementById('searchBox').addEventListener('input', render);
    document.getElementById('catFilter').addEventListener('change', render);
    document.getElementById('yrFilter').addEventListener('change', render);
    document.getElementById('resetBtn').addEventListener('click', () => {
      document.getElementById('searchBox').value = '';
      document.getElementById('catFilter').value = 'all';
      document.getElementById('yrFilter').value = '5';
      SORT = { key: 'cagrPct', dir: -1 };
      render();
    });
    // Sortable headers
    document.querySelectorAll('th[data-sort]').forEach(th => {
      th.addEventListener('click', () => {
        const k = th.getAttribute('data-sort');
        if (SORT.key === k) SORT.dir = -SORT.dir;
        else { SORT.key = k; SORT.dir = (k === 'name' || k === 'cat') ? 1 : -1; }
        document.querySelectorAll('.sort-ind').forEach(el => { el.textContent = '↕'; el.className = 'sort-ind text-slate-300'; });
        const ind = th.querySelector('.sort-ind');
        ind.textContent = SORT.dir === -1 ? '▼' : '▲';
        ind.className = 'sort-ind text-blue-600';
        render();
      });
    });
  });
});
</script>
</body>
</html>
"""
HTML = HTML.replace("__B64__", b64).replace("__GEN__", gen)
OUT.write_text(HTML)
print(f"Wrote {OUT} ({OUT.stat().st_size / 1024:.1f} KB)")
