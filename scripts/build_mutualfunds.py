#!/usr/bin/env python3
"""Build the Mutual Funds dashboard — single-file HTML with all schemes embedded."""
import json, gzip, base64
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
SRC  = ROOT / "scripts" / "mutual_funds.json"
OUT  = ROOT / "docs" / "mutual-funds.html"

mf = json.loads(SRC.read_text(encoding="utf-8"))
print(f"Building MF dashboard for {len(mf)} schemes...")

# ---------------------------------------------------------------------------
# Groww-style category normalization.
# Groww's screener (groww.in/mutual-funds/filter) groups funds into a small set
# of parent categories, each with SEBI subcategories. The raw `category` field
# from AMFI/mfapi is messy: clean SEBI names for open-ended funds, but garbage
# ("IDF", "Income", "1099 Days", "Growth") for closed-ended / FMP / defunct
# schemes that Groww does not list at all. We map every fund to a (group, sub)
# pair from Groww's taxonomy; anything that doesn't fit -> "Uncategorized".
# ---------------------------------------------------------------------------
import re as _re

GROUP_ORDER = ['Equity', 'Hybrid', 'Debt', 'Index / Other',
               'Commodities', 'Solution Oriented', 'Uncategorized']
SUB_ORDER = {
    'Equity': ['Large Cap', 'Large & Mid Cap', 'Mid Cap', 'Small Cap',
               'Multi Cap', 'Flexi Cap', 'ELSS (Tax Saver)', 'Sectoral / Thematic',
               'Focused', 'Value', 'Contra', 'Dividend Yield'],
    'Hybrid': ['Aggressive Hybrid', 'Balanced Advantage', 'Multi Asset Allocation',
               'Equity Savings', 'Arbitrage', 'Conservative Hybrid'],
    'Debt': ['Overnight', 'Liquid', 'Ultra Short Duration', 'Low Duration',
             'Money Market', 'Short Duration', 'Medium Duration',
             'Medium to Long Duration', 'Long Duration', 'Dynamic Bond',
             'Corporate Bond', 'Credit Risk', 'Banking & PSU', 'Gilt',
             'Gilt 10Y Constant', 'Floater'],
    'Index / Other': ['Index Funds', 'ETF', 'Fund of Funds'],
    'Commodities': ['Gold', 'Silver'],
    'Solution Oriented': ['Retirement', "Children's"],
    'Uncategorized': ['Uncategorized'],
}

def normalize_category(category, name=''):
    c = (category or '').lower()
    n = (name or '').lower()
    def has(*xs): return any(x in c for x in xs)
    # --- Equity ---
    if   has('large & mid', 'large and mid'): g, s = 'Equity', 'Large & Mid Cap'
    elif has('large cap'):           g, s = 'Equity', 'Large Cap'
    elif has('mid cap'):             g, s = 'Equity', 'Mid Cap'
    elif has('small cap'):           g, s = 'Equity', 'Small Cap'
    elif has('multi cap', 'multicap'): g, s = 'Equity', 'Multi Cap'
    elif has('flexi cap', 'flexicap'): g, s = 'Equity', 'Flexi Cap'
    elif has('elss', 'tax saver'):   g, s = 'Equity', 'ELSS (Tax Saver)'
    elif has('sectoral', 'thematic'): g, s = 'Equity', 'Sectoral / Thematic'
    elif has('focused'):             g, s = 'Equity', 'Focused'
    elif has('dividend yield'):      g, s = 'Equity', 'Dividend Yield'
    elif has('contra'):              g, s = 'Equity', 'Contra'
    # --- Hybrid ---
    elif has('aggressive'):          g, s = 'Hybrid', 'Aggressive Hybrid'
    elif has('balanced advantage', 'dynamic asset'): g, s = 'Hybrid', 'Balanced Advantage'
    elif has('multi asset', 'multi-asset'): g, s = 'Hybrid', 'Multi Asset Allocation'
    elif has('equity savings'):      g, s = 'Hybrid', 'Equity Savings'
    elif has('arbitrage'):           g, s = 'Hybrid', 'Arbitrage'
    elif has('conservative'):        g, s = 'Hybrid', 'Conservative Hybrid'
    # --- Debt ---
    elif has('overnight'):           g, s = 'Debt', 'Overnight'
    elif has('liquid'):              g, s = 'Debt', 'Liquid'
    elif has('ultra short'):         g, s = 'Debt', 'Ultra Short Duration'
    elif has('low duration'):        g, s = 'Debt', 'Low Duration'
    elif has('money market'):        g, s = 'Debt', 'Money Market'
    elif has('short duration'):      g, s = 'Debt', 'Short Duration'
    elif has('medium to long', 'medium-long'): g, s = 'Debt', 'Medium to Long Duration'
    elif has('medium duration'):     g, s = 'Debt', 'Medium Duration'
    elif has('long duration'):       g, s = 'Debt', 'Long Duration'
    elif has('dynamic bond'):        g, s = 'Debt', 'Dynamic Bond'
    elif has('corporate bond'):      g, s = 'Debt', 'Corporate Bond'
    elif has('credit risk'):         g, s = 'Debt', 'Credit Risk'
    elif has('banking'):             g, s = 'Debt', 'Banking & PSU'
    elif has('10 year constant', '10y constant'): g, s = 'Debt', 'Gilt 10Y Constant'
    elif has('gilt'):                g, s = 'Debt', 'Gilt'
    elif has('floater', 'floating rate'): g, s = 'Debt', 'Floater'
    elif has('value'):               g, s = 'Equity', 'Value'   # after the specific ones
    # --- Index / Other ---
    elif has('index'):               g, s = 'Index / Other', 'Index Funds'
    elif has('etf'):                 g, s = 'Index / Other', 'ETF'
    elif has('fof', 'fund of fund'): g, s = 'Index / Other', 'Fund of Funds'
    # --- Solution Oriented ---
    elif has('retirement'):          g, s = 'Solution Oriented', 'Retirement'
    elif has('children', 'child'):   g, s = 'Solution Oriented', "Children's"
    else:                            g, s = 'Uncategorized', 'Uncategorized'
    # Commodities are identified by fund name (Gold/Silver ETFs & FoFs), which
    # otherwise land in Index / Other or Uncategorized.
    if g in ('Index / Other', 'Uncategorized'):
        if   _re.search(r'\bgold\b', n):   g, s = 'Commodities', 'Gold'
        elif _re.search(r'\bsilver\b', n): g, s = 'Commodities', 'Silver'
    return g, s

from collections import Counter as _Counter
_dist = _Counter()
for r in mf:
    g, s = normalize_category(r.get('category', ''), r.get('name', ''))
    r['g'], r['s'] = g, s
    r.pop('cat', None)  # drop the old messy short-category field
    _dist[g] += 1
print("  Category groups: " + ", ".join(f"{g}={_dist[g]}" for g in GROUP_ORDER if _dist[g]))

raw = json.dumps(mf, separators=(",", ":")).encode()
gz  = gzip.compress(raw, compresslevel=9)
b64 = base64.b64encode(gz).decode()
print(f"  Raw {len(raw)/1024:.1f} KB → gzip {len(gz)/1024:.1f} KB → b64 {len(b64)/1024:.1f} KB")

# ---------------------------------------------------------------------------
# Daily NAV history for the exact-date calculator. fetch_mf_returns.py already
# builds the compact payload (shared YYYYMMDD axis + per-fund [startIdx, delta-
# encoded paise]) and writes it pre-gzipped+base64 to scripts/mf_history.b64.
# We just embed it. If absent (build without a fresh fetch), the date filter is
# disabled.
# ---------------------------------------------------------------------------
def _ymd(n):
    s = str(n)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"

HISTSRC = ROOT / "scripts" / "mf_history.b64"
if HISTSRC.exists():
    hist_b64 = HISTSRC.read_text(encoding="utf-8").strip()
    # Write the daily NAV history as an EXTERNAL gzip file. The page lazy-loads it only when the
    # custom-date calculator or a fund-detail modal needs it — instead of base64-inlining ~10 MB
    # into the HTML, which forced every first-time visitor to download it all up front.
    hist_gz = base64.b64decode(hist_b64)
    (OUT.parent / "mf_history.bin").write_bytes(hist_gz)
    print(f"  History: wrote external mf_history.bin ({len(hist_gz)/1024/1024:.1f} MB, lazy-loaded)")
    # The page needs the covered date span up front to bound the date pickers, but the history
    # itself only loads on first use — so bake the span in here rather than after the fetch.
    _hd = json.loads(gzip.decompress(hist_gz))["dates"]
    hist_lo, hist_hi = _ymd(_hd[0]), _ymd(_hd[-1])
    del _hd
    print(f"  History: covers {hist_lo} → {hist_hi}")
else:
    hist_lo = hist_hi = ""
    print("  History: scripts/mf_history.b64 not found — custom date filter disabled")

gen = datetime.now().strftime("%d %b %Y %H:%M")

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>STOCKSWORLD · Mutual Funds</title>
<meta name="description" content="Every growth-plan mutual fund on one screen — pick a category on the left, see its funds ranked in the middle and the selected fund's NAV chart and return calendars on the right." />
<script src="https://cdn.tailwindcss.com"></script>
<link rel="stylesheet" href="./theme.css" />
<script src="./theme.js"></script>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@5.2.1/dist/lightweight-charts.standalone.production.js"></script>
<script src="./os-kit.js"></script>
<style>
  body{font-feature-settings:"tnum" 1}
  #grid{grid-template-columns:minmax(236px,270px) minmax(310px,380px) minmax(0,1fr)}
  #grid.tbl{grid-template-columns:minmax(236px,270px) minmax(0,1fr)}
  #grid.tbl #pF,#grid.tbl #pD,#grid:not(.tbl) #pT{display:none}
  #gHead,.gr{grid-template-columns:minmax(0,1fr) 38px 56px}
  #fHead,.fr{grid-template-columns:minmax(0,1fr) 38px 64px}
  .gr.sub .n{padding-left:10px;color:var(--text-muted)} .gr.grp .n{font-weight:600} .gr.zero{opacity:.45}
  .os-pane.kb{border-color:var(--border-strong)}
  .tg{display:inline-block;margin-right:6px;font-size:9.5px;font-weight:600;letter-spacing:.04em;color:var(--text-faint);border:1px solid var(--border);border-radius:4px;padding:0 4px;vertical-align:1px;font-style:normal}
  #pF .os-ps{flex-wrap:nowrap} html[data-theme] #pF .os-ps input[type="search"]{flex:1 1 0;min-width:0;width:auto}
  #dTtl{font-size:15px;font-weight:700;color:var(--text);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  #dMeta{padding:7px 12px;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  #dStats{grid-template-columns:repeat(6,minmax(0,1fr)) !important}
  #dStats div{border-right:1px solid var(--border) !important;border-bottom:0 !important;padding:7px 9px}
  #dStats div:nth-child(-n+6){border-bottom:1px solid var(--border) !important} #dStats div:nth-child(6n){border-right:0 !important}
  #calBox{flex:1;min-height:0;overflow:auto;padding:10px 12px}
  #calBox h3{font-size:13px;font-weight:600;color:var(--text);margin:0 0 6px}
  #calBox p{font-size:11.5px;color:var(--text-faint);margin:8px 0 0;line-height:1.5}
  .cal{border-collapse:collapse;font-size:11.5px;width:100%;font-variant-numeric:tabular-nums}
  html[data-theme] .cal th{background:transparent;color:var(--text-faint);font-weight:500;font-size:10.5px;padding:4px 5px;text-align:right;border-bottom:1px solid var(--border);text-transform:uppercase;letter-spacing:.04em}
  html[data-theme] .cal th:first-child{text-align:left}
  html[data-theme] .cal td{padding:3px 5px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap}
  html[data-theme] .cal td.y{text-align:left;color:var(--text-muted);font-weight:500}
  .cal td.na{color:var(--text-faint)} .cal td.pos{color:var(--pos)} .cal td.neg{color:var(--neg)}
  html[data-theme] .cal td.yr,html[data-theme] .cal th.yr{border-left:1px solid var(--border);font-weight:600}
  #tBox{flex:1;min-height:0;overflow:auto}
  #tTbl{border-collapse:separate;border-spacing:0;font-size:12.5px;font-variant-numeric:tabular-nums;width:100%}
  html[data-theme] #tTbl th{position:sticky;top:0;z-index:2;background:var(--surface-2);color:var(--text-muted);font-weight:500;font-size:11.5px;padding:7px 8px;text-align:right;white-space:nowrap;border-bottom:1px solid var(--border);cursor:pointer;user-select:none;text-transform:none;letter-spacing:0}
  html[data-theme] #tTbl th.l{text-align:left} html[data-theme] #tTbl th.on{color:var(--accent-text)} html[data-theme] #tTbl th.sep,html[data-theme] #tTbl td.sep{border-left:1px solid var(--border)}
  html[data-theme] #tTbl td{padding:5px 8px;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap}
  html[data-theme] #tTbl td.l{text-align:left} #tTbl td.fn{white-space:normal;min-width:240px;cursor:pointer;color:var(--text)} #tTbl td.fn:hover b{color:var(--accent-text)}
  #tTbl td.fn b{font-weight:500} #tTbl td.fn small{display:block;font-size:11px;color:var(--text-faint)} #tTbl td.mu{color:var(--text-muted)}
  #rangeInfo{white-space:normal;margin:-2px 0 8px} #rangeInfo:empty{display:none}
  #custWarn{margin:0 0 10px;padding:7px 10px;border:1px solid var(--border);border-radius:6px;background:var(--warn-soft);color:var(--text);font-size:12.5px}
  #status{margin-bottom:10px}
  #how{margin-top:10px;font-size:12px;color:var(--text-faint);line-height:1.55} #how summary{cursor:pointer;color:var(--text-muted)}
  .os-asof a{color:var(--accent-text)}
  @media (max-width:1180px){ #grid{grid-template-columns:minmax(190px,220px) minmax(290px,340px) minmax(0,1fr)} #grid.tbl{grid-template-columns:minmax(190px,220px) minmax(0,1fr)} }
  @media (max-width:899px){ #grid,#grid.tbl{grid-template-columns:minmax(0,1fr)} #calBox{max-height:70vh} #tBox{max-height:70vh} #dMeta{white-space:normal}
    #dStats div{padding:6px 6px} #dStats b{font-size:12.5px} }
  @container (max-width:520px){ #dStats div{padding:6px 6px} #dStats b{font-size:12.5px} }
</style>
</head>
<body class="bg-slate-50 min-h-screen text-slate-800">

<header class="sticky top-0 z-40 bg-white/90 backdrop-blur-md border-b border-slate-200 shadow-sm">
  <div class="max-w-screen-2xl mx-auto px-4 h-14 flex items-center justify-between gap-4">
    <a href="./index.html" class="flex items-center gap-2.5 shrink-0 group">
      <span class="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-600 via-indigo-600 to-violet-600 text-white flex items-center justify-center font-extrabold text-[13px] shadow-md group-hover:scale-105 transition">SW</span>
      <span class="font-bold text-slate-900 tracking-tight hidden sm:block">STOCKS<span class="text-indigo-600">WORLD</span></span>
    </a>
    <nav></nav>
  </div>
</header>

<main id="pageMain" class="os-wide mx-auto px-4 py-4">
  <div class="os-vbar">
    <h1>Mutual Funds</h1>
    <div class="os-vtabs" id="views" role="tablist" aria-label="View"></div>
    <div class="os-asof">NAV snapshot · __GEN__ · <a href="./backtest.html">MF Backtest ↗</a></div>
  </div>
  <div id="status" class="os-empty">Loading mutual funds…</div>

  <div id="content" class="hidden">
    <div class="os-bar">
      <div class="os-sg" id="pers" role="group" aria-label="Return period shown and ranked by"></div>
      <div class="os-sg" id="plans" role="group" aria-label="Plan"></div>
      <span class="os-sg" role="group" aria-label="Minimum years of NAV data" title="Minimum years of NAV data since inception"><span class="lbl">Min years</span><span id="yrs" style="display:contents"></span></span>
      <span class="os-sg" id="cw" role="group" aria-label="Custom return window" title="Custom return window: the return of every fund between two dates of your choice">
        <span class="lbl">Custom</span>
        <input type="date" id="fromDate" min="__HIST_LO__" max="__HIST_HI__" aria-label="From date" />
        <input type="date" id="toDate" min="__HIST_LO__" max="__HIST_HI__" aria-label="To date" />
        <span id="cmodes" style="display:contents"></span>
        <button type="button" id="clearDates" title="Clear the custom window">Clear</button>
      </span>
      <span class="sp"></span>
      <button type="button" class="os-btn" id="resetBtn" title="Reset every filter">Reset</button>
    </div>
    <div id="rangeInfo" class="os-hint"></div>
    <div id="custWarn" class="hidden"></div>

    <div class="os-grid" id="grid">
      <section class="os-pane" id="pG" aria-label="Categories">
        <div class="os-ph"><span class="ttl">Categories</span><span class="r" id="gCount"></span></div>
        <div class="os-ch" id="gHead"></div>
        <div class="os-list" id="gList" role="listbox" aria-label="Categories"></div>
      </section>
      <section class="os-pane" id="pF" aria-label="Funds">
        <div class="os-ph"><span class="ttl" id="fTitle"></span><span class="r" id="fCount"></span></div>
        <div class="os-ps"><input id="searchBox" type="search" placeholder="Find a fund or AMC — e.g. Parag Parikh, HDFC" autocomplete="off" aria-label="Search by fund name or AMC" /></div>
        <div class="os-ch" id="fHead"></div>
        <div class="os-list" id="fList" role="listbox" aria-label="Funds"></div>
      </section>
      <section class="os-pane os-main" id="pD" aria-label="Fund detail">
        <div class="os-ph"><span id="dTtl"></span><span id="dTags"></span><span class="px" id="dPx"></span><span id="dChg"></span>
          <span class="r"><button type="button" class="os-btn os-focusbtn" id="dFocus" title="Hide the lists and give the fund the whole width (F)">Focus</button></span></div>
        <div id="dMeta"></div>
        <div class="os-ps"><div class="os-sg" id="dTabs" role="group" aria-label="What to show"></div>
          <div class="os-sg" id="cIv" role="group" aria-label="Bar size"></div><div class="os-sg" id="cRg" role="group" aria-label="Range"></div>
          <button type="button" class="os-btn" id="cLog" title="Logarithmic NAV scale">Log</button>
          <span class="os-sg hidden" id="dWinWrap" role="group" aria-label="Window in years"><span class="lbl">Window</span><span id="dWin" style="display:contents"></span></span>
          <span class="os-hint keys" style="margin-left:auto"><b>↑↓</b> move · <b>←→</b> list · <b>F</b> focus</span></div>
        <div class="os-chartwrap" id="cWrap"><div class="os-legend" id="cLegend"></div><div class="os-msg" id="cMsg"></div><div class="os-chart" id="cChart"></div></div>
        <div id="calBox" class="hidden"></div>
        <div class="os-stats" id="dStats"></div>
      </section>
      <section class="os-pane" id="pT" aria-label="All columns">
        <div class="os-ph"><span class="ttl" id="tTitle"></span><span class="r" id="tCount"></span></div>
        <div class="os-ps"><input id="searchBox2" type="search" placeholder="Find a fund or AMC" autocomplete="off" aria-label="Search by fund name or AMC" /></div>
        <div id="tBox"><table id="tTbl"><thead><tr id="tHead"></tr></thead><tbody id="resultsBody"></tbody></table></div>
      </section>
    </div>

    <details id="how"><summary>How these numbers are made</summary>
      <p>* Columns marked with an asterisk are annualized (CAGR). 1D–1Y are absolute returns.
      Data sources: AMFI NAVAll daily file (scheme master + current NAV) and mfapi.in (full NAV history).
      Growth plans only (no IDCW/dividend options), tagged <span class="tg">DIR</span> / <span class="tg">REG</span>.
      <b>Data floor:</b> Direct-plan NAV history starts Jan 2013 (when direct plans were introduced); Regular-plan history starts Apr 2006
      (AMFI's daily-NAV archive floor). So a fund older than 2006 shows max ~20 years even on its Regular plan — its true 1990s/2000s
      launch isn't available in free data. "Since inception" means since that data floor, not the fund's legal launch.
      Returns shown as "—" mean the fund hasn't existed for that lookback period (e.g. no 10Y return for a 6-year-old fund).
      A fund tagged <span class="tg">stale</span> has an old latest NAV; its short-period returns are suppressed.
      Side-pocket / Segregated schemes show -100% returns by design.
      Categories follow Groww's taxonomy (parent group → subcategory). Closed-ended, fixed-maturity (FMP), infrastructure-debt and other
      schemes Groww doesn't list are grouped under <b>Uncategorized</b>. The “Avg” beside a category is the plain average of the chosen
      period's return over its funds that have one, under the current Plan / Min years / search filters.
      The NAV chart, the custom window and the monthly / trailing / forward calendars read the daily NAV history (about 8 MB), which loads the first time one of them is needed.</p>
    </details>
  </div>
</main>

<script id="compressedData" type="application/octet-stream">__B64__</script>
<script>
'use strict';
const K = window.OSKit, $ = id => document.getElementById(id), esc = K.esc;
let ALL = [];
let BYCODE = {};
let BASE = [];              // after plan / years / search, before the category filter
let SHOWN = [];
let SORT = { key: 'cagrPct', dir: -1 };
let HIST = null;            // {dates:[YYYYMMDD], data:{code:[startIdx, delta-paise...]}}
let FROM = null, TO = null; // active custom return window
let PER = 'cagrPct', CAT = 'all', PLAN = 'Direct', MINYR = 0, QUERY = '', CUSTMODE = 'abs';
let SEL = null, VIEW = 'funds', TAB = 'chart', WIN = 5, PANE = 'fund', CH = null, DRAWN = '';
const PERS = [['r1d','1D'],['r1w','1W'],['r1m','1M'],['r3m','3M'],['r6m','6M'],['r1y','1Y'],['r3y','3Y*'],['r5y','5Y*'],['r10y','10Y*'],['cagrPct','Since incep.*'],['cust','Custom']];
const PERNAME = Object.fromEntries(PERS);
const GROUP_ORDER = ['Equity','Hybrid','Debt','Index / Other','Commodities','Solution Oriented','Uncategorized'];
const SUB_ORDER = {
  'Equity': ['Large Cap','Large & Mid Cap','Mid Cap','Small Cap','Multi Cap','Flexi Cap','ELSS (Tax Saver)','Sectoral / Thematic','Focused','Value','Contra','Dividend Yield'],
  'Hybrid': ['Aggressive Hybrid','Balanced Advantage','Multi Asset Allocation','Equity Savings','Arbitrage','Conservative Hybrid'],
  'Debt': ['Overnight','Liquid','Ultra Short Duration','Low Duration','Money Market','Short Duration','Medium Duration','Medium to Long Duration','Long Duration','Dynamic Bond','Corporate Bond','Credit Risk','Banking & PSU','Gilt','Gilt 10Y Constant','Floater'],
  'Index / Other': ['Index Funds','ETF','Fund of Funds'],
  'Commodities': ['Gold','Silver'],
  'Solution Oriented': ['Retirement',"Children's"],
  'Uncategorized': ['Uncategorized'],
};
let CATS = [];              // [{k,name,lv}] — every category that exists in the data, in Groww's order
const wide = () => window.innerWidth >= 900;

async function loadData() {
  $('status').textContent = 'Decoding…';
  const b64 = $('compressedData').textContent.replace(/\s+/g,'');
  const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
  const text = await new Response(stream).text();
  ALL = JSON.parse(text);
  ALL.forEach(r => { BYCODE[r.code] = r; });
  $('compressedData').remove();

  // The daily NAV history (chart, custom-date calculator, calendars) is a separate ~8 MB file
  // fetched lazily by ensureHistory() on first use — NOT loaded here — so the lists paint at once.

  // Two-level category list (Groww-style: parent group -> subcategory)
  const has = {};
  for (const r of ALL) { (has[r.g] = has[r.g] || {})[r.s] = 1; }
  CATS = [{ k: 'all', name: 'All funds', lv: 0 }];
  GROUP_ORDER.concat(Object.keys(has).filter(g => !GROUP_ORDER.includes(g))).forEach(g => {
    if (!has[g]) return;
    CATS.push({ k: 'G:::' + g, name: g, lv: 0 });
    const subs = (SUB_ORDER[g] || []).slice();
    Object.keys(has[g]).forEach(s => { if (!subs.includes(s)) subs.push(s); });
    if (subs.length === 1 && subs[0] === g) return;          // Uncategorized has one sub with the same name
    subs.forEach(s => { if (has[g][s]) CATS.push({ k: 'S:::' + g + ':::' + s, name: s, lv: 1 }); });
  });
  $('status').classList.add('hidden');
  $('content').classList.remove('hidden');
}

// ---- Exact-date return helpers ---------------------------------------------
function ymdToInt(s) { return parseInt(s.replace(/-/g, ''), 10); }      // "2020-03-23"->20200323
function intToYmd(n) { const s = '' + n; return s.slice(0,4)+'-'+s.slice(4,6)+'-'+s.slice(6,8); }
function ymdToDate(n) { const s = '' + n; return new Date(+s.slice(0,4), +s.slice(4,6) - 1, +s.slice(6,8)); }

// nearest trading day on/before target (binary search the shared date axis).
// A target before the axis starts clamps to the first day rather than failing: otherwise every
// NAV lookup returns null and the whole Custom column blanks out.
function dateToIdx(ymd) {
  const A = HIST.dates;
  if (ymd <= A[0]) return 0;
  if (ymd >= A[A.length - 1]) return A.length - 1;
  let lo = 0, hi = A.length - 1;
  while (lo < hi) { const mid = (lo + hi + 1) >> 1; if (A[mid] <= ymd) lo = mid; else hi = mid - 1; }
  return lo;
}
// decode a fund's delta-paise array into a cumulative Int32Array (cached); frees the raw array
function decoded(code) {
  let d = HIST.dec[code];
  if (d) return d;
  const a = HIST.data[code];
  if (!a) return null;
  const start = a[0], n = a.length - 1, out = new Int32Array(n);
  let acc = 0;
  for (let i = 0; i < n; i++) { acc += a[i + 1]; out[i] = acc; }
  d = { start: start, nav: out };
  HIST.dec[code] = d; HIST.data[code] = null;   // free raw to save memory
  return d;
}
function navAtIdx(code, gi) {           // NAV (paise) of fund `code` at axis index gi
  const d = decoded(code);
  if (!d) return null;
  const k = gi - d.start;
  if (k < 0 || k >= d.nav.length) return null;
  return d.nav[k];
}
// Most funds post NAV a day or two behind the newest fund on the shared axis, so an exact-day
// lookup at the To end blanks nearly every row. Fall back to the fund's latest NAV within
// `back` axis days; beyond that the fund is genuinely dead/dormant and null is right.
function navAtOrBefore(code, gi, back) {
  const d = decoded(code);
  if (!d) return null;
  const last = d.start + d.nav.length - 1;
  const g = Math.min(gi, last);
  if (g < d.start || gi - g > (back || 0)) return null;
  return d.nav[g - d.start];
}

// ---- per-fund detail: monthly / trailing / forward return calendars ----
function fundRange(code){ const d=decoded(code); if(!d) return null; return [HIST.dates[d.start], HIST.dates[d.start+d.nav.length-1]]; }
function meNavC(code,Y,M){ const idx=dateToIdx(Y*10000+M*100+31); const dd=HIST.dates[idx]; return (Math.floor(dd/10000)===Y && Math.floor(dd/100)%100===M) ? navAtIdx(code,idx) : null; }
const MN_=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function _cell(v,b){ return v==null ? '<td class="na'+(b?' yr':'')+'">·</td>'
  : '<td class="'+(b?'yr ':'')+(v>=0?'pos':'neg')+'">'+(v>=0?'+':'')+v.toFixed(1)+'</td>'; }
function calMonthly(code){
  const r=fundRange(code); if(!r) return ''; const fy=Math.floor(r[0]/10000), ly=Math.floor(r[1]/10000);
  let rows='';
  for(let Y=fy;Y<=ly;Y++){
    let cells='', yf=1, any=false;
    for(let m=1;m<=12;m++){
      const cur=meNavC(code,Y,m), prev=m===1?meNavC(code,Y-1,12):meNavC(code,Y,m-1);
      if(cur==null||prev==null||prev<=0){cells+=_cell(null);continue;}
      const ret=(cur/prev-1)*100; yf*=(1+ret/100); any=true; cells+=_cell(ret);
    }
    rows+='<tr><td class="y">'+Y+'</td>'+cells+_cell(any?(yf-1)*100:null,true)+'</tr>';
  }
  const head='<th>Year</th>'+MN_.map(m=>'<th>'+m+'</th>').join('')+'<th class="yr">Year</th>';
  return '<table class="cal"><thead><tr>'+head+'</tr></thead><tbody>'+rows+'</tbody></table>';
}
function calRolling(code,win,fwd){
  const r=fundRange(code); if(!r) return ''; const fy=Math.floor(r[0]/10000), ly=Math.floor(r[1]/10000);
  let rows='', any=false;
  for(let Y=fy;Y<=ly;Y++){
    let cells='', rowHas=false;
    for(let m=1;m<=12;m++){
      const a = fwd ? meNavC(code,Y,m) : meNavC(code,Y-win,m);
      const b = fwd ? meNavC(code,Y+win,m) : meNavC(code,Y,m);
      if(a==null||b==null||a<=0){cells+=_cell(null);continue;}
      cells+=_cell((Math.pow(b/a,1/win)-1)*100); rowHas=true; any=true;
    }
    if(rowHas) rows+='<tr><td class="y">'+Y+'</td>'+cells+'</tr>';
  }
  if(!any) return '<div class="os-empty">Not enough history for '+win+'-year windows.</div>';
  const head='<th>'+(fwd?'Invest in':'As of')+'</th>'+MN_.map(m=>'<th>'+m+'</th>').join('');
  return '<table class="cal"><thead><tr>'+head+'</tr></thead><tbody>'+rows+'</tbody></table>';
}
// Lazily fetch the ~8 MB daily NAV history — only when the chart, the custom-date calculator or a
// calendar first needs it. Cached after the first fetch.
let __histLoading = null;
async function ensureHistory() {
  if (HIST) return true;
  if (!__histLoading) __histLoading = (async () => {
    const buf = await (await fetch('./mf_history.bin')).arrayBuffer();
    const stream = new Blob([new Uint8Array(buf)]).stream().pipeThrough(new DecompressionStream('gzip'));
    HIST = JSON.parse(await new Response(stream).text());   // {dates:[YYYYMMDD], data:{code:[startIdx, delta-paise...]}}
    HIST.dec = {};
    const fd = $('fromDate'), td = $('toDate');
    if (fd && td && HIST.dates && HIST.dates.length) {
      const lo = intToYmd(HIST.dates[0]), hi = intToYmd(HIST.dates[HIST.dates.length - 1]);
      fd.min = td.min = lo; fd.max = td.max = hi;
    }
  })().catch(e => { __histLoading = null; HIST = null; });
  await __histLoading;
  return !!HIST;
}
function recomputeCust() {
  const f = $('fromDate').value;
  const t = $('toDate').value;
  const info = $('rangeInfo');
  if (!HIST || !f || !t || f >= t) {
    FROM = TO = null;
    for (const r of ALL) r.cust = null;
    CUSTSUB = 'set dates';
    info.textContent = (f && t && f >= t) ? 'From must be before To' : '';
    return;
  }
  FROM = f; TO = t;
  const mode = CUSTMODE;   // 'abs' | 'cagr'
  const fi = dateToIdx(ymdToInt(f)), ti = dateToIdx(ymdToInt(t));
  // actual elapsed years between the matched trading days
  const yf = (ymdToDate(HIST.dates[ti]) - ymdToDate(HIST.dates[fi])) / (365.25 * 86400000);
  const useCagr = (mode === 'cagr' && yf >= 1);   // don't annualize sub-1-year windows
  let n = 0;
  for (const r of ALL) {
    const nf = navAtIdx(r.code, fi), nt = navAtOrBefore(r.code, ti, 10);  // paise; ratio cancels /100
    if (nf != null && nt != null && nf > 0) {
      r.cust = useCagr ? (Math.pow(nt / nf, 1 / yf) - 1) * 100 : (nt - nf) / nf * 100;
      n++;
    } else r.cust = null;
  }
  // dateToIdx clamps to the axis, so say so when the window asked for more than we hold
  const af = intToYmd(HIST.dates[fi]), at = intToYmd(HIST.dates[ti]);
  const clipped = (ymdToInt(f) < HIST.dates[0]) || (ymdToInt(t) > HIST.dates[HIST.dates.length - 1]);
  CUSTSUB = af + ' → ' + at + (useCagr ? ' · CAGR' : ' · abs');
  info.textContent = 'Custom window: ' + n.toLocaleString() + ' funds · '
    + (useCagr ? 'annualized CAGR over ' + yf.toFixed(1) + 'y'
               : 'absolute % over the window' + (mode === 'cagr' ? ' (<1y, not annualized)' : ''))
    + ' · ' + af + ' → ' + at
    + (clipped ? ' · window clipped to available NAV data' : '');
}
let CUSTSUB = 'set dates';

function cleanName(s) {
  return (s || '')
    .replace(/\s*[-–]\s*(Regular|Direct)(\s*Plan)?(\s*[-–]?\s*Growth(\s*Option)?)?\s*$/i, '')
    .replace(/\s*[-–]\s*Growth(\s*Option|\s*Plan)?\s*$/i, '')
    .replace(/\s*[-–]\s*$/, '')
    .trim();
}
function inCat(r, cat) {
  if (cat === 'all') return true;
  if (cat.startsWith('G:::')) return r.g === cat.slice(4);
  const rest = cat.slice(4), i = rest.indexOf(':::');
  return r.g === rest.slice(0, i) && r.s === rest.slice(i + 3);
}
function applyFilters() {
  const q = QUERY.trim().toLowerCase();
  BASE = ALL.filter(r => {
    if (PLAN !== 'all' && (r.plan || 'Direct') !== PLAN) return false;
    if (r.years < MINYR) return false;
    if (q) {
      const hay = (r.short + ' ' + (r.amc || '') + ' ' + r.name).toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
  const s = BASE.filter(r => inCat(r, CAT));
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

function fmtRet(v, d) {
  if (v == null) return '<span style="color:var(--text-faint)">—</span>';
  return '<span class="' + (v >= 0 ? 'os-pos' : 'os-neg') + '">' + (v >= 0 ? '+' : '') + v.toFixed(d == null ? 1 : d) + '</span>';
}
const perDp = k => k === 'cagrPct' ? 2 : 1;
function tags(r, always) {
  const reg = (r.plan || 'Direct') === 'Regular';
  return ((always || PLAN === 'all') ? '<em class="tg" title="' + (reg ? 'Regular plan — older funds carry full pre-2013 history' : 'Direct plan') + '">' + (reg ? 'REG' : 'DIR') + '</em>' : '')
    + (r.stale ? '<em class="tg" title="Latest NAV is ' + (r.staleDays || '?') + ' days old — short-period returns suppressed">stale</em>' : '');
}
const arrow = k => SORT.key === k ? (SORT.dir === -1 ? ' ▼' : ' ▲') : '';

function renderCats() {
  const st = {};
  const add = (k, v) => { const o = st[k] || (st[k] = { n: 0, s: 0, c: 0 }); o.n++; if (v != null) { o.s += v; o.c++; } };
  for (const r of BASE) { const v = r[PER]; add('all', v); add('G:::' + r.g, v); add('S:::' + r.g + ':::' + r.s, v); }
  $('gHead').innerHTML = '<span>Category</span><span>Funds</span><span title="Plain average of the chosen period over the funds that have a figure">Avg ' + esc(PERNAME[PER].replace('Since incep.', 'SI')) + '</span>';
  let live = 0;
  $('gList').innerHTML = CATS.map(c => {
    const o = st[c.k] || { n: 0, c: 0, s: 0 }; if (o.n && c.lv === 1) live++;
    if (o.n && c.k.startsWith('G:::') && !CATS.some(x => x.lv === 1 && x.k.startsWith('S:::' + c.name + ':::'))) live++;
    return '<div class="os-row gr ' + (c.lv ? 'sub' : 'grp') + (o.n ? '' : ' zero') + (c.k === CAT ? ' on' : '') + '" role="option" data-k="' + esc(c.k) + '"><span class="n">' + esc(c.name) + '</span><span class="m">' + o.n.toLocaleString() + '</span><span class="v">' + (o.c ? fmtRet(o.s / o.c, perDp(PER)) : fmtRet(null)) + '</span></div>';
  }).join('');
  $('gCount').textContent = new Set(SHOWN.map(r => r.s)).size + ' in view';
  $('gCount').title = 'Sub-categories among the funds listed';
}
function catName() { const c = CATS.find(x => x.k === CAT); return c ? c.name : 'All funds'; }
function countText() { return SHOWN.length.toLocaleString() + ' fund' + (SHOWN.length === 1 ? '' : 's') + (SHOWN.length > 1500 ? ' · first 1,500 listed' : ''); }
function emptyText() { return 'No fund matches these filters.' + (QUERY ? ' Clear the search or pick another category.' : ''); }

function renderFunds() {
  $('fTitle').textContent = catName();
  $('fCount').textContent = countText();
  $('fHead').innerHTML = '<span data-k="short" class="' + (SORT.key === 'short' ? 'on' : '') + '">Fund' + arrow('short') + '</span><span data-k="years" class="' + (SORT.key === 'years' ? 'on' : '') + '" title="Years of NAV data">Yrs' + arrow('years') + '</span><span data-k="' + PER + '" class="' + (SORT.key === PER ? 'on' : '') + '">' + esc(PERNAME[PER].replace('Since incep.', 'SI')) + arrow(PER) + '</span>';
  // Limit rendered rows to first 1500 for speed
  const slice = SHOWN.slice(0, 1500);
  $('fList').innerHTML = slice.length ? slice.map(r =>
    '<div class="os-row fr' + (r.code === SEL ? ' on' : '') + '" role="option" data-c="' + esc(r.code) + '" title="' + esc(cleanName(r.short)) + (r.amc ? ' — ' + esc(r.amc) : '') + '"><span class="n">' + tags(r) + esc(cleanName(r.short)) + '</span><span class="m">' + r.years.toFixed(1) + '</span><span class="v">' + fmtRet(r[PER], perDp(PER)) + '</span></div>').join('')
    : '<div class="os-empty">' + emptyText() + '</div>';
}
const TCOLS = [['r1d','1D',1],['r1w','1W'],['r1m','1M'],['r3m','3M'],['r6m','6M'],['r1y','1Y'],['r3y','3Y*',1],['r5y','5Y*'],['r10y','10Y*'],['cagrPct','Since incep*',1],['cust','Custom',1]];
function renderTable() {
  $('tTitle').textContent = catName() + ' — all columns';
  $('tCount').textContent = countText();
  const th = (k, lb, c, t) => '<th data-sort="' + k + '" class="' + (c || '') + (SORT.key === k ? ' on' : '') + '"' + (t ? ' title="' + esc(t) + '"' : '') + '>' + lb + arrow(k) + '</th>';
  $('tHead').innerHTML = '<th class="l" style="cursor:default">#</th>' + th('short', 'Mutual fund', 'l') + th('s', 'Category', 'l') + th('years', 'Yrs', '', 'Years of NAV data since inception')
    + TCOLS.map(c => th(c[0], c[1], c[2] ? 'sep' : '', c[0] === 'cust' ? CUSTSUB : (c[1].endsWith('*') ? 'annualized (CAGR) %' : 'absolute % return'))).join('');
  const slice = SHOWN.slice(0, 1500);
  $('resultsBody').innerHTML = slice.length ? slice.map((r, i) =>
    '<tr><td class="l mu">' + (i + 1) + '</td>' +
    '<td class="l fn" data-detail="' + esc(r.code) + '" title="Open this fund’s NAV chart and return calendars"><b>' + tags(r, true) + esc(cleanName(r.short)) + '</b>' + (r.amc ? '<small>' + esc(r.amc) + '</small>' : '') + '</td>' +
    '<td class="l mu" title="' + esc(r.category || '') + '">' + esc(r.s) + '</td>' +
    '<td class="mu">' + r.years.toFixed(1) + '</td>' +
    TCOLS.map(c => '<td class="' + (c[2] ? 'sep' : '') + '">' + fmtRet(r[c[0]], perDp(c[0])) + '</td>').join('') + '</tr>').join('')
    : '<tr><td colspan="15" class="os-empty">' + emptyText() + '</td></tr>';
}

function render() {
  applyFilters();
  // keep a fund selected on laptop widths; on a phone nothing is selected until it is tapped (the NAV history is 8 MB)
  if (SEL != null && !SHOWN.some(r => r.code === SEL) && VIEW === 'funds') SEL = null;
  if (SEL == null && VIEW === 'funds' && wide() && SHOWN.length) SEL = SHOWN[0].code;
  $('grid').classList.toggle('tbl', VIEW === 'table');
  renderCats();
  if (VIEW === 'table') renderTable(); else { renderFunds(); renderDetail(); }
  $('pG').classList.toggle('kb', PANE === 'cat'); $('pF').classList.toggle('kb', PANE === 'fund');
  seg('pers', PER); seg('plans', PLAN); seg('yrs', String(MINYR)); seg('cmodes', CUSTMODE); seg('views', VIEW);
  $('cw').classList.toggle('on', !!(FROM && TO));
  custHint();
  K.fitGrid($('grid'));
}

// A window can be perfectly valid yet blank out every visible row — most often because the From
// date predates 2013 while the plan filter is on Direct (direct plans only exist from Jan 2013),
// or because every fund in the current filter launched later. Say which, instead of a bare "—".
function custHint() {
  const el = $('custWarn');
  if (!FROM || !TO || !HIST || SHOWN.some(r => r.cust != null)) { el.textContent = ''; el.classList.add('hidden'); return; }
  const plan = PLAN;
  const anyPlan = ALL.some(r => r.cust != null && (r.plan || 'Direct') !== plan);
  el.textContent = (plan === 'Direct' && anyPlan && FROM < '2013-01-01')
    ? '⚠ No Direct-plan fund existed on ' + FROM + ' — direct plans only start from Jan 2013. Switch Plan to Regular, or pick a later From date.'
    : '⚠ No fund in the current filter has NAV going back to ' + FROM + ' — pick a later From date or widen the filters.';
  el.classList.remove('hidden');
}

// ---- the selected fund: header, figures, NAV chart or a calendar ----
function navBars(code) {
  const d = decoded(code); if (!d) return [];
  const out = [];
  for (let i = 0; i < d.nav.length; i++) {
    const v = d.nav[i]; if (!(v > 0)) continue;
    const s = '' + HIST.dates[d.start + i];
    out.push({ ts: Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8)) / 1000, c: v / 100 });
  }
  return out;
}
const niceD = s => s ? K.niceDate(s) : '—';
function renderDetail() {
  const r = SEL == null ? null : BYCODE[SEL];
  const on = !!r;
  ['dMeta', 'dStats'].forEach(id => $(id).classList.toggle('hidden', !on));
  $('dTabs').parentNode.classList.toggle('hidden', !on);
  if (!on) {
    $('dTtl').textContent = 'Fund'; $('dTags').innerHTML = ''; $('dPx').textContent = ''; $('dChg').innerHTML = '';
    $('cWrap').classList.remove('hidden'); $('calBox').classList.add('hidden');
    if (CH) CH.clear(SHOWN.length ? 'Select a fund to see its NAV chart and return calendars.' : emptyText()); else $('cMsg').textContent = SHOWN.length ? 'Select a fund to see its NAV chart and return calendars.' : emptyText();
    DRAWN = ''; return;
  }
  $('dTtl').textContent = cleanName(r.short); $('dTtl').title = r.name || '';
  $('dTags').innerHTML = tags(r, true);
  $('dPx').textContent = r.latestNav == null ? '' : '₹' + Number(r.latestNav).toLocaleString('en-IN', { maximumFractionDigits: 4 });
  $('dChg').innerHTML = r.r1d == null ? '' : '<span class="os-chg ' + K.cls(r.r1d) + '">' + (r.r1d >= 0 ? '+' : '') + r.r1d.toFixed(2) + '%</span>';
  $('dMeta').innerHTML = esc(r.g) + ' › ' + esc(r.s) + (r.amc ? ' · ' + esc(r.amc) : '') + ' · NAV of ' + niceD(r.latestDate) + ' · data from ' + niceD(r.inceptionDate) + ' (' + r.years.toFixed(1) + 'y)';
  $('dMeta').title = r.category || '';
  const cell = (lb, v, d) => '<div><span>' + lb + '</span><b class="' + (v == null ? '' : v >= 0 ? 'os-pos' : 'os-neg') + '">' + (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(d || 1) + '%') + '</b></div>';
  $('dStats').innerHTML = cell('1D', r.r1d) + cell('1W', r.r1w) + cell('1M', r.r1m) + cell('3M', r.r3m) + cell('6M', r.r6m) + cell('1Y', r.r1y)
    + cell('3Y*', r.r3y) + cell('5Y*', r.r5y) + cell('10Y*', r.r10y) + cell('Since incep.*', r.cagrPct, 2)
    + '<div title="' + esc(CUSTSUB) + '"><span>Custom</span><b class="' + (r.cust == null ? '' : r.cust >= 0 ? 'os-pos' : 'os-neg') + '">' + (r.cust == null ? (FROM && TO ? '—' : 'set dates') : (r.cust >= 0 ? '+' : '') + r.cust.toFixed(1) + '%') + '</b></div>'
    + '<div title="Total (not annualized) return since inception"><span>Total</span><b class="' + (r.totalReturnPct == null ? '' : r.totalReturnPct >= 0 ? 'os-pos' : 'os-neg') + '">' + (r.totalReturnPct == null ? '—' : (r.totalReturnPct >= 0 ? '+' : '') + r.totalReturnPct.toFixed(1) + '%') + '</b></div>';
  const isChart = TAB === 'chart';
  $('cWrap').classList.toggle('hidden', !isChart); $('calBox').classList.toggle('hidden', isChart);
  ['cIv', 'cRg', 'cLog'].forEach(id => $(id).classList.toggle('hidden', !isChart));
  $('dWinWrap').classList.toggle('hidden', !(TAB === 'trailing' || TAB === 'forward'));
  seg('dTabs', TAB); seg('dWin', String(WIN));
  const key = r.code + '|' + TAB + '|' + WIN;
  if (key === DRAWN) return;
  DRAWN = key;
  if (isChart) drawChart(r.code); else drawCal(r.code);
}
async function drawChart(code) {
  const still = () => SEL === code && TAB === 'chart';
  if (!HIST) {
    CH.clear('Loading NAV history (about 8 MB)…');
    if (!await ensureHistory()) { if (still()) { CH.clear('Couldn’t load the NAV history. Check your connection, then pick the fund again.'); DRAWN = ''; } return; }
    if (!still()) return;
  }
  const bars = navBars(code);
  if (!bars.length) { CH.clear('No NAV history on file for this fund.'); return; }
  CH.show({ bars: bars, ohlc: false }, { name: cleanName(BYCODE[code].short) });
  seg('cIv', CH.iv); seg('cRg', CH.range);
}
async function drawCal(code) {
  const box = $('calBox'), still = () => SEL === code && TAB !== 'chart';
  if (!HIST) {
    box.innerHTML = '<div class="os-empty">Loading NAV history (about 8 MB)…</div>';
    if (!await ensureHistory()) { if (still()) { box.innerHTML = '<div class="os-empty">Couldn’t load the NAV history. Check your connection, then pick the fund again.</div>'; DRAWN = ''; } return; }
    if (!still()) return;
  }
  const win = WIN;
  box.innerHTML = TAB === 'monthly'
    ? '<h3>Monthly returns — each calendar month’s gain</h3>' + calMonthly(code) + '<p>Each month’s NAV return, in % (right “Year” column = calendar-year return). “·” = outside the fund’s data window.</p>'
    : TAB === 'trailing'
    ? '<h3>Trailing ' + win + '-year return — your ' + win + '-year CAGR as of each month</h3>' + calRolling(code, win, false) + '<p>' + win + '-year CAGR ending that month, in %. “·” = outside the fund’s data window.</p>'
    : '<h3>Forward ' + win + '-year return — invest at month-start &amp; hold ' + win + ' years</h3>' + calRolling(code, win, true) + '<p>' + win + '-year CAGR if you invested that month, in %. “·” = outside the fund’s data window.</p>';
  box.scrollTop = 0;
}

// ---- controls ----
function seg(id, val) { $(id).querySelectorAll('button[data-v]').forEach(b => b.classList.toggle('on', b.dataset.v === val)); }
function mkSeg(id, opts, fn) {
  const el = $(id);
  el.innerHTML = opts.map(o => '<button type="button" data-v="' + esc(o[0]) + '"' + (o[2] ? ' title="' + esc(o[2]) + '"' : '') + '>' + esc(o[1]) + '</button>').join('');
  el.addEventListener('click', e => { const b = e.target.closest('button[data-v]'); if (b && !b.disabled) fn(b.dataset.v); });
}
// Custom date-range window: recompute the per-fund window return, sort by it.
async function applyDateRange() {
  const f = $('fromDate').value, t = $('toDate').value;
  if (f && t && !HIST) {
    const info = $('rangeInfo'); info.textContent = 'Loading NAV history (about 8 MB)…';
    if (!await ensureHistory()) { info.textContent = 'Couldn’t load the NAV history. Retry.'; return; }
  }
  recomputeCust();
  if (FROM && TO) { SORT = { key: 'cust', dir: -1 }; PER = 'cust'; }
  else if (SORT.key === 'cust' || PER === 'cust') { SORT = { key: 'cagrPct', dir: -1 }; PER = 'cagrPct'; }
  render();
}
function setPer(k) {
  if (k === 'cust' && !(FROM && TO)) { $('rangeInfo').textContent = 'Custom: set the From and To dates first.'; $('fromDate').focus(); return; }
  PER = k; SORT = { key: k, dir: -1 }; render(); scrollSel(true);
}
function sortBy(k) {
  if (SORT.key === k) SORT.dir = -SORT.dir;
  else { SORT.key = k; SORT.dir = (k === 'short' || k === 's') ? 1 : -1; }
  render();
}
function selectFund(code, user) {
  SEL = code; PANE = 'fund';
  if (VIEW !== 'funds') { VIEW = 'funds'; render(); }
  else {
    $('fList').querySelectorAll('.os-row.on').forEach(x => x.classList.remove('on'));
    const row = $('fList').querySelector('.os-row[data-c="' + code + '"]'); if (row) row.classList.add('on');
    $('pG').classList.remove('kb'); $('pF').classList.add('kb');
    renderDetail();
  }
  scrollSel();
  if (user && !wide()) $('pD').scrollIntoView({ behavior: 'smooth', block: 'start' });
}
function scrollSel(top) {
  const row = $('fList').querySelector('.os-row.on'); if (row) row.scrollIntoView({ block: 'nearest' }); else if (top) $('fList').scrollTop = 0;
  const g = $('gList').querySelector('.os-row.on'); if (g) g.scrollIntoView({ block: 'nearest' });
}
function setCat(k) { CAT = k; SEL = null; render(); $('fList').scrollTop = 0; $('tBox').scrollTop = 0; scrollSel(); }
function move(d) {
  if (PANE === 'cat') {
    const i = CATS.findIndex(c => c.k === CAT), j = Math.max(0, Math.min(CATS.length - 1, (i < 0 ? 0 : i) + d));
    if (j !== i) setCat(CATS[j].k);
  } else {
    const list = SHOWN.slice(0, 1500); if (!list.length) return;
    const i = list.findIndex(r => r.code === SEL), j = i < 0 ? 0 : Math.max(0, Math.min(list.length - 1, i + d));
    if (j !== i) selectFund(list[j].code);
  }
}
function toggleFocus() { if (!wide() || VIEW !== 'funds') return; const on = $('grid').classList.toggle('focus'); $('dFocus').classList.toggle('on', on); }

document.addEventListener('DOMContentLoaded', () => {
  loadData().then(() => {
    CH = new K.Chart({ chart: $('cChart'), legend: $('cLegend'), msg: $('cMsg'), iv: 'D', range: '3Y' });
    const EC = ['#3b82f6', '#eab308', '#ef4444'], EN = [21, 50, 200];
    CH.legend = function (i) {             // a NAV has no open / high / low, so the legend says NAV, not O H L C
      const b = this.bars[i], el = this.o.legend, me = this; if (!b || !this.data) { el.innerHTML = ''; return; }
      const prev = i > 0 ? this.bars[i - 1].c : null, pc = prev ? (b.c / prev - 1) * 100 : null;
      el.innerHTML = '<div><b>' + esc(this.meta.name || '') + '</b> <span class="mut">· 1' + this.iv + ' · ' + K.niceDate(K.tsISO(b.ts)) + ' · NAV</span> ₹' + K.fmtN(b.c)
        + (pc == null ? '' : ' <span class="' + (K.cls(pc) || 'os-pos') + '">' + K.fmtPct(pc) + '</span>') + '</div><div>'
        + EN.map((n, j) => { const e = me.ema[j] && me.ema[j][i]; return '<span class="mut">EMA ' + n + '</span> <span style="color:' + EC[j] + '">' + (e == null ? '—' : K.fmtN(e)) + '</span>'; }).join(' &nbsp;') + '</div>';
    };
    mkSeg('views', [['funds', 'Funds'], ['table', 'All columns']], v => { VIEW = v; $('grid').classList.remove('focus'); $('dFocus').classList.remove('on'); DRAWN = ''; render(); });
    mkSeg('pers', PERS.map(p => [p[0], p[1], p[0] === 'cust' ? 'Return over your own From → To window' : p[1].endsWith('*') ? 'Annualized (CAGR)' : 'Absolute return']), setPer);
    mkSeg('plans', [['Direct', 'Direct'], ['Regular', 'Regular'], ['all', 'Both']], v => { PLAN = v; render(); });
    mkSeg('yrs', [['0', 'Any'], ['1', '1+'], ['3', '3+'], ['5', '5+'], ['10', '10+']], v => { MINYR = parseFloat(v) || 0; render(); });
    mkSeg('cmodes', [['abs', 'Abs', 'Show the custom window as an absolute return'], ['cagr', 'CAGR', 'Show the custom window annualized (windows under a year stay absolute)']], v => { CUSTMODE = v; applyDateRange(); });
    mkSeg('dTabs', [['chart', 'NAV chart'], ['monthly', 'Monthly'], ['trailing', 'Trailing'], ['forward', 'Forward']], v => { TAB = v; renderDetail(); K.fitGrid($('grid')); });
    mkSeg('dWin', [['3', '3y'], ['5', '5y'], ['7', '7y'], ['10', '10y']], v => { WIN = +v; renderDetail(); });
    mkSeg('cIv', [['D', 'D'], ['W', 'W'], ['M', 'M']], v => { CH.setIv(v); seg('cIv', CH.iv); seg('cRg', CH.range); });
    mkSeg('cRg', [['3M', '3M'], ['6M', '6M'], ['1Y', '1Y'], ['3Y', '3Y'], ['Max', 'Max']], v => { CH.setRange(v); seg('cRg', CH.range); });
    seg('cIv', 'D'); seg('cRg', '3Y');
    $('cLog').addEventListener('click', () => { CH.setLog(!CH.log); $('cLog').classList.toggle('on', CH.log); });
    $('dFocus').addEventListener('click', toggleFocus);

    const onSearch = e => { QUERY = e.target.value; $('searchBox').value = $('searchBox2').value = QUERY; render(); };
    $('searchBox').addEventListener('input', onSearch); $('searchBox2').addEventListener('input', onSearch);
    $('fromDate').addEventListener('change', applyDateRange);
    $('toDate').addEventListener('change', applyDateRange);
    $('clearDates').addEventListener('click', () => { $('fromDate').value = ''; $('toDate').value = ''; applyDateRange(); });
    $('gList').addEventListener('click', e => { const r = e.target.closest('.os-row'); if (r) { PANE = 'cat'; setCat(r.dataset.k); } });
    $('fList').addEventListener('click', e => { const r = e.target.closest('.os-row'); if (r) selectFund(r.dataset.c, true); });
    $('fHead').addEventListener('click', e => { const s = e.target.closest('span[data-k]'); if (s) sortBy(s.dataset.k); });
    $('tHead').addEventListener('click', e => { const t = e.target.closest('th[data-sort]'); if (t) sortBy(t.dataset.sort); });
    // All-columns table — click a fund to open its NAV chart and calendars in the Funds view
    $('resultsBody').addEventListener('click', e => { const t = e.target.closest('[data-detail]'); if (t) selectFund(t.dataset.detail, true); });
    $('resetBtn').addEventListener('click', () => {
      QUERY = ''; $('searchBox').value = $('searchBox2').value = '';
      CAT = 'all'; PLAN = 'Direct'; MINYR = 0; CUSTMODE = 'abs';
      $('fromDate').value = ''; $('toDate').value = '';
      recomputeCust();
      SORT = { key: 'cagrPct', dir: -1 }; PER = 'cagrPct'; SEL = null;
      render(); $('fList').scrollTop = 0;
    });
    document.addEventListener('keydown', e => {
      const t = e.target, typing = t && (t.tagName === 'INPUT' || t.tagName === 'SELECT' || t.tagName === 'TEXTAREA');
      if (e.key === 'Escape') { if (typing) t.blur(); else if ($('grid').classList.contains('focus')) toggleFocus(); return; }
      if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === 'f' || e.key === 'F') { toggleFocus(); return; }
      if (VIEW !== 'funds') return;
      const step = { ArrowDown: 1, ArrowUp: -1, PageDown: 10, PageUp: -10, Home: -1e6, End: 1e6 }[e.key];
      if (step) { e.preventDefault(); move(step); }
      else if (e.key === 'ArrowLeft') { e.preventDefault(); PANE = 'cat'; render(); }
      else if (e.key === 'ArrowRight') { e.preventDefault(); PANE = 'fund'; if (SEL == null && SHOWN.length) selectFund(SHOWN[0].code); else render(); }
    });
    let rz; window.addEventListener('resize', () => { clearTimeout(rz); rz = setTimeout(() => K.fitGrid($('grid')), 120); });
    render();
  }).catch(e => { $('status').textContent = 'Couldn’t read the fund list: ' + (e && e.message || e); });
});
</script>
</body>
</html>
"""
HTML = (HTML.replace("__B64__", b64).replace("__GEN__", gen)
            .replace("__HIST_LO__", hist_lo).replace("__HIST_HI__", hist_hi))
OUT.write_text(HTML, encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size / 1024:.1f} KB)")
