/* Strategy HOVER CARD — year-by-year returns + the four market-cycle windows, shown while the
 * pointer is over a strategy's name.
 *
 * ONE definition, shared by:
 *   docs/saved-strategies.html — the list, on every tab (Best return / New / Custom / Favourites)
 *   docs/all-picks.html        — the 🎯 board and cards
 *
 * Third file in the same family as bt-identity.js and live-perf.js, and for the same reason: the
 * card was private to Saved Strategies, and the second page that wanted it would otherwise have
 * carried a copy of the year-coverage rules, the snapshot lookup and the placement maths — three
 * things that drift silently, because a wrong number here still renders.
 *
 * Needs, in this order, BEFORE this file: bt-identity.js (identityKey/ruleKey), backtest-engine.js
 * (pct) and bt-names.js (nameWithBasis/strategyEnglish). It owns no data of its own — the host page
 * hands it live getters via scWire(), so a page that re-pulls its history or re-bakes its snapshot
 * never has to remember to push the new value in.
 */
'use strict';

/* ---------- what the host page provides ---------- */
let _scHist = () => [];      // the shared backtest history (runs carrying `yby` calendar-year rows)
let _scSnap = () => null;    // the baked wave snapshot: { date, w1:{start,end,results:{…}}, w2, w3, cycle }
function scWire(opts) {
  if (opts && typeof opts.hist === 'function') _scHist = opts.hist;
  if (opts && typeof opts.snap === 'function') _scSnap = opts.snap;
}
// Private — never borrow the page's `esc`: all-picks.html and saved-strategies.html define it
// differently (one null-safe, one not), and this file must render the same either way.
function _scEsc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

/* ---------- the four market-cycle windows ----------
 * Also the Best-return tab's preset buttons and the New tab's columns, so this is the single
 * definition of what "Wave 1" means anywhere on the site. `to: null` = the latest available data
 * date (SF.end / today after clamping). */
const PERIOD_PRESETS = {
  w1:    { from: '2020-03-31', to: '2021-09-30' },
  w2:    { from: '2023-03-31', to: '2024-09-30' },
  w3:    { from: '2026-03-31', to: null },
  cycle: { from: '2020-03-31', to: null },
};
const WAVE_ORDER = ['w1', 'w2', 'w3', 'cycle'];
const WAVE_NAME = { w1: 'Wave 1', w2: 'Wave 2', w3: 'Wave 3', cycle: 'Full cycle' };
const WAVE_ICON = { w1: '🌊', w2: '🌊', w3: '🌊', cycle: '🔄' };

// Snapshots baked BEFORE the stock-count split are keyed by the rule alone, so their numbers would
// all read "no data" until the next bake. Each stored row records the `topN` it was computed at, so
// a pre-split entry is attached ONLY to the row with that exact count — a 200-stock return must
// never be shown on the 5-stock row (that mix-up is what the split fixes).
function lookupResult(get, c) {
  const hit = get(identityKey(c));
  if (hit) return hit;
  const legacy = get(ruleKey(c)), n = c.topN != null ? c.topN : null;
  return (legacy && n != null && legacy.topN === n) ? legacy : null;
}
function snapFor(wave) { const s = _scSnap(); return s && s[wave]; }
function snapResult(wave, c) {
  const w = snapFor(wave), r = w && w.results;
  return r ? lookupResult(k => r[k], c) : null;
}
function monLabel(d) {
  const p = String(d || '').split('-');
  return p.length > 1 ? ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][+p[1]] + '’' + p[0].slice(2) : '';
}
// The window printed under a heading: the snapshot's REAL dates when baked (w3 and cycle end at the
// data date, which moves every day), the hard-coded preset otherwise.
function waveColWindow(key) {
  const w = snapFor(key), p = PERIOD_PRESETS[key] || {};
  const start = (w && w.start) || p.from;
  return monLabel(start) + '–' + (monLabel((w && w.end) || p.to) || 'date');
}
/* Why a strategy can have no wave numbers, in one place — the hover card, the 🆕 New tab's four
   columns and the Best-return column all say the same thing.
   It is NOT a data gap and never a zero: the bake runs after each daily data refresh over whatever
   strategies existed AT THAT MOMENT, so one saved since simply hasn't been through a bake yet.
   Deliberately does NOT claim "saved after the bake" — the snapshot's `date` is its DATA date
   (SF.end), not the time the bake ran, so comparing a save timestamp to it would assert something
   we can't actually check from here. */
function waveMissingWhy() {
  const s = _scSnap();
  return (s && s.date ? `Not in the latest bake (market data to ${s.date}).` : 'No wave snapshot has been baked yet.')
    + ' Every saved strategy is re-baked after each daily data refresh, so one saved since the last run shows up after the next.'
    + ((window.btSync && btSync.isOwner()) ? ' To fill it in now, hit ↻ Update snapshot on Saved Strategies.' : '');
}

/* ---------- year-by-year returns ----------
   Source = the `yby` array stock-backtest.html stamps on EVERY saved history run: [year, strategy%,
   nifty500%] calendar-year returns. Nothing is recomputed here, so the card opens instantly.
   Coverage matters: yByY() bases a run's FIRST year on the run's start value and ends its LAST year
   wherever the run stopped, so those two years are partial, not calendar-year returns. For each year
   we therefore take the run that covers the MOST of it (full coverage wins, newest run breaks ties)
   and mark anything short of a whole year with * so a part-year number is never read as a full one. */
const YR_FROM = 2020;
const YRG = new Map();          // row id -> {rep:{name,cfg,ts,id}, allIds:Set} — hover target → its strategy
function yearRows(g) {
  const k = identityKey(g.rep.cfg), sids = g.allIds;
  const n0 = g.rep.cfg.topN != null ? g.rep.cfg.topN : '';
  const best = new Map();       // year -> {ret, bench, cov, full, ts}
  let last = YR_FROM;
  for (const h of _scHist()) {
    if (!h.cfg || !h.yby || !h.yby.length) continue;
    if ((h.cfg.topN != null ? h.cfg.topN : '') !== n0) continue;            // same rule AND same stock count
    if (!((h.sid && sids && sids.has(h.sid)) || identityKey(h.cfg) === k)) continue;
    const t0 = Date.parse(h.cfg.start || ''), t1 = Date.parse(h.cfg.end || '');
    for (const y of h.yby) {
      const yr = +y[0]; if (!(yr >= YR_FROM) || y[1] == null) continue;
      const jan = Date.parse(yr + '-01-01'), dec = Date.parse(yr + '-12-31');
      const ok = isFinite(t0) && isFinite(t1);
      const cov  = ok ? Math.max(0, Math.min(t1, dec) - Math.max(t0, jan)) : 0;
      const full = ok && t0 <= jan && t1 >= dec;
      const prev = best.get(yr);
      if (!prev || cov > prev.cov || (cov === prev.cov && (h.ts || 0) > prev.ts))
        best.set(yr, { ret: y[1], bench: y[2], cov, full, ts: h.ts || 0 });
      if (yr > last) last = yr;
    }
  }
  if (!best.size) return [];
  const out = [];
  for (let yr = YR_FROM; yr <= last; yr++) {
    const b = best.get(yr);
    out.push({ year: yr, ret: b ? b.ret : null, bench: b ? b.bench : null, full: !!(b && b.full) });
  }
  return out;
}
function yearCardHTML(g) {
  const rows = yearRows(g);
  const head = `<div class="px-3 py-1.5 border-b border-slate-200 bg-slate-50">
      <div class="text-[11px] font-semibold text-slate-800 leading-snug line-clamp-2" title="Code-name: ${_scEsc(nameWithBasis(g.rep.name, g.rep.cfg))}">${_scEsc(strategyEnglish(g.rep.cfg))}</div>
      <div class="text-[10px] text-slate-500">Year-by-year &amp; market cycles${g.rep.cfg.topN != null ? ' · ' + g.rep.cfg.topN + ' stocks' : ''}</div>
    </div>`;
  // Empty is usually not a gap in the data but a stock-count mismatch: since counts became part of a
  // strategy's identity, a top-50 row must not borrow the top-5 run's years. Say so, don't just dash.
  if (!rows.length) return head + `<div class="px-3 py-3 text-[11px] text-slate-400">No year-wise data yet — no saved backtest of this strategy${g.rep.cfg.topN != null ? ` at ${g.rep.cfg.topN} stocks` : ''} covers ${YR_FROM} onward.</div>` + waveCardBlock(g.rep.cfg);
  const cell = v => v == null ? '<span class="text-slate-300">—</span>' : `<span class="font-semibold ${v >= 0 ? 'pos' : 'neg'}">${pct(v)}</span>`;
  // A run whose benchmark series is missing lands as null or a nonsense -100 (equity read as zero) —
  // show a dash rather than a wrong Nifty 500 number.
  const bench = v => (v == null || !isFinite(v) || v <= -99) ? '<span class="text-slate-300">—</span>' : `<span class="${v >= 0 ? 'text-slate-600' : 'text-slate-500'}">${pct(v)}</span>`;
  const part = rows.some(r => r.ret != null && !r.full);
  return head + `<table class="w-full text-[11px]">
    <thead><tr class="text-slate-400"><th class="text-left font-medium px-3 pt-1 pb-0.5">Year</th><th class="text-right font-medium px-2 pt-1 pb-0.5">Strategy</th><th class="text-right font-medium px-3 pt-1 pb-0.5">Nifty 500</th></tr></thead>
    <tbody>${rows.map(r => `<tr class="border-t border-slate-100">
      <td class="px-3 py-0.5 text-slate-600">${r.year}${r.ret != null && !r.full ? '<span class="text-amber-500">*</span>' : ''}</td>
      <td class="px-2 py-0.5 text-right">${cell(r.ret)}</td>
      <td class="px-3 py-0.5 text-right">${bench(r.bench)}</td></tr>`).join('')}</tbody></table>`
    + (part ? '<div class="px-3 py-1 text-[10px] text-slate-400 border-t border-slate-100 leading-tight">* part-year — the backtest window starts or ends mid-year</div>' : '')
    + waveCardBlock(g.rep.cfg);
}
/* The card's second block: the same four market-cycle returns the Best-return tab's preset buttons
   show, read straight from the baked snapshot — no compute, no engine, so the card still opens
   instantly. The two blocks answer different questions from different sources: calendar years come
   from YOUR saved backtests (`yby`, so they stop where your windows stop), the waves come from the
   bake of every strategy over the SAME four windows — which is what makes them comparable across
   strategies. A wave missing from the snapshot prints “—”, never a zero. */
function waveCardBlock(c) {
  const s = _scSnap();
  const any = WAVE_ORDER.some(k => { const r = snapResult(k, c); return r && r.ret != null; });
  const head = `<div class="px-3 pt-1.5 pb-0.5 border-t border-slate-200 bg-slate-50 flex items-baseline justify-between gap-2">
      <span class="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Market cycles</span>
      <span class="text-[9px] text-slate-400">${s && s.date ? 'snapshot ' + _scEsc(s.date) : ''}</span>
    </div>`;
  if (!any) return head + `<div class="px-3 py-2 text-[10.5px] text-slate-400 leading-snug">${_scEsc(waveMissingWhy())}</div>`;
  return head + `<table class="w-full text-[11px]"><tbody>${WAVE_ORDER.map(k => {
    const r = snapResult(k, c);
    const has = r && r.ret != null;
    const tip = has ? `${WAVE_NAME[k]} ${waveColWindow(k)} · return ${pct(r.ret)}`
        + (r.cagr != null ? ` · CAGR ${pct(r.cagr)}` : '')
        + (r.maxDD != null ? ` · MaxDD -${r.maxDD.toFixed(1)}%` : '')
        + (r.topN != null ? ` · top-${r.topN}` : '')
      : `${WAVE_NAME[k]} ${waveColWindow(k)} — not in the snapshot yet`;
    return `<tr class="border-t border-slate-100" title="${_scEsc(tip)}">
      <td class="px-3 py-0.5 text-slate-600 whitespace-nowrap">${WAVE_ICON[k]} ${_scEsc(WAVE_NAME[k])}</td>
      <td class="py-0.5 text-[10px] text-slate-400 whitespace-nowrap">${_scEsc(waveColWindow(k))}</td>
      <td class="px-3 py-0.5 text-right leading-tight">${has ? `<span class="font-semibold ${r.ret >= 0 ? 'pos' : 'neg'}">${pct(r.ret)}</span>`
        + (r.cagr != null ? `<span class="block text-[9px] text-slate-400">(CAGR ${pct(r.cagr)})</span>` : '')
        : '<span class="text-slate-300">—</span>'}</td></tr>`;
  }).join('')}</tbody></table>`;
}

/* ---------- the floating card itself ----------
 * Created on demand so a host page needs no markup of its own. The animation override is not
 * cosmetic: the card borrows .card for theming, and theme.css gives every .card the sw-rise
 * entrance animation — which restarts on each un-hide, so the card would fade+slide in over 0.5s
 * AND be measured 12px above where it lands. A tooltip must appear already in place. */
function scEnsureCard() {
  let box = document.getElementById('yrCard');
  if (!box) {
    box = document.createElement('div');
    box.id = 'yrCard';
    box.className = 'hidden fixed z-50 w-72 card shadow-xl overflow-hidden pointer-events-none';
    box.style.left = '0'; box.style.top = '0';
    document.body.appendChild(box);
  }
  if (!document.getElementById('scStyle')) {
    const st = document.createElement('style');
    st.id = 'scStyle';
    st.textContent = '#yrCard{animation:none!important;opacity:1}';
    document.head.appendChild(st);
  }
  return box;
}
function showYearCard(el, id) {
  const g = YRG.get(id); if (!g) return;
  const box = scEnsureCard();
  box.innerHTML = yearCardHTML(g);
  box.classList.remove('hidden');
  // ORDER MATTERS. Read the card's size FIRST: offsetWidth/offsetHeight flush the layout that setting
  // innerHTML + un-hiding just invalidated. Reading the anchor's rect before that flush returns stale
  // coordinates (~12px off here), which is enough to drop the card on top of the row it describes.
  // offset* are also the right measure for size — unlike getBoundingClientRect they don't report where
  // the box currently SITS, i.e. wherever the previous hover left it.
  const pad = 8, gap = 10, W = window.innerWidth, H = window.innerHeight;
  const w = box.offsetWidth, h = box.offsetHeight, r = el.getBoundingClientRect();
  // Place BESIDE the name, not under it: these lists are wide and the card is narrow, so there is
  // nearly always room to the right, and a side placement never covers the row you're pointing at —
  // nor does it need a tall viewport (below/above needs h+row px of clear space, which a short window
  // hasn't got). Vertically centred on the row, then clamped, so the card is always fully on screen.
  const left = (r.right + gap + w <= W - pad) ? r.right + gap
             : (r.left - gap - w >= pad)      ? r.left - gap - w
             : Math.max(pad, Math.min(r.left, W - w - pad));            // no side room: fall back under the name
  const wantTop = (left === Math.max(pad, Math.min(r.left, W - w - pad)) && left < r.right && left + w > r.left)
    ? r.bottom + 6                                                      // overlapping horizontally → sit below
    : r.top + r.height / 2 - h / 2;                                     // beside → centre on the row
  box.style.left = left + 'px';
  box.style.top = Math.max(pad, Math.min(wantTop, H - h - pad)) + 'px';
}
function hideYearCard() { const b = document.getElementById('yrCard'); if (b) b.classList.add('hidden'); }
/* Wire every [data-yrs] inside `root` (call after each render — the handlers die with the old DOM).
   Register each row with YRG first: scRegister(id, {name, cfg, ts}, idsSet). */
function scRegister(id, rep, allIds) { YRG.set(id, { rep, allIds: allIds || new Set([id]) }); }
function scAttach(root) {
  (root || document).querySelectorAll('[data-yrs]').forEach(el => {
    el.onmouseenter = () => showYearCard(el, el.dataset.yrs);
    el.onmouseleave = hideYearCard;
  });
}
// The card is position:fixed, so ANY scroll would leave it hanging over the wrong row. Capture
// phase, so an inner scroller (a table that scrolls in place) counts too. Registered once, here,
// rather than by each host page.
window.addEventListener('scroll', hideYearCard, true);
