/* Strategy-name display helpers — ONE definition, loaded by every page that shows a strategy name
 * (saved-strategies, all-picks, strategy-backtest, strategy-mixer, live-tracking, backtest-history,
 * stock-backtest). Mirrors bt-identity.js: DOM-free and dependency-free.
 *
 * Two jobs:
 *   basisSuffix / nameWithBasis — tag a name with its earnings basis (· Consolidated / · Standalone),
 *     only when the rule actually reads earnings. (2026-08-23: two rules identical but for con/std
 *     looked like duplicate names — the DII-holding pair.)
 *   strategyEnglish — spell the whole code-name out in plain words for the display, with the terse
 *     code-name kept alongside in the hover title.
 *
 * The earnings check is a PRIVATE `usesEarnings` (not `needsFund`) so this file never collides with the
 * global `function needsFund` that backtest-engine.js and stock-backtest.html each define. Keep the
 * FUND set in sync with those. strategyEnglish mirrors stock-backtest.html's strategyLabel() structure.
 */
'use strict';
const NAME_FUND_FIELDS = new Set(['profitYoyPct', 'profitBase', 'profitAccel', 'profitTTM', 'profitStreak', 'postDrift', 'composite']);
function usesEarnings(c) { return !!c && (NAME_FUND_FIELDS.has(c.sortBy) || (c.filters || []).some(f => NAME_FUND_FIELDS.has(f.field))); }
function basisSuffix(c) { return usesEarnings(c) ? (' · ' + (c && c.earnBasis === 'std' ? 'Standalone' : 'Consolidated')) : ''; }
// Rebalance method — the user asked for this after 8 look-alike favourites (2026-08-30): hold-vs-reset
// changes how a basket is managed month to month, so a name that hides it is ambiguous where it counts.
function methodSuffix(c) { return c && c.method ? (' · ' + (c.method === 'reset' ? 'Reset each cycle' : 'Hold winners')) : ''; }
function nameWithBasis(name, c) {
  name = name || '';
  const b = basisSuffix(c);  if (b && !name.includes(b)) name += b;
  const m = methodSuffix(c); if (m && !name.includes(m)) name += m;
  return name;
}

// Plain-English field vocabulary. `p:1` = the value is a percentage (append '%'); labels never end in
// '%' so the appended one never doubles up. Keep the key set aligned with stock-backtest.html FIELDS.
const EN_FIELD = {
  ret1m: { l: '1-month return', p: 1 }, ret3m: { l: '3-month return', p: 1 }, ret6m: { l: '6-month return', p: 1 }, ret12m: { l: '12-month return', p: 1 },
  accel: { l: 'momentum acceleration', p: 1 }, riskMom: { l: 'risk-adjusted momentum' }, postDrift: { l: 'post-result drift', p: 1 }, composite: { l: 'quality-momentum score' },
  d52: { l: '% below 52-week high', p: 1 }, d52_low_pct: { l: '% above 52-week low', p: 1 }, rangePos: { l: '52-week range position' }, daysHigh: { l: 'days since 52-week high' },
  dma50: { l: '% from 50-DMA', p: 1 }, dma200: { l: '% from 200-DMA', p: 1 }, indRank: { l: 'industry momentum rank' },
  vol: { l: 'annualised volatility', p: 1 }, beta: { l: 'beta vs Nifty' }, mdd6: { l: '6-month max drawdown', p: 1 }, upPct: { l: 'up-day consistency', p: 1 },
  turnover: { l: 'avg daily turnover (₹ lacs)' }, turnSurge: { l: 'turnover surge (5d÷90d)' }, volSurge: { l: 'volume surge (5d÷90d)' }, delivPct: { l: 'delivery', p: 1 },
  rsi: { l: 'RSI(14)' }, macd: { l: 'MACD histogram' }, stoch: { l: 'Stochastic %K' }, bollB: { l: 'Bollinger %b' },
  profitYoyPct: { l: 'net-profit YoY growth', p: 1 }, profitBase: { l: 'year-ago-quarter net profit (₹Cr)' }, profitAccel: { l: 'profit-growth acceleration (pts)' },
  profitTTM: { l: 'TTM profit growth', p: 1 }, profitStreak: { l: 'profit-growth streak (quarters)' },
  fiiPct: { l: 'FII holding', p: 1 }, fiiChgPp: { l: 'FII holding QoQ change (pp)' }, diiPct: { l: 'DII holding', p: 1 }, diiChgPp: { l: 'DII holding QoQ change (pp)' },
  mcap: { l: 'market cap (₹Cr)' }, hist_mcap: { l: 'historical market cap (₹Cr)' }
};
const EN_OP = { '<=': '≤', '>=': '≥', '<': '<', '>': '>', '=': '=', '==': '=' };
function enField(f) { return (EN_FIELD[f] && EN_FIELD[f].l) || f; }
function enFilter(x) { const m = EN_FIELD[x.field] || {}; return enField(x.field) + ' ' + (EN_OP[x.op] || x.op) + ' ' + x.val + (m.p ? '%' : ''); }
/* Full English name, in the USER'S convention (2026-09-01, portfolio-page rename):
 *   "<sort phrase> · <filter phrases> (<basis> <method>[ · non-default universe/freq])"
 * e.g. "Low DII · near 52WH · 2x from 52WL · profit >25% YoY (std hold)".
 * Nifty 500 + monthly are the defaults and stay silent; anything else rides in the bracket.
 * topN lives in the card meta ("Top 3 · Nifty 500"), not the name. Returns '' for a null cfg. */
const EN_SHORT = { diiPct: 'DII', fiiPct: 'FII', d52: 'off 52WH', d52_low_pct: 'off 52WL', rsi: 'RSI',
  accel: 'momentum accel', profitAccel: 'profit accel', profitYoyPct: 'profit YoY', profitTTM: 'TTM profit',
  profitStreak: 'streak', ret1m: '1m return', ret3m: '3m return', ret6m: '6m return', ret12m: '12m return',
  mcap: 'mcap', hist_mcap: 'mcap (hist)', vol: 'volatility', delivPct: 'delivery' };
function enSortShort(c) {
  const d = c.dir === 'high';
  switch (c.sortBy) {
    case 'diiPct': return (d ? 'High' : 'Low') + ' DII';
    case 'fiiPct': return (d ? 'High' : 'Low') + ' FII';
    case 'd52': return d ? 'Farthest below 52WH' : 'Near 52WH';
    case 'd52_low_pct': return d ? 'Top rise from 52WL' : 'Least off 52WL';
    case 'ret1m': case 'ret3m': case 'ret6m': case 'ret12m': {
      const n = c.sortBy.slice(3, -1); return (d ? 'Top ' : 'Worst ') + n + '-month return'; }
    case 'profitYoyPct': return (d ? 'Top' : 'Lowest') + ' profit growth';
    case 'profitAccel': return (d ? 'Top' : 'Lowest') + ' profit accel';
    case 'accel': return (d ? 'Top' : 'Lowest') + ' momentum accel';
    case 'profitTTM': return (d ? 'Top' : 'Lowest') + ' TTM profit growth';
    case 'mcap': case 'hist_mcap': return d ? 'Largest mcap' : 'Smallest mcap';
    case 'vol': return d ? 'Most volatile' : 'Least volatile';
    case 'turnover': return d ? 'Most traded' : 'Least traded';
  }
  return (d ? 'Top ' : 'Low ') + (EN_SHORT[c.sortBy] || enField(c.sortBy));
}
function enFilterShort(f) {
  const op = EN_OP[f.op] || f.op, v = +f.val, up = (f.op === '>' || f.op === '>=');
  switch (f.field) {
    case 'd52': return !up ? (v <= 15 ? 'near 52WH' : op + v + '% off 52WH') : op + v + '% below 52WH';
    case 'd52_low_pct': return up ? (v === 100 ? '2x from 52WL' : v === 200 ? '3x from 52WL' : op + v + '% off 52WL') : op + v + '% off 52WL';
    case 'profitYoyPct': return (up && v === 0) ? 'profit growth' : 'profit ' + op + v + '% YoY';
    case 'profitStreak': return v + 'q streak';
    case 'profitTTM': return (up && v === 0) ? 'TTM profit +ve' : 'TTM profit ' + op + v + '%';
    case 'profitAccel': return (up && v === 0) ? 'profit accel' : 'profit accel ' + op + v;
    case 'accel': return (up && v === 0) ? 'momentum accel' : 'momentum accel ' + op + v + '%';
    case 'fiiPct': return 'FII ' + op + v + '%';
    case 'diiPct': return 'DII ' + op + v + '%';
    case 'ret1m': case 'ret3m': case 'ret6m': case 'ret12m': {
      const n = f.field.slice(3, -1);
      return (up && v === 0) ? '+ve ' + n + 'm return' : n + 'm return ' + op + v + '%'; }
  }
  const m = EN_FIELD[f.field] || {};
  return (EN_SHORT[f.field] || m.l || f.field) + ' ' + op + ' ' + f.val + (m.p ? '%' : '');
}
function strategyEnglish(c) {
  if (!c || !c.sortBy) return '';   // null/empty cfg → caller falls back to the stored code-name
  const fils = (c.filters || []).map(enFilterShort).join(' · ');
  const par = [];
  if (c.earnBasis) par.push(c.earnBasis === 'std' ? 'std' : 'con');   // user 2026-09-01: basis shown whenever the cfg carries one
  par.push(c.method === 'reset' ? 'reset' : 'hold');
  if (c.indexName && c.indexName !== 'Nifty 500') par.push(String(c.indexName).replace('__FNO__', 'F&O'));
  if (c.freq && c.freq !== 1) par.push({ 3: 'quarterly', 6: 'half-yearly', 12: 'yearly' }[c.freq] || (c.freq + 'mo'));
  return enSortShort(c) + (fils ? ' · ' + fils : '') + ' (' + par.join(' ') + ')';
}
if (typeof module !== 'undefined' && module.exports) module.exports = { usesEarnings, basisSuffix, methodSuffix, nameWithBasis, strategyEnglish };
