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
function nameWithBasis(name, c) { const s = basisSuffix(c); name = name || ''; return (s && !name.endsWith(s)) ? name + s : name; }

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
// Full English name. Returns '' for a null/absent cfg — callers fall back to the stored code-name.
function strategyEnglish(c) {
  if (!c || !c.sortBy) return '';   // null/empty cfg → caller falls back to the stored code-name
  const FQ = { 1: 'Monthly', 3: 'Quarterly', 6: 'Half-yearly', 12: 'Yearly' }[c.freq] || (c.freq + 'mo');
  const uni = c.indexName ? String(c.indexName).replace('__FNO__', 'F&O') : (c.mcapFloor ? '≥₹' + (+c.mcapFloor).toLocaleString('en-IN') + 'L turnover' : 'All stocks');
  const pick = (c.dir === 'high' ? 'Top' : 'Lowest') + ' ' + (c.topN != null ? c.topN : '') + ' by ' + enField(c.sortBy);
  const fils = (c.filters || []).map(enFilter).join(' · ');
  const basis = usesEarnings(c) ? (' · ' + (c.earnBasis === 'std' ? 'standalone' : 'consolidated')) : '';
  return pick + ' · ' + uni + ' · ' + FQ + (fils ? ' · ' + fils : '') + basis;
}
if (typeof module !== 'undefined' && module.exports) module.exports = { usesEarnings, basisSuffix, nameWithBasis, strategyEnglish };
