/* LIVE strategy performance — the maths behind "what is each saved strategy doing RIGHT NOW".
 *
 * ONE definition, shared by
 *   docs/all-picks.html        — the 🎯 All Picks board (live view AND its 🕘 rewind)
 *   docs/saved-strategies.html — the 📈 Today's topper preset on the Best-return tab
 *
 * Split out for exactly the reason bt-identity.js was: these lived inside all-picks.html, and the
 * second page that needed them would otherwise have carried a second copy of the rebalance
 * schedule, the basket screen and the pricing rules — three places a fix could land in one and not
 * the other, with no error to show for it. The two pages would just quietly report different
 * numbers for the same strategy on the same day.
 *
 * Depends on backtest-engine.js being loaded FIRST (SF, SERIES, dayOff, priceAt, factorsAt,
 * passFilters, fieldVal, liveQuote). DOM-FREE and page-state-free — do not add either; whether a
 * page wants live quotes is passed in as `useLive`, never read off a page global.
 */
'use strict';

/* ---------- yielding between strategies ----------
 * Screening is synchronous and blocking, so a long run has to hand the event loop back between
 * strategies or the progress line never paints and the tab looks hung.
 * NOT setTimeout: a BACKGROUNDED tab throttles timers to roughly one callback a MINUTE, which turns
 * a 3-minute run into an hour-long one for anyone who clicks the button and switches tab — measured
 * on 2026-08-27, when Saved Strategies' first live leaderboard used setTimeout(0) and advanced ONE
 * strategy per ~90 s with document.visibilityState === 'hidden', while All Picks (which already used
 * this channel) finished 41 in three minutes on an equally hidden tab. A MessageChannel task is not
 * a timer task, so it isn't throttled; it yields just as well.                                    */
// A FRESH channel per yield, not one shared instance: a shared port has a single onmessage, so two
// loops yielding at once (a custom-window backtest and the live leaderboard, say) would overwrite
// each other's handler and one of them would wait on a wake-up that never comes — an unrecoverable
// hang. One channel per call costs nothing next to the screen it is yielding around.
function lpYield() {
  if (typeof MessageChannel === 'undefined') return new Promise(r => setTimeout(r, 0));
  return new Promise(r => { const c = new MessageChannel(); c.port1.onmessage = () => r(); c.port2.postMessage(0); });
}

/* ---------- strategy schedule ----------
 * A deployed strategy rebalances on MONTH-ENDS, every `freq` months, phased from its own start
 * month — the same cadence simulate() walks. The last such boundary before today is when the
 * basket now held was formed, so it is the only honest anchor for "how is it doing".
 *
 * The grid runs in BOTH directions from that phase month, so a rewind ALWAYS lands on a boundary.
 * `cfg.start` is only the From box of the backtest window that happened to be on the form when the
 * strategy was saved (stock-backtest.html readCfg) — bt-identity.js deliberately keeps it OUT of a
 * strategy's identity, and applyCfg rolls `end` forward on every load while leaving `start` frozen.
 * It is a research window, NOT a deployment date, so it must not gate anything. Until 2026-08-13
 * this walked FORWARD from start and returned null when no boundary preceded the date, which the
 * board rendered as "not started": a rewind to May-2023 blanked the return AND the holdings of the
 * three strategies whose saved window began 2026-03-31 (the Wave-3 preset). For a MONTHLY strategy
 * — every one saved so far — `start` moves the anchor not at all, so that null was its only effect.
 * The phase still matters at freq > 1 (start 2020-01-31 vs 2020-03-31 anchors Apr vs Mar), so it
 * is kept. Cross-check: the Strategy page never had this gate (strategy-backtest monthEndBefore()
 * just walks back from the date), so the two pages disagreed on the same strategy.               */
function lpMonthEndOf(y, m) { return new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10); }   // m = 1..12
function lpLastRebalance(cfg, today) {
  const freq = Math.max(1, +cfg.freq || 1);
  const start = (cfg.start && /^\d{4}-\d{2}/.test(cfg.start)) ? cfg.start : '2020-01-31';
  const t0 = (+start.slice(0, 4)) * 12 + (+start.slice(5, 7) - 1);      // the phase month, as an index
  const tNow = (+today.slice(0, 4)) * 12 + (+today.slice(5, 7) - 1);
  const floor = (typeof SF !== 'undefined' && SF) ? (SF.dailyFrom || SF.start || '') : '';
  let k = Math.floor((tNow - t0) / freq);   // grid point in/before today's month — NEGATIVE when rewound past `start`
  for (let i = 0; i < 3; i++, k--) {        // step back while today sits before its own month-end
    const t = t0 + k * freq;
    if (t < 0) break;                       // off the calendar
    const me = lpMonthEndOf(Math.floor(t / 12), (t % 12) + 1);
    // Before the dataset begins there is no basket to screen — say "not started" rather than
    // report an empty screen as "in cash", which would assert the rule found nothing.
    if (me < today) return me < floor ? null : me;
  }
  return null;
}
// A month-end can fall on a weekend/holiday — snap to the last day that actually traded, so the
// entry price and the 52w window share one anchor (mirrors simulate()'s snapTD).
// The list is cached, and lpResetTD() drops it: a rewind past ~2020 PREPENDS deep-history bars to
// these series, and a list cached from before that load stops at 2019 — every older date would
// fail to snap. Call lpResetTD() after ANY ensureHistoryFor() that may have loaded new bars.
let _LP_TD = null;
function lpResetTD() { _LP_TD = null; }
function lpTdList() {
  if (!_LP_TD) {
    const set = new Set();
    for (const r of ['RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','ITC','SBIN','LT']) {
      const s = SERIES[r]; if (s && s.d) for (const o of s.d) set.add(o);
    }
    _LP_TD = [...set].sort((a, b) => a - b);
  }
  return _LP_TD;
}
function lpTdIdxLE(off) {
  const T = lpTdList();
  let lo = 0, hi = T.length - 1, ans = -1;
  while (lo <= hi) { const m = (lo + hi) >> 1; if (T[m] <= off) { ans = m; lo = m + 1; } else hi = m - 1; }
  return ans;
}
function lpSnapTD(off) { const i = lpTdIdxLE(off); return i < 0 ? off : _LP_TD[i]; }
// The session before `off` — the "previous close" a rewound day's move is measured from.
function lpPrevTD(off) { const i = lpTdIdxLE(off); return i > 0 ? _LP_TD[i - 1] : off - 1; }

/* ---------- screening, with the expensive pass shared across strategies ----------
 * factorsAt() is the costly step (it walks the whole universe). Every strategy on the same
 * universe + date can share ONE pass, so we compute it with all factor families switched on and
 * then just filter/sort per strategy — the same steps screenAsOf() takes internally.
 * The cache is keyed by universe+basis+date only, so lpResetRows() must be called at the start of
 * every rebuild — market data reloaded under it (a deep-history pull, a fresh daily bin) would
 * otherwise be screened with rows computed off the old bars.                                    */
const _LP_ROWS = new Map();
function lpResetRows() { _LP_ROWS.clear(); }
function lpRowsAt(cfg, date) {
  const key = [cfg.indexName || '', cfg.mcapFloor || 0, cfg.earnBasis || 'con', date].join('|');
  let r = _LP_ROWS.get(key);
  if (!r) {
    const superCfg = { indexName: cfg.indexName, mcapFloor: cfg.mcapFloor || 0,
      earnBasis: cfg.earnBasis || 'con', sortBy: 'composite', dir: 'high', topN: 1, freq: 1,
      // these only trip needsTech/needsFund/needsShp — filters are applied per strategy below
      filters: [{ field:'stoch' }, { field:'profitYoyPct' }, { field:'fiiPct' }] };
    r = factorsAt(dayOff(date), superCfg);
    _LP_ROWS.set(key, r);
  }
  return r;
}
function lpBasketAt(cfg, date, topN) {
  const scored = lpRowsAt(cfg, date).filter(r => r.rsi != null);   // factors computable AT ALL on this date
  let rows = scored.filter(r => passFilters(r, cfg.filters));
  rows = rows.filter(r => fieldVal(r, cfg.sortBy) != null);
  rows = rows.slice().sort((a, b) => {
    const x = fieldVal(a, cfg.sortBy), y = fieldVal(b, cfg.sortBy);
    return cfg.dir === 'high' ? y - x : x - y;
  });
  const out = rows.slice(0, topN);
  out.noData = !scored.length;   // "couldn't screen" is not "screened, nothing passed" — callers word it differently
  return out;
}

/* ---------- pricing ----------
 * Two baskets answer two different questions:
 *   picks — what the strategy qualifies for RIGHT NOW (identical to the strategy page's Live
 *           Picks tab), shown with the live price and today's move. This is the list you'd act on.
 *   held  — the basket formed at the last rebalance. It is what the headline return is measured
 *           on, so that number reflects the position actually carried rather than a hypothetical
 *           one.
 * Membership of both is fixed between rebalances, so a ↻ refresh only re-quotes and re-prices.
 * `useLive` = consult the live quotes (false for a rewind — there were none that day).           */
// The day's move. LIVE: only from a real quote — null without one, rather than a stale number off
// the baked closes. REWIND (prevOff set): that session's close over the one before it.
function lpDayMove(x, q, prevOff) {
  if (q && q.ltp != null && q.prevClose > 0) return (q.ltp / q.prevClose - 1) * 100;
  if (prevOff == null || x.live == null) return null;
  const pv = priceAt(x.tkr, prevOff);
  return pv > 0 ? (x.live / pv - 1) * 100 : null;
}
function lpPriceHold(h, off, prevOff, useLive) {  // held: the day's move on what it actually owns
  const q = (useLive && typeof liveQuote === 'function') ? liveQuote(h.sym) : null;   // no live quotes in rewind
  h.live = (q && q.ltp != null) ? q.ltp : priceAt(h.tkr, off);
  h.ret = (h.live != null && h.entry > 0) ? (h.live / h.entry - 1) * 100 : null;
  h.day = lpDayMove(h, q, prevOff);               // same basis as lpPricePick, so a row's headline and its chips can't disagree
  return h;
}
function lpPricePick(p, off, prevOff, useLive) {  // displayed: price on the day + that day's move
  const q = (useLive && typeof liveQuote === 'function') ? liveQuote(p.sym) : null;
  p.live = (q && q.ltp != null) ? q.ltp : priceAt(p.tkr, off);
  p.day = lpDayMove(p, q, prevOff);
  return p;
}
// Equal-weight aggregate over the held basket, on whichever leg is asked for: `ret` runs
// from each holding's entry price (since the last rebalance), `day` from its previous
// close. null when no holding carries that leg yet — which is NOT the same as holding
// nothing, so callers test held.length for "in cash" and this for "no price yet".
function lpAvg(held, key) {
  const v = held.map(h => h[key]).filter(x => x != null);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}
