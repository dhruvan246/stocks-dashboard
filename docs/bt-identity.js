/* Strategy IDENTITY — the single definition, shared by the pages.
 *
 * A baked result is stored under identityKey(cfg) and looked up by the page under the same key, so
 * the two MUST agree exactly. They used to be separate copies in saved-strategies.html and
 * strategy-backtest.html; the 2026-08-03 topN change had to be made in both, which is exactly the
 * silent-drift risk this file removes — a mismatched key doesn't error, it just shows "not baked
 * yet" forever. Hence one file, loaded via <script src="./bt-identity.js"></script> everywhere.
 *
 * DOM-FREE and dependency-free — do not add either.
 */
'use strict';
// Identity = the SELECTION RULE **at one stock count**. Excludes `method` (hold/reset is
// execution-only) and start/end (the window), but INCLUDES `topN`: the same rule at 5 stocks and at
// 200 stocks is two strategies, listed as two rows each with its own return.
// Why topN is part of identity (changed 2026-08-03): merged rows were ranked at whichever stock
// count was saved LAST — buildGroups picks the newest entry as `rep` and the wave bake runs it with
// no topN override. So saving a 200-stock run of an existing 5-stock rule silently re-baked that
// row's Wave-3 return at 200 stocks (+1.96% instead of +46%) and dropped it from #5 to #34 of 35.
function identityKey(c) { return ruleKey(c) + '|' + (c.topN != null ? c.topN : ''); }
// The selection rule WITHOUT the stock count — i.e. identityKey as it was before the split. Used ONLY
// to derive the live-picks `sid` ('st' + hash36(key), see runLogPicks): tracking series are keyed by
// sid in Supabase sw_picks_log, so deriving them from the split key would have orphaned every existing
// series. Picks stay logged once per RULE (a top-5 basket is a prefix of the same rule's top-200).
// Treats boundary-only operator differences as the same rule: `<`≡`<=`, `>`≡`>=` (same field, value
// and direction), so two strategies differing only by `<` vs `<=` don't split into separate rows,
// while opposite directions (`>0` vs `<0`) or different values stay distinct.
function ruleKey(c) {
  const normOp = op => ({ '<=': '<', '>=': '>' }[op] || op);
  const f = (c.filters || []).map(x => x.field + normOp(x.op) + x.val).sort().join(',');
  return [c.indexName || '', c.mcapFloor || 0, c.freq, c.sortBy, c.dir, c.lookback || 1, c.earnBasis || 'con', f].join('|');
}
// Variant identity within one strategy = window + rebalance method. (topN stays in the key too, so a
// run that reaches a row via `sid` rather than config can never collide with a same-window run here.)
function winKey(c) { return (c.start || '') + '|' + (c.method || '') + '|' + (c.topN != null ? c.topN : ''); }
// One entry per identity, the NEWEST save representing it — the same `rep` rule buildGroups() applies
// in saved-strategies.html, minus the per-window detail the bake has no use for. Keeping the choice of
// representative here is what makes the baked row and the displayed row the same strategy.
function bakeGroups(items) {
  const out = new Map();
  for (const it of (items || [])) {
    if (!it || !it.cfg) continue;
    const k = identityKey(it.cfg), prev = out.get(k);
    if (!prev || (it.ts || 0) > (prev.ts || 0)) out.set(k, it);
  }
  return out;
}
if (typeof module !== 'undefined' && module.exports) module.exports = { identityKey, ruleKey, winKey, bakeGroups };
