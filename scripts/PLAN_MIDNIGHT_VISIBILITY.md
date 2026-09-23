# PLAN — Midnight visibility rule (the 15:30 gate retired)  — started 2026-09-23 20:40 IST

**User decision (2026-09-23, verbatim intent):** "I am going to exit the stocks on rebalance day close
and buy the stocks on next day opening, so I am planning to remove that 3:30 gate — when I buy at 9:20
next morning it should take all the data even if the profits / FII holding / DII holding are declared
after 3:30 but before 12 a.m. on the rebalance day." Confirmed twice ("till the midnight of that date,
not the previous 3:30 gate"), then "ok do it".

## The rule, stated once

A filing is visible to a rebalance dated R iff its **calendar filing date ≤ R**. Time of day no longer
matters; a filing at 22:00 on R counts for R. A filing on R+1 morning does NOT count for R (cutoff is
midnight, the user's stated boundary — simpler and slightly conservative vs 09:15).

The engines already compare at date granularity (`annDate <= rebalanceDate`, `sub <= rebalanceDate`),
so **the engine's comparison does not change**. Everything that changes is on the data side: the stored
visibility dates must be the RAW filing day again, and the writers must stop shifting them.

**NOT in scope (user's call, still open):** simulating the buy at R+1 OPEN. The engine has no open
prices (closes only, verified). The 23-Sep slippage study measured the user's plan (sell R close, buy
R+1 open) at ≈ −3.2%/yr on ⭐ The Eight vs the close-to-close backtest, and both legs at R+1 open at
≈ −0.25%/yr. Recorded in the recap; no code built for it here.

## Where the 15:30 shift lived (measured 2026-09-23, HEAD 871fff1e4) → what changes

| # | Writer / store | Old behaviour | Change |
|---|---|---|---|
| 1 | `update_fundamentals.gated_ann()` (NSE ingestion) | broadcast > 15:30 → next weekday | returns the broadcast's calendar day |
| 2 | `reconcile_missing_quarters.gated_ann()` (BSE DT_TM, nightly) | same | same |
| 3 | nightly `gate_1530.py --apply` (refresh-fundamentals.yml) | month-end cells with all-after-close BSE broadcasts → next trading day | **replaced by `ungate_1530.py --apply`**: the exact mirror — a cell sitting on the trading day (or weekday) AFTER a month-end D, whose scrip's BSE Result broadcasts on D were all after 15:30, goes back to D. Same conservatism (no BSE record → untouched). Nightly, idempotent, audit log `_ungate_restores.json`. `build_gate_events.py --ungate` builds its events. |
| 4 | `backfill_ann_dates_bse.py --reapply` (nightly) | `override` entries carry RAW BSE dates but are applied earlier-only with a +4-day "gate buffer" (a stored date within [ann, ann+4] was assumed to be the gated form) | buffer → 0: any stored date later than the BSE first-public day is a lag under the midnight rule |
| 5 | `ann_date_fills.json` `exact` entries (4,733) | "asserted both ways" and written in GATED form | the 813 whose own note carries the corroborated BSE timestamp one gate-step before `ann` (after-close or non-trading day) are rewritten to that raw day; old value kept as `ann_1530`. The other 3,920 already equal the raw day (filed before close on a trading day). |
| 6 | `bse_result_fills.json` (3 entries) | `ann` gated, `filed` raw | `ann` → date(`filed`) where they differ |
| 7 | `fetch_shareholding.visible_iso()` (NSE SHP ingestion, quarterly + events + revisions) | > 15:30 or non-trading day → next trading day | returns the broadcast's calendar day (fallback: raw submissionDate) |
| 8 | `shp_lag_fix.json` (35,069 `gated_1530` entries with `ts`) and `shp_sub_dates.json` (3,878) | `sub` = gated form of `ts` | `sub` = calendar day of `ts`; old value kept as `sub_1530`; `days_later` recomputed; `rule: "midnight"`. The feed builder's serve-time re-assert matches a stored date against `was` OR `sub_1530` (history holds either the raw day or the gated one, depending on which campaign wrote it). |
| 9 | rows ingested with `visible_iso` (21–23 Sep 2026) and NSE-dated `shp_revisions.json` rows | gated at ingestion, no ledger | `fetch_shareholding.py --regate`: for the last 3 quarter-ends + events, a stored/revision date that equals the LEGACY gate of the master's broadcast is moved to the midnight day. Runs after every fetch in refresh-shareholding.yml (rebuild-proof). |
| 10 | `guard_shp_gate.py` | asserts the 15:30 gate is wired | asserts the MIDNIGHT rule is wired (same shape, new self-test dates); new `guard_visibility_rule.py` does the same for fundamentals and refuses a workflow that still runs `gate_1530.py` |
| 11 | engine twins (`backtest-engine.js`, `stock-backtest.html`) | comments describe the 15:30 close gate | comments updated; no logic change, no ENGINE_VER bump; SW cache v177 |
| 12 | qe+28d SHP convention (pre-2014) | chosen so a clockless date never lands ON a screen | unchanged (still the measured late floor) — the collision argument is moot under the midnight rule, noted in the comment |

## Consequences to state plainly

* postDrift / `lastResultDate` reference for an after-close filer becomes the filing day's close (the
  PRE-reaction close) instead of the next day's. For a before-close filer it was already the filing
  day's close. Documented; no engine change.
* `build_quarterly_results.py` `rx` (result-day reaction %) uses `close(ann)/close(prev)`: for an
  after-close filer the reaction now shows on the day AFTER `ann`, so `rx` reads the pre-reaction day.
  Page metric only; noted as a follow-up (needs the filing time, which results_feed.json carries for
  recent filings).
* Live "Today's Picks" run the next morning: the last ingestion runs are 23:15 IST (fundamentals) and
  21:40 IST (shareholding). A filing after those is in the backtest's window but not in the live picks
  until the next run. Follow-up: an ~08:00 IST refresh of both.
* Filings shifted by `gated_ann` on NON-month-end days (2026-07-08 → today) that the BSE cache does not
  cover stay +1 day. Zero effect on monthly rebalances (only a filing dated exactly on a rebalance day
  changes an outcome); affects postDrift's reference day by one for those cells.

## Verification gate (§39 + data rules)

1. `python3 -m py_compile` every touched script; run both guards locally (they self-test the rule).
2. Fundamentals: `ungate_1530.py` dry → apply → second run decides 0; JSL Sep-2020 proof reads
   20201030 again; HEXAGON Mar-2026 reads 20260630; both files (docs + scripts mirror) identical cells.
3. SHP: `fetch_shareholding.py --feed-only` → every served-date change is EARLIER, never later; count
   matches the ledger rewrite; ZEEMEDIA 2026-06-25 event serves 20260630; 20MICRONS Jun-2020 serves
   20200708; cell_fix WARN count unchanged (baseline measured before the rewrite).
4. Engine pages load with zero console errors after the SW bump.
5. Push to main (file-scoped), Pages deploy, LIVE re-verify of the three proof cells ~20 min later
   (CI race), then dispatch bake-snapshots / bake-waves / monthly-returns so saved-strategy numbers
   are re-baked on the new dates.

## Log (2026-09-23, worktree ~/stocks-wt/midnight-gate, branched from origin/main c211fec15)
- 20:40 plan written; code changes: update_fundamentals.gated_ann + reconcile_missing_quarters.gated_ann →
  broadcast day; fetch_shareholding: visible_iso → broadcast day, legacy_gate_iso kept as recogniser,
  `--regate` pass, `_reassert_sub` matches `sub_1530`; backfill_ann_dates_bse override buffer +4 → 0;
  build_gate_events `--ungate`; NEW ungate_1530.py (mirror), guard_visibility_rule.py,
  rewrite_ledgers_midnight.py; guard_shp_gate.py → midnight self-tests + by-function check (the old
  count-of-2 check had been failing every shareholding run since 22-Sep 19:42Z, after §145's SME pass added a
  third harmless call — 2 failed runs, no ingestion since 22-Sep 18:04 IST); both workflows; engine
  comments (both twins) + sw v179.
- 20:55 ledger rewrite APPLIED: shp_lag_fix 32,994 entries (11 of them to `was`), shp_sub_dates 3,878,
  ann_date_fills exact 380 (374 from the note's arrow, 6 from the month-end BSE cache; 4,252 exact entries
  have no arrow in their note and were left as-is — a gated one there is a one-day-late visibility on a
  non-month-end, no backtest effect), bse_result_fills 3. ann_cell_fix (SYNGENE/SPENCERS/INFOBEAN pre-listing
  floors) untouched: not gate shifts.
- 20:57 SHP feed rebuilt (--feed-only, no network): 36,831 served dates moved EARLIER, 0 later, 35 more
  re-filing rows qualify as strictly-later (§142k); WARN count 4 = baseline 4; ZEEMEDIA 2026-06-25 event
  20260701 → 20260630, 20MICRONS Jun-2020 20200709 → 20200708. The feed rebuild reproduces the committed
  feed byte-for-byte before the rewrite (0 of 98,664 rows differ) — the A/B baseline is clean.
- 20:58 fundamentals: `--reapply` 1,005 cells (docs) / 692 (mirror); `build_gate_events --calendar --ungate`
  → 5,260 events / 228 month-ends; 37 month-ends have no BSE times in the cache and this Mac gets HTTP 403
  from BSE → left for tonight's CI nightly (reaches BSE; the mirror is idempotent). ungate: restorable 1,705
  events, before-close 146, no BSE record 2,536, no scripcode 873 → 2,960 cells restored (docs) / 2,608
  (mirror); second pass 0; JSL Sep-2020 20201102 → 20201030; HEXAGON Mar-2026 20260701 → 20260630.
  Net before→after: 3,969 ann cells earlier, 0 later, 0 value cells changed. docs-vs-mirror populated-ann
  disagreements 201 → 18 (pre-existing class, reduced).
- A/B harness: scratchpad sim_ab.js (site engine under Node, prices/membership from the live site,
  fundamentals+SHP from a local dir) — first pass ran on different bin days (22 vs 23 Sep), discarded.
- 21:15 identical-bin re-run (both legs end 2026-09-23, 78 rebalances): 10 of 12 strategy entries byte-identical
  (0 baskets changed); profitYoyPct-std 99.54 → 99.05 (4 baskets), d52_low_pct-std 95.95 → 97.04 (2 baskets);
  The Eight mean 91.23 → 91.31; 6 of 624 baskets changed; maxDD identical. Table in runbook §149.
- 21:05 pages: stock-backtest.html + saved-strategies.html on a local preview of the worktree (port 8905) —
  zero console errors (only the Tailwind CDN warning).
- 21:11 pushed 937eda128 (sw v179 after a rebase conflict with another session's v177/v178); Pages deployed; LIVE
  JSL 20201030 / HEXAGON 20260630 / ZEEMEDIA 20260630 / sw v179. Dispatched refresh-shareholding (green; guard
  fixed; --regate moved 3,710 quarterly / 3 event / 7 revision history rows to the filing day) and
  refresh-fundamentals (green; BSE 403 for all 37 pending month-ends from CI too; mirror idempotent 0; LIVE held).
- 21:30-22:10 USER CHALLENGE ("many companies declare after 3:30 on rebalance day") → runbook §149a: 406 after-close
  N500 filer-days, 359 made visible by the heal, 47 non-quarter BSE Result items; candidate lists change on up to
  24/78 dates but top-3 on 5; MIRROR GAP: weekend-dated after-close Friday filings (362 cells, 354 Sat / 8 Sun)
  restored, pushed 9a0d408d8, LIVE verified (METROPOLIS/LEMONTREE Mar-2020 → 20200529). Final A/B on the
  completed data: unchanged — same 6 of 624 baskets, The Eight mean 91.23 → 91.31.
