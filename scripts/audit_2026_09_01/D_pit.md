# D — Point-in-time integrity audit of LIVE backtest inputs (2026-09-01)

Auditor: subagent D. Repo READ-ONLY; all inputs fetched from origin/main (hash-verified) or the
live sf-data Pages site the engine actually loads. Full numbers + examples: `D_evidence.json`
(same directory). Engine ground truth established by reading `origin/main:docs/backtest-engine.js`
and its twin `docs/stock-backtest.html` — never assumed.

**Inputs audited (exact provenance)**
- `docs/sf_fundamentals.json` @ origin/main (sha 90d4afe…): 3,956 symbols, 222,142 basis-cells.
- `docs/shp_engine.json` @ origin/main: 93,692 rows (3,977 event rows). Row = `[qe, fii%, dii%, subYYYYMMDD]`
  (slot semantics read from `loadShp`/`shpAt` + `build_engine_feed()` in `scripts/fetch_shareholding.py`
  L938-1000; values are PERCENT 0-100, sub 99999999 = un-dated sentinel).
- `scripts/shp_history.json` (sha a11ad63…), `docs/shareholding.json` @ origin/main.
- Prices/delivery: the LIVE layout the engine fetches from `https://dhruvan246.github.io/sf-data/`
  (`sf_meta.json` rev c4cfef6947, end **2026-08-31**): `sf_recent_1.bin` + `sf_deep_1/2.bin`, merged
  exactly as `_loadDeep` does. NOTE: `docs/sf_stock_data.bin` in the repo ends 2026-06-13 and is NOT
  what the engine loads — see screen 12.
- `scripts/ann_date_fills.json` @ origin/main: 6,834 entries (1,434 fill / 4,858 override / 542 exact).

Verdict scale: CONFIRMED (defect measured) / SUSPECT (anomalous, not proven) / KNOWN-OPEN
(documented class, fresh count) / benign (measured clean or by-design conservative).

---

## Screen 1 — ann==0 census (§91 class)
Method: np non-null with ann==0, per basis, by quarter year.
**Count: 274 std + 264 con cells** (0 cells have ann=null with PAT present; null anns pair only with
null PAT). Concentrated 2017-2025 (std peaks: 2025:49, 2024:45; con: 2025:61, 2024:58).
**Severity: KNOWN-OPEN (§91), engine-guarded.** Both twins require `ann > 0` before visibility
(backtest-engine.js L575/595/639/695; stock-backtest.html L1019/1037/1067/1107) — so ann=0 today
means *invisible quarter* (coverage loss, conservative), not look-ahead.
Heal route: `ann_date_fills.json` fill entries (BSE archive resolve) + nightly `--reapply`.

## Screen 2 — impossible ann dates
Method: full-store sweep, both bases, with type census (ann cells: 171,597 int + 50,545 null; no
floats/strings/invalid shapes).
- **ann < qe: 0. ann == qe: 0. Invalid calendar days: 0. Future (>20260901): 0.** → benign/CLEAN.
  (The §119d "corrupted qe+1..+6 writer" class at its extreme is gone; near-qe residue in screen 12.)
- ann > qe+300d: **1,337 cells**, worst BHARATIDIL Mar-2018 filed 2025-11-15 (2,786d), AIFL,
  EDUCOMP (CIRP/suspension filers). Late = seen late by the engine = matches reality. **benign**.

## Screen 3 — (ann − qe) distribution by year
std medians 2008→2026: 32/30/34/40/39/39/42/38/41/42/44/43/43/42/41/40/42/42/44 d, p95 60-122 —
healthy, matches real filing law (45d limit post-2015, 60d for Q4). **2000-2007: median 45, p95 45**
— i.e. the year's dates are essentially ONE value. Flagged → screen 5. con mirrors std.

## Screen 4 — pre-first-bar ann (§99 floor)
Method: alias-group MIN first traded bar (FUND_ALIAS parsed from the live engine, 627 entries;
groups unioned) from the LIVE price layout; flag ann < firstBar − 7d.
**Count: 7,081 cells / 510 symbols** (median 946d before first bar, max 3,793d — CANHLIFE carries
2015-2016 quarters dated qe+45/60 against a first bar of 2025-10-17). 461/510 symbols have first
bar ≥ 2015 → these are IPO-era listings whose pre-listing fundamentals carry fabricated
conventional dates (1,438 of the 7,081 are EXACTLY qe+45). Separately, 971 FUND symbols have no
price series at all in the live layout (never tradable).
**Severity: SUSPECT (semantics), structurally untradable.** The engine cannot screen/hold/price a
symbol before its first bar (§99's own control), and postDrift prices at the result date resolve
null pre-bar — so no trade-level look-ahead is reachable. But §99's `ann = max(qe+45, first bar)`
floor was applied only at the aggregator fill path (30 cells in Aug); it was never retro-applied
store-wide, and the class keeps growing with every IPO backfill (2020-2025 dominate the by-year
histogram). At the first post-listing rebalance these quarters flood in at once — defensible via
the prospectus argument, but the *dates* remain fabrications.
Heal route: batch `exact` entries flooring to alias-group first bar via `ann_date_fills.json` +
`--reapply` (policy call — §99 measured zero covered-cell loss for the floor).

## Screen 5 — convention fingerprints (exact-offset spikes)
Method: per-year histogram of (ann−qe); spike = offset with n≥30 and >6× its ±3d neighbourhood.
**The qe+45 fabrication era is still in the store, wholesale:** exact-45 share of all std anns:
2000 **97.3%**, 2001 **97.9%**, 2002 76.6%, 2003 66.2%, 2004 64.2%, 2005 64.8%, 2006 60.1%,
2007 54.4%, decaying to 12-15% 2009-2012 (a 45d lag is also a *real* value post-2008, so exact-45
there is not all fabrication). Minor qe+60 spikes 2007-2017 (n=30-341). Totals: **17,178 exact-45
cells, 7,449 pre-2008**.
**Look-ahead risk subset, measured:** pre-2008 March (audited Q4) quarters stamped 45d = **1,654
cells**, while the era's own *real* (non-45) March-quarter lags run med 30 / p75 53 / p95 89d — so
for the slow-filing tail the convention fabricates visibility ~1-6 weeks early.
**Severity: KNOWN-OPEN (§52/§99 convention class), the largest residual look-ahead surface in the
fundamentals store.** No dated public source is known to reach pre-2008 (BSE ann-stream starts
Jan-2014); heal options are policy, not data: un-date (SHP §105 precedent — serve sentinel,
engine excludes) or re-floor Q4 cells to a measured era p75 — both via ledger + rebuild, do NOT
edit the derived JSON.

## Screen 6 — weekend ann dates
2004: **53.7% Saturday + 13.3% Sunday**; 2005: **34.9% Sunday**; 2009/2010/2015 elevated
(11-16%). Cause identified, not guessed: qe+45 lands on fixed calendar days (15-May / 14-Aug /
14-Nov / 14-Feb), which fall on weekends in exactly those years — this is screen 5's class wearing
a calendar tell, plus 2010's 14-Nov-Sun / 14-Feb-Sun. Modern years run 7-16% Saturday (Indian
boards genuinely meet Saturdays; 2021 Sunday 3.2% is COVID-era). 3,057 Sunday anns total.
**Severity: SUSPECT-only corroboration of screen 5; no independent action.** A weekend ann is
never same-day tradable (rebalances are trading days), so no direct look-ahead.

## Screen 7 — shp_engine point-in-time
Slot semantics from code (ground truth): `[qe, fii%, dii%, sub]`; engine visibility `sub <=
rebalanceDate` (engine L773, twin L991); sentinel 99999999 stamped qe+28 at load (§120; census-
proven strictly before every possible month-end screen).
- (a) visibility < qe (quarter rows): **0**. (g) event rows visible before as-on date: **0** (3,977 event rows). benign.
- (b) sub − qe > 120d: **130** rows (SOLARINDS Mar-2020 346d, VIDEOIND, MAHLIFE…) — real late
  filers, seen late = correct. benign.
- (c) fii/dii outside [0,100]: **0** (code expects percent; confirmed builder comment L72). benign.
- (d) spike >15pp that reverts: **15** (8 fii, 7 dii): HAL Sep-2021 dii 20.5→5.4→18.2,
  SHRIRAMFIN Mar-2005 fii 0.26→21.1→0.83, MRPL Mar-2003, RNAVAL Dec-2015 fii 46.4, MFSL 2011,
  VAIBHAVGBL Mar-2016 (both legs), GESHIP Jun-2016, IEL Dec-2024, SHRIRAMCIT Sep-2020.
  **SUSPECT** — single-quarter stake round-trips this size are usually parse/basis defects
  (§118's Any-Other block is one known mechanism). Heal route: re-read the filings; fix via
  `shp_history.json` ledger path (`shp_cell_fix.json`), never `shp_engine.json` directly.
- (e) duplicate qe rows: **0**. (f) sub null/0: **0** — the "0 <= anything" leak has no instances;
  sentinel handling verified in both twins (qePlus28 stamping, engine L757, twin L748). benign.
- Un-dated sentinels: **22,408**, ALL pre-Jun-2016 (0 post-2016) — by design (§105/§120),
  late-biased. KNOWN-OPEN: real-date recovery for pre-2014 continues.

## Screen 8 — shp_engine vs richer stores
- vs `shp_history.json`: **89,715 overlapping (sym,qe) cells, 0 disagreements > 0.5pp** (fii or dii).
- vs `docs/shareholding.json` (page store, 8 quarters): **17,390 overlaps, 0 disagreements**.
**benign — the three SHP stores are mutually consistent.**

## Screen 9 — delivery (dv) PIT + coverage
dv is same-day EOD-published data — no PIT defect possible by construction; screened for coverage
and range instead, on the LIVE layout (recent+deep merged as the engine does):
- Range violations (<0 or >100 after the hasHL/×10 normalisation the engine applies): **0**.
- Coverage: **96.8-100% of trading symbols each year 2002-2026**; 0% pre-2002 (delivery data does
  not exist there — known, delivPct is labelled "2002+" in the UI). Alive-2026 symbols with no dv
  ever: **0**. **benign/GREEN.**

## Screen 10 — ledger application check
Method: not a 30-sample — **all 6,834** `ann_date_fills.json` entries verified against live
`sf_fundamentals.json` with kind-specific invariants from `apply_ledger`
(scripts/backfill_ann_dates_bse.py L131-190): fill ⇒ cell no longer 0; override ⇒ stored ≤
ledger+4d (the §12 gate window); exact ⇒ stored ∈ [ann, ann+4] (gate may bump).
**Result: 1,434 fill + 4,858 override + 542 exact ALL verified applied; 0 misapplied, 0 stale.**
The rebuild-proofing (`--reapply` nightly) is working. **benign/GREEN.**

## Screen 11 — 15:30 gate interaction (code-read, with cites)
- Data carries DATE-only ann ints (type census: no time anywhere). The engine treats
  `ann <= rebalanceDate` as visible AT that day's 15:30 close (`profitAt` backtest-engine.js L595,
  `profitMetrics` L639, `lastResultDate` L695; twin stock-backtest.html L1037/1067/1107; buys price
  at `priceAt(tkr, off)` = the screen date's close). So a date-only ann equal to a rebalance date
  IS same-day-tradable — the half-day look-ahead is prevented only DATA-SIDE, per §12:
  `update_fundamentals.gated_ann` bumps post-15:30 NSE ingestions, and the nightly
  "15:30 gate re-run" step in `.github/workflows/refresh-fundamentals.yml` (L287-300:
  `build_gate_events.py --calendar` → `fetch_filing_times.py` → `gate_1530.py --apply`) re-bumps
  what backfill writers regrow, followed by the §104 `--reapply` + `--recent 220` reconcile (L182-185).
- Gate liveness verified on origin: `filing_times_cache.json.gz` last committed 2026-08-29,
  `gate_calendar.json` 2026-08-31, ann ledger 2026-08-30 — the nightly is running.
- Exposure measured: **7,280 ann cells (4.28%) sit exactly ON a month-end rebalance day**; the
  gate's documented conservative rule leaves a cell same-day-visible when BSE has no record of
  that day's broadcast time. That residue is a bounded, monitored **KNOWN-OPEN half-day class**
  (it cannot be closed without a second time source). SHP: 376 sub dates sit on rebalance days
  (§105 applied 24,688 15:30 shifts upstream); synthetic qe+28 stamps are census-proven never to
  coincide with a screen day (§120 addendum).

## Screen 12 — other anomalies chased
1. **Near-qe fast anns (1-6d after quarter end): 321 cells**, 2021 alone 85 (ATCOM Jun-2021 ann
   Jul-1, RUNGTAIR Jul-3, HIMATSEIDE Sep-2024 ann Oct-1…). Real results 1-6d after quarter close
   are near-impossible; this is the §119d corrupted-writer shape beyond the 163 already healed.
   **SUSPECT — highest-value small batch to adjudicate next** (BSE window per cell → `exact` entries).
2. **APOLLOTYRE 20140331 duplicate row** — the last survivor of the split std/con pair class
   (engine's pairwise-gap TTM guard defends against double-count). KNOWN-OPEN, count fresh: 1.
3. **Basis-split ann dates (annStd≠annCon, both >0): 1,961 cells** (2018-2020 heavy). Legitimate
   two-filing-events class; §119c bans auto-healing these (a min() override could CREATE a con
   look-ahead). benign-by-policy, listed for awareness.
4. **Stale monolith trap:** `docs/sf_stock_data.bin` at origin ends **2026-06-13**, while the
   engine's real source (sf-data Pages, meta rev c4cfef6947) ends **2026-08-31**. Not a live
   defect (the engine never fetches the monolith — SF_BASE, engine L115), but any audit/tool that
   reads the repo copy silently analyses 11-week-old prices. Candidate for a README note or removal.
5. **§118 Any-Other→DII** (`OLD_OTHER_TO_DII=True`, fetch_shareholding.py L113): old-format rows
   fold "Other institutions" into DII. KNOWN-OPEN by design; the screen-7d spikes are its likely
   visible edge.

---

## Summary table

| # | Screen | Count | Verdict |
|---|--------|-------|---------|
| 1 | ann==0 with PAT | 274 std + 264 con | KNOWN-OPEN §91 — engine-guarded, coverage loss only |
| 2 | ann<qe / ==qe / invalid / future | 0 / 0 / 0 / 0 | benign (CLEAN) |
| 2b | ann>qe+300d | 1,337 | benign (real late filers) |
| 3 | lag distribution | 2008+ healthy; 2000-07 degenerate | → screen 5 |
| 4 | ann before alias-group first bar | 7,081 cells / 510 syms | SUSPECT — untradable pre-bar, §99 floor never retro-applied |
| 5 | exact qe+45 fabrication | 17,178 cells; pre-2008 7,449; March-qtr look-ahead subset 1,654 | KNOWN-OPEN — largest residual look-ahead surface |
| 6 | weekend anns | 2004-05 extreme (conv artifact); 3,057 Sundays total | SUSPECT-only corroboration |
| 7 | shp_engine PIT | vis<qe 0; dup 0; range 0; sub0 0; spikes 15; lag>120d 130; sentinels 22,408 pre-2014 | benign except 15 spike-reverts (SUSPECT) |
| 8 | SHP store cross-checks | 107,105 overlaps, 0 disagreements | benign (GREEN) |
| 9 | delivery dv | 96.8-100%/yr 2002+; 0 range errors | benign (GREEN) |
| 10 | ledger application | 6,834/6,834 verified, 0 misapplied | benign (GREEN) |
| 11 | 15:30 gate | data-side gate live; 7,280 cells on rebalance days; no-BSE-record residue | KNOWN-OPEN (bounded half-day class) |
| 12 | fast 1-6d anns / dup row / stale monolith | 321 / 1 / — | SUSPECT / KNOWN-OPEN / audit-trap |

Heal routing (all): ann-date fixes ONLY through `scripts/ann_date_fills.json` (fill/override/exact)
+ nightly `--reapply`; SHP fixes through `scripts/shp_history.json`-side ledgers
(`shp_sub_dates.json`, `shp_cell_fix.json`, `shp_lag_fix.json`) then rebuild `shp_engine.json`;
never edit derived docs/ JSONs (nightly CI clobbers them).
