# F — DII/FII values · results-date provenance · engine temporal semantics
Audit session 2026-09-01. Every number below was measured this session against the LIVE exports in
`$SP/live/` (origin/main data), origin/main code via `git show`, the true live price asset
`$SP/live_true/sf_stock_data_LIVE.bin` (end=2026-08-31), or a live BSE `AnnSubCategoryGetData` fetch.
Data sources per screen are stated inline. No repo files were modified.

---

## PART 1 — DII/FII VALUE CORRECTNESS

### 1a. Pipeline map (origin/main code, cites)
- **Fetcher** `scripts/fetch_shareholding.py`: NSE master per QE season → per-filing XBRL →
  `parse_shp()` (L553-731). New format (≥Sep-2022): FII=`InstitutionsForeignMember`,
  DII=`InstitutionsDomesticMember`, fractions, scale-anchor ladder L624-632, partition gate L676-678,
  §22j share-count precision pass L680-716 (4dp, filer's own base inferred from largest ≥1% category).
  Old format (≤Jun-2022): FII=FPI+FVCI, DII=MF+AIF+VCF+banks+ins+PF, `OLD_OTHER_TO_DII=True` (L113)
  routes `OtherInstitutionsMember` → dii (L656); reconcile gate `|fii+dii − o_inst| ≤ 0.35` (L658);
  never zero-defaults an unknown vintage (L637).
- **Durable store** `scripts/shp_history.json` `{SYM:{QE:[prom,fii,dii,mf,ins,"sub-date",nsh?]}}`;
  heal chain at every load: `apply_bse_hist_ledger → apply_refine_ledger → apply_mf_heal_ledger →
  apply_cell_fix` (order L757-761; cell_fix outranks, `_cell_eq` tolerance one 2dp step, L400-443).
- **Page feed** `docs/shareholding.json` = `build_feed()` (last 8 QEs, aligned arrays).
- **Engine feed** `docs/shp_engine.json` = `build_engine_feed()` (L938-1000):
  `{SYM:[[qeInt,fii,dii,subInt],…]}`; pre-Jun-2016 rows with the qe+21d convention and no
  `shp_sub_dates.json` entry are served `sub=99999999` (UNDATED_SUB, L926/L971-975); event rows
  (`shp_events.json`, §22k) merged by as-on date, quarter-end wins collisions (L988-991).
- **CI**: `.github/workflows/refresh-shareholding.yml`, 12:40 + 20:40 IST daily.

### 1b. §118 SW-2 (Any-Other DII inflation) — applied, with a 13-cell hole
**CONFIRMED (new defect): 13 foreign-confirmed heals never landed — blocked by a blanket skip.**
`scripts/_shp_other_inst_sweep.py` `cmd_apply()` L422-424 skips any (sym,qe) that ALREADY has a
`shp_cell_fix.json` entry — regardless of the entry's purpose. All 13 blocked cells had a prior
**sub-date-only** (or revision) entry, so their dii→fii heal was silently never written and the live
engine feed still serves inflated DII / understated FII:

| sym | qe | any-other block (pp) | live dii | should be |
|---|---|---|---|---|
| JISLJALEQS | 2016-06-30 | 10.79 | 14.25 | 3.43 |
| JISLJALEQS | 2016-09-30 → 2017-12-31 (6 qtrs) | 2.25–2.86 | 4.87–8.47 | 2.00–6.22 |
| RELCAPITAL | 2016-06-30 | 9.70 | 21.90 | 12.14 |
| RELIGARE | 2019-09-30 | 5.66 | 13.03 | 7.37 |
| SATIN | 2019-09/12-30, 2020-03-31 | 2.97 | 26.2–28.3 | 23.3–25.3 |
| 21STCENMGM | 2020-03-31 | 0.35 | 0.50 | 0.15 |

Named holders in the audit's own evidence (IFC, ADB, Morgan Stanley Mauritius, "Foreign Bank" rows)
are curated-foreign. **Heal route:** merge the SW-2 correction INTO the existing `shp_cell_fix`
entries (update `cell` fii/dii values, keep the corrected sub-date; `was` = current stored), or teach
`cmd_apply` to merge instead of skip; then `fetch_shareholding.py --apply-ledgers` and re-verify the
13 cells in the served `docs/shp_engine.json`.

**Verified applied (benign): 901/914 foreign-confirmed heals live** (recomputed from the audit
ledger's own components: live dii == domestic-only `dom`, live fii == stored_fii + `oth`, tolerance
0.011); 143/143 domestic-kept cells unchanged; spot cells JSWSTEEL Jun-16 (fii 35.12/dii 1.98),
PETRONET Jun-16 (dii 6.48), JUBLPHARMA Dec-15 (dii 0.11) live-verified in the engine feed.
**KNOWN-OPEN:** 1,453 HOLD cells (name-unknown 1,067 + names-insufficient 366 + mixed 20) and
110 `mismatch-other-source` cells remain unadjudicated in `_shp_other_inst_audit.json`.

### 1c. Cross-store agreement — CLEAN
(sym,qe) overlap, |Δfii| or |Δdii| > 0.5pp: **page-vs-history 0 of 17,390 cells; engine-vs-history
0 of 89,715 cells.** 3,977 engine-only rows are §22k event rows (by design). NB all three stores
derive from one history+ledger chain, so this proves consistency, not external correctness (§22h
covers the latter).

### 1d. Component sanity — CLEAN; §22i residue re-measured
On all 89,715 shp_history cells: **0** negative components, **0** values >100, **0** fii+dii >
(100−prom)+2, **0** mf>dii, **0** ins>dii. §22i swallowed-block residue (edge screen, value==0
beside a >1pp neighbour): **fii=0: 206 cells/178 syms; dii=0: 163 cells/133 syms** — KNOWN-OPEN
(runbook §22i quotes ~200 dii edge cells; the dii side has never been audited).

### 1e. Plausibility series — CLEAN
Matched-panel median QoQ delta (symbols present in both quarters) per quarter 2010→2026:
**max |median ΔFII| = 0.17pp (2014-06), max |median ΔDII| = 0.105pp (2019-09); every other quarter
≤0.1pp** — no systematic parse break anywhere, including across the Sep-2022 format boundary
(median Δ +0.001/+0.000; the INFY-class DR reclassification is per-stock, invisible at the median).
The raw cross-sectional median does jump at Sep-2019 (universe n 643→1,746 — the deep-history
coverage start), a composition artifact, not a parse break.

### SW-1 un-dating (§105/§118) — VERIFIED SERVED
Live `shp_engine.json`: 27,337 pre-Jun-2016 rows → **22,408 served sub=99999999**, 4,929 dated;
all **341** rows still dated exactly qe+21d have a `shp_sub_dates.json` entry (real measured dates
that happen to equal the deadline). 0 unevidenced convention dates served.

---

## PART 2 — RESULTS-DATE PROVENANCE (sf_fundamentals annStd/annCon)

### 2a. Writers + provenance taxonomy (measured on 170,617 dated np-cells, live store)
Writers found (origin code): `update_fundamentals.py` NSE ingestion (`gated_ann` 15:30 gate at L32,
fill-only upsert L246-259, sibling-basis ann copy L295-300); `fill_ann_dates.py` (null→qe+45/60
SEBI deadline, demotes ann≤qe); `agg_tools/apply_agg_pat_fills.py` (`ann=max(qe+45d, first bar)` §99);
`backfill_ann_dates_bse.py` + `scripts/ann_date_fills.json` (fill/override/exact kinds, apply_ledger
L131-190); §102e `_staleness_fix/apply_redating.py` + tracked `redate_ledger.json`; nightly
`gate_1530.py` (month-end post-15:30 bumps); backfill campaign writers stamp their own dates.

| class | cells | basis of count |
|---|---|---|
| (iii) convention qe+45d | 17,178 | exact-signature census (stable: qe+45 never lands on a month-end, so gate bumps don't erode it) |
| (iii) convention qe+60d (Mar QE) | 4,266 | same |
| (i)/(iv) ann_date_fills ledger-asserted | 10,580 cells / 6,834 keys (fill 1,434, override 4,858, exact 542) | joined ledger→store: **0 violations** (every override ≤ ledger+4d, every exact == ledger) |
| (i) §102e BSE-redated | 22,671 basis-rows | `scripts/_staleness_fix/redate_ledger.json` (tracked), each with news_dt+newssub |
| (v) unknown (ann=0 sentinel) | 538 | direct |
| (ii) remainder — NSE broadcast_Date (gated) or backfill-writer stamps | ~117k | residual; carries §104's documented 1–70d NSE-lag exposure; exact split NSE-vs-backfill is not derivable from the repo (writers don't tag provenance in the store) |

**SUSPECT (rebuild-proofness):** the 22,671 §102e redates are applied to files but are NOT in the
nightly `--reapply` chain (refresh-fundamentals.yml reapplies only `ann_date_fills.json` +
fund/revop cell-fix). A full rebuild would resurrect the NSE dates for that set. Low probability
(rebuilds are rare; nightly upsert is fill-only so no nightly erosion — verified L251-254), but the
§104 "rebuild-proof" property does not extend to this ledger.

### 2b. The Result-category non-results trap — the nightly reconciler is UNGUARDED, and it has already fired
**CONFIRMED (top finding).** The go-forward §104 reconciler (`backfill_ann_dates_bse.py`
`reconcile_recent`, nightly `--recent 220 --limit 120`) resolves candidates via
`fetch_insurers.datebound` (strCat=-1) + `is_result_filing` (fetch_insurers.py L84-113) +
`resolve()` (backfill_ann_dates_bse.py L99-129). **None of the §119b/e poison vetoes exist in this
path**: no delay/extension-notice veto, no future-tense meeting-notice veto, no Reg 23(9) RPT veto,
no advance/provisional-note veto, no corroboration test ("outcome/submission wording, Result
category, or same-day board-meeting trail" — §119e's own crib). A Result-hit headline that states
the period parses as "exact" and wins.

Live poison, ground-truthed this session: **BANKINDIA|20260331 stored ann=20260402 (both bases,
np 3015.79/3087.76)** via ledger entry `bse:recon:exact, override:true, was:20260508`. The Apr-2
row is *"Q4 Quarter/Year Ended 31St March, 2026 Financial Result (Provisional)"* (category General,
17:52 — a provisional-figures note); the audited results were filed **2026-05-08 17:40** ("Financial
Result For The 4Th Quarter...", 2 rows). Unless the Apr-2 PDF prints the identical audited PAT
(unread — flagged), this is a **36-day look-ahead on a PSU large cap in the current results season,
written by the nightly**. Two earlier recon poisonings (ALMONDZ|20260331 = BM-outcome 13:42 non-results
date; ANANTRAJ|20260331 = subsidiary-incorporation outcome) were caught and replaced by the
seq-audit's exact entries — proving the class regrows. **Heal route:** exact entry for BANKINDIA
(gated date = 2026-05-11, Mon after the Fri 17:40 post-close filing) after reading the Apr-2 PDF;
port the §119 vetoes + corroboration gate into `resolve()` (or a recon-path wrapper) — the seq/solo
rules too, not just exact (SUZLON class).

**Residue screens (live store, measured):**
- **Physically-impossible lags:** 378 dated cells with ann ≤ qe+7d (**207 at ≤3d; 33 of those
  qe≥2023**). Ground-truthed: HIMATSEIDE|20240930 stored 20241001 vs real 2024-11-14 14:52
  (**44d look-ahead, 2024**); RTNPOWER|20140930 stored 20141007 vs real 2014-11-10 20:05
  (**34d look-ahead**). §119d's "corrupted qe+1..+6 writer" class is alive beyond the 163 healed.
- **Early-vs-own-habit:** 107 cells with lag<25d where the symbol's neighbour-median lag is ≥35d
  later (habit ≤120d). Disagreement names no side: spot checks show many are the cell being RIGHT
  and the neighbours late (TCS con med=62d is itself the defect — below).
- **Stale-by-one-quarter (ann == next quarter's ann, lag ≥75d):** 943 cells/371 syms — mixed class:
  genuine same-day double filings (BALLARPUR) vs comparative-column stamping. Timely-filer core
  (symbol median lag <35d): 130 cells/102 (sym,qe), most of which are §99 IPO-floor benign; the
  proven-wrong subset is the TCS type: **TCS Jun/Dec-2015 + Jun/Dec-2016 (std+con) carry the NEXT
  quarter's date** — BSE ground truth TCS Jun-2015 public 2015-07-09 16:39 vs stored 2015-10-13
  (95d stale); BAJAJFINSV Mar-2015 public 2015-05-20 vs stored 2015-07-22. Conservative-late (stale
  screens/wrong YoY timing), not look-ahead. These sit in §119c's left-alone classes.
- **Pre-first-bar ann dates (§99c re-census, TRUE live bin end=2026-08-31):** **7,305 cells /
  528 symbols** dated before the symbol's first traded bar (prior census 5,247; the number GREW —
  the frozen scratch copy undercounted by 2,283). Engine-inert at screen time (factorsAt requires a
  fresh price bar), but wrong provenance; split pre-listing vs tape-seam still unmeasured.
  MSUMI's 2020 quarters (stored ann 2020-11-10) vs first bar 2022-03-28 verified as the type case.

### 2c. Ground truth, 15 stratified cells (BSE AnnSubCategoryGetData, live fetches this session)
| sym\|qe | stored | BSE first-public | verdict |
|---|---|---|---|
| TCS\|20150630 | 20151013 | 2015-07-09 16:39 | WRONG-LATE 95d (next-Q date) |
| BAJAJFINSV\|20150331 | 20150722 | 2015-05-20 16:17 | WRONG-LATE 62d (next-Q date) |
| RTNPOWER\|20140930 | 20141007 | 2014-11-10 20:05 | WRONG-EARLY 34d (look-ahead) |
| HIMATSEIDE\|20240930 | 20241001 | 2024-11-14 14:52 | WRONG-EARLY 44d (look-ahead) |
| BANKINDIA\|20260331 | 20260402 | 2026-05-08 17:40 (audited); Apr-2 = provisional note | WRONG-EARLY 36d (recon-written; PDF unread) |
| ALMONDZ\|20260331 | 20260522 | 2026-05-22 14:56 | CORRECT (exact-entry heal verified) |
| GSKCONS\|20181231 | 20190214 (backlog override) | 2019-02-14 16:10 | CORRECT (raw-date convention; non-month-end) |
| INDIGO\|20190630 | 20190719 | 2019-07-19 16:07 (post-close) | date right, ungated — benign for monthly rebalances |
| SSWL\|20190630 | 20190708 | 2019-07-08 16:17 (post-close) | same |
| MSUMI\|20200930 | 20201110 | no BSE listing then; first bar 2022-03-28 | pre-listing class (§99c), engine-inert |
| M&MFIN\|20050630 | 20060317 | BSE archive EMPTY (all-cat, 2005) | NOT VERIFIABLE by this route |
| GOKEX\|20070331 | 20070405 (lag 5d) | window has rows, NO results row | NOT VERIFIABLE; 5d for Q4 audited stays SUSPECT |
| ONWARDTEC\|20070331 | 20070403 | same era wall | NOT VERIFIABLE |
| OFSS\|20010930 | 20020628 | archive empty 2001 | NOT VERIFIABLE |
| KARMAENG\|20181231 | 20190214 | no scrip in by_id (delisted map not loaded here) | not attempted |
Routes walked for the unverifiable era: BSE AnnSubCategoryGetData strCat=Result AND strCat=-1
(both 200-OK, 0 rows / 0 results rows). NSE archive per-company results index reaches ~2015+ only
(§63f) — 2001-2007 stored dates rest on convention/aggregator provenance and cannot be
exchange-verified from public archives.

---

## PART 3 — ENGINE TEMPORAL SEMANTICS (docs/backtest-engine.js + docs/stock-backtest.html twin, origin)

### 3a. Visibility gates per factor group (line cites = backtest-engine.js / twin)
- **Price/technical** (ret*/rsi/d52/dma/vol/beta/macd/stoch/bollB/turnover/volSurge): no ann gate
  needed; all windows end at the screen offset `off`. Entry-freshness gate: last bar within 14d
  (28d pre-2002) — factorsAt L802-812. Audited every window in computeTech (L514-545): none reads
  a bar > off. **CLEAN.**
- **Profit factors**: `q[np] != null && q[ann] > 0 && q[ann] <= dateInt` — profitAt L595,
  profitMetrics L639, lastResultDate L693-696 (postDrift); ann=0 sentinel excluded (§91c fix
  present, both twins). **CLEAN gates; base selection issues below.**
- **SHP**: shpAt L773 `sub <= dateInt`; QoQ delta only if the previous quarter's own sub ≤ date
  (L783); Sep-2022 format boundary excluded (L780); event rows delta vs latest visible row
  (L787-790). Un-dated pre-2014 rows stamped qe+28d at load (loadShp L228-230, `qePlus28` L230;
  twin L748-750) — §120 e13, measured-conservative. **CLEAN.**
- **Delivery** (`delivPct`, computeTech L532): 28d average of `dv[k]` for bars ≤ off, including day
  T itself, whose delivery % publishes ~post-close — a same-evening nuance at 1/20th window weight.
  **benign (noted).**
- **Membership**: membersAsOf L468-475 — PIT snapshots, no pre-first-snapshot floor (empty set,
  loud); simulate clamps start to membershipStart (L897-902). **CLEAN.**
- **No factor falls back to a non-PIT default on null/0 dates** (grepped `!= null` on date fields:
  none in a gate position).

### 3b. Lookback windows & execution timing
Convention established from code: rebalance day = last trading day ≤ calendar month-end
(snapTD, L918-922); every factor window ENDS at that day's close; entry/exit price = the SAME close
(factorsAt r.price L816; simulate trades at r.price / markPrice(off)). So the signal includes T's
close and the trade executes at T's close — the standard close-to-close convention; no factor's
window reaches past the execution bar, and no factor is computed at T while executing at T−k or
vice versa. `retPctAt(off,30)` endpoints = last close ≤ off / ≤ off−30 (L497-506); accel =
ret30(T) − ret30(T−30) (L515). **§117 F-03 end-date guard verified present in BOTH twins**
(engine L937, twin L1233): the end month marks equity and closes but never opens. ENGINE_VER = e13
(twin L673). **No off-by-one defect found.**

### 3c. YoY/TTM base selection — TTM fixed; accel/streak still array-offset (CONFIRMED, both twins)
- `yoy`/`profitAt` base: calendar (`npAt(q[0] − 10000)`, exact date match) — CLEAN (L642-643).
- `profitTTM`: e6/e7 fix verified on origin in BOTH twins — every PAIRWISE gap of the 4-row window
  must be exactly 3 months (monthIdx check, engine L661-662, twin L1082-1083), catching both gaps
  and duplicate-qe rows. **CLEAN.**
- **`profitAccel` = yoy − yoyOf(arr[ci−1]) (engine L646, twin L1074): ARRAY-adjacent, not
  calendar-adjacent.** When the calendar-previous quarter is missing, the "previous YoY" is 2+
  quarters old — a mislabeled acceleration. Blast radius measured on live sf_fundamentals:
  **1,516 (row,basis) cells across 810 symbols** where both YoYs resolve and the array-adjacent row
  is NOT the calendar-previous quarter (2019-2025 heavy: 154/117/196/315/180/153/149 by year).
- **`profitStreak` (engine L665, twin L1086): array-consecutive walk** — a positive streak silently
  continues across a missing quarter: **495 cells / 330 symbols** (both flanking YoYs >0, gap ≠3mo).
  Heal route: apply the same monthIdx===3 step test to the accel pair and the streak walk (gap →
  accel=null / streak breaks), both twins, bump ENGINE_VER.
- Duplicate-qe rows (which also corrupt array walks): **1 symbol remains (APOLLOTYRE)** — matches
  the known open item.

### 3d. Basis selection — PIT-clean; one data-side SUSPECT
`basis='con'` (default) tries con first only if `_conFreshEnough` (con's latest VISIBLE quarter
within 12 months of std's — L570-583, dead-con guard e9), each basis gated by its OWN ann column
(tries = [[3,4],[1,2]], L589/L636); per-field std fill-in (ttm/accel) documented and gated
`ni===3` (L667-672). lastResultDate takes the LATEST ann across bases (L693-700) — the earnings
event is basis-agnostic. **No engine-side cross-basis date borrowing = no look-ahead.**
**SUSPECT (data-side):** `update_fundamentals.py` L295-300 copies the sibling basis's ann into a
null ann when np exists ("same board meeting"). For pre-FY2020 annually-filed consolidated values
backfilled without a date, annCon inherits std's QUARTERLY date — a potential con look-ahead. Not
measurable post-hoc from the store (copies are indistinguishable from genuine same-day filings);
bounded by cells with annStd==annCon in the pre-2020 era. Flag for the con-provenance campaign.

### 3e. §12 15:30 gate locus
The engine compares DATE ints only — the gate is enforced entirely data-side (gated_ann at NSE
ingestion; nightly gate_1530 for month-end events; §104 override +4d buffer; §105 SHP shifts;
§120 qe+28 chosen to dodge every possible screen day). Residue: non-month-end post-close raw dates
(INDIGO/SSWL/GSKCONS above) — invisible to monthly rebalances; only ad-hoc `screenAsOf`/
`computeHold` on such a date could see a half-day look-ahead. **benign as shipped.**

---

## Price-source note (contamination check)
`$SP/live/sf_stock_data.bin` was found overwritten with the frozen repo copy (end=2026-06-13,
5,148 syms) — the only screens that read it were the first-bar measurements; both were re-run
against `$SP/live_true/sf_stock_data_LIVE.bin` (end=2026-08-31, 4,572 syms) and the numbers above
are from the re-run (pre-first-bar census 5,022→7,305; MSUMI first bar unchanged). Every other
Part 1/2/3 measurement read shp_history/shp_engine/shareholding/sf_fundamentals/ledgers (origin
exports), origin/main code, or live BSE — unaffected.
